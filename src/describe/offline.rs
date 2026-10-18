// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Drafting from metadata the caller already read, with no Google client.
//!
//! [`describe_offline`] runs the describe pipeline ([`describe_discovered`])
//! over a catalog built from supplied table metadata (the answers of
//! [`crate::exec::table_meta`], or raw `tables.get` resources) instead of a
//! warehouse:
//!
//! - nothing reaches BigQuery: the [`Offline`] warehouse refuses every
//!   query (no profile, no check runs; checks reach the judge as failed),
//!   and answers table reads only from the supplied metadata and the store;
//! - the store is reused: tables unchanged since an earlier run are not
//!   drafted again (the caller loads its prior into the config), upstream
//!   tables the store knows supply their descriptions, and the store's
//!   column lineage (a static export's, or an earlier run's) is the lineage
//!   source, so pass-through and renamed columns inherit rather than being
//!   drafted;
//! - a view's defining SQL is its producer.
//!
//! The only network a run makes is its model's. [`ModelOnly`] is the
//! request policy that keeps it so: one host, never a Google one.

use crate::base::key::{split_fqn, UriBuf};
use crate::catalog::CatalogBuilder;
use crate::describe::describe::{
    describe_discovered, spec_from_meta, DescribeConfig, Discovery, Report,
};
use crate::describe::producers::Producer;
use crate::describe::state;
use crate::exec::metadata::{parse_table, TableMeta, TableMetadata};
use crate::exec::{DryRun, Error, QueryResult, RunOpts, Warehouse};
use crate::lineage::{Column, Direction, Graph, LineageSource};
use crate::llm::Model;
use graph_r::{EdgeType, Store, UrlKey};
use objstore_r::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::future::{ready, Ready};

/// Table metadata as supplied: each `(project.dataset.table, resource)`.
pub type Supplied = Vec<(String, TableMeta)>;

/// Read supplied metadata: a JSON array whose elements are
/// [`crate::exec::table_meta`] answers (`{"table", "available", "type",
/// "description", "schema", "view_sql", ...}`; unavailable ones are
/// skipped) or raw table resources carrying a `"table"` name. At most
/// `max_tables`.
pub fn parse_catalog(json: &str, max_tables: usize) -> Result<Supplied, String> {
    use crate::base::json::{Event, Scanner};
    let mut s = Scanner::new(json);
    if s.expect().map_err(|e| e.to_string())? != Event::ArrayStart {
        return Err("catalog metadata: not a JSON array".into());
    }
    let mut out = Vec::new();
    let mut buf = String::new();
    loop {
        let before = s.offset();
        match s.expect().map_err(|e| e.to_string())? {
            Event::ArrayEnd => break,
            Event::ObjectStart => {
                let start = json[before..].find('{').map_or(before, |i| before + i);
                let mut table: Option<String> = None;
                let mut available = true;
                let mut view_sql: Option<String> = None;
                loop {
                    match s.expect().map_err(|e| e.to_string())? {
                        Event::ObjectEnd => break,
                        Event::Key(k) => {
                            let v = s.expect().map_err(|e| e.to_string())?;
                            match (k.as_plain(), v) {
                                (Some("table"), Event::Str(t)) => {
                                    table = Some(
                                        t.decode(&mut buf).map_err(|e| e.to_string())?.to_owned(),
                                    );
                                }
                                (Some("available"), Event::Bool(b)) => available = b,
                                (Some("view_sql"), Event::Str(q)) => {
                                    view_sql = Some(
                                        q.decode(&mut buf).map_err(|e| e.to_string())?.to_owned(),
                                    );
                                }
                                (_, v) => s.skip(v).map_err(|e| e.to_string())?,
                            }
                        }
                        _ => return Err("catalog metadata: expected a key".into()),
                    }
                }
                let raw = &json[start..s.offset()];
                let Some(table) = table.filter(|_| available) else { continue };
                let name = crate::base::key::BqName::parse(&table)
                    .ok()
                    .filter(|n| n.level() == crate::base::key::Level::Table)
                    .ok_or_else(|| {
                        format!("catalog metadata: {table:?} is not project.dataset.table")
                    })?
                    .dotted();
                let mut meta = parse_table(Bytes::copy_from_slice(raw.as_bytes()))
                    .map_err(|e| format!("catalog metadata for {name}: {e}"))?;
                if meta.view_query.is_none() {
                    meta.view_query = view_sql;
                }
                if out.len() == max_tables {
                    return Err(format!("catalog metadata: more than {max_tables} tables"));
                }
                out.push((name, meta));
            }
            other => s.skip(other).map_err(|e| e.to_string())?,
        }
    }
    Ok(out)
}

/// A warehouse with no warehouse behind it: queries are refused, table
/// reads answer from the supplied metadata, then from the store.
#[derive(Debug)]
pub struct Offline<'a> {
    metas: HashMap<String, TableMeta>,
    store: Option<&'a Store>,
}

impl<'a> Offline<'a> {
    /// Over `supplied`, falling back to `store`.
    #[must_use]
    pub fn new(supplied: &Supplied, store: Option<&'a Store>) -> Self {
        Self { metas: supplied.iter().cloned().collect(), store }
    }

    /// A table the store holds, as a table resource: its description and
    /// its columns' types and descriptions.
    fn store_table(&self, p: &str, d: &str, t: &str) -> Option<TableMeta> {
        let snap = self.store?.snapshot();
        let tkey = UrlKey::of(UriBuf::new().table(p, d, t));
        let node = snap.node(tkey)?;
        let mut json = String::from("{\"type\":");
        crate::base::json::escape_into(
            snap.attr(tkey, state::attr::TYPE).unwrap_or("TABLE"),
            &mut json,
        );
        if let Some(desc) = node.snippet.filter(|s| !s.is_empty()) {
            json.push_str(",\"description\":");
            crate::base::json::escape_into(desc, &mut json);
        }
        json.push_str(",\"schema\":{\"fields\":[");
        let mut first = true;
        let mut it = snap.neighbors(tkey);
        while let Some(e) = graph_r::LendingIterator::next(&mut it) {
            if e.etype != EdgeType::Contains {
                continue;
            }
            let Some(c) = snap.node(e.dst_key) else { continue };
            let Some(name) = c.url.split_once('#').map(|(_, n)| n) else { continue };
            if !first {
                json.push(',');
            }
            first = false;
            json.push_str("{\"name\":");
            crate::base::json::escape_into(name, &mut json);
            json.push_str(",\"type\":");
            crate::base::json::escape_into(
                snap.attr(e.dst_key, state::attr::TYPE).unwrap_or("STRING"),
                &mut json,
            );
            if let Some(desc) = c.snippet.filter(|s| !s.is_empty()) {
                json.push_str(",\"description\":");
                crate::base::json::escape_into(desc, &mut json);
            }
            json.push('}');
        }
        json.push_str("]}}");
        parse_table(Bytes::from(json)).ok()
    }
}

fn offline<T>() -> Ready<crate::exec::Result<T>> {
    ready(Err(Error::Denied { message: "offline: no warehouse is reached".into() }))
}

impl Warehouse for Offline<'_> {
    type Dry<'b>
        = Ready<crate::exec::Result<DryRun>>
    where
        Self: 'b;
    type Run<'b>
        = Ready<crate::exec::Result<QueryResult>>
    where
        Self: 'b;
    fn dry_run<'b>(&'b self, _: &'b str) -> Self::Dry<'b> {
        offline()
    }
    fn run<'b>(&'b self, _: &'b str, _: RunOpts) -> Self::Run<'b> {
        offline()
    }
}

impl TableMetadata for Offline<'_> {
    type Get<'b>
        = Ready<crate::exec::Result<Option<TableMeta>>>
    where
        Self: 'b;
    type Patch<'b>
        = Ready<crate::exec::Result<TableMeta>>
    where
        Self: 'b;
    fn table_get<'b>(
        &'b self,
        project: &'b str,
        dataset: &'b str,
        table: &'b str,
    ) -> Self::Get<'b> {
        let fqn = format!("{}.{dataset}.{table}", crate::base::key::canonical_project(project));
        let hit =
            self.metas.get(&fqn).cloned().or_else(|| self.store_table(project, dataset, table));
        ready(Ok(hit))
    }
    fn table_patch<'b>(
        &'b self,
        _: &'b str,
        _: &'b str,
        _: &'b str,
        _: Bytes,
        _: &'b str,
    ) -> Self::Patch<'b> {
        offline()
    }
}

/// The column lineage a store holds (`Derives` edges: a static export's,
/// or an earlier describe's), as a lineage source.
#[derive(Debug)]
pub struct StoreLineage<'a>(pub Option<&'a Store>);

/// A store node's URL as a column.
fn column_of(url: &str) -> Option<Column> {
    let rest = url.strip_prefix(state::ROOT)?;
    let (table, column) = rest.split_once('#')?;
    let mut parts = table.splitn(3, '/');
    let (p, d, t) = (parts.next()?, parts.next()?, parts.next()?);
    Some(Column::new(format!("{p}.{d}.{t}"), column))
}

impl LineageSource for StoreLineage<'_> {
    type Search<'b>
        = Ready<crate::lineage::Result<Graph>>
    where
        Self: 'b;

    fn search<'b>(
        &'b self,
        roots: &'b [Column],
        max_depth: u32,
        dir: Direction,
    ) -> Self::Search<'b> {
        let mut g = Graph::new();
        let Some(store) = self.0 else { return ready(Ok(g)) };
        let snap = store.snapshot();
        let mut uri = UriBuf::new();
        let mut walk = graph_r::Walk::new();
        for r in roots {
            let Some((p, d, t)) = split_fqn(&r.table) else { continue };
            let key = UrlKey::of(uri.column(p, d, t, &r.column));
            snap.walk(
                key,
                dir,
                max_depth,
                |e| e == EdgeType::Derives,
                &mut walk,
                |h| {
                    let (child, parent) =
                        if dir == Direction::Out { (h.from, h.key) } else { (h.key, h.from) };
                    let url = |k: UrlKey| snap.node(k).and_then(|n| column_of(n.url));
                    if let (Some(c), Some(p)) = (url(child), url(parent)) {
                        g.add(c, p, h.exact_chain);
                    }
                    true
                },
            );
        }
        ready(Ok(g))
    }
}

/// Draft descriptions for the `supplied` tables with `model`, reaching no
/// warehouse (see the module docs). `cfg.targets` is ignored: the supplied
/// tables are the scope; `cfg.profile` is forced off. Unchanged tables in
/// `cfg.prior` are reused. Writes nothing; the caller saves the report.
pub async fn describe_offline<M: Model>(
    model: &M,
    supplied: &Supplied,
    store: Option<&Store>,
    cfg: &DescribeConfig,
) -> Report {
    let mut cfg = cfg.clone();
    cfg.profile = false;
    let home = supplied
        .first()
        .and_then(|(t, _)| split_fqn(t).map(|(p, _, _)| p.to_owned()))
        .unwrap_or_default();
    let mut builder = CatalogBuilder::new(home.clone());
    let mut producers: BTreeMap<String, Producer> = BTreeMap::new();
    let mut in_scope = Vec::new();
    let mut warnings = Vec::new();
    for (t, meta) in supplied {
        let Some((p, d, n)) = split_fqn(t) else {
            warnings.push(format!("{t}: not project.dataset.table"));
            continue;
        };
        let qualified = if p == home { d.to_owned() } else { format!("{p}.{d}") };
        builder.add_table(spec_from_meta(&qualified, n, meta));
        if let Some(q) = &meta.view_query {
            producers.insert(
                t.clone(),
                Producer {
                    table: t.clone(),
                    statement_type: "VIEW".into(),
                    query: q.clone(),
                    writes: 0,
                    writers: 0,
                    first_ms: 0,
                    last_ms: meta.last_modified_ms,
                    others: Vec::new(),
                },
            );
        } else if let Some(sql) = store.and_then(|s| {
            let k = UrlKey::of(UriBuf::new().table(p, d, n));
            s.snapshot().attr(k, state::attr::SQL).map(str::to_owned)
        }) {
            // The producing SQL an earlier run found.
            let mut prod = Producer {
                table: t.clone(),
                statement_type: "RECORDED".into(),
                query: sql,
                writes: 0,
                writers: 0,
                first_ms: 0,
                last_ms: 0,
                others: Vec::new(),
            };
            if let Some(st) = store.and_then(|s| {
                let k = UrlKey::of(UriBuf::new().table(p, d, n));
                s.snapshot().attr(k, state::attr::STATEMENT).map(str::to_owned)
            }) {
                prod.statement_type = st;
            }
            producers.insert(t.clone(), prod);
        }
        in_scope.push(t.clone());
    }
    let mut note = String::new();
    let _ = write!(note, "offline: {} tables from supplied metadata; no warehouse", in_scope.len());
    warnings.insert(0, note);
    let w = Offline::new(supplied, store);
    let d = Discovery {
        builder,
        home,
        in_scope,
        metas: supplied.iter().cloned().collect(),
        producers,
        warnings,
        unreadable: Vec::new(),
        spend: crate::base::Spend::default(),
    };
    describe_discovered(model, &w, &StoreLineage(store), &cfg, d).await
}

/// A request policy for a drafting run: only `host` (the model's), over
/// https (or plain http to loopback, the transport's own rule), never a
/// Google host, and no query.
#[cfg(feature = "http")]
#[derive(Debug, Clone, Copy)]
pub struct ModelOnly<'a> {
    host: &'a str,
}

/// Hosts a drafting run never reaches, whatever it is configured with.
pub const GOOGLE_SUFFIXES: [&str; 5] =
    ["googleapis.com", "google.internal", "google.com", "googleusercontent.com", "gcr.io"];

/// `host` is a Google host (or the instance metadata server's address).
#[must_use]
pub fn is_google_host(host: &str) -> bool {
    let h = host.trim_end_matches('.').to_ascii_lowercase();
    h == "169.254.169.254"
        || h == "metadata"
        || GOOGLE_SUFFIXES.iter().any(|s| h == *s || h.ends_with(&format!(".{s}")))
}

#[cfg(feature = "http")]
impl<'a> ModelOnly<'a> {
    /// Only `host`; `None` when it is a Google host.
    #[must_use]
    pub fn new(host: &'a str) -> Option<Self> {
        (!is_google_host(host) && !host.is_empty()).then_some(Self { host })
    }
}

#[cfg(feature = "http")]
impl objstore_r::policy::RequestPolicy for ModelOnly<'_> {
    fn check(
        &self,
        _: objstore_r::transport::Method,
        url: &objstore_r::policy::Url,
    ) -> Result<(), objstore_r::PolicyError> {
        let host = url.host_str().unwrap_or("");
        if is_google_host(host) || !host.eq_ignore_ascii_case(self.host) {
            return Err(objstore_r::PolicyError::Endpoint {
                reason: "a drafting run reaches only its model",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn google_hosts_are_known() {
        for h in [
            "bigquery.googleapis.com",
            "metadata.google.internal",
            "169.254.169.254",
            "METADATA.GOOGLE.INTERNAL.",
            "accounts.google.com",
        ] {
            assert!(is_google_host(h), "{h}");
        }
        for h in ["gateway.example.com", "notgoogleapis.com.example", "localhost"] {
            assert!(!is_google_host(h), "{h}");
        }
    }

    #[test]
    fn catalog_takes_answers_and_resources() {
        let json = r#"[
            {"table":"P.d.t","available":true,"reason":"ok","type":"VIEW","schema":{"fields":[{"name":"a","type":"STRING"}]},"view_sql":"SELECT 1 AS a"},
            {"table":"p.d.gone","available":false,"reason":"no_access","message":"x"},
            {"table":"p.d.raw","type":"TABLE","description":"Raw.","schema":{"fields":[]}}
        ]"#;
        let c = parse_catalog(json, 10).unwrap();
        let names: Vec<&str> = c.iter().map(|(t, _)| t.as_str()).collect();
        assert_eq!(names, ["p.d.t", "p.d.raw"]);
        assert_eq!(c[0].1.view_query.as_deref(), Some("SELECT 1 AS a"));
        assert_eq!(c[1].1.description.as_deref(), Some("Raw."));
        assert!(parse_catalog(json, 1).is_err());
        assert!(parse_catalog("{}", 10).is_err());
        assert!(parse_catalog(r#"[{"table":"nodots"}]"#, 10).is_err());
    }

    #[test]
    fn store_columns_parse_back() {
        assert_eq!(column_of("sql://bigquery/p/d/t#a.b"), Some(Column::new("p.d.t", "a.b")));
        assert_eq!(column_of("sql://bigquery/p/d/t"), None);
    }
}
