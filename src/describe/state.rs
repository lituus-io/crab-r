// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! A describe run's state is a graph-r store: the incremental record a rerun
//! reuses from, and the metadata every lookup reads — one structure, no
//! report re-parsing, no second image format.
//!
//! - **Nodes**, keyed by canonical URI (`sql://bigquery/p/d/t#col`, the same
//!   [`crate::base::EntityKey`] the catalog uses): a root, datasets, tables and
//!   columns. A node's title is its name, its snippet the description, a
//!   table's content hash its incremental prekey.
//! - **Edges**: `Contains` (root → dataset → table → column), `Derives`
//!   (column → the columns it is computed from; `EXACT` for unchanged
//!   copies), `Conditions` (table → the columns deciding its rows).
//!   Consumers outside the described datasets derive from ours.
//! - **Attributes**: types, producing SQL, provenance, confidence, checks,
//!   and the static mark of a node a lineage export declared
//!   ([`crate::describe::ingest`]).
//!
//! Freshness rides on graph-r's TTL: a reused table is touched `Unchanged`
//! (its revalidation interval grows), a re-described one is upserted and
//! touched `Changed`, so [`graph_r::Snapshot::due`] orders what to look at
//! next. A reused table costs one small op.

use crate::base::key::UriBuf;
use crate::describe::describe::{Action, Report, TableReport};
use crate::describe::incremental::{Prior, PriorColumn, PriorTable};
use crate::lineage::Column;
use graph_r::format::base::eflags::EXACT;
use graph_r::{DocRecord, EdgeType, Outcome, Snapshot, Store, Touch, Txn, UrlKey};
use std::collections::{BTreeSet, HashSet};

/// Attribute keys of the describe state.
pub mod attr {
    /// Table: `TABLE`, `VIEW`, ... Column: its type.
    pub const TYPE: u8 = 1;
    /// Table: producing statement type.
    pub const STATEMENT: u8 = 2;
    /// Table: producing SQL.
    pub const SQL: u8 = 3;
    /// Table: rows.
    pub const ROWS: u8 = 4;
    /// Table: full incremental key (hex); absent while incomplete.
    pub const KEY: u8 = 5;
    /// Table: grain columns, one per line.
    pub const GRAIN: u8 = 6;
    /// Table: check evidence (JSON array).
    pub const CHECKS: u8 = 7;
    /// The description the warehouse had.
    pub const EXISTING: u8 = 8;
    /// Column: what happened (`generate`, `flag`, ...).
    pub const ACTION: u8 = 9;
    /// Column: confidence.
    pub const CONFIDENCE: u8 = 10;
    /// Column: transform (`passthrough`, `aggregate`, ...).
    pub const TRANSFORM: u8 = 11;
    /// Column: producing expression.
    pub const EXPRESSION: u8 = 12;
    /// Column: semantic type.
    pub const SEMANTIC: u8 = 13;
    /// Column: unit.
    pub const UNIT: u8 = 14;
    /// Column: ids of its checks, comma separated.
    pub const CHECK_IDS: u8 = 15;
    /// Any node: declared by a static lineage export (see
    /// [`super::super::ingest`]); the declaring resource, or `declared`.
    /// A describe keeps it, and keeps the `Derives` edges such a node
    /// has, beside what it traced itself.
    pub const STATIC: u8 = 16;
}

/// Key of the root node.
pub const ROOT: &str = "sql://bigquery/";

/// Graph keys of a table and its URI.
fn table_uri(uri: &mut UriBuf, fqn: &str) -> Option<String> {
    let (p, d, t) = crate::base::key::split_fqn(fqn)?;
    Some(uri.table(p, d, t).to_owned())
}

fn column_uri(uri: &mut UriBuf, c: &Column) -> Option<String> {
    let (p, d, t) = crate::base::key::split_fqn(&c.table)?;
    Some(uri.column(p, d, t, &c.column).to_owned())
}

fn upsert(
    w: &mut Txn<'_>,
    url: &str,
    title: &str,
    snippet: Option<&str>,
    hash: u64,
    now: u64,
) -> graph_r::Result<UrlKey> {
    let key = UrlKey::of(url);
    w.upsert_node(&DocRecord {
        url,
        url_key: key,
        content_hash: hash,
        fetched_at_ms: now,
        title: Some(title),
        snippet,
        etag: None,
        pinned: false,
    })?;
    Ok(key)
}

/// Outbound edges of `key` of the given type, as the snapshot has them.
fn edges_of(snap: &Snapshot<'_>, key: UrlKey, etype: EdgeType) -> Vec<UrlKey> {
    let mut out = Vec::new();
    let mut it = snap.neighbors(key);
    while let Some(e) = graph_r::LendingIterator::next(&mut it) {
        if e.etype == etype {
            out.push(e.dst_key);
        }
    }
    out
}

/// Record `report` in `store`: re-described tables in full, reused ones as a
/// freshness touch. Tables a run did not cover (an unreadable dataset) keep
/// what an earlier run recorded.
///
/// One transaction: the recorded hierarchy this unions with is read under the
/// store's commit lock, so a concurrent save into the same store (another
/// process, another handle) is built on, never overwritten.
pub fn save(store: &Store, report: &Report, now_ms: u64) -> graph_r::Result<()> {
    store.transact(|w| {
        let mut uri = UriBuf::new();
        // The transaction's caught-up view (what `w.snapshot()` returns), taken
        // from the store so staging through `w` can borrow it mutably.
        let snap = store.snapshot();
        // Hierarchy: root → datasets → tables, unioned with what is recorded.
        // Nodes written in this batch: lineage endpoints never re-upsert them.
        let mut written: HashSet<UrlKey> = HashSet::new();
        let root = UrlKey::of(ROOT);
        if snap.node(root).is_none() {
            upsert(w, ROOT, "bigquery", None, 0, now_ms)?;
        }
        let mut datasets: BTreeSet<(String, String)> = BTreeSet::new();
        for t in &report.tables {
            if let Some((p, d, _)) = crate::base::key::split_fqn(&t.table) {
                datasets.insert((p.to_owned(), d.to_owned()));
            }
        }
        let mut root_edges: BTreeSet<UrlKey> =
            edges_of(&snap, root, EdgeType::Contains).into_iter().collect();
        for (p, d) in &datasets {
            let url = uri.dataset(p, d).to_owned();
            let key = UrlKey::of(&url);
            if snap.node(key).is_none() {
                upsert(w, &url, &format!("{p}.{d}"), None, 0, now_ms)?;
            }
            root_edges.insert(key);
            let mut tables: BTreeSet<UrlKey> =
                edges_of(&snap, key, EdgeType::Contains).into_iter().collect();
            for t in report.tables.iter().filter(|t| {
                crate::base::key::split_fqn(&t.table).is_some_and(|(tp, td, _)| tp == p && td == d)
            }) {
                if let Some(u) = table_uri(&mut uri, &t.table) {
                    tables.insert(UrlKey::of(&u));
                }
            }
            let tables: Vec<_> = tables.into_iter().map(|k| (k, EdgeType::Contains, 1)).collect();
            w.set_edges(key, &tables);
        }
        let root_edges: Vec<_> =
            root_edges.into_iter().map(|k| (k, EdgeType::Contains, 1)).collect();
        w.set_edges(root, &root_edges);

        for t in &report.tables {
            let Some(turl) = table_uri(&mut uri, &t.table) else { continue };
            let tkey = UrlKey::of(&turl);
            if t.reused && snap.node(tkey).is_some() {
                touch(w, tkey, Outcome::Unchanged, now_ms)?;
                continue;
            }
            save_table(w, &snap, &mut uri, &mut written, t, &turl, now_ms)?;
            touch(w, tkey, Outcome::Changed, now_ms)?;
        }
        Ok(())
    })
}

fn touch(w: &mut Txn<'_>, key: UrlKey, outcome: Outcome, now: u64) -> graph_r::Result<()> {
    w.touch(key, Touch { checked_at_ms: now, outcome, content_hash: None, etag: None }).map(|_| ())
}

fn save_table(
    w: &mut Txn<'_>,
    snap: &Snapshot<'_>,
    uri: &mut UriBuf,
    written: &mut HashSet<UrlKey>,
    t: &TableReport,
    turl: &str,
    now: u64,
) -> graph_r::Result<()> {
    let tkey = upsert(w, turl, &t.table, t.description.as_deref(), t.prekey, now)?;
    written.insert(tkey);
    let key_hex = t.key.map(|k| format!("{k:016x}"));
    let grain = t.grain.join("\n");
    let checks = crate::describe::report::checks_json(t);
    let rows = t.rows.to_string();
    let mut attrs: Vec<(u8, &str)> = vec![
        (attr::TYPE, &t.kind),
        (attr::ROWS, &rows),
        (attr::GRAIN, &grain),
        (attr::CHECKS, &checks),
    ];
    if let Some(p) = &t.producer {
        attrs.push((attr::STATEMENT, &p.statement_type));
        attrs.push((attr::SQL, &p.query));
    }
    if let Some(k) = &key_hex {
        attrs.push((attr::KEY, k));
    }
    if let Some(e) = &t.existing {
        attrs.push((attr::EXISTING, e));
    }
    let table_mark = snap.attr(tkey, attr::STATIC);
    if let Some(m) = table_mark {
        attrs.push((attr::STATIC, m));
    }
    w.set_attrs(tkey, &attrs)?;

    let mut table_edges = Vec::new();
    for c in &t.columns {
        let col = Column::new(t.table.clone(), &c.column);
        let Some(curl) = column_uri(uri, &col) else { continue };
        let ckey = upsert(w, &curl, &c.column, c.description.as_deref(), 0, now)?;
        written.insert(ckey);
        table_edges.push((ckey, EdgeType::Contains, 1));
        let transform =
            c.transform.map(|x| format!("{x:?}").to_ascii_lowercase()).unwrap_or_default();
        let ids = c.checks.iter().map(ToString::to_string).collect::<Vec<_>>().join(",");
        let mut attrs: Vec<(u8, &str)> = vec![
            (attr::TYPE, &c.data_type),
            (attr::ACTION, c.action.as_str()),
            (attr::TRANSFORM, &transform),
            (attr::CHECK_IDS, &ids),
        ];
        if let Some(m) = snap.attr(ckey, attr::STATIC) {
            attrs.push((attr::STATIC, m));
        }
        for (k, v) in [
            (attr::EXISTING, &c.existing),
            (attr::CONFIDENCE, &c.confidence),
            (attr::EXPRESSION, &c.expression),
            (attr::SEMANTIC, &c.semantic_type),
            (attr::UNIT, &c.unit),
        ] {
            if let Some(v) = v {
                attrs.push((k, v));
            }
        }
        w.set_attrs(ckey, &attrs)?;
        // A single source copied unchanged (by the SQL, or as the lineage
        // service recorded it) is an exact copy.
        let exact = c.sources.len() == 1
            && matches!(
                c.transform,
                Some(crate::ir::Transform::Passthrough | crate::ir::Transform::Rename) | None
            );
        let mut derives = Vec::new();
        for s in &c.sources {
            if let Some(key) = external(w, snap, uri, written, s, now)? {
                derives.push((key, EdgeType::Derives, 1, if exact { EXACT } else { 0 }));
            }
        }
        if snap.attr(ckey, attr::STATIC).is_some() {
            declared(snap, ckey, &mut derives);
        }
        w.set_edges_with_flags(ckey, &derives);
        for k in &c.consumers {
            let Some(key) = external(w, snap, uri, written, k, now)? else { continue };
            let mut theirs: Vec<(UrlKey, EdgeType, u16)> = edges_of(snap, key, EdgeType::Derives)
                .into_iter()
                .map(|d| (d, EdgeType::Derives, 1))
                .collect();
            if !theirs.iter().any(|e| e.0 == ckey) {
                theirs.push((ckey, EdgeType::Derives, 1));
            }
            w.set_edges(key, &theirs);
        }
    }
    for k in &t.conditions {
        if let Some(key) = external(w, snap, uri, written, k, now)? {
            table_edges.push((key, EdgeType::Conditions, 1));
        }
    }
    if table_mark.is_some() {
        let mut d = Vec::new();
        declared(snap, tkey, &mut d);
        table_edges.extend(d.into_iter().map(|(k, t, w, _)| (k, t, w)));
    }
    w.set_edges(tkey, &table_edges);
    Ok(())
}

/// A static node's recorded `Derives` edges the traced set lacks: a
/// declaration the trace did not find (the program says it, the job history
/// may not yet) is kept, not dropped. Traced edges come first and win on
/// the same source, so their flags stand.
fn declared(snap: &Snapshot<'_>, key: UrlKey, edges: &mut Vec<(UrlKey, EdgeType, u16, u8)>) {
    let mut it = snap.neighbors(key);
    while let Some(e) = graph_r::LendingIterator::next(&mut it) {
        if e.etype == EdgeType::Derives && !edges.iter().any(|x| x.0 == e.dst_key) {
            edges.push((e.dst_key, EdgeType::Derives, e.weight, e.flags));
        }
    }
}

/// The node of a column known only through lineage (outside the described
/// tables): created with its name so lookups can show it.
fn external(
    w: &mut Txn<'_>,
    snap: &Snapshot<'_>,
    uri: &mut UriBuf,
    written: &mut HashSet<UrlKey>,
    c: &Column,
    now: u64,
) -> graph_r::Result<Option<UrlKey>> {
    let Some(url) = column_uri(uri, c) else { return Ok(None) };
    let key = UrlKey::of(&url);
    if !written.contains(&key) && snap.node(key).is_none_or(|n| n.url.is_empty()) {
        upsert(w, &url, &c.column, None, 0, now)?;
        written.insert(key);
    }
    Ok(Some(key))
}

fn action_of(s: &str) -> Option<Action> {
    Some(match s {
        "keep" => Action::Keep,
        "confirm" => Action::Confirm,
        "flag" => Action::Flag,
        "propagate" => Action::Propagate,
        "generate" => Action::Generate,
        "skip" => Action::Skip,
        _ => return None,
    })
}

/// What earlier runs recorded: every completed table, ready for reuse.
#[must_use]
pub fn load(store: &Store) -> Prior {
    let snap = store.snapshot();
    let mut prior = Prior::default();
    let root = UrlKey::of(ROOT);
    for ds in edges_of(&snap, root, EdgeType::Contains) {
        for tkey in edges_of(&snap, ds, EdgeType::Contains) {
            let Some(node) = snap.node(tkey) else { continue };
            let Some(key) =
                snap.attr(tkey, attr::KEY).and_then(|k| u64::from_str_radix(k, 16).ok())
            else {
                continue;
            };
            let mut columns = Vec::new();
            for ckey in edges_of(&snap, tkey, EdgeType::Contains) {
                let Some(c) = snap.node(ckey) else { continue };
                let Some(action) = snap.attr(ckey, attr::ACTION).and_then(action_of) else {
                    continue;
                };
                let s = |k| snap.attr(ckey, k).map(str::to_owned);
                columns.push(PriorColumn {
                    column: c.title.unwrap_or("").to_owned(),
                    action,
                    existing: s(attr::EXISTING),
                    description: c.snippet.map(str::to_owned),
                    semantic_type: s(attr::SEMANTIC),
                    unit: s(attr::UNIT),
                    confidence: s(attr::CONFIDENCE),
                    checks: snap
                        .attr(ckey, attr::CHECK_IDS)
                        .unwrap_or("")
                        .split(',')
                        .filter_map(|x| x.parse().ok())
                        .collect(),
                });
            }
            prior.insert(
                node.title.unwrap_or("").to_owned(),
                PriorTable {
                    prekey: node.content_hash,
                    key,
                    existing: snap.attr(tkey, attr::EXISTING).map(str::to_owned),
                    description: node.snippet.map(str::to_owned),
                    grain: snap
                        .attr(tkey, attr::GRAIN)
                        .unwrap_or("")
                        .lines()
                        .map(str::to_owned)
                        .collect(),
                    columns,
                    checks_json: snap.attr(tkey, attr::CHECKS).unwrap_or("[]").to_owned(),
                },
            );
        }
    }
    prior
}
