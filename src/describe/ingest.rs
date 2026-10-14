// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Static lineage into a describe store: the nodes and edges a deployment
//! program's SQL lineage export declares (`bq://project/dataset/table#column`
//! ids, `contains` / `derives_from` / `column_derives_from` edges), written
//! as the same nodes a describe run keys, so lookups answer for declared
//! tables offline and a later describe enriches those nodes in place.
//!
//! Ingest only adds. A node that exists keeps its title, description and
//! content hash (a describe wrote them, and they are worth more than a
//! name); an attribute it already has keeps its value except the static
//! mark; an edge set gains the declared edges and loses none. So the
//! store's own judgements are never overwritten by a declaration, and a
//! declaration dropped from the export stays until the store is rebuilt.
//!
//! One commit or none: the whole input is parsed and validated, then diffed
//! against a snapshot, before a writer is opened. Ingesting the same export
//! twice commits once. Validation comes first because a graph-r writer keeps
//! staged ops across a failed batch, so a refusal halfway through would leak
//! half an ingest into the next commit.
//!
//! A static node never carries [`attr::KEY`], so [`super::state::load`]
//! offers no reuse for it and the first describe that covers it does the
//! work in full.

use crate::base::json::{Event, Scanner};
use crate::base::key::{BqName, Level, NameError, UriBuf, BQ_SCHEME};
use crate::describe::state::{attr, ROOT};
use graph_r::writer::{MAX_ATTRS, MAX_LABEL};
use graph_r::{DocRecord, EdgeType, Snapshot, Store, UrlKey};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

/// The static mark's value when the export names no declaring resource.
pub const DECLARED: &str = "declared";

/// Which input list an entry came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// `nodes`.
    Node,
    /// `edges`.
    Edge,
}

impl Side {
    /// `node` or `edge`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Edge => "edge",
        }
    }
}

/// An entry refused, by its position: the reason never repeats the entry,
/// which may be anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rejected {
    /// The list it came from.
    pub side: Side,
    /// Its index there.
    pub index: usize,
    /// Why.
    pub reason: &'static str,
}

/// What an ingest did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ingested {
    /// Whether anything was written (false: the store already held it all).
    pub committed: bool,
    /// Nodes created.
    pub created: usize,
    /// Existing nodes whose attributes changed.
    pub updated: usize,
    /// Edges added.
    pub edges: usize,
    /// Entries understood but out of scope, counted by name: a node kind or
    /// edge relationship crab-r does not model, or `relationship:scheme`
    /// for a modelled edge whose endpoint is not a BigQuery entity.
    pub skipped: BTreeMap<String, usize>,
    /// Entries refused.
    pub rejected: Vec<Rejected>,
}

impl Ingested {
    /// As JSON: `{"committed", "created", "updated", "edges", "skipped":
    /// {name: count}, "rejected": [{"side", "index", "reason"}]}`.
    #[must_use]
    pub fn to_json(&self) -> String {
        let mut out = String::with_capacity(128 + 64 * self.rejected.len());
        let mut w = crate::base::json::Writer::new(&mut out);
        w.begin_object();
        w.key("committed").bool(self.committed);
        w.key("created").uint(self.created as u64);
        w.key("updated").uint(self.updated as u64);
        w.key("edges").uint(self.edges as u64);
        w.key("skipped").begin_object();
        for (k, v) in &self.skipped {
            w.key(k).uint(*v as u64);
        }
        w.end_object();
        w.key("rejected").begin_array();
        for r in &self.rejected {
            w.begin_object();
            w.key("side").string(r.side.as_str());
            w.key("index").uint(r.index as u64);
            w.key("reason").string(r.reason);
            w.end_object();
        }
        w.end_array();
        w.end_object();
        out
    }
}

/// The fields of one export entry ingest reads; the rest are skipped.
#[derive(Debug, Default)]
struct Entry<'a> {
    id: Option<Cow<'a, str>>,
    kind: Option<Cow<'a, str>>,
    data_type: Option<Cow<'a, str>>,
    urn: Option<Cow<'a, str>>,
    relationship: Option<Cow<'a, str>>,
    source: Option<Cow<'a, str>>,
    target: Option<Cow<'a, str>>,
}

/// Each entry of a JSON array of objects, or `None` for an element that is
/// not an object. Strings stay borrowed unless they carry an escape.
fn entries(json: &str) -> crate::base::Result<Vec<Option<Entry<'_>>>> {
    let mut sc = Scanner::new(json);
    if sc.expect()? != Event::ArrayStart {
        return Err(crate::base::Error::json("expected an array"));
    }
    let mut out = Vec::new();
    loop {
        match sc.expect()? {
            Event::ArrayEnd => break,
            Event::ObjectStart => {}
            other => {
                sc.skip(other)?;
                out.push(None);
                continue;
            }
        }
        let mut e = Entry::default();
        while let Event::Key(k) = sc.expect()? {
            let slot = match k.as_plain() {
                Some("id") => &mut e.id,
                Some("kind") => &mut e.kind,
                Some("data_type") => &mut e.data_type,
                Some("defined_by_urn") => &mut e.urn,
                Some("relationship") => &mut e.relationship,
                Some("source_id") => &mut e.source,
                Some("target_id") => &mut e.target,
                _ => {
                    sc.skip_value()?;
                    continue;
                }
            };
            match sc.expect()? {
                Event::Str(s) => {
                    *slot = Some(if let Some(p) = s.as_plain() {
                        Cow::Borrowed(p)
                    } else {
                        let mut buf = String::new();
                        crate::base::json::unescape(s.raw(), &mut buf)?;
                        Cow::Owned(buf)
                    });
                }
                // A non-string field reads as absent: the entry is then
                // refused for what it lacks, not guessed at.
                other => sc.skip(other)?,
            }
        }
        out.push(Some(e));
    }
    if sc.next_event()?.is_some() {
        return Err(crate::base::Error::json("trailing content after the array"));
    }
    Ok(out)
}

/// What the input declares, keyed by node: the store's target state.
#[derive(Debug, Default)]
struct Plan {
    /// Every node, with its title and the attributes to make sure of.
    nodes: HashMap<UrlKey, PlanNode>,
    /// Edges to make sure of, by source.
    edges: HashMap<UrlKey, Vec<(UrlKey, EdgeType)>>,
}

#[derive(Debug)]
struct PlanNode {
    url: String,
    title: String,
    /// The data type or table kind, written only where the node has none.
    kind: Option<String>,
    /// The static mark a node entry gave: its declaring resource, or
    /// [`DECLARED`]. `None` for a node named only as an ancestor or an edge
    /// endpoint, which gets [`DECLARED`] only where it has no mark, so an
    /// edges-only ingest never erases the resource a node entry recorded.
    mark: Option<String>,
}

impl Plan {
    fn edge(&mut self, from: UrlKey, to: UrlKey, etype: EdgeType) {
        let out = self.edges.entry(from).or_default();
        if !out.iter().any(|(d, _)| *d == to) {
            out.push((to, etype));
        }
    }

    /// A name, its ancestors, and the containment between them. Returns
    /// its key; a name's own kind and mark win over an ancestor's default.
    fn name(
        &mut self,
        uri: &mut UriBuf,
        n: &BqName<'_>,
        kind: Option<&str>,
        mark: Option<&str>,
    ) -> Option<UrlKey> {
        let url = n.uri(uri)?.to_owned();
        let key = UrlKey::of(&url);
        let parent = match n.level() {
            Level::Column => n.table_of(),
            Level::Table => n.dataset_of(),
            Level::Dataset | Level::Project => None,
        };
        let parent_key = match parent {
            Some(p) => self.name(uri, &p, None, None)?,
            None => UrlKey::of(ROOT),
        };
        self.edge(parent_key, key, EdgeType::Contains);
        let title = match n.level() {
            Level::Column => n.column.unwrap_or_default().to_owned(),
            _ => n.dotted(),
        };
        let node = self.nodes.entry(key).or_insert_with(|| PlanNode {
            url,
            title,
            kind: None,
            mark: None,
        });
        if let Some(k) = kind {
            node.kind = Some(k.to_owned());
        }
        if let Some(m) = mark {
            node.mark = Some(m.to_owned());
        }
        Some(key)
    }
}

/// A `bq://` id parsed, or `Ok(None)` for another scheme (a routine, a
/// resource URN), or the reason it is refused.
fn bq<'a>(id: &'a str, uri: &mut UriBuf) -> Result<Option<BqName<'a>>, &'static str> {
    if !id.starts_with(BQ_SCHEME) {
        return Ok(None);
    }
    let n = BqName::parse(id).map_err(|e| match e {
        NameError::Malformed => "not a BigQuery id",
        NameError::BadComponent => "a BigQuery id with a character no name can hold",
        NameError::TooLong => "a BigQuery id component is too long",
    })?;
    // graph-r refuses a url over its label limit mid-batch; refuse it here.
    if n.uri(uri).is_some_and(|u| u.len() > MAX_LABEL) {
        return Err("a BigQuery id too long to store");
    }
    Ok(Some(n))
}

/// The scheme of an id that is not `bq://` (`bq-routine`, `urn`, ...).
fn scheme(id: &str) -> &str {
    let s = id.split_once("://").or_else(|| id.split_once(':')).map_or("other", |(s, _)| s);
    if !s.is_empty() && s.len() <= 32 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        s
    } else {
        "other"
    }
}

/// A count keyed by a name from the input, bounded so hostile input cannot
/// grow the report without limit.
fn count(skipped: &mut BTreeMap<String, usize>, name: &str) {
    const MAX_NAMES: usize = 64;
    let ok = name.len() <= 64
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b':'));
    let name = if ok && (skipped.len() < MAX_NAMES || skipped.contains_key(name)) {
        name
    } else {
        "other"
    };
    *skipped.entry(name.to_owned()).or_default() += 1;
}

fn plan_nodes(nodes: &[Option<Entry<'_>>], plan: &mut Plan, out: &mut Ingested, uri: &mut UriBuf) {
    let reject = |out: &mut Ingested, index, reason| {
        out.rejected.push(Rejected { side: Side::Node, index, reason });
    };
    for (index, e) in nodes.iter().enumerate() {
        let Some(e) = e else {
            reject(out, index, "not an object");
            continue;
        };
        let (Some(id), Some(kind)) = (e.id.as_deref(), e.kind.as_deref()) else {
            reject(out, index, "no string id and kind");
            continue;
        };
        let n = match bq(id, uri) {
            Ok(Some(n)) => n,
            Ok(None) => {
                count(&mut out.skipped, kind);
                continue;
            }
            Err(reason) => {
                reject(out, index, reason);
                continue;
            }
        };
        let fits = match n.level() {
            Level::Project => kind == "project",
            Level::Dataset => kind == "dataset",
            Level::Column => kind == "column",
            Level::Table => !matches!(kind, "project" | "dataset" | "column"),
        };
        if !fits {
            reject(out, index, "the kind does not match the id");
            continue;
        }
        if e.urn.as_deref().is_some_and(|u| u.len() > MAX_LABEL)
            || e.data_type.as_deref().is_some_and(|t| t.len() > MAX_LABEL)
        {
            reject(out, index, "a field too long to store");
            continue;
        }
        if n.level() == Level::Project {
            // The store's root holds every project's datasets: nothing to write.
            continue;
        }
        // A column's kind is its data type; a table's, its kind as the
        // warehouse spells it (`TABLE`, `VIEW`, `MATERIALIZED_VIEW`).
        let k = match n.level() {
            Level::Column => e.data_type.as_deref().map(Cow::Borrowed),
            Level::Table => Some(Cow::Owned(kind.to_ascii_uppercase())),
            _ => None,
        };
        plan.name(uri, &n, k.as_deref(), Some(e.urn.as_deref().unwrap_or(DECLARED)));
    }
}

fn plan_edges(edges: &[Option<Entry<'_>>], plan: &mut Plan, out: &mut Ingested, uri: &mut UriBuf) {
    let reject = |out: &mut Ingested, index, reason| {
        out.rejected.push(Rejected { side: Side::Edge, index, reason });
    };
    for (index, e) in edges.iter().enumerate() {
        let Some(e) = e else {
            reject(out, index, "not an object");
            continue;
        };
        let (Some(rel), Some(src), Some(dst)) =
            (e.relationship.as_deref(), e.source.as_deref(), e.target.as_deref())
        else {
            reject(out, index, "no string relationship, source_id and target_id");
            continue;
        };
        let (etype, levels): (EdgeType, &[Level]) = match rel {
            "contains" => (EdgeType::Contains, &[]),
            "derives_from" => (EdgeType::Derives, &[Level::Table]),
            "column_derives_from" => (EdgeType::Derives, &[Level::Column]),
            _ => {
                count(&mut out.skipped, rel);
                continue;
            }
        };
        let (s, d) = match (bq(src, uri), bq(dst, uri)) {
            (Err(reason), _) | (_, Err(reason)) => {
                reject(out, index, reason);
                continue;
            }
            (Ok(Some(s)), Ok(Some(d))) => (s, d),
            (Ok(s), Ok(_)) => {
                let other = if s.is_none() { src } else { dst };
                count(&mut out.skipped, &format!("{rel}:{}", scheme(other)));
                continue;
            }
        };
        let fits = if etype == EdgeType::Contains {
            // The target sits directly under the source.
            let parent = match d.level() {
                Level::Column => d.table_of(),
                Level::Table => d.dataset_of(),
                Level::Dataset => Some(BqName { dataset: None, ..d }),
                Level::Project => None,
            };
            parent == Some(s)
        } else {
            levels.contains(&s.level()) && s.level() == d.level()
        };
        if !fits {
            reject(out, index, "the ids' levels do not fit the relationship");
            continue;
        }
        if etype == EdgeType::Contains {
            // Containment follows from the ids; the edge only declares them.
            if s.level() != Level::Project {
                plan.name(uri, &s, None, None);
            }
            plan.name(uri, &d, None, None);
            continue;
        }
        let (Some(from), Some(to)) =
            (plan.name(uri, &s, None, None), plan.name(uri, &d, None, None))
        else {
            continue;
        };
        plan.edge(from, to, etype);
    }
}

/// Ingest a static lineage export into `store`: `nodes_json` and
/// `edges_json` are its `nodes` and `edges` arrays, as the export writes
/// them. Malformed JSON is an error and writes nothing; an entry that is
/// well-formed JSON but not a usable node or edge is reported in
/// [`Ingested::rejected`] and the rest are ingested.
pub fn ingest(
    store: &Store,
    nodes_json: &str,
    edges_json: &str,
    now_ms: u64,
) -> Result<Ingested, String> {
    let nodes = entries(nodes_json).map_err(|e| format!("nodes: {e}"))?;
    let edges = entries(edges_json).map_err(|e| format!("edges: {e}"))?;
    let mut out = Ingested::default();
    let mut plan = Plan::default();
    let mut uri = UriBuf::new();
    plan_nodes(&nodes, &mut plan, &mut out, &mut uri);
    plan_edges(&edges, &mut plan, &mut out, &mut uri);
    apply(store, &plan, &mut out, now_ms).map_err(|e| format!("store: {e}"))?;
    Ok(out)
}

/// One staged change, decided against the snapshot before any is staged.
enum Op<'p> {
    /// The store's root, when this is its first content.
    Root,
    Node(&'p PlanNode),
    Attrs(UrlKey, Vec<(u8, String)>),
    Edges(UrlKey, Vec<(UrlKey, EdgeType, u16, u8)>),
}

fn apply(store: &Store, plan: &Plan, out: &mut Ingested, now: u64) -> graph_r::Result<()> {
    let ops = {
        let snap = store.snapshot();
        diff(&snap, plan, out)?
    };
    if ops.is_empty() {
        return Ok(());
    }
    let mut w = store.writer()?;
    for op in &ops {
        match op {
            Op::Root => w.upsert_node(&DocRecord {
                url: ROOT,
                url_key: UrlKey::of(ROOT),
                content_hash: 0,
                fetched_at_ms: now,
                title: Some("bigquery"),
                snippet: None,
                etag: None,
                pinned: false,
            })?,
            Op::Node(n) => w.upsert_node(&DocRecord {
                url: &n.url,
                url_key: UrlKey::of(&n.url),
                content_hash: 0,
                fetched_at_ms: now,
                title: Some(&n.title),
                snippet: None,
                etag: None,
                pinned: false,
            })?,
            Op::Attrs(key, attrs) => {
                let attrs: Vec<(u8, &str)> = attrs.iter().map(|(k, v)| (*k, v.as_str())).collect();
                w.set_attrs(*key, &attrs)?;
            }
            Op::Edges(key, edges) => w.set_edges_with_flags(*key, edges),
        }
    }
    w.commit()?;
    out.committed = true;
    Ok(())
}

fn diff<'p>(
    snap: &Snapshot<'_>,
    plan: &'p Plan,
    out: &mut Ingested,
) -> graph_r::Result<Vec<Op<'p>>> {
    let mut ops = Vec::new();

    // Deterministic order: the same input stages the same frames.
    let mut keys: Vec<&UrlKey> = plan.nodes.keys().collect();
    keys.sort_unstable();
    for key in keys {
        let n = &plan.nodes[key];
        let exists = snap.node(*key).is_some_and(|x| !x.url.is_empty());
        if !exists {
            ops.push(Op::Node(n));
            out.created += 1;
        }
        let mut attrs: Vec<(u8, String)> = Vec::new();
        snap.attrs(*key, |k, v| attrs.push((k, v.to_owned())));
        let mut changed = false;
        if let Some(kind) = &n.kind {
            if !attrs.iter().any(|(k, _)| *k == attr::TYPE) {
                attrs.push((attr::TYPE, kind.clone()));
                changed = true;
            }
        }
        match (attrs.iter_mut().find(|(k, _)| *k == attr::STATIC), &n.mark) {
            (Some((_, v)), Some(m)) if v != m => {
                v.clone_from(m);
                changed = true;
            }
            (Some(_), _) => {}
            (None, m) => {
                attrs.push((attr::STATIC, m.as_deref().unwrap_or(DECLARED).to_owned()));
                changed = true;
            }
        }
        if attrs.len() > MAX_ATTRS {
            return Err(graph_r::Error::format("too many attributes"));
        }
        if changed {
            out.updated += usize::from(exists);
            ops.push(Op::Attrs(*key, attrs));
        }
    }
    let mut sources: Vec<&UrlKey> = plan.edges.keys().collect();
    sources.sort_unstable();
    for src in sources {
        let mut have: Vec<(UrlKey, EdgeType, u16, u8)> = Vec::new();
        let mut it = snap.neighbors(*src);
        while let Some(e) = graph_r::LendingIterator::next(&mut it) {
            have.push((e.dst_key, e.etype, e.weight, e.flags));
        }
        let before = have.len();
        for &(dst, etype) in &plan.edges[src] {
            if !have.iter().any(|h| h.0 == dst) {
                have.push((dst, etype, 1, 0));
            }
        }
        if have.len() > before {
            out.edges += have.len() - before;
            ops.push(Op::Edges(*src, have));
        }
    }
    if !ops.is_empty() && snap.node(UrlKey::of(ROOT)).is_none() {
        // A static-only store has the root a describe would have written,
        // so every lookup and a later describe see one shape.
        ops.insert(0, Op::Root);
    }
    Ok(ops)
}

/// [`ingest`] into the store at `dir`, created when absent.
pub fn ingest_dir(dir: &str, nodes_json: &str, edges_json: &str) -> Result<Ingested, String> {
    let exists = std::path::Path::new(dir).join(graph_r::store::BASE_FILE).exists();
    let store =
        if exists { Store::open(dir) } else { Store::create(dir, graph_r::Config::default()) }
            .map_err(|e| format!("{dir}: {e}"))?;
    ingest(&store, nodes_json, edges_json, graph_r::fsio::now_ms())
}

/// One line per rejected entry, for a log.
#[must_use]
pub fn summary(i: &Ingested) -> String {
    let mut s = format!(
        "static lineage: {} created, {} updated, {} edges added{}",
        i.created,
        i.updated,
        i.edges,
        if i.committed { "" } else { " (nothing new)" }
    );
    for (k, v) in &i.skipped {
        let _ = write!(s, "; skipped {v} {k}");
    }
    if !i.rejected.is_empty() {
        let _ = write!(s, "; rejected {}", i.rejected.len());
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(nodes: &str, edges: &str) -> (Plan, Ingested) {
        let (mut p, mut out, mut uri) = (Plan::default(), Ingested::default(), UriBuf::new());
        plan_nodes(&entries(nodes).unwrap(), &mut p, &mut out, &mut uri);
        plan_edges(&entries(edges).unwrap(), &mut p, &mut out, &mut uri);
        (p, out)
    }

    #[test]
    fn entries_borrow_plain_strings_and_decode_escaped_ones() {
        let e = entries(r#"[{"id":"bq://p/d","kind":"data\u0073et","x":{"y":[1]}}, 3, {"id":1}]"#)
            .unwrap();
        assert_eq!(e.len(), 3);
        let first = e[0].as_ref().unwrap();
        assert!(matches!(first.id, Some(Cow::Borrowed("bq://p/d"))));
        assert!(matches!(first.kind.as_deref(), Some("dataset")));
        assert!(matches!(first.kind, Some(Cow::Owned(_))));
        assert!(e[1].is_none());
        assert!(e[2].as_ref().unwrap().id.is_none(), "a number is not an id");
    }

    #[test]
    fn a_name_brings_its_ancestors_and_their_containment() {
        let (p, out) = plan(r#"[{"id":"bq://p/d/t#c","kind":"column","data_type":"INT64"}]"#, "[]");
        assert!(out.rejected.is_empty());
        let mut uri = UriBuf::new();
        let (d, t, c) = (
            UrlKey::of(uri.dataset("p", "d")),
            UrlKey::of(uri.table("p", "d", "t")),
            UrlKey::of(uri.column("p", "d", "t", "c")),
        );
        assert_eq!(p.nodes.len(), 3);
        assert_eq!(p.edges[&UrlKey::of(ROOT)], vec![(d, EdgeType::Contains)]);
        assert_eq!(p.edges[&d], vec![(t, EdgeType::Contains)]);
        assert_eq!(p.edges[&t], vec![(c, EdgeType::Contains)]);
        assert_eq!(p.nodes[&c].kind.as_deref(), Some("INT64"));
        assert_eq!(p.nodes[&c].mark.as_deref(), Some(DECLARED));
        assert_eq!(p.nodes[&t].mark, None, "an ancestor is not a declaration of its own");
        assert_eq!(p.nodes[&t].title, "p.d.t");
        assert_eq!(p.nodes[&c].title, "c");
    }

    #[test]
    fn kinds_must_match_the_level_of_the_id() {
        for (id, kind, ok) in [
            ("bq://p", "project", true),
            ("bq://p", "dataset", false),
            ("bq://p/d", "dataset", true),
            ("bq://p/d", "table", false),
            ("bq://p/d/t", "view", true),
            ("bq://p/d/t", "materialized_view", true),
            ("bq://p/d/t", "column", false),
            ("bq://p/d/t#c", "column", true),
            ("bq://p/d/t/c", "column", true),
            ("bq://p/d/t#c", "table", false),
        ] {
            let (_, out) = plan(&format!(r#"[{{"id":"{id}","kind":"{kind}"}}]"#), "[]");
            assert_eq!(out.rejected.is_empty(), ok, "{id} as {kind}");
        }
        let (p, _) = plan(r#"[{"id":"bq://p/d/t","kind":"materialized_view"}]"#, "[]");
        let t = UrlKey::of(UriBuf::new().table("p", "d", "t"));
        assert_eq!(p.nodes[&t].kind.as_deref(), Some("MATERIALIZED_VIEW"));
    }

    #[test]
    fn edges_must_fit_their_relationship() {
        for (rel, src, dst, ok) in [
            ("contains", "bq://p", "bq://p/d", true),
            ("contains", "bq://p", "bq://q/d", false),
            ("contains", "bq://p/d", "bq://p/d/t", true),
            ("contains", "bq://p/d", "bq://p/e/t", false),
            ("contains", "bq://p/d/t", "bq://p/d/t#c", true),
            ("contains", "bq://p/d/t", "bq://p/d/u#c", false),
            ("contains", "bq://p/d/t#c", "bq://p/d/t", false),
            ("derives_from", "bq://p/d/t", "bq://q/e/u", true),
            ("derives_from", "bq://p/d/t#c", "bq://q/e/u#c", false),
            ("derives_from", "bq://p/d", "bq://q/e", false),
            ("column_derives_from", "bq://p/d/t#c", "bq://q/e/u/c", true),
            ("column_derives_from", "bq://p/d/t", "bq://q/e/u", false),
            ("column_derives_from", "bq://p/d/t#c", "bq://q/e/u", false),
        ] {
            let e =
                format!(r#"[{{"relationship":"{rel}","source_id":"{src}","target_id":"{dst}"}}]"#);
            let (_, out) = plan("[]", &e);
            assert_eq!(out.rejected.is_empty(), ok, "{rel} {src} {dst}");
        }
    }

    #[test]
    fn out_of_scope_entries_are_counted_by_name() {
        let (p, out) = plan(
            r#"[{"id":"bq-routine://p/d/r","kind":"routine"},{"id":"urn:x","kind":"stack"}]"#,
            r#"[{"relationship":"writes_to","source_id":"bq-routine://p/d/r","target_id":"bq://p/d/t"},
                {"relationship":"derives_from","source_id":"bq-routine://p/d/r","target_id":"bq://p/d/t"},
                {"relationship":"derives_from","source_id":"bq://p/d/t","target_id":"urn:pulumi:x"}]"#,
        );
        assert!(p.nodes.is_empty() && out.rejected.is_empty());
        let names: Vec<_> = out.skipped.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        assert_eq!(
            names,
            vec![
                ("derives_from:bq-routine", 1),
                ("derives_from:urn", 1),
                ("routine", 1),
                ("stack", 1),
                ("writes_to", 1)
            ]
        );
    }

    #[test]
    fn schemes_and_counted_names_are_sanitised() {
        assert_eq!(scheme("bq-routine://p/d/r"), "bq-routine");
        assert_eq!(scheme("urn:pulumi:a"), "urn");
        assert_eq!(scheme("no scheme"), "other");
        assert_eq!(scheme("a b://x"), "other");
        let mut m = BTreeMap::new();
        count(&mut m, "fine_name");
        count(&mut m, "bad name");
        count(&mut m, &"x".repeat(65));
        assert_eq!(m.keys().map(String::as_str).collect::<Vec<_>>(), vec!["fine_name", "other"]);
        assert_eq!(m["other"], 2);
    }

    #[test]
    fn the_report_is_json() {
        let mut i = Ingested { committed: true, created: 1, ..Ingested::default() };
        i.skipped.insert("writes_to".into(), 2);
        i.rejected.push(Rejected { side: Side::Edge, index: 4, reason: "not an object" });
        assert_eq!(
            i.to_json(),
            r#"{"committed":true,"created":1,"updated":0,"edges":0,"skipped":{"writes_to":2},"rejected":[{"side":"edge","index":4,"reason":"not an object"}]}"#
        );
        assert_eq!(
            summary(&i),
            "static lineage: 1 created, 0 updated, 0 edges added; skipped 2 writes_to; rejected 1"
        );
    }
}
