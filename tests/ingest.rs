// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Static lineage into a describe store, end to end: a deployment program's
// lineage export (captured as emitted, `tests/data/static_lineage`) answers
// every lookup offline; a later describe enriches the same nodes and keeps
// what was declared; a repeat ingest writes nothing; hostile entries are
// refused by position without being repeated, and never leave half an
// ingest behind.

#![cfg(feature = "cli")]

use crab_r::base::key::UriBuf;
use crab_r::describe::app::lookup;
use crab_r::describe::describe::{Action, ColumnReport, Report, TableReport};
use crab_r::describe::ingest::{ingest, ingest_dir, Ingested, Rejected, Side, DECLARED};
use crab_r::describe::state::{self, attr};
use crab_r::lineage::Column;
use graph_r::{EdgeType, Store, UrlKey};
use std::path::PathBuf;

const NODES: &str = include_str!("data/static_lineage/nodes.json");
const EDGES: &str = include_str!("data/static_lineage/edges.json");

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("crab-ingest-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn s(p: &std::path::Path) -> String {
    p.to_string_lossy().into_owned()
}

fn look(cmd: &str, store: &std::path::Path, name: &str) -> String {
    lookup(cmd, &[s(store), name.to_owned()]).unwrap()
}

fn table_key(p: &str, d: &str, t: &str) -> UrlKey {
    UrlKey::of(UriBuf::new().table(p, d, t))
}

fn column_key(p: &str, d: &str, t: &str, c: &str) -> UrlKey {
    UrlKey::of(UriBuf::new().column(p, d, t, c))
}

fn derives(snap: &graph_r::Snapshot<'_>, k: UrlKey) -> Vec<UrlKey> {
    let mut out = Vec::new();
    let mut it = snap.neighbors(k);
    while let Some(e) = graph_r::LendingIterator::next(&mut it) {
        if e.etype == EdgeType::Derives {
            out.push(e.dst_key);
        }
    }
    out
}

fn wal_len(d: &std::path::Path) -> u64 {
    std::fs::read_dir(d)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().contains("wal"))
        .map(|e| e.metadata().unwrap().len())
        .sum()
}

#[test]
fn the_export_ingests_as_is_and_answers_every_lookup_offline() {
    let d = dir("as-is");
    let done = ingest_dir(&s(&d), NODES, EDGES).unwrap();
    assert!(done.committed);
    assert_eq!(done.rejected, vec![]);
    // 2 datasets, 4 tables, 8 columns; the projects are the root's.
    assert_eq!((done.created, done.updated), (14, 0));
    // 14 containment edges, 2 table and 4 column derivations.
    assert_eq!(done.edges, 20);
    let skipped: Vec<(&str, usize)> = done.skipped.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    assert_eq!(
        skipped,
        vec![("defined_by", 4), ("derives_from:bq-routine", 1), ("routine", 1), ("writes_to", 1)]
    );

    let view = look("table", &d, "bq://proj/mart/orders_v");
    assert!(view.starts_with("proj/mart/orders_v [static]"), "{view}");
    assert!(view.contains("kind: VIEW") && view.contains("from other/raw/events"), "{view}");
    assert!(view.contains("proj/mart/orders_v#amt [static]"), "{view}");
    assert_eq!(view, look("table", &d, "proj.mart.orders_v"));

    // The three spellings of a column are one node.
    let doubled = look("lookup", &d, "bq://proj/mart/orders_t#doubled");
    assert!(doubled.contains("from proj/mart/orders#amount"), "{doubled}");
    assert_eq!(doubled, look("lookup", &d, "bq://proj/mart/orders_t/doubled"));
    assert_eq!(doubled, look("lookup", &d, "proj.mart.orders_t.doubled"));
    let amount = look("lookup", &d, "bq://proj/mart/orders#amount");
    assert!(
        amount.contains("type: NUMERIC") && amount.contains("feeds proj/mart/orders_t#doubled")
    );

    // Column lineage, both ways, across projects.
    let up = look("upstream", &d, "bq://proj/mart/orders_v#amt");
    assert!(up.contains("other/raw/events#amount"), "{up}");
    let down = look("downstream", &d, "proj.mart.orders.id");
    assert!(down.contains("proj/mart/orders_t#id"), "{down}");
    // Table lineage, both ways.
    let up = look("upstream", &d, "proj.mart.orders_t");
    assert!(up.lines().nth(1).unwrap().contains("proj/mart/orders [static]"), "{up}");
    let down = look("downstream", &d, "bq://other/raw/events");
    assert!(down.contains("proj/mart/orders_v"), "{down}");
    // A name of the wrong level, or none at all, is not found.
    assert!(lookup("table", &[s(&d), "bq://proj/mart/orders#id".into()]).is_err());
    assert!(lookup("lookup", &[s(&d), "bq://proj/mart/orders".into()]).is_err());
    assert!(lookup("upstream", &[s(&d), "bq://proj/mart/absent".into()]).is_err());

    // Declared tables carry no incremental key: a describe gets no reuse.
    let store = Store::open(&d).unwrap();
    assert!(state::load(&store).is_empty());
    let snap = store.snapshot();
    let v = table_key("proj", "mart", "orders_v");
    assert_eq!(
        snap.attr(v, attr::STATIC),
        Some("urn:pulumi:dev::lineage::gcp:bigquery/table:Table::v")
    );
    assert_eq!(snap.attr(table_key("other", "raw", "events"), attr::STATIC), Some(DECLARED));
    assert_eq!(snap.node(v).unwrap().title, Some("proj.mart.orders_v"));
}

#[test]
fn a_repeat_ingest_commits_nothing() {
    let d = dir("repeat");
    assert!(ingest_dir(&s(&d), NODES, EDGES).unwrap().committed);
    let wal = wal_len(&d);
    let store = Store::open(&d).unwrap();
    let again = ingest(&store, NODES, EDGES, 2).unwrap();
    assert_eq!((again.committed, again.created, again.updated, again.edges), (false, 0, 0, 0));
    drop(store);
    assert_eq!(wal_len(&d), wal, "a repeat ingest appended to the log");
    // The edges alone name every node: still nothing to do.
    let store = Store::open(&d).unwrap();
    assert!(!ingest(&store, "[]", EDGES, 3).unwrap().committed);
}

#[test]
fn a_self_derivation_is_skipped_and_a_repeat_commits_nothing() {
    // A column written from itself (`SET c = f(c)`), here in the export's
    // `#c` spelling and the path spelling, which name one node. The store
    // keeps no edge from a node to itself, so staging one would commit it
    // again on every repeat.
    let d = dir("self");
    let e = r#"[{"relationship":"column_derives_from","source_id":"bq://p/dd/t/C","target_id":"bq://p/dd/t#C"}]"#;
    assert!(ingest_dir(&s(&d), "[]", e).unwrap().committed);
    let col = column_key("p", "dd", "t", "C");
    let store = Store::open(&d).unwrap();
    assert!(derives(&store.snapshot(), col).is_empty());
    drop(store);
    let wal = wal_len(&d);
    let store = Store::open(&d).unwrap();
    let again = ingest(&store, "[]", e, 2).unwrap();
    assert!(!again.committed, "a repeat ingest wrote: {again:?}");
    drop(store);
    assert_eq!(wal_len(&d), wal, "a repeat ingest appended to the log");
}

fn column(name: &str, sources: &[&str], description: &str) -> ColumnReport {
    ColumnReport {
        column: name.into(),
        data_type: "INT64".into(),
        action: Action::Generate,
        existing: None,
        description: Some(description.into()),
        semantic_type: None,
        unit: None,
        transform: None,
        expression: None,
        sources: sources
            .iter()
            .map(|s| {
                let (t, c) = s.rsplit_once('.').unwrap();
                Column::new(t, c)
            })
            .collect(),
        hops: vec![],
        confidence: Some("high".into()),
        checks: vec![],
        consumers: vec![],
    }
}

fn described(columns: Vec<ColumnReport>) -> Report {
    Report {
        tables: vec![TableReport {
            table: "proj.mart.orders_t".into(),
            kind: "TABLE".into(),
            rows: 10,
            bytes: 100,
            last_modified_ms: 0,
            producer: None,
            existing: None,
            description: Some("Orders, doubled.".into()),
            grain: vec![],
            filters: vec![],
            staged: vec![],
            conditions: vec![],
            columns,
            checks: vec![],
            model_calls: 1,
            warnings: vec![],
            pending: false,
            spend: Default::default(),
            meta: None,
            prekey: 7,
            key: Some(9),
            reused: false,
            carried_checks: None,
        }],
        ..Report::default()
    }
}

#[test]
fn a_describe_enriches_declared_nodes_and_keeps_what_was_declared() {
    let d = dir("enrich");
    ingest_dir(&s(&d), NODES, EDGES).unwrap();
    let store = Store::open(&d).unwrap();
    // The trace found `id`'s source but not `doubled`'s.
    let report = described(vec![
        column("id", &["proj.mart.orders.id"], "Order id."),
        column("doubled", &[], "Twice the amount."),
    ]);
    state::save(&store, &report, 5).unwrap();
    let snap = store.snapshot();
    let t = table_key("proj", "mart", "orders_t");
    let dbl = column_key("proj", "mart", "orders_t", "doubled");
    assert_eq!(snap.node(t).unwrap().snippet, Some("Orders, doubled."));
    assert_eq!(snap.node(dbl).unwrap().snippet, Some("Twice the amount."));
    assert_eq!(snap.attr(dbl, attr::ACTION), Some("generate"));
    // The static marks and the declared lineage survive the describe.
    assert_eq!(snap.attr(t, attr::STATIC), Some(DECLARED));
    assert_eq!(snap.attr(dbl, attr::STATIC), Some(DECLARED));
    assert_eq!(derives(&snap, dbl), vec![column_key("proj", "mart", "orders", "amount")]);
    assert_eq!(derives(&snap, t), vec![table_key("proj", "mart", "orders")]);
    // The table is now a reusable record.
    assert_eq!(state::load(&store).len(), 1);
    // The lookup reports what the describe did, not merely that it was declared.
    let line = look("lookup", &d, "bq://proj/mart/orders_t#doubled");
    assert!(line.starts_with("proj/mart/orders_t#doubled [generate] — Twice"), "{line}");
    // Ingesting again changes nothing the describe wrote.
    drop(snap);
    let again = ingest(&store, NODES, EDGES, 6).unwrap();
    assert!(!again.committed, "{again:?}");
}

#[test]
fn an_ingest_into_a_described_store_never_overwrites_a_judgement() {
    let d = dir("described");
    let store = Store::create(&d, graph_r::Config::default()).unwrap();
    state::save(&store, &described(vec![column("doubled", &[], "Twice the amount.")]), 1).unwrap();
    let done = ingest(&store, NODES, EDGES, 2).unwrap();
    assert!(done.committed);
    // orders_t, its dataset and `doubled` existed and gained the mark.
    assert_eq!((done.created, done.updated), (11, 3));
    let snap = store.snapshot();
    let t = table_key("proj", "mart", "orders_t");
    let dbl = column_key("proj", "mart", "orders_t", "doubled");
    assert_eq!(snap.node(dbl).unwrap().snippet, Some("Twice the amount."));
    assert_eq!(snap.attr(dbl, attr::TYPE), Some("INT64"));
    assert_eq!(snap.attr(dbl, attr::ACTION), Some("generate"));
    assert_eq!(snap.attr(t, attr::TYPE), Some("TABLE"));
    assert_eq!(snap.attr(t, attr::KEY), Some("0000000000000009"));
    assert_eq!(snap.node(t).unwrap().content_hash, 7);
    assert_eq!(state::load(&store).len(), 1);
}

#[test]
fn table_and_column_keys_stay_distinct() {
    // This store keys the exact URI (graph-r's `UrlKey::of`), with no url
    // canonicalisation, so a column's `#name` never collapses onto its table.
    let d = dir("distinct");
    ingest_dir(&s(&d), NODES, EDGES).unwrap();
    let store = Store::open(&d).unwrap();
    let snap = store.snapshot();
    let t = table_key("proj", "mart", "orders");
    let c = column_key("proj", "mart", "orders", "id");
    assert_ne!(t, c);
    assert_eq!(snap.node(c).unwrap().url, "sql://bigquery/proj/mart/orders#id");
    assert_eq!(snap.node(t).unwrap().url, "sql://bigquery/proj/mart/orders");
}

// ---- security --------------------------------------------------------------

const HOSTILE: &str = "`; DROP TABLE x; --";

#[test]
fn hostile_entries_are_refused_by_position_and_never_repeated() {
    let d = dir("hostile");
    let long = "t".repeat(1500);
    let nodes = format!(
        r#"[
        {{"id":"bq://p/d/t{HOSTILE}","kind":"table"}},
        {{"id":"bq://p/d/../t","kind":"table"}},
        {{"id":"bq://p/d/t#c","kind":"table"}},
        "a plain string entry",
        {{"kind":"table"}},
        {{"id":7,"kind":"table"}},
        {{"id":"bq://p/{long}/{long}","kind":"table"}},
        {{"id":"bq://p/d/ok","kind":"table","defined_by_urn":"{big}"}},
        {{"id":"bq://p/d/good","kind":"table"}}
    ]"#,
        big = "u".repeat(4000)
    );
    let edges = r#"[
        {"relationship":"derives_from","source_id":"bq://p/d/good","target_id":"bq://p/d/t\n"},
        {"relationship":"derives_from","source_id":"bq://p/d/good","target_id":"bq://p/d/good#c"},
        {"relationship":"contains","source_id":"bq://p/x","target_id":"bq://p/d/good"},
        {"relationship":"column_derives_from","source_id":"bq://p/d/good"}
    ]"#;
    let done = ingest_dir(&s(&d), &nodes, edges).unwrap();
    let at = |side, index| done.rejected.iter().find(|r| r.side == side && r.index == index);
    for i in [0, 1, 2, 3, 4, 5, 6, 7] {
        assert!(at(Side::Node, i).is_some(), "node {i} was not refused: {done:?}");
    }
    for i in 0..4 {
        assert!(at(Side::Edge, i).is_some(), "edge {i} was not refused: {done:?}");
    }
    assert_eq!(done.rejected.len(), 12);
    assert_eq!(done.created, 2, "only p.d and p.d.good");
    let json = done.to_json();
    for bad in [HOSTILE, "..", &long[..100], "uuuu", "plain string"] {
        assert!(!json.contains(bad), "the report repeats input: {json}");
    }
    let store = Store::open(&d).unwrap();
    let snap = store.snapshot();
    assert!(snap.node(table_key("p", "d", "good")).is_some());
    assert!(snap.node(table_key("p", "d", "ok")).is_none());
}

#[test]
fn malformed_json_writes_nothing() {
    let d = dir("malformed");
    let store = Store::create(&d, graph_r::Config::default()).unwrap();
    for (nodes, edges) in [
        ("[", "[]"),
        ("{}", "[]"),
        (NODES, "[] trailing"),
        (NODES, r#"[{"relationship":"contains","source_id":"bq://p","target_id":"bq://p/d"}"#),
        ("[\"\\uZZZZ\"]", "[]"),
    ] {
        assert!(ingest(&store, nodes, edges, 1).is_err());
    }
    assert!(store.snapshot().node(UrlKey::of(state::ROOT)).is_none());
    // And the store is usable afterwards: nothing was left staged.
    let done = ingest(
        &store,
        "[]",
        r#"[{"relationship":"contains","source_id":"bq://p/d","target_id":"bq://p/d/t"}]"#,
        2,
    )
    .unwrap();
    assert_eq!((done.created, done.edges), (2, 2));
}

#[test]
fn a_refused_entry_never_leaves_staged_writes_for_the_next_commit() {
    // A url past graph-r's label limit would fail inside a writer batch and
    // leave the batch's earlier ops staged; it must be refused up front.
    let d = dir("staged");
    let big = "d".repeat(1000);
    let nodes = format!(
        r#"[{{"id":"bq://p/a","kind":"dataset"}},{{"id":"bq://p/{big}/{big}#{c}","kind":"column"}}]"#,
        c = "c".repeat(300)
    );
    let store = Store::create(&d, graph_r::Config::default()).unwrap();
    let done = ingest(&store, &nodes, "[]", 1).unwrap();
    assert_eq!(
        done.rejected,
        vec![Rejected { side: Side::Node, index: 1, reason: "a BigQuery id too long to store" }]
    );
    let next = ingest(&store, r#"[{"id":"bq://q/b","kind":"dataset"}]"#, "[]", 2).unwrap();
    assert_eq!(next.created, 1);
    let snap = store.snapshot();
    let mut uri = UriBuf::new();
    assert!(snap.node(UrlKey::of(uri.dataset("p", "a"))).is_some());
    assert!(snap.node(UrlKey::of(uri.dataset("p", &big))).is_none());
}

#[test]
fn skipped_names_are_bounded() {
    let edges: Vec<String> = (0..500)
        .map(|i| format!(r#"{{"relationship":"rel_{i}","source_id":"a","target_id":"b"}}"#))
        .chain(std::iter::once(format!(
            r#"{{"relationship":"{HOSTILE}","source_id":"a","target_id":"b"}}"#
        )))
        .collect();
    let d = dir("bounded");
    let done: Ingested = ingest_dir(&s(&d), "[]", &format!("[{}]", edges.join(","))).unwrap();
    assert!(done.skipped.len() <= 65, "{}", done.skipped.len());
    assert_eq!(done.skipped.values().sum::<usize>(), 501);
    assert!(!done.to_json().contains("DROP"));
    assert!(!done.committed);
}
