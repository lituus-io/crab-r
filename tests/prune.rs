// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// The store's TTL: a prune removes exactly the table subtrees no describe or
// ingest has written or touched since the cutoff, keeps fresh and pinned
// ones, rewrites the hierarchy around what went (a dataset left empty goes
// too) and leaves no edge pointing at a removed node.

use crab_r::describe::describe::{Action, ColumnReport, Report, TableReport};
use crab_r::describe::lifecycle::prune_at;
use crab_r::describe::state::{self, Pruned, ROOT};
use crab_r::lineage::Column;
use graph_r::{Config, EdgeType, Snapshot, Store, UrlKey};
use std::path::PathBuf;

const DAY: u64 = 86_400_000;

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("crab-prune-it-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn column(name: &str, sources: &[&str]) -> ColumnReport {
    ColumnReport {
        column: name.into(),
        data_type: "INT64".into(),
        action: Action::Generate,
        existing: None,
        description: Some(format!("{name}.")),
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
        confidence: None,
        checks: vec![],
        consumers: vec![],
    }
}

fn table(name: &str, columns: Vec<ColumnReport>, reused: bool) -> TableReport {
    TableReport {
        table: name.into(),
        kind: "TABLE".into(),
        rows: 1,
        bytes: 1,
        last_modified_ms: 0,
        producer: None,
        existing: None,
        description: Some(format!("{name}.")),
        grain: vec![],
        filters: vec![],
        staged: vec![],
        conditions: vec![],
        columns,
        checks: vec![],
        model_calls: 0,
        warnings: vec![],
        pending: false,
        spend: Default::default(),
        meta: None,
        prekey: 1,
        key: Some(1),
        reused,
        carried_checks: None,
    }
}

fn save(store: &Store, tables: Vec<TableReport>, at: u64) {
    state::save(store, &Report { tables, ..Report::default() }, at).unwrap();
}

fn key(uri: &str) -> UrlKey {
    UrlKey::of(&format!("{ROOT}{uri}"))
}

fn contains(snap: &Snapshot<'_>, k: UrlKey) -> Vec<UrlKey> {
    let mut out = Vec::new();
    let mut it = snap.neighbors(k);
    while let Some(e) = graph_r::LendingIterator::next(&mut it) {
        if e.etype == EdgeType::Contains {
            out.push(e.dst_key);
        }
    }
    out.sort_unstable();
    out
}

fn derives(snap: &Snapshot<'_>, k: UrlKey) -> Vec<UrlKey> {
    let mut out = Vec::new();
    let mut it = snap.neighbors(k);
    while let Some(e) = graph_r::LendingIterator::next(&mut it) {
        if e.etype == EdgeType::Derives {
            out.push(e.dst_key);
        }
    }
    out
}

#[test]
fn a_prune_removes_exactly_the_stale_subtrees() {
    let d = dir("exact");
    let store = Store::open_or_create(&d, Config::default()).unwrap();
    // Day 1: everything described.
    save(
        &store,
        vec![
            table("p.old.a", vec![column("x", &[])], false),
            table("p.old.b", vec![column("x", &[])], false),
            table("p.mix.stale", vec![column("x", &[]), column("y", &[])], false),
            table("p.mix.pinned", vec![column("x", &[])], false),
            table("p.mix.col_pinned", vec![column("x", &[]), column("y", &[])], false),
            table("p.mix.reused", vec![column("x", &[])], false),
        ],
        DAY,
    );
    // Day 9: a fresh table reading a stale one, and a reuse touch.
    save(
        &store,
        vec![
            table("p.mix.fresh", vec![column("x", &["p.mix.stale.x"])], false),
            table("p.mix.reused", vec![], true),
        ],
        9 * DAY,
    );
    store
        .transact(|w| {
            w.set_pinned(key("p/mix/pinned"), true)?;
            w.set_pinned(key("p/mix/col_pinned#y"), true)?;
            Ok(())
        })
        .unwrap();
    let generation = store.generation();
    drop(store);

    // Ten days in, a five-day TTL.
    let (p, regenerated) = prune_at(&d, 5, 10 * DAY).unwrap();
    assert!(!regenerated);
    assert_eq!(p, Pruned { tables: 3, columns: 4 });

    let store = Store::open_read_only(&d).unwrap();
    assert_eq!(store.generation(), generation + 1, "one compaction");
    let snap = store.snapshot();
    for gone in ["p/old", "p/old/a", "p/old/a#x", "p/old/b", "p/old/b#x", "p/mix/stale"] {
        assert!(snap.node(key(gone)).is_none(), "{gone} removed");
    }
    for gone in ["p/mix/stale#x", "p/mix/stale#y"] {
        assert!(snap.node(key(gone)).is_none(), "{gone} removed");
        assert!(snap.attr(key(gone), state::attr::TYPE).is_none(), "{gone}: its attributes too");
    }
    for kept in [
        "p/mix",
        "p/mix/fresh",
        "p/mix/fresh#x",
        "p/mix/pinned",
        "p/mix/pinned#x",
        "p/mix/col_pinned",
        "p/mix/col_pinned#x",
        "p/mix/col_pinned#y",
        "p/mix/reused",
        "p/mix/reused#x",
    ] {
        assert!(snap.node(key(kept)).is_some(), "{kept} kept");
    }
    assert_eq!(contains(&snap, UrlKey::of(ROOT)), vec![key("p/mix")], "the empty dataset dropped");
    let mut mix =
        vec![key("p/mix/fresh"), key("p/mix/pinned"), key("p/mix/col_pinned"), key("p/mix/reused")];
    mix.sort_unstable();
    assert_eq!(contains(&snap, key("p/mix")), mix);
    assert!(derives(&snap, key("p/mix/fresh#x")).is_empty(), "no edge into a removed column");
    // What is left reads as before.
    let prior = state::load(&store);
    assert_eq!(prior.len(), 4);
    for t in ["p.mix.col_pinned", "p.mix.fresh", "p.mix.pinned", "p.mix.reused"] {
        assert!(prior.get(t).is_some(), "{t}");
    }
    drop(snap);
    drop(store);

    // Again: nothing that old is left, so nothing is written.
    let wal = std::fs::read(d.join(graph_r::store::WAL_FILE)).unwrap();
    assert_eq!(prune_at(&d, 5, 10 * DAY).unwrap(), (Pruned::default(), false));
    assert_eq!(std::fs::read(d.join(graph_r::store::WAL_FILE)).unwrap(), wal);
    assert_eq!(Store::open_read_only(&d).unwrap().generation(), generation + 1);
}

#[test]
fn a_prune_of_a_damaged_store_regenerates_it() {
    let d = dir("damaged");
    drop(Store::open_or_create(&d, Config::default()).unwrap());
    std::fs::write(d.join(graph_r::store::BASE_FILE), b"not a base").unwrap();
    assert_eq!(prune_at(&d, 1, 10 * DAY).unwrap(), (Pruned::default(), true));
    assert!(Store::open_read_only(&d).unwrap().is_empty());
}
