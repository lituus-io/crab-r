// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Several writers on one describe store. graph-r takes the writer lock per
// commit, so handles in other processes (or this one) write the same store
// concurrently. A save or an ingest unions with what is recorded and replaces
// whole edge sets, so it must read that state under the commit lock: a writer
// whose view predates another's commit builds on it, never overwrites it.

#![cfg(feature = "cli")]

use crab_r::base::key::UriBuf;
use crab_r::describe::describe::{Report, TableReport};
use crab_r::describe::ingest::ingest;
use crab_r::describe::state;
use graph_r::{Config, EdgeType, Store, UrlKey};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};

const NODES: &str = include_str!("data/static_lineage/nodes.json");
const EDGES: &str = include_str!("data/static_lineage/edges.json");

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("crab-writers-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn report(table: &str) -> Report {
    Report {
        tables: vec![TableReport {
            table: table.into(),
            kind: "TABLE".into(),
            rows: 1,
            bytes: 1,
            last_modified_ms: 0,
            producer: None,
            existing: None,
            description: Some(format!("{table}, described.")),
            grain: vec![],
            filters: vec![],
            staged: vec![],
            conditions: vec![],
            columns: vec![],
            checks: vec![],
            model_calls: 1,
            warnings: vec![],
            pending: false,
            spend: Default::default(),
            meta: None,
            prekey: 1,
            key: Some(2),
            reused: false,
            carried_checks: None,
        }],
        ..Report::default()
    }
}

fn contains(snap: &graph_r::Snapshot<'_>, k: UrlKey) -> Vec<UrlKey> {
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

fn dataset(p: &str, d: &str) -> UrlKey {
    UrlKey::of(UriBuf::new().dataset(p, d))
}

fn table(p: &str, d: &str, t: &str) -> UrlKey {
    UrlKey::of(UriBuf::new().table(p, d, t))
}

fn root() -> UrlKey {
    UrlKey::of(state::ROOT)
}

#[test]
fn a_save_from_a_stale_handle_keeps_the_other_handles_dataset() {
    let d = dir("stale-save");
    let a = Store::open_or_create(&d, Config::default()).unwrap();
    let b = Store::open(&d).unwrap();
    state::save(&a, &report("proj.mart.orders_t"), 1).unwrap();
    // `b` opened before that commit; its save unions the root's datasets.
    state::save(&b, &report("proj.sales.leads_t"), 2).unwrap();

    let fresh = Store::open_read_only(&d).unwrap();
    let snap = fresh.snapshot();
    let mut want = vec![dataset("proj", "mart"), dataset("proj", "sales")];
    want.sort_unstable();
    assert_eq!(contains(&snap, root()), want);
    assert_eq!(contains(&snap, dataset("proj", "mart")), vec![table("proj", "mart", "orders_t")]);
    assert_eq!(state::load(&fresh).len(), 2);
}

#[test]
fn a_save_from_a_stale_handle_keeps_the_other_handles_table_in_a_shared_dataset() {
    let d = dir("stale-table");
    let a = Store::open_or_create(&d, Config::default()).unwrap();
    let b = Store::open(&d).unwrap();
    state::save(&a, &report("proj.mart.orders_t"), 1).unwrap();
    state::save(&b, &report("proj.mart.items_t"), 2).unwrap();

    let fresh = Store::open_read_only(&d).unwrap();
    let mut want = vec![table("proj", "mart", "orders_t"), table("proj", "mart", "items_t")];
    want.sort_unstable();
    assert_eq!(contains(&fresh.snapshot(), dataset("proj", "mart")), want);
}

#[test]
fn an_ingest_from_a_stale_handle_keeps_a_described_table() {
    let d = dir("stale-ingest");
    let a = Store::open_or_create(&d, Config::default()).unwrap();
    let b = Store::open(&d).unwrap();
    state::save(&a, &report("proj.mart.extra_t"), 1).unwrap();
    // The export declares `proj.mart` too; its containment set must keep
    // the table `a` described.
    let done = ingest(&b, NODES, EDGES, 2).unwrap();
    assert!(done.committed);

    let fresh = Store::open_read_only(&d).unwrap();
    let snap = fresh.snapshot();
    assert!(contains(&snap, dataset("proj", "mart")).contains(&table("proj", "mart", "extra_t")));
    assert!(contains(&snap, root()).contains(&dataset("proj", "mart")));
    assert_eq!(state::load(&fresh).len(), 1);
}

#[test]
fn eight_handles_saving_at_once_lose_nothing() {
    const N: usize = 8;
    let d = dir("eight");
    Store::open_or_create(&d, Config::default()).unwrap();
    let go = Arc::new(Barrier::new(N));
    let workers: Vec<_> = (0..N)
        .map(|i| {
            let (d, go) = (d.clone(), Arc::clone(&go));
            std::thread::spawn(move || {
                let store = Store::open_or_create(&d, Config::default()).unwrap();
                go.wait();
                state::save(&store, &report(&format!("proj.d{}.t{i}", i % 3)), i as u64).unwrap();
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }

    let fresh = Store::open_read_only(&d).unwrap();
    let snap = fresh.snapshot();
    assert_eq!(contains(&snap, root()).len(), 3);
    let tables: usize =
        (0..3).map(|k| contains(&snap, dataset("proj", &format!("d{k}"))).len()).sum();
    assert_eq!(tables, N);
    assert_eq!(state::load(&fresh).len(), N);
}

#[test]
fn handles_racing_to_create_one_store_all_land() {
    const N: usize = 8;
    let d = dir("create");
    let go = Arc::new(Barrier::new(N));
    let workers: Vec<_> = (0..N)
        .map(|i| {
            let (d, go) = (d.clone(), Arc::clone(&go));
            std::thread::spawn(move || {
                go.wait();
                let store = Store::open_or_create(&d, Config::default()).unwrap();
                state::save(&store, &report(&format!("proj.mart.t{i}")), i as u64).unwrap();
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    assert_eq!(state::load(&Store::open_read_only(&d).unwrap()).len(), N);
}
