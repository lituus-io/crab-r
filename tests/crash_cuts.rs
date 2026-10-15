// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// A save or an ingest cut short. graph-r appends a transaction's ops as
// consecutive log frames and replays a log up to its first torn frame, so a
// writer killed mid-append (or a power cut before the fsync lands) leaves a
// prefix of the transaction. Cut at every frame boundary and inside every
// frame, the store must read without error, must never present a table as a
// reusable record unless that record is complete, and must come out
// identical to an uninterrupted run once the same work is repeated.

#![cfg(feature = "cli")]

use crab_r::describe::describe::{Action, ColumnReport, Report, TableReport};
use crab_r::describe::ingest::ingest;
use crab_r::describe::state;
use crab_r::lineage::Column;
use graph_r::format::wal::{FRAME_OVERHEAD, HEADER_LEN};
use graph_r::store::WAL_FILE;
use graph_r::{Config, Store};
use std::path::{Path, PathBuf};

const NODES: &str = include_str!("data/static_lineage/nodes.json");
const EDGES: &str = include_str!("data/static_lineage/edges.json");

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("crab-cuts-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
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

fn table(name: &str, key: u64, description: &str, columns: Vec<ColumnReport>) -> TableReport {
    TableReport {
        table: name.into(),
        kind: "TABLE".into(),
        rows: 10,
        bytes: 100,
        last_modified_ms: 0,
        producer: None,
        existing: None,
        description: Some(description.into()),
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
        prekey: key,
        key: Some(key),
        reused: false,
        carried_checks: None,
    }
}

/// The first describe: one table.
fn first() -> Report {
    Report {
        tables: vec![table(
            "proj.mart.orders_t",
            9,
            "Orders, doubled.",
            vec![
                column("id", &["proj.mart.orders.id"], "Order id."),
                column("doubled", &[], "Twice the amount."),
            ],
        )],
        ..Report::default()
    }
}

/// The second describe: that table re-described (new key, new and changed
/// columns) and a table in a new dataset.
fn second() -> Report {
    Report {
        tables: vec![
            table(
                "proj.mart.orders_t",
                10,
                "Orders with their doubled amount.",
                vec![
                    column("id", &["proj.mart.orders.id"], "Order identifier."),
                    column("doubled", &["proj.mart.orders.amount"], "Amount times two."),
                    column("tripled", &["proj.mart.orders.amount"], "Amount times three."),
                ],
            ),
            table(
                "proj.sales.leads_t",
                11,
                "Leads.",
                vec![
                    column("lead_id", &[], "Lead identifier."),
                    column("score", &[], "Lead score."),
                ],
            ),
        ],
        ..Report::default()
    }
}

/// Everything a reader can observe, freshness counters aside (a repeated
/// touch only counts one more check): every live node with its labels,
/// attributes and outbound edges, sorted.
fn dump(store: &Store) -> Vec<String> {
    let snap = store.snapshot();
    let mut out = Vec::new();
    snap.for_each_live(|n| {
        let mut attrs = Vec::new();
        snap.attrs(n.key, |k, v| attrs.push(format!("{k}={v}")));
        attrs.sort();
        let mut edges = Vec::new();
        let mut it = snap.neighbors(n.key);
        while let Some(e) = graph_r::LendingIterator::next(&mut it) {
            edges.push(format!("{:?}>{:?}/{}", e.etype, e.dst_key, e.flags));
        }
        edges.sort();
        out.push(format!(
            "{} t={:?} s={:?} h={} f={} a=[{}] e=[{}]",
            n.url,
            n.title,
            n.snippet,
            n.content_hash,
            n.flags,
            attrs.join(","),
            edges.join(",")
        ));
    });
    out.sort();
    out
}

/// Byte offsets of every frame boundary in `wal` at or after `from`.
fn boundaries(wal: &[u8], from: usize) -> Vec<usize> {
    let mut pos = HEADER_LEN;
    let mut out = Vec::new();
    while pos + FRAME_OVERHEAD <= wal.len() {
        if pos >= from {
            out.push(pos);
        }
        let len = u32::from_le_bytes(wal[pos..pos + 4].try_into().unwrap()) as usize;
        pos += FRAME_OVERHEAD + len;
    }
    assert_eq!(pos, wal.len(), "the log ends on a frame boundary");
    out.push(pos);
    out
}

/// A copy of the store in `src` whose log is cut to `len` bytes.
fn cut(src: &Path, dst: &Path, len: usize) {
    let _ = std::fs::remove_dir_all(dst);
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_file() {
            std::fs::copy(e.path(), dst.join(e.file_name())).unwrap();
        }
    }
    let wal = std::fs::OpenOptions::new().write(true).open(dst.join(WAL_FILE)).unwrap();
    wal.set_len(len as u64).unwrap();
}

/// Every cut point of the log `after` added to `before` bytes: each frame
/// boundary, and a byte into each frame (a torn frame).
fn cuts(store: &Path, before: usize) -> Vec<usize> {
    let wal = std::fs::read(store.join(WAL_FILE)).unwrap();
    let b = boundaries(&wal, before);
    assert!(b.len() > 3, "the transaction spans several frames: {b:?}");
    let mut out = b.clone();
    out.extend(b.iter().take(b.len() - 1).map(|&p| p + FRAME_OVERHEAD + 1));
    out.sort_unstable();
    out
}

fn wal_len(store: &Path) -> usize {
    std::fs::metadata(store.join(WAL_FILE)).unwrap().len() as usize
}

#[test]
fn a_save_cut_anywhere_never_offers_a_partial_record_and_heals() {
    let base = dir("save");
    let store = Store::create(&base, Config::default()).unwrap();
    ingest(&store, NODES, EDGES, 1).unwrap();
    state::save(&store, &first(), 2).unwrap();
    let before = state::load(&store);
    let mark = wal_len(&base);
    state::save(&store, &second(), 3).unwrap();
    let after = state::load(&store);
    let whole = dump(&store);
    drop(store);
    assert_ne!(before, after);

    let work = base.with_extension("cut");
    for at in cuts(&base, mark) {
        cut(&base, &work, at);
        // A reader sees a consistent prefix: every recorded table is the
        // complete record of one of the two runs, never a mixture.
        let prior = state::load(&Store::open_read_only(&work).unwrap());
        for t in &second().tables {
            let got = prior.get(&t.table);
            assert!(
                got.is_none() || got == before.get(&t.table) || got == after.get(&t.table),
                "cut at {at}: {} is offered for reuse as a partial record: {got:?}",
                t.table
            );
        }
        // The next run repeats the work and lands exactly where an
        // uninterrupted run did.
        let store = Store::open(&work).unwrap();
        state::save(&store, &second(), 3).unwrap();
        assert_eq!(state::load(&store), after, "cut at {at}: the repeat did not heal");
        assert_eq!(dump(&store), whole, "cut at {at}: the healed store differs");
    }
}

#[test]
fn an_ingest_cut_anywhere_reads_cleanly_and_heals() {
    let base = dir("ingest");
    let store = Store::create(&base, Config::default()).unwrap();
    let mark = wal_len(&base);
    assert!(ingest(&store, NODES, EDGES, 1).unwrap().committed);
    let whole = dump(&store);
    drop(store);

    let work = base.with_extension("cut");
    for at in cuts(&base, mark) {
        cut(&base, &work, at);
        let _ = dump(&Store::open_read_only(&work).unwrap());
        let store = Store::open(&work).unwrap();
        ingest(&store, NODES, EDGES, 1).unwrap();
        assert_eq!(dump(&store), whole, "cut at {at}: the repeat ingest did not heal");
        assert!(!ingest(&store, NODES, EDGES, 1).unwrap().committed, "cut at {at}: not settled");
    }
}
