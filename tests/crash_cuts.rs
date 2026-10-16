// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// A save or an ingest cut short. graph-r appends a transaction as one
// checksummed log record and replays only whole records, so a writer killed
// mid-append (or a power cut before the fsync lands) leaves the transaction
// out entirely. Cut anywhere inside the record, the store must read without
// error, must be exactly the store from before the transaction (no table
// offered for reuse but a complete one), and must come out identical to an
// uninterrupted run once the same work is repeated.

#![cfg(feature = "cli")]

use crab_r::describe::describe::{Action, ColumnReport, Report, TableReport};
use crab_r::describe::ingest::ingest;
use crab_r::describe::state;
use crab_r::lineage::Column;
use graph_r::format::wal::{HEADER_LEN, RECORD_HEADER_LEN};
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

/// Byte offsets of every record boundary in `wal` at or after `from`. A
/// record's first four bytes are its whole length, header included.
fn boundaries(wal: &[u8], from: usize) -> Vec<usize> {
    let mut pos = HEADER_LEN;
    let mut out = Vec::new();
    while pos + RECORD_HEADER_LEN <= wal.len() {
        if pos >= from {
            out.push(pos);
        }
        pos += u32::from_le_bytes(wal[pos..pos + 4].try_into().unwrap()) as usize;
    }
    assert_eq!(pos, wal.len(), "the log ends on a record boundary");
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

/// The transaction appended after `before` bytes is one record: its start and
/// end, and cut points across it — every byte of its header and of its
/// last 64 bytes, and every 61st byte between (an odd stride, so the cuts land
/// at every alignment).
fn cuts(store: &Path, before: usize) -> (usize, Vec<usize>) {
    let wal = std::fs::read(store.join(WAL_FILE)).unwrap();
    let b = boundaries(&wal, before);
    assert_eq!(b.len(), 2, "the transaction is one record: {b:?}");
    let (start, end) = (b[0], b[1]);
    let mut out: Vec<usize> = (start..end)
        .filter(|&p| p < start + RECORD_HEADER_LEN || p + 64 >= end || (p - start) % 61 == 0)
        .collect();
    out.push(end);
    (end, out)
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
    let unsaved = dump(&store);
    let mark = wal_len(&base);
    state::save(&store, &second(), 3).unwrap();
    let after = state::load(&store);
    let whole = dump(&store);
    drop(store);
    assert_ne!(before, after);

    let work = base.with_extension("cut");
    let (end, points) = cuts(&base, mark);
    for at in points {
        cut(&base, &work, at);
        // A reader sees a consistent prefix: every recorded table is the
        // complete record of one of the two runs, never a mixture.
        let reader = Store::open_read_only(&work).unwrap();
        let prior = state::load(&reader);
        if at < end {
            // Cut short, the save is absent: the store is the one from before.
            assert_eq!(prior, before, "cut at {at}: part of the save is visible");
            assert_eq!(dump(&reader), unsaved, "cut at {at}: the store moved");
        }
        drop(reader);
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
    let (end, points) = cuts(&base, mark);
    for at in points {
        cut(&base, &work, at);
        let seen = dump(&Store::open_read_only(&work).unwrap());
        if at < end {
            assert!(seen.is_empty(), "cut at {at}: part of the ingest is visible: {seen:?}");
        }
        let store = Store::open(&work).unwrap();
        ingest(&store, NODES, EDGES, 1).unwrap();
        assert_eq!(dump(&store), whole, "cut at {at}: the repeat ingest did not heal");
        assert!(!ingest(&store, NODES, EDGES, 1).unwrap().committed, "cut at {at}: not settled");
    }
}
