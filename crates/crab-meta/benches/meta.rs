// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Metadata image at plan scale: 10k tables × 50 columns (500k columns) in
// ten lineage layers, every column copied from the layer below and joined
// to a second source. Budgets (plan E2): open ≤ 1 ms, column lookup
// ≤ 200 ns, a 10-hop walk ≤ 50 µs.

use crab_meta::{Bytes, ColumnEntry, MetaBuilder, MetaImage, Provenance, TableEntry, Walk};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

const TABLES: usize = 10_000;
const COLS: usize = 50;
const LAYERS: usize = 10;

fn name(t: usize) -> String {
    format!("proj-{}.layer_{}.table_{t:05}", t % 7, t % LAYERS)
}

fn build() -> Vec<u8> {
    let mut b = MetaBuilder::new();
    for t in 0..TABLES {
        let cols = (0..COLS)
            .map(|c| ColumnEntry {
                path: format!("column_{c:02}"),
                data_type: "STRING".into(),
                description: (t % LAYERS == 0).then(|| format!("Source column {c} of table {t}.")),
                provenance: Provenance::Declared,
                ..ColumnEntry::default()
            })
            .collect();
        b.table(
            TableEntry {
                name: name(t),
                kind: "TABLE".into(),
                statement_type: Some("MERGE".into()),
                producer_sql: Some(format!("MERGE `{}` USING ...", name(t))),
                rows: t as u64,
                ..TableEntry::default()
            },
            cols,
        );
    }
    // Table t (layer L > 0) copies table t-1 (layer L-1) and reads t-2.
    for t in (0..TABLES).filter(|t| t % LAYERS != 0) {
        for c in 0..COLS {
            let col = format!("column_{c:02}");
            b.edge((&name(t), &col), (&name(t - 1), &col), true);
            if t >= 2 {
                b.edge(
                    (&name(t), &col),
                    (&name(t - 2), &format!("column_{:02}", (c + 1) % COLS)),
                    false,
                );
            }
        }
    }
    b.build().unwrap()
}

fn bench(c: &mut Criterion) {
    let t0 = std::time::Instant::now();
    let bytes = Bytes::from(build());
    eprintln!("built {} MB in {:?}", bytes.len() >> 20, t0.elapsed());
    let img = MetaImage::from_bytes(bytes.clone()).unwrap();
    assert_eq!(img.column_count(), TABLES * COLS);
    let deepest = name(TABLES - 1);

    c.bench_function("open", |b| {
        b.iter(|| MetaImage::from_bytes(black_box(bytes.clone())).unwrap())
    });
    c.bench_function("table lookup", |b| b.iter(|| img.table(black_box(&deepest)).unwrap().rows()));
    c.bench_function("column lookup", |b| {
        b.iter(|| img.column(black_box(&deepest), black_box("COLUMN_25")).unwrap().id())
    });
    let start = img.column(&deepest, "column_25").unwrap().id();
    let mut w = Walk::new();
    c.bench_function("upstream walk 10 hops", |b| {
        b.iter(|| {
            let mut n = 0u32;
            img.upstream(black_box(start), 10, &mut w, |_| {
                n += 1;
                true
            });
            n
        })
    });
    c.bench_function("nearest described", |b| {
        b.iter(|| img.nearest_described(black_box(start), 10, &mut w).len())
    });
    let root = img.column(&name(TABLES - LAYERS), "column_00").unwrap().id();
    c.bench_function("downstream walk 10 hops", |b| {
        b.iter(|| {
            let mut n = 0u32;
            img.downstream(black_box(root), 10, &mut w, |_| {
                n += 1;
                true
            });
            n
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
