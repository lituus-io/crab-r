// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Linking at deployment scale: 10 000 tables × 50 columns (500 k columns)
// in 200 datasets. Index build once; per-question link must stay far below
// the engine's per-question overhead budget.

use crab_r::catalog::{Catalog, CatalogBuilder, ColumnSpec, TableKind, TableSpec};
use crab_r::link::{LinkOpts, SchemaIndex};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

const WORDS: &[&str] = &[
    "order", "customer", "product", "invoice", "payment", "shipment", "account", "event",
    "session", "campaign", "store", "region", "supplier", "employee", "ticket", "device",
];

fn catalog() -> Catalog {
    let mut b = CatalogBuilder::new("p");
    for t in 0..10_000usize {
        let name = format!("{}_{}_{t}", WORDS[t % 16], WORDS[(t / 16) % 16]);
        let columns = (0..50usize)
            .map(|c| ColumnSpec {
                path: if c == 0 {
                    "id".into()
                } else if c < 4 {
                    format!("{}_id", WORDS[(t + c) % 16])
                } else {
                    format!("{}_{}_{c}", WORDS[(t * 7 + c) % 16], WORDS[c % 16])
                },
                data_type: "STRING".into(),
                ordinal: c as u32 + 1,
                nullable: true,
                partitioning: false,
                hidden: false,
                clustering: None,
                description: None,
            })
            .collect();
        b.add_table(TableSpec {
            dataset: format!("ds{}", t % 200),
            name,
            kind: TableKind::Table,
            description: Some(format!("{} data", WORDS[t % 16])),
            row_count: 1,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns,
        });
    }
    Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
}

fn bench(c: &mut Criterion) {
    let cat = catalog();
    let mut g = c.benchmark_group("link");
    g.sample_size(10);
    g.bench_function("build_10k_tables", |b| b.iter(|| black_box(SchemaIndex::build(&cat).len())));
    g.finish();
    let idx = SchemaIndex::build(&cat);
    c.bench_function("link/question", |b| {
        b.iter(|| {
            black_box(idx.link(
                black_box("total payment amount per customer region for invoices last quarter"),
                &[],
                LinkOpts::default(),
            ))
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
