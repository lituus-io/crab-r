// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Catalog at enterprise scale: 10 000 tables × 50 columns (500 000 entities,
// 20 datasets). Reports image size, build and open (full validation) time,
// and lookup latency by name and by key.

use crab_r::base::key::{column_key, table_key};
use crab_r::catalog::{Catalog, CatalogBuilder, ColumnSpec, TableKind, TableSpec};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

const TYPES: [&str; 6] = ["INT64", "STRING", "TIMESTAMP", "NUMERIC", "BOOL", "DATE"];

fn build(tables: usize, cols: usize) -> Vec<u8> {
    let mut b = CatalogBuilder::new("my-project");
    for t in 0..tables {
        b.add_table(TableSpec {
            dataset: format!("ds_{:02}", t % 20),
            name: format!("table_{t:05}"),
            kind: TableKind::Table,
            description: Some(format!("Table {t} holding business facts about entity {}", t % 97)),
            row_count: t as u64 * 1000,
            logical_bytes: (t as u64) << 20,
            last_modified_ms: 1_790_000_000_000,
            require_partition_filter: t % 3 == 0,
            columns: (0..cols)
                .map(|c| ColumnSpec {
                    path: format!(
                        "column_{c:02}_{}",
                        ["id", "name", "amount", "created_at", "status"][c % 5]
                    ),
                    data_type: TYPES[c % TYPES.len()].into(),
                    ordinal: c as u32 + 1,
                    nullable: c != 0,
                    partitioning: c == 3,
                    hidden: false,
                    clustering: (c == 1).then_some(1),
                    description: (c % 4 == 0).then(|| format!("Description of column {c}")),
                })
                .collect(),
        });
    }
    b.build(1, 0).unwrap()
}

fn bench(c: &mut Criterion) {
    let img = build(10_000, 50);
    let cat = Catalog::from_bytes(objstore_r::Bytes::from(img.clone())).unwrap();
    let (d, t, cols) = cat.counts();
    eprintln!(
        "catalog: {d} datasets, {t} tables, {cols} columns, image {:.1} MiB ({:.1} B/column)",
        img.len() as f64 / (1 << 20) as f64,
        img.len() as f64 / f64::from(cols)
    );

    let mut g = c.benchmark_group("catalog_10k_tables");
    g.sample_size(10);
    g.bench_function("build", |b| b.iter(|| black_box(build(10_000, 50).len())));
    g.bench_function("open_validate", |b| {
        b.iter(|| {
            black_box(Catalog::from_bytes(objstore_r::Bytes::from(img.clone())).unwrap().counts())
        })
    });
    g.finish();

    let mut g = c.benchmark_group("catalog_lookup");
    let names: Vec<(String, String)> = (0..1000)
        .map(|i| {
            (
                format!("ds_{:02}", (i * 7) % 20),
                format!("table_{:05}", ((i * 7) % 20) + 20 * (i % 400)),
            )
        })
        .collect();
    g.bench_function("find_table", |b| {
        let mut i = 0;
        b.iter(|| {
            let (ds, tb) = &names[i % names.len()];
            i += 1;
            black_box(cat.find_table(ds, tb))
        });
    });
    let tid = cat.find_table("ds_07", "table_00007").unwrap();
    g.bench_function("find_column_ci", |b| {
        b.iter(|| black_box(cat.find_column(tid, black_box("COLUMN_42_amount"))))
    });
    let tk = table_key("my-project", "ds_07", "table_00007");
    g.bench_function("table_by_key", |b| b.iter(|| black_box(cat.table_by_key(black_box(tk)))));
    let ck = column_key("my-project", "ds_07", "table_00007", "column_42_amount");
    g.bench_function("column_by_key", |b| b.iter(|| black_box(cat.column_by_key(black_box(ck)))));
    g.bench_function("scan_table_columns", |b| {
        b.iter(|| black_box(cat.columns(tid).map(|c| c.path.len()).sum::<usize>()))
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
