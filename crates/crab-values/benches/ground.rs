// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Value index at deployment scale: 2 000 profiled columns × 200 values
// (400 k values). Build once; grounding and literal checks per question.

use crab_catalog::{Catalog, CatalogBuilder, ColumnSpec, ProfileSpec, TableKind, TableSpec};
use crab_values::ValueIndex;
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

const WORDS: &[&str] = &[
    "north", "south", "east", "west", "alpha", "beta", "gamma", "delta", "river", "stone", "field",
    "harbor", "maple", "cedar", "summit", "valley",
];

fn catalog() -> Catalog {
    let mut b = CatalogBuilder::new("p");
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    for t in 0..100 {
        let cols: Vec<ColumnSpec> = (0..20)
            .map(|c| ColumnSpec {
                path: format!("c{c}"),
                data_type: "STRING".into(),
                ordinal: c + 1,
                nullable: true,
                partitioning: false,
                hidden: false,
                clustering: None,
                description: None,
            })
            .collect();
        b.add_table(TableSpec {
            dataset: "d".into(),
            name: format!("t{t}"),
            kind: TableKind::Table,
            description: None,
            row_count: 1,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns: cols,
        });
        for c in 0..20 {
            let values: Vec<(String, u64)> = (0..200)
                .map(|i| {
                    let r = next();
                    (
                        format!(
                            "{} {} {i}",
                            WORDS[(r % 16) as usize],
                            WORDS[((r >> 8) % 16) as usize]
                        ),
                        1000 - i,
                    )
                })
                .collect();
            b.set_profile(
                "d",
                &format!("t{t}"),
                &format!("c{c}"),
                ProfileSpec {
                    rows: 1000,
                    nulls: 0,
                    distinct: 200,
                    min: None,
                    max: None,
                    values,
                    values_complete: c % 2 == 0,
                    sampled: false,
                },
            );
        }
    }
    Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
}

fn bench(c: &mut Criterion) {
    let cat = catalog();
    c.bench_function("values/build_400k", |b| b.iter(|| black_box(ValueIndex::build(&cat).len())));
    let idx = ValueIndex::build(&cat);
    let q = "What was the total revenue for customers in North Harbor 17 and the Cedar Valey region last quarter?";
    c.bench_function("values/ground_question", |b| {
        b.iter(|| black_box(idx.ground(black_box(q), 8)))
    });
    let col = cat.find_column(cat.find_table("d", "t7").unwrap(), "c4").unwrap();
    c.bench_function("values/check_literal", |b| {
        b.iter(|| black_box(idx.check(col, black_box("north harbor 17"))))
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
