// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Result-hashing throughput on a 100 000-row, 6-column result.

use crab_r::base::batch::{Batch, Cell, ColType};
use crab_r::verify::{hash_batch_with, Scratch};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;

fn bench(c: &mut Criterion) {
    let cols = [
        ("id", ColType::Int64),
        ("name", ColType::String),
        ("amount", ColType::Numeric),
        ("ratio", ColType::Float64),
        ("day", ColType::Date),
        ("ok", ColType::Bool),
    ];
    let n = 100_000;
    let mut b = Batch::new(&cols, n);
    let names: Vec<String> = (0..n).map(|i| format!("customer-{i:06}")).collect();
    for (i, name) in names.iter().enumerate() {
        b.columns[0].push(Cell::I64(i as i64)).unwrap();
        b.columns[1].push(Cell::Str(name)).unwrap();
        b.columns[2].push(Cell::I128(i as i128 * 1_234_567)).unwrap();
        b.columns[3].push(Cell::F64(i as f64 / 7.0)).unwrap();
        b.columns[4].push(Cell::I32(20_000 + (i % 365) as i32)).unwrap();
        b.columns[5].push(if i % 11 == 0 { Cell::Null } else { Cell::Bool(i % 2 == 0) }).unwrap();
    }
    let bytes: usize = 8 + 16 + 16 + 8 + 4 + 1;
    let mut g = c.benchmark_group("result_hash");
    g.throughput(Throughput::Bytes((bytes * n) as u64));
    let mut s = Scratch::default();
    g.bench_function("100k_rows_6_cols", |bch| {
        bch.iter(|| black_box(hash_batch_with(black_box(&b), &mut s)))
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
