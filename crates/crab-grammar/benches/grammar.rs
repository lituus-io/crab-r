// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Grammar build and emission for a context of 60 tables × 40 columns (the
// size of a full schema-token budget): built once per engine / session.

use crab_grammar::{Grammar, Table};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

fn bench(c: &mut Criterion) {
    let names: Vec<(String, String, Vec<String>)> = (0..60)
        .map(|t| {
            (format!("p.d.t{t}"), format!("t{t}"), (0..40).map(|c| format!("col_{c}")).collect())
        })
        .collect();
    let tables: Vec<Table<'_>> = names
        .iter()
        .map(|(f, a, cs)| Table {
            fqn: f,
            alias: a,
            columns: cs.iter().map(String::as_str).collect(),
        })
        .collect();
    c.bench_function("grammar/build_2400_cols", |b| {
        b.iter(|| black_box(Grammar::sql(black_box(&tables), crab_ir::funcs::NAMES).rules()))
    });
    let g = Grammar::sql(&tables, crab_ir::funcs::NAMES);
    c.bench_function("grammar/lark", |b| b.iter(|| black_box(g.lark().len())));
    c.bench_function("grammar/gbnf", |b| b.iter(|| black_box(g.gbnf().len())));
}

criterion_group!(benches, bench);
criterion_main!(benches);
