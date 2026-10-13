// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Per-candidate cost of the semantic pipeline: parse → bind → fingerprint →
// canonical SQL, against a catalog with 2 000 tables (the plan budgets
// parse+bind+canon ≤ 30 µs p50 per candidate).

use crab_r::catalog::{Catalog, CatalogBuilder, ColumnSpec, TableKind, TableSpec};
use crab_r::ir::canon::emit_canonical;
use crab_r::ir::fingerprint::fingerprint;
use crab_r::ir::{BindOpts, Binder};
use crab_r::sql::{Mode, Parser};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;

fn catalog() -> Catalog {
    let col = |p: &str, t: &str, o: u32| ColumnSpec {
        path: p.into(),
        data_type: t.into(),
        ordinal: o,
        nullable: true,
        partitioning: false,
        hidden: false,
        clustering: None,
        description: None,
    };
    let mut b = CatalogBuilder::new("proj");
    let spec = |name: &str, columns: Vec<ColumnSpec>| TableSpec {
        dataset: "shop".into(),
        name: name.into(),
        kind: TableKind::Table,
        description: None,
        row_count: 0,
        logical_bytes: 0,
        last_modified_ms: 0,
        require_partition_filter: false,
        columns,
    };
    b.add_table(spec(
        "orders",
        vec![
            col("order_id", "INT64", 1),
            col("customer_id", "INT64", 2),
            col("status", "STRING", 3),
            col("order_ts", "TIMESTAMP", 4),
            col("is_test", "BOOL", 5),
            col("items", "ARRAY<STRUCT<sku STRING, qty INT64>>", 6),
            col("items.sku", "STRING", 6),
            col("items.qty", "INT64", 6),
        ],
    ));
    b.add_table(spec(
        "customers",
        vec![col("id", "INT64", 1), col("country", "STRING", 2), col("name", "STRING", 3)],
    ));
    b.add_table(spec(
        "line_items",
        vec![col("order_id", "INT64", 1), col("price", "FLOAT64", 2), col("qty", "INT64", 3)],
    ));
    b.add_table(spec("refunds", vec![col("order_id", "INT64", 1), col("amount", "NUMERIC", 2)]));
    for i in 0..2000 {
        b.add_table(spec(
            &format!("filler_{i:04}"),
            (0..40).map(|c| col(&format!("c{c}"), "STRING", c + 1)).collect(),
        ));
    }
    Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
}

const QUERIES: &[&str] = &[
    "SELECT o.status, COUNT(*) AS n FROM orders AS o WHERE o.order_ts >= TIMESTAMP '2026-04-01' AND o.is_test = FALSE GROUP BY o.status ORDER BY n DESC LIMIT 10",
    "WITH first_orders AS (SELECT o.customer_id, MIN(o.order_ts) AS first_ts FROM orders AS o GROUP BY o.customer_id), rep AS (SELECT o.customer_id FROM orders AS o JOIN first_orders AS f ON f.customer_id = o.customer_id WHERE o.order_ts > f.first_ts GROUP BY o.customer_id) SELECT c.country, COUNT(DISTINCT r.customer_id) AS repeat_customers FROM rep AS r JOIN customers AS c ON c.id = r.customer_id GROUP BY c.country ORDER BY repeat_customers DESC",
    "SELECT DATE_TRUNC(DATE(o.order_ts), MONTH) AS m, SUM(li.price * li.qty) - COALESCE(SUM(rf.amount), 0) AS net FROM orders AS o JOIN line_items AS li ON li.order_id = o.order_id LEFT JOIN refunds AS rf ON rf.order_id = o.order_id WHERE o.status IN ('shipped', 'delivered') GROUP BY m QUALIFY ROW_NUMBER() OVER (ORDER BY net DESC) <= 12",
    "SELECT it.sku, SUM(it.qty) AS units FROM orders AS o, UNNEST(o.items) AS it WHERE o.order_ts BETWEEN TIMESTAMP '2026-01-01' AND TIMESTAMP '2026-06-30' GROUP BY it.sku HAVING units > 100",
];

fn bench(c: &mut Criterion) {
    let cat = catalog();
    let opts =
        BindOpts { default_project: "proj", default_dataset: Some("shop"), mode: Mode::Strict };
    let mut parser = Parser::new();
    let mut binder = Binder::new();
    let mut out = String::with_capacity(1024);
    for q in QUERIES {
        let parsed = parser.parse(q, Mode::Strict).unwrap();
        let bound = binder.bind(&parsed, &cat, opts);
        assert!(
            bound.ok(),
            "{q}: {:?}",
            bound.diags.iter().map(|d| d.message(q)).collect::<Vec<_>>()
        );
    }
    let mut g = c.benchmark_group("candidate");
    g.throughput(Throughput::Elements(QUERIES.len() as u64));
    g.bench_function("parse+bind", |b| {
        b.iter(|| {
            for q in QUERIES {
                let parsed = parser.parse(black_box(q), Mode::Strict).unwrap();
                black_box(binder.bind(&parsed, &cat, opts).ok());
            }
        });
    });
    g.bench_function("parse+bind+fingerprint+canonical", |b| {
        b.iter(|| {
            for q in QUERIES {
                let parsed = parser.parse(black_box(q), Mode::Strict).unwrap();
                let bound = binder.bind(&parsed, &cat, opts);
                black_box(fingerprint(&parsed, bound, &cat));
                out.clear();
                emit_canonical(&parsed, bound, &cat, &mut out);
                black_box(out.len());
            }
        });
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
