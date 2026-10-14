// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Tracing producers (plan E1/E4 budget: ≤ 5 ms per producer): a CTAS through
// CTEs, a dbt incremental script (DECLARE, temp table, MERGE), and an
// INSERT plus UPDATE — against a catalog of 2 000 tables, as in a real
// warehouse where the binder must find the few the SQL names.

use crab_r::catalog::{Catalog, CatalogBuilder, ColumnSpec, TableKind, TableSpec};
use crab_r::describe::trace::trace;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn col(path: &str, ty: &str, o: u32) -> ColumnSpec {
    ColumnSpec {
        path: path.into(),
        data_type: ty.into(),
        ordinal: o,
        nullable: true,
        partitioning: false,
        hidden: false,
        clustering: None,
        description: None,
    }
}

fn table(ds: &str, name: &str, cols: Vec<ColumnSpec>) -> TableSpec {
    TableSpec {
        dataset: ds.into(),
        name: name.into(),
        kind: TableKind::Table,
        description: None,
        row_count: 0,
        logical_bytes: 0,
        last_modified_ms: 0,
        require_partition_filter: false,
        columns: cols,
    }
}

fn catalog() -> Catalog {
    let mut b = CatalogBuilder::new("p");
    for i in 0..2_000 {
        b.add_table(table(
            &format!("ds{}", i % 20),
            &format!("t{i}"),
            (0..30).map(|c| col(&format!("c{c}"), "STRING", c + 1)).collect(),
        ));
    }
    b.add_table(table(
        "raw",
        "orders",
        vec![
            col("order_id", "INT64", 1),
            col("customer_id", "INT64", 2),
            col("amount", "NUMERIC", 3),
            col("status", "STRING", 4),
            col("order_ts", "TIMESTAMP", 5),
        ],
    ));
    b.add_table(table("raw", "crm", vec![col("id", "INT64", 1), col("email", "STRING", 2)]));
    b.add_table(table(
        "mart",
        "dim_customer",
        vec![
            col("customer_id", "INT64", 1),
            col("email", "STRING", 2),
            col("updated_at", "TIMESTAMP", 3),
        ],
    ));
    Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
}

const CTAS: &str = "create or replace table p.mart.dim_customer as
with o as (select customer_id, max(order_ts) as last_ts, sum(amount) as total from p.raw.orders where status != 'test' group by customer_id),
c as (select id, lower(email) as email from p.raw.crm)
select o.customer_id, c.email, o.last_ts as updated_at from o join c on c.id = o.customer_id";

const DBT: &str = "declare cutoff date default date '2024-01-01';
create or replace temporary table `dim_customer__dbt_tmp` as (
  with o as (select customer_id, max(order_ts) as last_ts from p.raw.orders where date(order_ts) >= cutoff group by customer_id)
  select o.customer_id, c.email, o.last_ts as updated_at from o join p.raw.crm c on c.id = o.customer_id
);
merge into `p`.`mart`.`dim_customer` as DBT_INTERNAL_DEST
using (select customer_id, email, updated_at from `dim_customer__dbt_tmp`) as DBT_INTERNAL_SOURCE
on DBT_INTERNAL_SOURCE.customer_id = DBT_INTERNAL_DEST.customer_id
when matched then update set email = DBT_INTERNAL_SOURCE.email, updated_at = DBT_INTERNAL_SOURCE.updated_at
when not matched then insert (customer_id, email, updated_at) values (customer_id, email, updated_at);";

const UPDATE: &str = "insert into p.mart.dim_customer (customer_id, updated_at)
select customer_id, max(order_ts) from p.raw.orders group by customer_id;
update p.mart.dim_customer d set email = lower(c.email) from p.raw.crm c where c.id = d.customer_id;";

fn bench(c: &mut Criterion) {
    let cat = catalog();
    let cols: Vec<String> = ["customer_id", "email", "updated_at"].map(String::from).to_vec();
    for (name, sql) in
        [("trace ctas", CTAS), ("trace dbt script", DBT), ("trace insert+update", UPDATE)]
    {
        assert!(trace(sql, "p.mart.dim_customer", &cols, &cat).unwrap().warnings.is_empty());
        c.bench_function(name, |b| {
            b.iter(|| {
                trace(black_box(sql), "p.mart.dim_customer", &cols, &cat).unwrap().columns.len()
            })
        });
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
