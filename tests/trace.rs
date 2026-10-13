// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Producer SQL traced onto target columns: CTAS through CTEs, INSERT with
// and without a column list, a view's column list, MERGE from a subquery
// (through its assignments) and from a table.

use crab_r::catalog::{Catalog, CatalogBuilder, ColumnSpec, TableKind, TableSpec};
use crab_r::describe::trace::{trace, TableTrace};
use crab_r::ir::Transform;
use crab_r::sql::ast::StmtKind;

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

fn t(ds: &str, name: &str, cols: Vec<ColumnSpec>) -> TableSpec {
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

fn cat() -> Catalog {
    let mut b = CatalogBuilder::new("p");
    b.add_table(t(
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
    b.add_table(t("raw", "crm", vec![col("id", "INT64", 1), col("email", "STRING", 2)]));
    b.add_table(t(
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

fn cols(t: &TableTrace) -> Vec<(String, Transform, Vec<String>)> {
    t.columns
        .iter()
        .map(|c| {
            (c.column.clone(), c.transform, c.sources.iter().map(ToString::to_string).collect())
        })
        .collect()
}

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| (*s).to_owned()).collect()
}

#[test]
fn ctas_through_ctes_with_filters_and_grain() {
    let sql = "CREATE OR REPLACE TABLE `p.mart.customer_month` AS
        WITH o AS (SELECT customer_id, amount, DATE_TRUNC(DATE(order_ts), MONTH) AS month FROM raw.orders WHERE status != 'TEST')
        SELECT o.customer_id AS cust_id, month, SUM(amount) AS revenue FROM o GROUP BY 1, 2";
    let tr = trace(sql, "p.mart.customer_month", &names(&["cust_id", "month", "revenue"]), &cat())
        .unwrap();
    assert_eq!(tr.statement, StmtKind::CreateTable);
    assert_eq!(
        cols(&tr),
        vec![
            ("cust_id".into(), Transform::Rename, names(&["p.raw.orders.customer_id"])),
            ("month".into(), Transform::Derived, names(&["p.raw.orders.order_ts"])),
            ("revenue".into(), Transform::Aggregate, names(&["p.raw.orders.amount"])),
        ]
    );
    assert_eq!(tr.filters, vec!["GROUP BY 1, 2"]);
    let month = &tr.columns[1];
    assert!(
        month.hops.iter().any(|h| h.starts_with(
            "`o`: month := DATE_TRUNC(DATE(order_ts), MONTH) | WHERE status != 'TEST'"
        )),
        "{:?}",
        month.hops
    );
    assert!(tr.warnings.is_empty(), "{:?}", tr.warnings);
}

#[test]
fn insert_column_lists_and_positional_inserts() {
    let c = cat();
    let tr = trace(
        "INSERT INTO p.mart.dim_customer (email, customer_id) SELECT LOWER(email), id FROM raw.crm",
        "p.mart.dim_customer",
        &names(&["customer_id", "email", "updated_at"]),
        &c,
    )
    .unwrap();
    assert_eq!(
        cols(&tr),
        vec![
            ("email".into(), Transform::Derived, names(&["p.raw.crm.email"])),
            ("customer_id".into(), Transform::Rename, names(&["p.raw.crm.id"]))
        ]
    );
    let tr = trace(
        "INSERT p.mart.dim_customer SELECT id, email, CURRENT_TIMESTAMP() FROM raw.crm",
        "p.mart.dim_customer",
        &names(&["customer_id", "email", "updated_at"]),
        &c,
    )
    .unwrap();
    assert_eq!(tr.columns[2].transform, Transform::Constant);
    assert_eq!(tr.columns[0].column, "customer_id");
    let tr = trace(
        "CREATE VIEW p.mart.v (cust, mail) AS SELECT id, email FROM raw.crm",
        "p.mart.v",
        &names(&["cust", "mail"]),
        &c,
    )
    .unwrap();
    assert_eq!(cols(&tr)[1], ("mail".into(), Transform::Rename, names(&["p.raw.crm.email"])));
}

#[test]
fn merge_maps_columns_through_its_assignments() {
    let c = cat();
    let sql = "MERGE p.mart.dim_customer t
        USING (SELECT id AS customer_id, LOWER(email) AS email_norm FROM raw.crm) s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET email = s.email_norm, updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (customer_id, email, updated_at) VALUES (s.customer_id, s.email_norm, CURRENT_TIMESTAMP())";
    let tr = trace(sql, "p.mart.dim_customer", &names(&["customer_id", "email", "updated_at"]), &c)
        .unwrap();
    let got = cols(&tr);
    assert!(
        got.contains(&("email".into(), Transform::Derived, names(&["p.raw.crm.email"]))),
        "{got:?}"
    );
    assert!(got.contains(&("updated_at".into(), Transform::Constant, vec![])), "{got:?}");
    assert!(
        got.contains(&("customer_id".into(), Transform::Rename, names(&["p.raw.crm.id"]))),
        "{got:?}"
    );
    let email = tr.columns.iter().find(|c| c.column == "email").unwrap();
    assert_eq!(email.expression.as_deref(), Some("s.email_norm"));
    assert!(email.hops.iter().any(|h| h.contains("LOWER(email)")), "{:?}", email.hops);
    // USING a table: assignments read that table's columns.
    let tr = trace(
        "MERGE p.mart.dim_customer t USING raw.crm AS s ON t.customer_id = s.id WHEN NOT MATCHED THEN INSERT (customer_id, email) VALUES (s.id, s.email)",
        "p.mart.dim_customer",
        &names(&["customer_id", "email", "updated_at"]),
        &c,
    )
    .unwrap();
    assert_eq!(
        cols(&tr),
        vec![
            ("customer_id".into(), Transform::Rename, names(&["p.raw.crm.id"])),
            ("email".into(), Transform::Passthrough, names(&["p.raw.crm.email"]))
        ]
    );
}

#[test]
fn unparsable_or_row_less_producers_are_reported() {
    let c = cat();
    assert!(
        trace("SELEC nope", "p.mart.x", &[], &c).is_err(),
        "unknown statements are skipped, leaving nothing that writes rows"
    );
    assert!(trace("SELECT FROM WHERE", "p.mart.x", &[], &c)
        .unwrap_err()
        .contains("does not parse"));
    assert!(trace("INSERT p.mart.x (a) VALUES (1)", "p.mart.x", &names(&["a"]), &c).is_err());
    assert!(trace("DECLARE x INT64;", "p.mart.x", &[], &c).is_err());
}

#[test]
fn the_jobs_query_is_valid_googlesql() {
    let cfg = crab_r::describe::producers::JobsConfig {
        job_projects: vec![],
        region: "us".into(),
        lookback_days: 30,
        max_bytes_billed: 1 << 30,
    };
    let q =
        crab_r::describe::producers::jobs_query("my-project", &cfg, &["my-project.mart".into()])
            .unwrap();
    let mut p = crab_r::sql::Parser::new();
    p.parse(&q, crab_r::sql::Mode::Full).unwrap_or_else(|e| panic!("{e:?}\n{q}"));
}

#[test]
fn scripts_are_traced_through_their_temporary_tables() {
    let cat = cat();
    let target = names(&["customer_id", "email", "updated_at"]);
    // dbt incremental on BigQuery: stage the new rows, then merge them.
    let dbt = "\
declare dbt_partitions_for_replacement array<date>;
create or replace temporary table `dim_customer__dbt_tmp` as (
  with o as (select customer_id, max(order_ts) as last_ts from p.raw.orders group by customer_id)
  select o.customer_id, c.email, o.last_ts as updated_at
  from o join p.raw.crm c on c.id = o.customer_id
);
merge into `p`.`mart`.`dim_customer` as DBT_INTERNAL_DEST
using (select customer_id, email, updated_at from `dim_customer__dbt_tmp`) as DBT_INTERNAL_SOURCE
on DBT_INTERNAL_SOURCE.customer_id = DBT_INTERNAL_DEST.customer_id
when matched then update set email = DBT_INTERNAL_SOURCE.email, updated_at = DBT_INTERNAL_SOURCE.updated_at
when not matched then insert (customer_id, email, updated_at) values (customer_id, email, updated_at);";
    let tr = trace(dbt, "p.mart.dim_customer", &target, &cat).unwrap();
    assert_eq!(tr.staged, names(&["dim_customer__dbt_tmp"]));
    assert!(tr.warnings.is_empty(), "{:?}", tr.warnings);
    let c = cols(&tr);
    let by = |n: &str| c.iter().find(|x| x.0 == n).cloned().unwrap();
    assert_eq!(by("email").2, names(&["p.raw.crm.email"]), "through the temp table to the source");
    assert_eq!(by("updated_at").2, names(&["p.raw.orders.order_ts"]));
    assert_eq!(by("updated_at").1, Transform::Aggregate);
    assert_eq!(by("customer_id").2, names(&["p.raw.orders.customer_id"]));

    // Staged twice (one temp table built from another), then MERGE USING
    // the table directly, and a CTAS from a staged table without an alias.
    let chain = "\
create temp table a as select order_id, customer_id, amount from p.raw.orders where status = 'done';
create temp table b as select customer_id, sum(amount) as total from a group by customer_id;
create or replace table p.mart.totals as select customer_id, total from b;";
    let tr = trace(chain, "p.mart.totals", &names(&["customer_id", "total"]), &cat).unwrap();
    assert_eq!(tr.staged, names(&["b"]));
    let c = cols(&tr);
    assert_eq!(c[1].2, names(&["p.raw.orders.amount"]), "{c:?}");
    assert_eq!(c[1].1, Transform::Aggregate);
    assert_eq!(tr.statement, StmtKind::CreateTable);

    // Nothing staged: traced as before, no rewrite.
    let plain = "create or replace table p.mart.totals as select customer_id, amount as total from p.raw.orders";
    let tr = trace(plain, "p.mart.totals", &names(&["customer_id", "total"]), &cat).unwrap();
    assert!(tr.staged.is_empty());
}

#[test]
fn several_writers_union_their_lineage() {
    let cat = cat();
    let target = names(&["customer_id", "email", "updated_at"]);
    // The daily merge from orders, and a one-off backfill from the CRM.
    let daily = "insert into p.mart.dim_customer (customer_id, updated_at) \
                 select customer_id, max(order_ts) from p.raw.orders group by customer_id";
    let backfill = "insert into p.mart.dim_customer (customer_id, email, updated_at) \
                    select id, email, cast(null as timestamp) from p.raw.crm";
    let mut tr = trace(daily, "p.mart.dim_customer", &target, &cat).unwrap();
    tr.absorb(trace(backfill, "p.mart.dim_customer", &target, &cat).unwrap(), "writer 2 (INSERT)");
    let c = cols(&tr);
    let by = |n: &str| c.iter().find(|x| x.0 == n).cloned().unwrap();
    assert_eq!(by("customer_id").2, names(&["p.raw.orders.customer_id", "p.raw.crm.id"]));
    assert_eq!(
        by("customer_id").1,
        Transform::Derived,
        "passthrough in one, aggregate key in the other"
    );
    assert_eq!(by("email").2, names(&["p.raw.crm.email"]), "set only by the backfill");
    let email = tr.columns.iter().find(|x| x.column == "email").unwrap();
    assert_eq!(email.hops[0], "set only by writer 2 (INSERT)");
    let ua = tr.columns.iter().find(|x| x.column == "updated_at").unwrap();
    assert!(ua.hops.iter().any(|h| h.starts_with("writer 2 (INSERT): ")), "{:?}", ua.hops);
    assert!(tr.filters.iter().any(|f| f.starts_with("GROUP BY")));
}

#[test]
fn updates_are_writers_of_the_columns_they_set() {
    let cat = cat();
    let target = names(&["customer_id", "email", "updated_at"]);
    // Insert, then a fix-up UPDATE joining another table.
    let script = "\
insert into p.mart.dim_customer (customer_id, updated_at)
select customer_id, max(order_ts) from p.raw.orders group by customer_id;
update `p.mart.dim_customer` d set email = lower(c.email)
from p.raw.crm c where c.id = d.customer_id;";
    let tr = trace(script, "p.mart.dim_customer", &target, &cat).unwrap();
    assert_eq!(tr.statement, StmtKind::Insert);
    assert!(tr.warnings.is_empty(), "{:?}", tr.warnings);
    let c = cols(&tr);
    let by = |n: &str| c.iter().find(|x| x.0 == n).cloned().unwrap();
    assert_eq!(by("email").2, names(&["p.raw.crm.email"]), "set by the UPDATE");
    assert_eq!(by("updated_at").2, names(&["p.raw.orders.order_ts"]), "the INSERT's lineage stays");
    let email = tr.columns.iter().find(|x| x.column == "email").unwrap();
    assert_eq!(email.hops[0], "set only by UPDATE");
    assert!(tr.filters.iter().any(|f| f.starts_with("UPDATE: WHERE")), "{:?}", tr.filters);

    // Quoted, project-qualified names on both sides, as the job history
    // records them (found live: the FROM span began inside the backticks).
    let quoted = "UPDATE `p.mart.dim_customer` d\nSET email = c.email\nFROM `p.raw.crm` c\nWHERE c.id = d.customer_id AND d.customer_id > 0;";
    let tr = trace(quoted, "p.mart.dim_customer", &target, &cat).unwrap();
    assert!(tr.warnings.is_empty(), "{:?}", tr.warnings);
    assert_eq!(cols(&tr)[0].2, names(&["p.raw.crm.email"]));

    // An UPDATE on its own (a job of its own in the history).
    let alone =
        "UPDATE p.mart.dim_customer SET updated_at = CURRENT_TIMESTAMP() WHERE email IS NULL";
    let tr = trace(alone, "p.mart.dim_customer", &target, &cat).unwrap();
    assert_eq!(tr.statement, StmtKind::Update);
    assert_eq!(cols(&tr).iter().map(|x| x.0.as_str()).collect::<Vec<_>>(), vec!["updated_at"]);
}

#[test]
fn a_script_writing_several_tables_is_traced_for_the_one_asked() {
    let cat = cat();
    let script = "\
create temp table t as select customer_id, max(order_ts) as last_ts from p.raw.orders group by customer_id;
merge p.mart.dim_customer d using t s on d.customer_id = s.customer_id
when matched then update set updated_at = s.last_ts;
create or replace table p.mart.totals as select customer_id, sum(amount) as total from p.raw.orders group by 1;
update p.mart.totals set total = 0 where total is null;";
    let dim =
        trace(script, "p.mart.dim_customer", &names(&["customer_id", "email", "updated_at"]), &cat)
            .unwrap();
    assert_eq!(dim.statement, StmtKind::Merge);
    assert_eq!(dim.staged, names(&["t"]));
    assert_eq!(
        cols(&dim),
        vec![("updated_at".into(), Transform::Aggregate, names(&["p.raw.orders.order_ts"]))]
    );
    let totals = trace(script, "p.mart.totals", &names(&["customer_id", "total"]), &cat).unwrap();
    assert_eq!(totals.statement, StmtKind::CreateTable);
    let t = totals.columns.iter().find(|c| c.column == "total").unwrap();
    assert!(t.hops.iter().any(|h| h.starts_with("UPDATE: ")), "its own UPDATE only: {:?}", t.hops);
}

#[test]
fn script_variables_bind_as_their_declared_values() {
    let cat = cat();
    let script = "\
DECLARE cutoff, other DATE DEFAULT DATE '2024-01-01';
DECLARE loop_ INT64 DEFAULT loop_;
INSERT INTO p.mart.dim_customer (customer_id, updated_at)
SELECT customer_id, MAX(order_ts) FROM p.raw.orders
WHERE DATE(order_ts) >= cutoff AND customer_id > loop_ GROUP BY customer_id;";
    let tr =
        trace(script, "p.mart.dim_customer", &names(&["customer_id", "email", "updated_at"]), &cat)
            .unwrap();
    // The self-referencing DEFAULT terminates (one substitution pass) and is
    // reported; the real variable binds as its value.
    assert_eq!(tr.warnings, vec!["Unrecognized name: loop_".to_owned()]);
    assert!(tr.filters[0].contains("(DATE '2024-01-01')"), "{:?}", tr.filters);
    assert_eq!(cols(&tr)[1].2, names(&["p.raw.orders.order_ts"]));
}

#[test]
fn conditions_cover_filters_joins_grain_and_merge_matching() {
    let cat = cat();
    let target = names(&["customer_id", "email", "updated_at"]);
    let ctas = "create or replace table p.mart.dim_customer as
        select o.customer_id, c.email, max(o.order_ts) as updated_at
        from p.raw.orders o join p.raw.crm c on c.id = o.customer_id
        where o.status != 'test' group by 1, 2";
    let tr = trace(ctas, "p.mart.dim_customer", &target, &cat).unwrap();
    let got: Vec<String> = tr.conditions.iter().map(ToString::to_string).collect();
    assert_eq!(
        got,
        names(&[
            "p.raw.crm.email",
            "p.raw.crm.id",
            "p.raw.orders.customer_id",
            "p.raw.orders.status"
        ])
    );
    // MERGE: the ON clause matches on the source's customer id.
    let merge = "merge p.mart.dim_customer t
        using (select customer_id, max(order_ts) as ts from p.raw.orders where amount > 0 group by customer_id) s
        on t.customer_id = s.customer_id
        when matched then update set updated_at = s.ts";
    let tr = trace(merge, "p.mart.dim_customer", &target, &cat).unwrap();
    let got: Vec<String> = tr.conditions.iter().map(ToString::to_string).collect();
    assert_eq!(got, names(&["p.raw.orders.amount", "p.raw.orders.customer_id"]));
}
