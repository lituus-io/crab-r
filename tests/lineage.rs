// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Column lineage through CTEs, joins, set operations (every branch), FROM
// subqueries, star expansion, casts, aggregates, windows, UNNEST elements,
// nested struct fields and correlated subqueries (credited by binding, not
// by name).

use crab_r::catalog::{Catalog, CatalogBuilder, ColumnSpec, TableKind, TableSpec};
use crab_r::ir::{conditions, lineage, BindOpts, Binder, ColumnLineage, Transform};
use crab_r::sql::{Mode, Parser};

fn col(path: &str, ty: &str, ordinal: u32) -> ColumnSpec {
    ColumnSpec {
        path: path.into(),
        data_type: ty.into(),
        ordinal,
        nullable: true,
        partitioning: false,
        hidden: false,
        clustering: None,
        description: None,
    }
}

fn table(ds: &str, name: &str, columns: Vec<ColumnSpec>) -> TableSpec {
    TableSpec {
        dataset: ds.into(),
        name: name.into(),
        kind: TableKind::Table,
        description: None,
        row_count: 0,
        logical_bytes: 0,
        last_modified_ms: 0,
        require_partition_filter: false,
        columns,
    }
}

fn catalog() -> Catalog {
    let mut b = CatalogBuilder::new("p");
    b.add_table(table(
        "raw",
        "orders",
        vec![
            col("order_id", "INT64", 1),
            col("customer_id", "INT64", 2),
            col("amount", "NUMERIC", 3),
            col("status", "STRING", 4),
            col("currency", "STRING", 5),
            col("order_ts", "TIMESTAMP", 6),
            col("is_test", "BOOL", 7),
            col("items", "ARRAY<STRUCT<sku STRING, qty INT64>>", 8),
            col("items.sku", "STRING", 8),
            col("items.qty", "INT64", 8),
        ],
    ));
    b.add_table(table(
        "raw",
        "fx_rates",
        vec![col("currency", "STRING", 1), col("rate", "FLOAT64", 2)],
    ));
    b.add_table(table("raw", "crm_a", vec![col("id", "INT64", 1), col("email", "STRING", 2)]));
    b.add_table(table("raw", "crm_b", vec![col("cust_no", "INT64", 1), col("email", "STRING", 2)]));
    b.add_table(table(
        "raw",
        "users",
        vec![
            col("id", "INT64", 1),
            col("addr", "STRUCT<city STRING>", 2),
            col("addr.city", "STRING", 2),
        ],
    ));
    Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
}

/// Lineage of the last writing statement's query, with source columns as
/// `table.path` strings.
fn trace(sql: &str) -> Vec<(ColumnLineage, Vec<String>)> {
    let cat = catalog();
    let mut p = Parser::new();
    let script = p.parse_script(sql, Mode::Full).unwrap_or_else(|e| panic!("{e:?}"));
    let stmt = script.last_write().unwrap();
    let parsed = script.query(stmt).unwrap();
    let mut b = Binder::new();
    let bound = b.bind(
        &parsed,
        &cat,
        BindOpts { default_project: "p", default_dataset: None, mode: Mode::Full },
    );
    assert!(bound.ok(), "{:?}", bound.diags.iter().map(|d| d.message(sql)).collect::<Vec<_>>());
    lineage(&parsed, bound, &cat)
        .into_iter()
        .map(|l| {
            let s = l.sources.iter().map(|&c| {
                let c = cat.column(c).unwrap();
                format!("{}.{}", cat.table(c.table).unwrap().name, c.path)
            });
            let names = s.collect();
            (l, names)
        })
        .collect()
}

fn by<'a>(t: &'a [(ColumnLineage, Vec<String>)], name: &str) -> &'a (ColumnLineage, Vec<String>) {
    t.iter().find(|(l, _)| l.name.as_deref() == Some(name)).unwrap_or_else(|| {
        panic!("no output {name}: {:?}", t.iter().map(|x| &x.0.name).collect::<Vec<_>>())
    })
}

#[test]
fn ctas_through_ctes_and_joins() {
    let t = trace(
        "CREATE OR REPLACE TABLE `p.mart.customer_month` AS
         WITH o AS (
           SELECT customer_id, amount, status, currency, DATE_TRUNC(DATE(order_ts), MONTH) AS month
           FROM `p.raw.orders` WHERE is_test = FALSE
         ),
         fx AS (SELECT currency, rate FROM raw.fx_rates)
         SELECT o.customer_id AS cust_id, o.month,
                SUM(IF(o.status = 'REFUND', -o.amount, o.amount) * fx.rate) AS net_revenue_cad
         FROM o LEFT JOIN fx USING (currency)
         GROUP BY 1, 2",
    );
    let (cust, s) = by(&t, "cust_id");
    assert_eq!(s, &["orders.customer_id"]);
    assert_eq!(cust.transform, Transform::Rename);
    assert_eq!(cust.hops[0].relation.as_deref(), Some("o"));
    assert!(cust.hops[0].select.is_some(), "the CTE's SELECT carries its WHERE");
    let (month, s) = by(&t, "month");
    assert_eq!(
        (s.as_slice(), month.transform),
        (&["orders.order_ts".to_owned()][..], Transform::Derived)
    );
    let (rev, s) = by(&t, "net_revenue_cad");
    assert_eq!(s, &["fx_rates.rate", "orders.amount", "orders.status"]);
    assert_eq!(rev.transform, Transform::Aggregate);
    assert!(rev.hops.iter().any(|h| h.relation.as_deref() == Some("fx")));
}

#[test]
fn merge_source_unions_trace_every_branch() {
    let t = trace(
        "MERGE `p.mart.dim_customer` t
         USING (SELECT id AS customer_id, UPPER(email) AS email FROM `p.raw.crm_a`
                UNION ALL SELECT cust_no, email FROM `p.raw.crm_b`) s
         ON t.customer_id = s.customer_id
         WHEN MATCHED THEN UPDATE SET email = s.email",
    );
    assert_eq!(by(&t, "customer_id").1, &["crm_a.id", "crm_b.cust_no"]);
    let (email, s) = by(&t, "email");
    assert_eq!(s, &["crm_a.email", "crm_b.email"]);
    assert_eq!(email.transform, Transform::Derived, "UPPER in one branch");
    assert!(email.hops.iter().any(|h| h.set_branch));
}

#[test]
fn star_casts_constants_windows() {
    let t = trace("SELECT * FROM (SELECT currency, rate * 100 AS pct FROM raw.fx_rates)");
    assert_eq!(by(&t, "currency").0.transform, Transform::Passthrough);
    assert_eq!(by(&t, "pct").0.transform, Transform::Derived);
    let t = trace("SELECT CAST(amount AS STRING) AS amount_s, 1 AS one, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts) AS rn FROM raw.orders");
    assert_eq!(by(&t, "amount_s").0.transform, Transform::Cast);
    assert_eq!(by(&t, "one").0.transform, Transform::Constant);
    let (rn, s) = by(&t, "rn");
    assert_eq!(
        (rn.transform, s.as_slice()),
        (Transform::Window, &["orders.customer_id".to_owned(), "orders.order_ts".to_owned()][..])
    );
    let t = trace("SELECT * FROM raw.fx_rates");
    assert_eq!(by(&t, "rate").1, &["fx_rates.rate"]);
    assert_eq!(by(&t, "rate").0.transform, Transform::Passthrough);
}

#[test]
fn nested_fields_unnest_elements_and_correlated_subqueries() {
    let t = trace("SELECT u.addr.city AS city FROM raw.users AS u");
    assert_eq!(by(&t, "city").1, &["users.addr.city"], "the nested field, not the struct");
    let t =
        trace("SELECT it.sku AS sku, it.qty * 2 AS q2 FROM raw.orders AS o, UNNEST(o.items) AS it");
    assert_eq!(by(&t, "sku").1, &["orders.items.sku"]);
    assert_eq!(by(&t, "q2").1, &["orders.items.qty"]);
    // The inner `o.order_id`-like reference binds to the inner relation;
    // `o.customer_id` from the outer query is credited to the outer table.
    let t = trace(
        "SELECT o.order_id,
                (SELECT MAX(f.rate) FROM raw.fx_rates AS f WHERE f.currency = o.currency) AS max_rate
         FROM raw.orders AS o",
    );
    let (mr, s) = by(&t, "max_rate");
    assert_eq!(s, &["fx_rates.currency", "fx_rates.rate", "orders.currency"]);
    assert!(mr.via_subquery);
    assert_eq!(by(&t, "order_id").0.transform, Transform::Passthrough);
}

#[test]
fn insert_and_view_statements_trace_their_queries() {
    let t = trace("INSERT INTO p.mart.t (a, b) SELECT customer_id, SUM(amount) FROM raw.orders GROUP BY customer_id");
    assert_eq!(t.len(), 2);
    assert_eq!(t[1].1, &["orders.amount"]);
    assert_eq!(t[1].0.transform, Transform::Aggregate);
    let t = trace("CREATE VIEW p.mart.v AS SELECT id AS user_id FROM raw.users");
    assert_eq!(by(&t, "user_id").0.transform, Transform::Rename);
}

#[test]
fn conditions_are_the_columns_deciding_which_rows() {
    let cat = catalog();
    let sql = "CREATE OR REPLACE TABLE `p.mart.customer_month` AS
         WITH o AS (
           SELECT customer_id, amount, currency, DATE_TRUNC(DATE(order_ts), MONTH) AS month
           FROM `p.raw.orders` WHERE is_test = FALSE AND status != 'TEST'
         ),
         fx AS (SELECT currency, rate FROM raw.fx_rates)
         SELECT o.customer_id AS cust_id, o.month, SUM(o.amount * fx.rate) AS rev
         FROM o LEFT JOIN fx ON fx.currency = o.currency
         WHERE o.customer_id IN (SELECT id FROM raw.crm_a WHERE email IS NOT NULL)
         GROUP BY 1, 2
         HAVING SUM(o.amount) > 0";
    let mut p = Parser::new();
    let script = p.parse_script(sql, Mode::Full).unwrap();
    let parsed = script.query(script.last_write().unwrap()).unwrap();
    let mut b = Binder::new();
    let bound = b.bind(
        &parsed,
        &cat,
        BindOpts { default_project: "p", default_dataset: None, mode: Mode::Full },
    );
    let names: Vec<String> = conditions(&parsed, bound, &cat)
        .into_iter()
        .map(|c| {
            let c = cat.column(c).unwrap();
            format!("{}.{}", cat.table(c.table).unwrap().name, c.path)
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    assert_eq!(
        names,
        vec![
            "crm_a.email",        // the IN subquery's filter
            "crm_a.id",           // the IN subquery's values
            "fx_rates.currency",  // join key
            "orders.amount",      // HAVING
            "orders.currency",    // join key, through the CTE
            "orders.customer_id", // WHERE and GROUP BY 1
            "orders.is_test",     // the CTE's WHERE
            "orders.order_ts",    // GROUP BY 2 (month)
            "orders.status",
        ]
    );
}
