// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Binder behaviour against a realistic catalog: resolution (aliases, table
// names, CTEs, subqueries, nested fields, UNNEST), GoogleSQL rules
// (grouping, aggregate/window placement, types, partition filters),
// repair-grade diagnostics, and meaning-level fingerprints.

use crab_r::catalog::{Catalog, CatalogBuilder, ColumnSpec, TableKind, TableSpec, TypeCode};
use crab_r::ir::fingerprint::{fingerprint, template_fingerprint};
use crab_r::ir::{BindOpts, Binder, DiagKind};
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

fn table(ds: &str, name: &str, rpf: bool, columns: Vec<ColumnSpec>) -> TableSpec {
    TableSpec {
        dataset: ds.into(),
        name: name.into(),
        kind: TableKind::Table,
        description: None,
        row_count: 0,
        logical_bytes: 0,
        last_modified_ms: 0,
        require_partition_filter: rpf,
        columns,
    }
}

fn catalog() -> Catalog {
    let mut b = CatalogBuilder::new("proj");
    b.add_table(table(
        "shop",
        "orders",
        true,
        vec![
            col("order_id", "INT64", 1),
            col("customer_id", "INT64", 2),
            col("status", "STRING", 3),
            ColumnSpec { partitioning: true, ..col("order_ts", "TIMESTAMP", 4) },
            col("amount", "NUMERIC", 5),
            col("items", "ARRAY<STRUCT<sku STRING, qty INT64>>", 6),
            col("items.sku", "STRING", 6),
            col("items.qty", "INT64", 6),
            col("tags", "ARRAY<STRING>", 7),
        ],
    ));
    b.add_table(table(
        "shop",
        "customers",
        false,
        vec![
            col("id", "INT64", 1),
            col("name", "STRING", 2),
            col("country", "STRING", 3),
            col("addr", "STRUCT<city STRING, zip STRING>", 4),
            col("addr.city", "STRING", 4),
            col("addr.zip", "STRING", 4),
            col("signup_date", "DATE", 5),
        ],
    ));
    b.add_table(table(
        "shop",
        "line_items",
        false,
        vec![
            col("order_id", "INT64", 1),
            col("sku", "STRING", 2),
            col("price", "FLOAT64", 3),
            col("qty", "INT64", 4),
        ],
    ));
    Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
}

const OPTS: BindOpts<'static> =
    BindOpts { default_project: "proj", default_dataset: Some("shop"), mode: Mode::Strict };

/// Bind `sql`; return (diag kinds, rendered messages).
fn check(cat: &Catalog, sql: &str, mode: Mode) -> (Vec<DiagKind>, Vec<String>) {
    let mut p = Parser::new();
    let parsed = p.parse(sql, Mode::Full).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut b = Binder::new();
    let bound = b.bind(&parsed, cat, BindOpts { mode, ..OPTS });
    (
        bound.diags.iter().map(|d| d.kind).collect(),
        bound.diags.iter().map(|d| d.message(sql)).collect(),
    )
}

fn ok(cat: &Catalog, sql: &str) {
    let (kinds, msgs) = check(cat, sql, Mode::Strict);
    assert!(kinds.is_empty(), "{sql}\n{msgs:#?}");
}

fn errs(cat: &Catalog, sql: &str) -> Vec<String> {
    let (kinds, msgs) = check(cat, sql, Mode::Strict);
    assert!(!kinds.is_empty(), "expected errors for {sql}");
    msgs
}

const TS: &str = "o.order_ts >= TIMESTAMP '2026-01-01'";

#[test]
fn valid_queries_bind_cleanly() {
    let c = catalog();
    for sql in [
        format!("SELECT o.status, COUNT(*) AS n FROM orders AS o WHERE {TS} GROUP BY o.status ORDER BY n DESC"),
        format!("SELECT c.country, SUM(o.amount) FROM shop.orders o JOIN customers c ON c.id = o.customer_id WHERE {TS} GROUP BY 1"),
        "SELECT orders.status FROM `proj.shop.orders` AS orders WHERE orders.order_ts > TIMESTAMP '2026-01-01'".to_owned(),
        format!("SELECT it.sku, SUM(it.qty) FROM orders AS o, UNNEST(o.items) AS it WHERE {TS} GROUP BY it.sku"),
        format!("SELECT it.sku FROM orders AS o, o.items AS it WHERE {TS}"),
        format!("SELECT t FROM orders AS o, UNNEST(o.tags) AS t WITH OFFSET AS i WHERE {TS} AND i < 3"),
        "SELECT c.addr.city, c.name FROM customers AS c WHERE c.addr.zip LIKE '9%'".to_owned(),
        "WITH big AS (SELECT customer_id, SUM(amount) AS total FROM orders WHERE order_ts > TIMESTAMP '2026-01-01' GROUP BY customer_id) SELECT c.name, b.total FROM big AS b JOIN customers AS c ON c.id = b.customer_id WHERE b.total > 100".to_owned(),
        "SELECT s.n FROM (SELECT COUNT(*) AS n FROM line_items) AS s".to_owned(),
        "SELECT DATE_TRUNC(c.signup_date, MONTH) AS m, COUNT(*) FROM customers c GROUP BY m HAVING COUNT(*) > 1".to_owned(),
        "SELECT EXTRACT(WEEK(MONDAY) FROM c.signup_date), DATE_DIFF(CURRENT_DATE(), c.signup_date, DAY) FROM customers c".to_owned(),
        "SELECT c.name FROM customers c WHERE c.signup_date >= '2026-01-01' AND c.id IN (SELECT li.order_id FROM line_items li)".to_owned(),
        "SELECT c.name FROM customers c WHERE EXISTS (SELECT 1 FROM line_items li WHERE li.order_id = c.id)".to_owned(),
        "SELECT li.sku, ROW_NUMBER() OVER (PARTITION BY li.order_id ORDER BY li.price DESC) AS rn FROM line_items li QUALIFY rn = 1".to_owned(),
        "SELECT li.sku, li.price * li.qty AS total FROM line_items li QUALIFY ROW_NUMBER() OVER (ORDER BY total DESC) <= 3".to_owned(),
        "SELECT li.sku, SUM(li.price * li.qty) / COUNT(DISTINCT li.order_id) AS aov FROM line_items li GROUP BY li.sku".to_owned(),
        "SELECT * EXCEPT (addr) FROM customers".to_owned(),
        "SELECT sku FROM line_items UNION ALL SELECT name FROM customers ORDER BY sku".to_owned(),
        "SELECT SAFE_DIVIDE(SUM(li.price), NULLIF(COUNT(*), 0)) FROM line_items li".to_owned(),
        "SELECT CASE WHEN li.qty > 10 THEN 'bulk' ELSE 'single' END AS kind, COUNT(*) FROM line_items li GROUP BY kind".to_owned(),
    ] {
        ok(&c, &sql);
    }
}

#[test]
fn output_columns_are_named_and_typed() {
    let c = catalog();
    let mut p = Parser::new();
    let sql = "SELECT c.country, COUNT(*) AS n, AVG(li.qty) FROM customers c, line_items li GROUP BY c.country";
    let parsed = p.parse(sql, Mode::Strict).unwrap();
    let mut b = Binder::new();
    let bound = b.bind(&parsed, &c, OPTS);
    assert!(bound.ok());
    let outs = bound.output_columns();
    assert_eq!(outs.len(), 3);
    let names: Vec<&str> = outs.iter().map(|o| o.name.map_or("", |n| n.sp.text(sql))).collect();
    assert_eq!(names, ["country", "n", ""]);
    assert_eq!(outs[0].ty.code, TypeCode::String);
    assert_eq!(outs[1].ty.code, TypeCode::Int64);
    assert_eq!(outs[2].ty.code, TypeCode::Float64, "AVG(INT64) is FLOAT64");
    assert_eq!(bound.tables.len(), 2);
}

#[test]
fn unknown_names_get_bigquery_style_suggestions() {
    let c = catalog();
    let m = errs(&c, "SELECT c.contry FROM customers c");
    assert_eq!(m, ["Unrecognized name: contry; Did you mean country?"]);
    let m = errs(&c, "SELECT nam FROM customers");
    assert_eq!(m, ["Unrecognized name: nam; Did you mean name?"]);
    let m = errs(&c, "SELECT 1 FROM shop.customer");
    assert_eq!(m, ["Table not found: shop.customer; Did you mean customers?"]);
    let m = errs(&c, "SELECT DATE_TRNC(c.signup_date, MONTH) FROM customers c");
    assert_eq!(m, ["Function not found: DATE_TRNC; Did you mean DATE_TRUNC?"]);
    let m = errs(&c, "SELECT c.addr.cty FROM customers c");
    assert_eq!(m, ["Field name cty does not exist in STRUCT; Did you mean city?"]);
}

#[test]
fn ambiguity_and_scoping() {
    let c = catalog();
    let (k, _) = check(&c, "SELECT order_id FROM line_items a, line_items b", Mode::Strict);
    assert_eq!(k, [DiagKind::AmbiguousName]);
    ok(&c, "SELECT a.order_id FROM line_items a, line_items b");
    // Correlated reference to an outer scope.
    ok(&c, "SELECT (SELECT MAX(li.price) FROM line_items li WHERE li.order_id = c.id) FROM customers c");
    // A FROM alias is not visible outside its query.
    let (k, _) = check(&c, "SELECT li.sku FROM (SELECT * FROM line_items li) AS s", Mode::Strict);
    assert_eq!(k, [DiagKind::UnknownName]);
}

#[test]
fn arrays_require_unnest() {
    let c = catalog();
    let (k, _) = check(&c, &format!("SELECT o.items.sku FROM orders o WHERE {TS}"), Mode::Strict);
    assert_eq!(k, [DiagKind::NeedsUnnest]);
}

#[test]
fn grouping_and_placement_rules() {
    let c = catalog();
    let (k, m) = check(
        &c,
        "SELECT c.country, c.name, COUNT(*) FROM customers c GROUP BY c.country",
        Mode::Strict,
    );
    assert_eq!(k, [DiagKind::NotGrouped], "{m:?}");
    assert!(m[0].contains("c.name"));
    let (k, _) = check(&c, "SELECT c.name, COUNT(*) FROM customers c", Mode::Strict);
    assert_eq!(k, [DiagKind::NotGrouped]);
    let (k, _) = check(&c, "SELECT c.name FROM customers c WHERE COUNT(*) > 1", Mode::Strict);
    assert_eq!(k, [DiagKind::AggregateNotAllowed]);
    let (k, _) = check(&c, "SELECT SUM(COUNT(*)) FROM customers", Mode::Strict);
    assert_eq!(k, [DiagKind::NestedAggregate]);
    let (k, _) = check(&c, "SELECT ROW_NUMBER() FROM customers", Mode::Strict);
    assert_eq!(k, [DiagKind::WindowNeedsOver]);
    let (k, _) =
        check(&c, "SELECT name FROM customers WHERE ROW_NUMBER() OVER () = 1", Mode::Strict);
    assert_eq!(k, [DiagKind::WindowNotAllowed]);
    let (k, _) = check(&c, "SELECT UPPER(name) OVER () FROM customers", Mode::Strict);
    assert_eq!(k, [DiagKind::OverNotAllowed]);
    // Grouped by expression; the same expression may be selected.
    ok(&c, "SELECT UPPER(c.country), COUNT(*) FROM customers c GROUP BY UPPER(c.country)");
    ok(&c, "SELECT c.country AS k, COUNT(*) FROM customers c GROUP BY k");
    ok(&c, "SELECT c.country FROM customers c GROUP BY ALL");
}

#[test]
fn type_rules_follow_bigquery() {
    let c = catalog();
    let (k, _) = check(&c, "SELECT 1 FROM line_items li WHERE li.qty = 'ten'", Mode::Strict);
    assert_eq!(k, [DiagKind::TypeMismatch], "INT64 = STRING");
    ok(&c, "SELECT 1 FROM customers c WHERE c.signup_date = '2026-01-01'");
    let (k, _) = check(&c, "SELECT SUM(c.name) FROM customers c", Mode::Strict);
    assert_eq!(k, [DiagKind::NotNumeric]);
    let (k, _) = check(&c, "SELECT 1 FROM customers c WHERE c.name", Mode::Strict);
    assert_eq!(k, [DiagKind::NotBoolean]);
    let (k, _) = check(&c, "SELECT COUNT(1, 2) FROM customers", Mode::Strict);
    assert_eq!(k, [DiagKind::ArgCount { min: 0, max: 1, got: 2 }]);
}

#[test]
fn partition_filters_are_required_where_the_table_demands() {
    let c = catalog();
    let m = errs(&c, "SELECT o.status FROM orders o WHERE o.status = 'x'");
    assert_eq!(m, ["Cannot query over table orders without a filter over column(s) that can be used for partition elimination; Did you mean order_ts?"]);
    ok(&c, &format!("SELECT o.status FROM orders o WHERE {TS}"));
    ok(&c, "SELECT o.status FROM orders o JOIN customers c ON c.id = o.customer_id AND o.order_ts > TIMESTAMP '2026-01-01'");
}

#[test]
fn full_mode_tolerates_foreign_objects() {
    let c = catalog();
    let (k, _) = check(&c, "SELECT x.a, my_udf(x.b) FROM other_ds.mystery x", Mode::Full);
    assert!(k.is_empty(), "{k:?}");
    let (k, _) = check(&c, "SELECT x.a FROM other_ds.mystery x", Mode::Strict);
    assert_eq!(k, [DiagKind::UnknownTable]);
}

#[test]
fn set_operations_check_arity() {
    let c = catalog();
    let (k, _) = check(
        &c,
        "SELECT sku, qty FROM line_items UNION ALL SELECT name FROM customers",
        Mode::Strict,
    );
    assert_eq!(k, [DiagKind::SetOpArity]);
}

fn fp(c: &Catalog, sql: &str) -> (u128, u128) {
    let mut p = Parser::new();
    let parsed = p.parse(sql, Mode::Full).unwrap();
    let mut b = Binder::new();
    let bound = b.bind(&parsed, c, BindOpts { mode: Mode::Full, ..OPTS });
    (fingerprint(&parsed, bound, c), template_fingerprint(&parsed, bound, c))
}

#[test]
fn fingerprints_capture_meaning_not_text() {
    let c = catalog();
    let base = fp(&c, "SELECT c.country, COUNT(*) FROM customers AS c WHERE c.name = 'x' AND c.id > 3 GROUP BY c.country").0;
    for same in [
        "select customers.country, count(*) from shop.customers where customers.id > 3 and customers.name = 'x' group by 1",
        "SELECT x.country, COUNT(*) AS n FROM `proj.shop.customers` x WHERE (3 < x.id) AND ('x' = x.name) GROUP BY x.country",
    ] {
        assert_eq!(fp(&c, same).0, base, "{same}");
    }
    for different in [
        "SELECT c.country, COUNT(*) FROM customers AS c WHERE c.name = 'y' AND c.id > 3 GROUP BY c.country",
        "SELECT c.country, COUNT(*) FROM customers AS c WHERE c.name = 'x' AND c.id >= 3 GROUP BY c.country",
        "SELECT c.country, COUNT(DISTINCT c.id) FROM customers AS c WHERE c.name = 'x' AND c.id > 3 GROUP BY c.country",
        "SELECT c.country, COUNT(*) FROM customers AS c WHERE c.name = 'x' OR c.id > 3 GROUP BY c.country",
    ] {
        assert_ne!(fp(&c, different).0, base, "{different}");
    }
    // Templates ignore constants.
    let t1 = fp(&c, "SELECT name FROM customers WHERE country = 'CA' LIMIT 10").1;
    let t2 = fp(&c, "SELECT name FROM customers WHERE country = 'US' LIMIT 5").1;
    assert_eq!(t1, t2);
    assert_ne!(
        fp(&c, "SELECT name FROM customers WHERE country = 'CA'").0,
        fp(&c, "SELECT name FROM customers WHERE country = 'US'").0
    );
}

#[test]
fn self_joins_keep_their_sides_apart() {
    let c = catalog();
    let a = fp(&c, "SELECT a.sku FROM line_items a JOIN line_items b ON a.order_id = b.order_id WHERE a.qty > 1").0;
    let b = fp(&c, "SELECT a.sku FROM line_items a JOIN line_items b ON a.order_id = b.order_id WHERE b.qty > 1").0;
    assert_ne!(a, b);
}

#[test]
fn binder_is_reusable_across_statements() {
    let c = catalog();
    let mut p = Parser::new();
    let mut b = Binder::new();
    for sql in ["SELECT nam FROM customers", "SELECT name FROM customers"] {
        let parsed = p.parse(sql, Mode::Strict).unwrap();
        let bound = b.bind(&parsed, &c, OPTS);
        assert_eq!(bound.ok(), sql.contains("name"));
    }
}

#[test]
fn scalar_subqueries_do_not_leak_outputs_or_aggregation() {
    // Regression: a subquery bound inside the select list used to add its
    // own output to the outer SELECT's range, and its aggregate made the
    // outer SELECT look aggregated ("neither grouped nor aggregated").
    let c = catalog();
    let mut p = Parser::new();
    let sql = "SELECT o.order_id, (SELECT MAX(x.amount) FROM shop.orders AS x WHERE x.customer_id = o.customer_id AND x.order_ts > TIMESTAMP '2024-01-01') AS top, o.status FROM shop.orders AS o WHERE o.order_ts > TIMESTAMP '2024-01-01'";
    let parsed = p.parse(sql, Mode::Strict).unwrap();
    let mut b = Binder::new();
    let bound = b.bind(
        &parsed,
        &c,
        BindOpts { default_project: "proj", default_dataset: None, mode: Mode::Strict },
    );
    assert!(bound.ok(), "{:?}", bound.diags.iter().map(|d| d.message(sql)).collect::<Vec<_>>());
    let names: Vec<&str> =
        bound.output_columns().iter().map(|o| o.name.map_or("?", |n| parsed.text(n.sp))).collect();
    assert_eq!(names, vec!["order_id", "top", "status"], "exactly the outer select list, in order");
}

#[test]
fn geography_json_and_arrays_are_not_comparable() {
    let mut b = CatalogBuilder::new("proj");
    b.add_table(table(
        "geo",
        "places",
        false,
        vec![
            col("g", "GEOGRAPHY", 1),
            col("j", "JSON", 2),
            col("a", "ARRAY<INT64>", 3),
            col("n", "INT64", 4),
        ],
    ));
    let c = Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap();
    let mut p = Parser::new();
    let mut bind = Binder::new();
    for (sql, ty) in [
        ("SELECT COUNTIF(g != g) FROM geo.places", "GEOGRAPHY"),
        ("SELECT * FROM geo.places WHERE j = j", "JSON"),
        ("SELECT * FROM geo.places WHERE a = a", "ARRAY"),
    ] {
        let parsed = p.parse(sql, Mode::Strict).unwrap();
        let bound = bind.bind(
            &parsed,
            &c,
            BindOpts { default_project: "proj", default_dataset: None, mode: Mode::Strict },
        );
        let msgs: Vec<String> = bound.diags.iter().map(|d| d.message(sql)).collect();
        assert!(msgs.iter().any(|m| m.contains(&format!("Equality is not defined for arguments of type {ty}"))), "{sql}: {msgs:?}");
    }
    let parsed = p
        .parse("SELECT COUNTIF(n != 1) FROM geo.places WHERE g IS NOT NULL", Mode::Strict)
        .unwrap();
    assert!(bind
        .bind(
            &parsed,
            &c,
            BindOpts { default_project: "proj", default_dataset: None, mode: Mode::Strict }
        )
        .ok());
}
