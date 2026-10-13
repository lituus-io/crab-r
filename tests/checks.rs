// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Model-written checks under guards: valid aggregates merged into one query
// (deduplicated), everything that could read another table, write, smuggle
// a statement or comment, or scan too much refused with a reason; sampling
// of large tables; query checks return rows.

use crab_r::base::batch::{Batch, Cell, ColType};
use crab_r::catalog::{Catalog, CatalogBuilder, ColumnSpec, TableKind, TableSpec};
use crab_r::describe::checks::{run, Check, CheckSql, Outcome, RunnerConfig, Target};
use crab_r::exec::{DryRun, QueryResult, RunOpts, Warehouse};
use objstore_r::join::spin_on as block_on;
use std::cell::RefCell;
use std::future::{ready, Ready};

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

fn cat() -> Catalog {
    let mut b = CatalogBuilder::new("p");
    for (name, cols) in [
        (
            "orders",
            vec![
                col("order_id", "INT64", 1),
                col("amount", "NUMERIC", 2),
                col("status", "STRING", 3),
                col("customer_id", "INT64", 4),
            ],
        ),
        ("crm", vec![col("id", "INT64", 1)]),
    ] {
        b.add_table(TableSpec {
            dataset: "d".into(),
            name: name.into(),
            kind: TableKind::Table,
            description: None,
            row_count: 0,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns: cols,
        });
    }
    Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
}

#[derive(Default)]
struct Wh {
    scan: u64,
    runs: RefCell<Vec<(String, RunOpts)>>,
    /// SQL containing this fails at run time (and its dry run fails too).
    fail_on: Option<&'static str>,
}

impl Warehouse for Wh {
    type Dry<'a> = Ready<crab_r::exec::Result<DryRun>>;
    type Run<'a> = Ready<crab_r::exec::Result<QueryResult>>;
    fn dry_run<'a>(&'a self, sql: &'a str) -> Self::Dry<'a> {
        if self.fail_on.is_some_and(|f| sql.contains(f)) {
            return ready(Err(crab_r::exec::Error::Query {
                reason: "invalidQuery".into(),
                message: "Equality is not defined for arguments of type GEOGRAPHY".into(),
                pos: None,
            }));
        }
        ready(Ok(DryRun { bytes_processed: self.scan, schema: vec![] }))
    }
    fn run<'a>(&'a self, sql: &'a str, opts: RunOpts) -> Self::Run<'a> {
        self.runs.borrow_mut().push((sql.to_owned(), opts));
        if self.fail_on.is_some_and(|f| sql.contains(f)) {
            return ready(Err(crab_r::exec::Error::Query {
                reason: "invalidQuery".into(),
                message: "Equality is not defined for arguments of type GEOGRAPHY".into(),
                pos: None,
            }));
        }
        let n = sql.matches(" AS c").count().max(1);
        let names: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
        let schema: Vec<(&str, ColType)> =
            names.iter().map(|s| (s.as_str(), ColType::Int64)).collect();
        let mut b = Batch::new(&schema, 1);
        for (i, c) in b.columns.iter_mut().enumerate() {
            c.push(Cell::I64(i as i64 * 10)).unwrap();
        }
        ready(Ok(QueryResult {
            batch: b,
            total_rows: 1,
            truncated: false,
            bytes_processed: self.scan,
            bytes_billed: 10 << 20,
            cache_hit: false,
            wall_ms: 1,
        }))
    }
}

fn agg(id: u32, e: &str) -> Check {
    Check {
        id,
        column: Some("amount".into()),
        claim: format!("claim {id}"),
        sql: CheckSql::Aggregate(e.into()),
        expect: None,
        tests_existing: false,
    }
}

#[test]
fn aggregates_merge_into_one_scan_and_bad_checks_are_refused() {
    let c = cat();
    let t = Target {
        id: c.find_table("d", "orders").unwrap(),
        fqn: "p.d.orders",
        bytes: 100 << 20,
        source: None,
    };
    let checks = vec![
        agg(1, "COUNTIF(amount < 0)"),
        agg(2, "COUNT(*) - COUNT(DISTINCT order_id)"),
        agg(3, "countif(amount   < 0)"),            // duplicate of 1
        agg(4, "amount > 0"),                       // not an aggregate
        agg(5, "(SELECT COUNT(*) FROM `p.d.crm`)"), // reads another table
        agg(6, "COUNT(*)) AS v FROM `p.d.crm` --"), // comment smuggling
        agg(7, "COUNT(*); DROP TABLE p.d.orders"),  // statement smuggling
        agg(8, "COUNTIF(amonut IS NULL)"),          // unknown column
        agg(9, "COUNT(*) FROM `p.d.crm` UNION ALL SELECT (1"), // query smuggling
        Check {
            id: 10,
            column: None,
            claim: "no deletes".into(),
            sql: CheckSql::Query("DELETE FROM p.d.orders WHERE TRUE".into()),
            expect: None,
            tests_existing: false,
        },
    ];
    let w = Wh { scan: 5 << 20, ..Wh::default() };
    let (res, billed) = block_on(run(&w, &c, t, &checks, RunnerConfig::default()));
    let runs = w.runs.borrow();
    assert_eq!(runs.len(), 1, "one scan for every aggregate: {runs:?}");
    assert_eq!(runs[0].0, "SELECT (COUNTIF(amount < 0)) AS c0, (COUNT(*) - COUNT(DISTINCT order_id)) AS c1 FROM `p.d.orders`");
    assert_eq!(runs[0].1.max_bytes_billed, Some(11 << 20), "10 MiB floor + headroom");
    assert_eq!(billed, 10 << 20);
    assert_eq!(res[0].outcome, Outcome::Value("0".into()));
    assert_eq!(res[1].outcome, Outcome::Value("10".into()));
    assert_eq!(res[2].outcome, Outcome::Value("0".into()), "deduplicated, same answer");
    let why = |i: usize| match &res[i].outcome {
        Outcome::Rejected(w) => w.clone(),
        other => panic!("#{i} not rejected: {other:?}"),
    };
    assert!(why(3).contains("must aggregate"), "{}", why(3));
    assert!(why(4).contains("only the table itself"), "{}", why(4));
    assert!(why(5).contains("comment"), "{}", why(5));
    assert!(why(6).contains("';'"), "{}", why(6));
    assert!(why(7).contains("amonut"), "{}", why(7));
    assert!(why(8).contains("single expression") || why(8).contains("parse"), "{}", why(8));
    assert!(why(9).contains("parse"), "writes never run: {}", why(9));
}

#[test]
fn cost_caps_sampling_and_query_checks() {
    let c = cat();
    let big = Target {
        id: c.find_table("d", "orders").unwrap(),
        fqn: "p.d.orders",
        bytes: 10 << 30,
        source: None,
    };
    let w = Wh { scan: 1 << 30, ..Wh::default() };
    let checks = vec![
        agg(1, "COUNTIF(status IS NULL)"),
        Check { id: 2, column: Some("customer_id".into()), claim: "every customer exists".into(), sql: CheckSql::Query("SELECT COUNT(*) AS orphans FROM p.d.orders AS o LEFT JOIN p.d.crm AS c ON o.customer_id = c.id WHERE c.id IS NULL".into()), expect: Some("0".into()), tests_existing: false },
    ];
    let (res, _) = block_on(run(&w, &c, big, &checks, RunnerConfig::default()));
    let runs = w.runs.borrow();
    assert!(
        runs[0].0.ends_with("FROM `p.d.orders` TABLESAMPLE SYSTEM (10 PERCENT)"),
        "{}",
        runs[0].0
    );
    assert!(res[0].sampled);
    assert!(
        matches!(&res[1].outcome, Outcome::Rows(rows) if rows[0] == vec!["c0".to_owned()] && rows.len() == 2)
    );
    drop(runs);
    // Over the byte cap: refused before any spend.
    let w = Wh { scan: 50 << 30, ..Wh::default() };
    let (res, billed) = block_on(run(&w, &c, big, &checks[..1], RunnerConfig::default()));
    assert!(
        matches!(&res[0].outcome, Outcome::Failed(m) if m.contains("byte cap")),
        "{:?}",
        res[0].outcome
    );
    assert_eq!((billed, w.runs.borrow().len()), (0, 0));
}

#[test]
fn one_invalid_aggregate_does_not_sink_the_others() {
    let c = cat();
    let t = Target {
        id: c.find_table("d", "orders").unwrap(),
        fqn: "p.d.orders",
        bytes: 1 << 20,
        source: None,
    };
    // The binder cannot know this fails at run time; the warehouse does.
    let w = Wh { scan: 1 << 20, fail_on: Some("LENGTH(status) > 100"), ..Wh::default() };
    let checks = vec![
        agg(1, "COUNTIF(amount < 0)"),
        agg(2, "COUNTIF(LENGTH(status) > 100)"),
        agg(3, "MAX(amount)"),
    ];
    let (res, _) = block_on(run(&w, &c, t, &checks, RunnerConfig::default()));
    assert!(
        matches!(&res[1].outcome, Outcome::Failed(m) if m.contains("GEOGRAPHY")),
        "{:?}",
        res[1].outcome
    );
    assert_eq!(res[0].outcome, Outcome::Value("0".into()));
    assert!(matches!(res[2].outcome, Outcome::Value(_)), "{:?}", res[2].outcome);
    let runs = w.runs.borrow();
    assert_eq!(
        runs.len(),
        1,
        "the merged query failed its free dry run; only the retry without the culprit ran"
    );
    assert!(!runs[0].0.contains("LENGTH(status)") && runs[0].0.contains("AS c1"), "{}", runs[0].0);
}
