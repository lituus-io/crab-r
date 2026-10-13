// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// The eval runner end to end, offline: a scripted model answering by
// question, a warehouse returning results by SQL shape, execution-match
// scoring into a scorecard.

use crab_r::base::batch::{Batch, Cell, ColType};
use crab_r::catalog::{Catalog, CatalogBuilder, ColumnSpec, TableKind, TableSpec};
use crab_r::engine::{Engine, EngineConfig};
use crab_r::eval::run::{evaluate, RunConfig};
use crab_r::eval::{Dataset, Scorecard, Verdict};
use crab_r::exec::{DryRun, QueryResult, RunOpts, Warehouse};
use crab_r::llm::{
    CacheSupport, Candidates, Caps, ConstraintSupport, Finish, GenRequest, Model, Price, Sink,
    Usage,
};
use objstore_r::join::spin_on as block_on;
use std::future::{ready, Ready};

struct ByQuestion {
    caps: Caps,
}

impl Model for ByQuestion {
    type Gen<'a, S>
        = Ready<crab_r::llm::Result<Usage>>
    where
        Self: 'a,
        S: Sink + 'a;

    fn caps(&self) -> &Caps {
        &self.caps
    }

    fn generate<'a, S: Sink + 'a>(
        &'a self,
        req: &'a GenRequest<'a>,
        out: &'a mut Candidates,
        sink: &'a mut S,
    ) -> Self::Gen<'a, S> {
        let q = req.prompt.parts.last().map_or("", |p| p.text);
        let reply = if q.contains("per status") {
            "SELECT o.status, COUNT(*) AS n FROM `proj.shop.orders` AS o GROUP BY o.status"
        } else if q.contains("how many orders") {
            "SELECT COUNT(*) AS n FROM `proj.shop.orders` AS o WHERE o.status = 'shipped'"
        } else {
            "SELECT o.weather FROM `proj.shop.orders` AS o"
        };
        out.reset(1);
        out.push(0, reply, sink);
        out.set_finish(0, Finish::Stop);
        ready(Ok(Usage {
            input_tokens: 500,
            cached_tokens: 400,
            output_tokens: 30,
            micro_usd: 250,
            estimated: false,
            wall_ms: 1,
        }))
    }
}

struct Wh;

fn batch(rows: &[(&str, i64)]) -> Batch {
    let mut b = Batch::new(&[("status", ColType::String), ("n", ColType::Int64)], rows.len());
    for (s, n) in rows {
        b.columns[0].push(Cell::Str(s)).unwrap();
        b.columns[1].push(Cell::I64(*n)).unwrap();
    }
    b
}

impl Warehouse for Wh {
    type Dry<'a> = Ready<crab_r::exec::Result<DryRun>>;
    type Run<'a> = Ready<crab_r::exec::Result<QueryResult>>;

    fn dry_run<'a>(&'a self, _: &'a str) -> Self::Dry<'a> {
        ready(Ok(DryRun { bytes_processed: 1 << 20, schema: vec![] }))
    }

    fn run<'a>(&'a self, sql: &'a str, _: RunOpts) -> Self::Run<'a> {
        let b = if sql.contains("GROUP BY") {
            // Engine and gold agree on per-status counts (rows in different order).
            if sql.contains("t0") {
                batch(&[("shipped", 61), ("pending", 22)])
            } else {
                batch(&[("pending", 22), ("shipped", 61)])
            }
        } else if sql.contains("'shipped'") {
            let mut b = Batch::new(&[("n", ColType::Int64)], 1);
            b.columns[0].push(Cell::I64(61)).unwrap();
            b
        } else {
            // Gold counts all orders: the engine's filtered count is wrong.
            let mut b = Batch::new(&[("n", ColType::Int64)], 1);
            b.columns[0].push(Cell::I64(83)).unwrap();
            b
        };
        let rows = b.rows() as u64;
        ready(Ok(QueryResult {
            batch: b,
            total_rows: rows,
            truncated: false,
            bytes_processed: 1 << 20,
            bytes_billed: 10 << 20,
            cache_hit: false,
            wall_ms: 3,
        }))
    }
}

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
    b.add_table(TableSpec {
        dataset: "shop".into(),
        name: "orders".into(),
        kind: TableKind::Table,
        description: None,
        row_count: 83,
        logical_bytes: 0,
        last_modified_ms: 0,
        require_partition_filter: false,
        columns: vec![col("order_id", "INT64", 1), col("status", "STRING", 2)],
    });
    Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
}

#[test]
fn scores_execution_match_abstention_and_cost() {
    let caps = Caps {
        id: "q".into(),
        constraints: ConstraintSupport { lark: false, gbnf: false, json_schema: false },
        max_candidates: 1,
        max_stream_candidates: 1,
        cache: CacheSupport::Implicit,
        context_tokens: 100_000,
        max_output_tokens: 1000,
        streaming: true,
        price: Price::default(),
    };
    let engine = Engine::new(ByQuestion { caps }, Wh, catalog(), EngineConfig::new("proj"));
    let ds = Dataset::from_json_array(
        "mini",
        r#"[
          {"question_id": 1, "db_id": "shop", "question": "Orders per status?", "SQL": "SELECT status, COUNT(*) FROM shop.orders GROUP BY status"},
          {"question_id": 2, "db_id": "shop", "question": "how many orders are there?", "SQL": "SELECT COUNT(*) FROM shop.orders"},
          {"question_id": 3, "db_id": "shop", "question": "What was the weather?", "SQL": ""}
        ]"#,
    )
    .unwrap();
    let traces = block_on(evaluate(
        &engine,
        &Wh,
        &ds.items,
        RunConfig { concurrency: 3, ..RunConfig::default() },
    ));
    let verdicts: Vec<Verdict> = traces.iter().map(|t| t.record.verdict).collect();
    assert_eq!(verdicts, [Verdict::Correct, Verdict::Wrong, Verdict::Abstained], "{traces:#?}");
    assert!(traces[2].note.as_deref().unwrap().contains("weather"), "abstention keeps the reason");
    let records: Vec<_> = traces.into_iter().map(|t| t.record).collect();
    let card = Scorecard::from_records(&records);
    assert!((card.accuracy - 0.5).abs() < 1e-12);
    assert!((card.false_success_rate - 1.0 / 3.0).abs() < 1e-12);
    assert!((card.decline_recall - 1.0).abs() < 1e-12, "the unanswerable question was declined");
    assert!(card.cached_share > 0.4, "cached prefix tokens are accounted");
}
