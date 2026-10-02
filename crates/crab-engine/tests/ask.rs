// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Session::ask end to end with a scripted model (streams in small chunks)
// and a scripted warehouse: happy path, targeted repair, stream-abort,
// cost guardrail, warehouse-side repair, abstention, prompt cache order.

use crab_catalog::{Catalog, CatalogBuilder, ColumnSpec, TableKind, TableSpec};
use crab_core::batch::{Batch, Cell, ColType};
use crab_core::date::Date;
use crab_engine::crab_exec::{DryRun, Error as WhError, QueryResult, RunOpts, Warehouse};
use crab_engine::crab_llm::{
    CacheSupport, Candidates, Caps, Constraint, ConstraintSupport, Finish, Flow, GenRequest, Model,
    Price, Sink, Usage,
};
use crab_engine::{AbstainReason, Engine, EngineConfig, Outcome, Stage};
use std::cell::{Cell as StdCell, RefCell};
use std::future::{ready, Future, Ready};

// ---- scripted model --------------------------------------------------------

/// (system text, [(part text, cache boundary)]) per call.
type SeenPrompt = (String, Vec<(String, bool)>);

struct ScriptModel {
    caps: Caps,
    replies: RefCell<Vec<&'static str>>,
    prompts: RefCell<Vec<SeenPrompt>>,
    /// (n, temperature) per call.
    requests: RefCell<Vec<(u8, f32)>>,
    /// Output constraint per call ("none", "lark:<text>", "gbnf:<text>").
    constraints: RefCell<Vec<String>>,
    chunks_sent: StdCell<usize>,
}

impl ScriptModel {
    fn new(replies: Vec<&'static str>) -> Self {
        Self {
            caps: Caps {
                id: "script".into(),
                constraints: ConstraintSupport { lark: false, gbnf: false, json_schema: false },
                max_candidates: 1,
                max_stream_candidates: 1,
                cache: CacheSupport::Implicit,
                context_tokens: 1_000_000,
                max_output_tokens: 4096,
                streaming: true,
                price: Price { input: 300_000, cached_input: 75_000, output: 2_500_000 },
            },
            replies: RefCell::new(replies),
            prompts: RefCell::new(Vec::new()),
            requests: RefCell::new(Vec::new()),
            constraints: RefCell::new(Vec::new()),
            chunks_sent: StdCell::new(0),
        }
    }
}

impl ScriptModel {
    /// A model with native n-sampling (one prompt charge for n samples).
    fn native(replies: Vec<&'static str>) -> Self {
        let mut m = Self::new(replies);
        m.caps.max_candidates = 8;
        m
    }
}

impl Model for ScriptModel {
    type Gen<'a, S>
        = Ready<crab_engine::crab_llm::Result<Usage>>
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
        self.prompts.borrow_mut().push((
            req.prompt.system.to_owned(),
            req.prompt.parts.iter().map(|p| (p.text.to_owned(), p.cache_break)).collect(),
        ));
        self.requests.borrow_mut().push((req.n, req.temperature));
        self.constraints.borrow_mut().push(match req.constraint {
            Constraint::Lark(g) => format!("lark:{g}"),
            Constraint::Gbnf(g) => format!("gbnf:{g}"),
            Constraint::JsonSchema(j) => format!("json:{j}"),
            Constraint::None => "none".into(),
        });
        out.reset(usize::from(req.n));
        for c in 0..usize::from(req.n) {
            let reply = self.replies.borrow_mut().remove(0);
            let bytes = reply.as_bytes();
            let mut i = 0;
            let mut finish = Finish::Stop;
            while i < bytes.len() {
                let mut j = (i + 8).min(bytes.len());
                while !reply.is_char_boundary(j) {
                    j += 1;
                }
                self.chunks_sent.set(self.chunks_sent.get() + 1);
                if out.push(c, &reply[i..j], sink) == Flow::Abort {
                    finish = Finish::Aborted;
                    break;
                }
                i = j;
            }
            out.set_finish(c, finish);
        }
        ready(Ok(Usage {
            input_tokens: 1000,
            cached_tokens: 0,
            output_tokens: 50,
            micro_usd: 425,
            estimated: false,
            wall_ms: 1,
        }))
    }
}

// ---- scripted warehouse ----------------------------------------------------

#[derive(Default)]
struct ScriptWarehouse {
    scan_bytes: u64,
    dry_error_on: Option<&'static str>,
    dry_runs: RefCell<Vec<String>>,
    runs: RefCell<Vec<(String, RunOpts)>>,
    /// SQL containing the pattern returns a one-row result with this `n`.
    results_by: Vec<(&'static str, i64)>,
}

fn result() -> QueryResult {
    let mut b = Batch::new(&[("status", ColType::String), ("n", ColType::Int64)], 2);
    b.columns[0].push(Cell::Str("shipped")).unwrap();
    b.columns[1].push(Cell::I64(61)).unwrap();
    b.columns[0].push(Cell::Str("pending")).unwrap();
    b.columns[1].push(Cell::I64(22)).unwrap();
    QueryResult {
        batch: b,
        total_rows: 2,
        truncated: false,
        bytes_processed: 2048,
        bytes_billed: 10 << 20,
        cache_hit: false,
        wall_ms: 5,
    }
}

impl Warehouse for ScriptWarehouse {
    type Dry<'a> = Ready<crab_engine::crab_exec::Result<DryRun>>;
    type Run<'a> = Ready<crab_engine::crab_exec::Result<QueryResult>>;

    fn dry_run<'a>(&'a self, sql: &'a str) -> Self::Dry<'a> {
        self.dry_runs.borrow_mut().push(sql.to_owned());
        if let Some(bad) = self.dry_error_on {
            if sql.contains(bad) {
                return ready(Err(WhError::Query {
                    reason: "invalidQuery".into(),
                    message: format!("No matching signature for {bad}"),
                    pos: None,
                }));
            }
        }
        ready(Ok(DryRun { bytes_processed: self.scan_bytes, schema: vec![] }))
    }

    fn run<'a>(&'a self, sql: &'a str, opts: RunOpts) -> Self::Run<'a> {
        self.runs.borrow_mut().push((sql.to_owned(), opts));
        if let Some(&(_, n)) = self.results_by.iter().find(|(p, _)| sql.contains(p)) {
            let mut b = Batch::new(&[("n", ColType::Int64)], 1);
            b.columns[0].push(Cell::I64(n)).unwrap();
            return ready(Ok(QueryResult {
                batch: b,
                total_rows: 1,
                truncated: false,
                bytes_processed: 2048,
                bytes_billed: 10 << 20,
                cache_hit: false,
                wall_ms: 5,
            }));
        }
        ready(Ok(result()))
    }
}

// ---- fixtures ----------------------------------------------------------------

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
    b.add_dataset("shop", "US");
    b.add_table(TableSpec {
        dataset: "shop".into(),
        name: "orders".into(),
        kind: TableKind::Table,
        description: Some("One row per order".into()),
        row_count: 1000,
        logical_bytes: 0,
        last_modified_ms: 0,
        require_partition_filter: false,
        columns: vec![
            col("order_id", "INT64", 1),
            col("status", "STRING", 2),
            col("order_ts", "TIMESTAMP", 3),
        ],
    });
    Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
}

fn block_on<F: Future>(f: F) -> F::Output {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let mut f = std::pin::pin!(f);
    loop {
        if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

const GOOD: &str =
    "```sql\nSELECT o.status, COUNT(*) AS n FROM `proj.shop.orders` AS o GROUP BY o.status\n```";

fn engine(replies: Vec<&'static str>, wh: ScriptWarehouse) -> Engine<ScriptModel, ScriptWarehouse> {
    Engine::new(ScriptModel::new(replies), wh, catalog(), EngineConfig::new("proj"))
}

#[test]
fn answers_with_canonical_sql_rows_and_exact_spend() {
    let e = engine(vec![GOOD], ScriptWarehouse { scan_bytes: 4096, ..ScriptWarehouse::default() });
    let mut s = e.session();
    s.set_today(Date::parse("2026-09-29").unwrap());
    let mut stages = Vec::new();
    let mut ev = |st: Stage| stages.push(st);
    let r = block_on(s.ask("How many orders per status?", &mut ev)).unwrap();
    let Outcome::Answer(a) = r.outcome else { panic!("{:?}", r.outcome) };
    assert_eq!(
        a.sql,
        "SELECT t0.status, COUNT(*) AS n FROM `proj.shop.orders` AS t0 GROUP BY t0.status"
    );
    assert_eq!(a.result.batch.rows(), 2);
    assert_eq!(a.repairs, 0);
    assert_eq!(
        (r.spend.input_tokens, r.spend.output_tokens, r.spend.micro_usd, r.spend.warehouse_bytes),
        (1000, 50, 425, 10 << 20)
    );
    assert_eq!(
        stages.first(),
        Some(&Stage::Context { tables: 1, tokens: e.schema().estimated_tokens() })
    );
    assert!(
        stages.contains(&Stage::Estimated { bytes: 4096 }) && stages.last() == Some(&Stage::Done)
    );
    // Byte cap: the estimate plus headroom, floored at BigQuery's 10 MiB minimum.
    let runs = e.warehouse().runs.borrow();
    assert_eq!(
        runs[0].1.max_bytes_billed,
        Some((10 << 20) + (1 << 20)),
        "one table: 10 MiB floor plus headroom"
    );
    // Prompt cache order: static system, schema (cache boundary), today, question.
    let prompts = e.model().prompts.borrow();
    let (system, parts) = &prompts[0];
    assert!(system.starts_with("You translate a question into one BigQuery GoogleSQL query."));
    assert!(
        parts[0].0.starts_with("TABLE `proj.shop.orders` AS o") && parts[0].1,
        "schema first, marked cacheable"
    );
    assert!(parts[1].0.starts_with("Today is 2026-09-29"));
    assert_eq!(parts[2].0, "Question: How many orders per status?\nSQL:");
}

#[test]
fn a_validation_failure_is_repaired_with_bigquery_style_diagnostics() {
    let bad = "SELECT o.statuz, COUNT(*) AS n FROM `proj.shop.orders` AS o GROUP BY o.statuz";
    let e = engine(vec![bad, GOOD], ScriptWarehouse::default());
    let mut s = e.session();
    let r = block_on(s.ask("How many orders per status?", &mut ())).unwrap();
    let Outcome::Answer(a) = r.outcome else { panic!("{:?}", r.outcome) };
    assert_eq!(a.repairs, 1);
    let prompts = e.model().prompts.borrow();
    let repair = &prompts[1].1[2].0;
    assert!(repair.contains("Your previous SQL:\nSELECT o.statuz"), "{repair}");
    assert!(repair.contains("- Unrecognized name: statuz; Did you mean status?"), "{repair}");
    assert_eq!(
        prompts[1].1[0].0, prompts[0].1[0].0,
        "repair keeps the cached schema prefix byte-identical"
    );
    assert!(
        e.warehouse().dry_runs.borrow().len() == 1,
        "the invalid SQL never reached the warehouse"
    );
}

#[test]
fn prose_is_aborted_mid_stream_and_repaired() {
    let prose = "I think the best way to answer this question is to count the orders grouped by their status value, which gives...";
    let e = engine(vec![prose, GOOD], ScriptWarehouse::default());
    let mut s = e.session();
    let mut aborted = false;
    let mut ev = |st: Stage| aborted |= st == Stage::StreamAborted;
    let r = block_on(s.ask("How many orders per status?", &mut ev)).unwrap();
    assert!(matches!(r.outcome, Outcome::Answer(_)));
    assert!(aborted);
    // The prose reply was cut after a couple of chunks; the good one is fenced and
    // is cut at its closing fence.
    let prose_chunks = prose.len().div_ceil(8);
    assert!(
        e.model().chunks_sent.get() < prose_chunks + GOOD.len().div_ceil(8),
        "stream-abort saved output"
    );
}

#[test]
fn expensive_queries_are_refused_before_any_spend() {
    let e =
        engine(vec![GOOD], ScriptWarehouse { scan_bytes: 50 << 30, ..ScriptWarehouse::default() });
    let mut s = e.session();
    let r = block_on(s.ask("How many orders per status?", &mut ())).unwrap();
    assert!(
        matches!(r.outcome, Outcome::Abstain(AbstainReason::TooExpensive { bytes, limit }) if bytes == 50 << 30 && limit == 10 << 30)
    );
    assert!(e.warehouse().runs.borrow().is_empty(), "nothing executed");
    assert_eq!(r.spend.warehouse_bytes, 0);
}

#[test]
fn warehouse_side_errors_feed_the_repair_loop() {
    let wrong = "SELECT o.status, COUNT(*) AS n FROM `proj.shop.orders` AS o WHERE STARTS_WITH(o.status, 'x') GROUP BY o.status";
    let e = engine(
        vec![wrong, GOOD],
        ScriptWarehouse { dry_error_on: Some("STARTS_WITH"), ..ScriptWarehouse::default() },
    );
    let mut s = e.session();
    let r = block_on(s.ask("How many orders per status?", &mut ())).unwrap();
    let Outcome::Answer(a) = r.outcome else { panic!("{:?}", r.outcome) };
    assert_eq!(a.repairs, 1);
    let prompts = e.model().prompts.borrow();
    assert!(prompts[1].1[2].0.contains("No matching signature for STARTS_WITH"));
}

#[test]
fn abstains_with_reasons_after_the_allowed_repairs() {
    let bad = "SELECT o.nope FROM `proj.shop.orders` AS o";
    let e = engine(vec![bad, bad], ScriptWarehouse::default());
    let mut s = e.session();
    let r = block_on(s.ask("?", &mut ())).unwrap();
    match r.outcome {
        Outcome::Abstain(AbstainReason::InvalidSql { problems }) => {
            assert_eq!(problems, ["Unrecognized name: nope"])
        }
        other => panic!("{other:?}"),
    }
    assert!(e.warehouse().dry_runs.borrow().is_empty());
    assert_eq!(r.spend.input_tokens, 2000, "two model calls were charged");
}

#[test]
fn a_declared_non_answer_abstains_with_the_reason() {
    let e =
        engine(vec!["CANNOT_ANSWER: the schema has no weather data"], ScriptWarehouse::default());
    let mut s = e.session();
    let mut aborted = false;
    let mut ev = |st: Stage| aborted |= st == Stage::StreamAborted;
    let r = block_on(s.ask("What is the weather?", &mut ev)).unwrap();
    assert!(
        matches!(r.outcome, Outcome::Abstain(AbstainReason::NotAnswerable { ref reason }) if reason == "the schema has no weather data"),
        "{:?}",
        r.outcome
    );
    assert!(!aborted, "the sentinel is not treated as broken SQL");
    assert!(e.warehouse().dry_runs.borrow().is_empty());
}

#[test]
fn joins_get_a_byte_cap_per_referenced_table() {
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
    for name in ["orders", "users"] {
        b.add_table(TableSpec {
            dataset: "shop".into(),
            name: name.into(),
            kind: TableKind::Table,
            description: None,
            row_count: 0,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns: vec![col("id", "INT64", 1), col("user_id", "INT64", 2)],
        });
    }
    let cat = Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap();
    let sql = "SELECT COUNT(*) AS n FROM `proj.shop.orders` AS o JOIN `proj.shop.users` AS u ON u.id = o.user_id";
    let e = Engine::new(
        ScriptModel::new(vec![sql]),
        ScriptWarehouse { scan_bytes: 4 << 20, ..ScriptWarehouse::default() },
        cat,
        EngineConfig::new("proj"),
    );
    let mut s = e.session();
    let r = block_on(s.ask("How many orders have a user?", &mut ())).unwrap();
    assert!(matches!(r.outcome, Outcome::Answer(_)), "{:?}", r.outcome);
    assert_eq!(
        e.warehouse().runs.borrow()[0].1.max_bytes_billed,
        Some((20 << 20) + (1 << 20)),
        "two tables: 20 MiB floor plus headroom"
    );
}

fn profiled_catalog() -> Catalog {
    use crab_catalog::ProfileSpec;
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
    b.add_dataset("shop", "US");
    b.add_table(TableSpec {
        dataset: "shop".into(),
        name: "orders".into(),
        kind: TableKind::Table,
        description: None,
        row_count: 1000,
        logical_bytes: 0,
        last_modified_ms: 0,
        require_partition_filter: false,
        columns: vec![
            col("order_id", "INT64", 1),
            col("status", "STRING", 2),
            col("country", "STRING", 3),
        ],
    });
    let prof = |vals: &[&str], complete: bool| ProfileSpec {
        rows: 1000,
        nulls: 0,
        distinct: vals.len() as u64,
        min: None,
        max: None,
        values: vals.iter().enumerate().map(|(i, v)| ((*v).to_owned(), 100 - i as u64)).collect(),
        values_complete: complete,
        sampled: false,
    };
    b.set_profile("shop", "orders", "status", prof(&["Complete", "Shipped", "Cancelled"], true));
    let countries: Vec<String> =
        (0..40).map(|i| format!("Country{i:02}")).chain(["Brasil".to_owned()]).collect();
    let refs: Vec<&str> = countries.iter().map(String::as_str).collect();
    b.set_profile("shop", "orders", "country", prof(&refs, true));
    Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
}

#[test]
fn question_values_are_grounded_and_impossible_literals_repaired() {
    let bad = "SELECT COUNT(*) AS n FROM `proj.shop.orders` AS o WHERE o.status = 'complete' AND o.country IN ('Brazil')";
    let good = "SELECT COUNT(*) AS n FROM `proj.shop.orders` AS o WHERE o.status = 'Complete' AND o.country IN ('Brasil')";
    let e = Engine::new(
        ScriptModel::new(vec![bad, good]),
        ScriptWarehouse::default(),
        profiled_catalog(),
        EngineConfig::new("proj"),
    );
    let mut s = e.session();
    let r = block_on(s.ask("How many complete orders from Brazil?", &mut ())).unwrap();
    let Outcome::Answer(a) = r.outcome else { panic!("{:?}", r.outcome) };
    assert_eq!(a.repairs, 1);
    let prompts = e.model().prompts.borrow();
    // Small complete sets are listed in the schema; large ones are grounded per question.
    assert!(
        prompts[0].1[0]
            .0
            .contains("o.status STRING  -- values: 'Complete', 'Shipped', 'Cancelled'"),
        "{}",
        prompts[0].1[0].0
    );
    let tail = &prompts[0].1[2].0;
    assert!(tail.starts_with("Values in the data that the question refers to (use these exact literals):\n  o.country = 'Brasil'  -- \"Brazil\"\n"), "{tail}");
    assert!(
        !tail.contains("o.status ="),
        "exact matches on fully listed columns add nothing: {tail}"
    );
    let repair = &prompts[1].1[2].0;
    assert!(
        repair.contains(
            "- Value 'complete' never occurs in column o.status; did you mean 'Complete'?"
        ),
        "{repair}"
    );
    assert!(
        repair
            .contains("- Value 'Brazil' never occurs in column o.country; did you mean 'Brasil'?"),
        "{repair}"
    );
    assert_eq!(
        e.warehouse().dry_runs.borrow().len(),
        1,
        "the empty-result query never reached the warehouse"
    );
}

#[test]
fn an_unfixed_literal_runs_on_the_last_round() {
    let bad = "SELECT COUNT(*) AS n FROM `proj.shop.orders` AS o WHERE o.status = 'Returned'";
    let e = Engine::new(
        ScriptModel::new(vec![bad, bad]),
        ScriptWarehouse::default(),
        profiled_catalog(),
        EngineConfig::new("proj"),
    );
    let mut s = e.session();
    let r = block_on(s.ask("How many returned orders?", &mut ())).unwrap();
    let Outcome::Answer(a) = r.outcome else { panic!("{:?}", r.outcome) };
    assert_eq!(a.repairs, 1, "profiles can be stale: the last round runs as written");
    let prompts = e.model().prompts.borrow();
    assert!(prompts[1].1[2].0.contains("- Value 'Returned' never occurs in column o.status; its values are 'Complete', 'Shipped', 'Cancelled'"), "{}", prompts[1].1[2].0);
}

// ---- adaptive sampling, voting, cache ----------------------------------------

const SHIPPED: &str =
    "SELECT COUNT(*) AS n FROM `proj.shop.orders` AS o WHERE o.status = 'shipped'";
const SHIPPED_TOO: &str =
    "select count(*) as shipped_orders from `proj.shop.orders` as o where o.status = 'shipped'";
const SHIPPED_IN: &str =
    "SELECT COUNT(o.order_id) AS n FROM `proj.shop.orders` AS o WHERE o.status IN ('shipped')";
const PENDING: &str =
    "SELECT COUNT(*) AS n FROM `proj.shop.orders` AS o WHERE o.status = 'pending'";
const ALL: &str = "SELECT COUNT(*) AS n FROM `proj.shop.orders` AS o";

fn voting_warehouse() -> ScriptWarehouse {
    ScriptWarehouse {
        results_by: vec![("'shipped'", 61), ("'pending'", 22)],
        ..ScriptWarehouse::default()
    }
}

#[test]
fn agreeing_samples_cost_one_call_and_one_execution() {
    let e = Engine::new(
        ScriptModel::native(vec![SHIPPED, SHIPPED_TOO]),
        voting_warehouse(),
        catalog(),
        EngineConfig::new("proj"),
    );
    let mut s = e.session();
    let mut stages = Vec::new();
    let r = block_on(s.ask("How many shipped orders?", &mut |st| stages.push(st))).unwrap();
    let Outcome::Answer(a) = r.outcome else { panic!("{:?}", r.outcome) };
    assert_eq!((a.confidence, a.samples, a.alternatives.len()), (1.0, 2, 0));
    assert_eq!(
        *e.model().requests.borrow(),
        vec![(2, 0.5)],
        "one call, two samples, one prompt charge"
    );
    assert_eq!(e.warehouse().runs.borrow().len(), 1, "same fingerprint: executed once");
    assert!(stages.contains(&Stage::Voting { groups: 1, top: 2, total: 2 }), "{stages:?}");
}

#[test]
fn different_sql_with_the_same_result_agrees() {
    let e = Engine::new(
        ScriptModel::native(vec![SHIPPED, SHIPPED_IN]),
        voting_warehouse(),
        catalog(),
        EngineConfig::new("proj"),
    );
    let mut s = e.session();
    let r = block_on(s.ask("How many shipped orders?", &mut ())).unwrap();
    let Outcome::Answer(a) = r.outcome else { panic!("{:?}", r.outcome) };
    assert_eq!(a.confidence, 1.0);
    assert_eq!(e.warehouse().runs.borrow().len(), 2, "two distinct queries, executed concurrently");
    assert_eq!(e.model().requests.borrow().len(), 1, "they agree: no more samples");
}

#[test]
fn disagreement_draws_more_samples_once_and_votes() {
    let e = Engine::new(
        ScriptModel::native(vec![SHIPPED, PENDING, SHIPPED_TOO, PENDING, SHIPPED_IN]),
        voting_warehouse(),
        catalog(),
        EngineConfig::new("proj"),
    );
    let mut s = e.session();
    let mut stages = Vec::new();
    let r = block_on(s.ask("How many shipped orders?", &mut |st| stages.push(st))).unwrap();
    let Outcome::Answer(a) = r.outcome else { panic!("{:?}", r.outcome) };
    assert_eq!(
        *e.model().requests.borrow(),
        vec![(2, 0.5), (3, 0.5)],
        "the rest of the samples in one extra call"
    );
    assert_eq!(a.result.batch.columns[0].get(0), Cell::I64(61));
    assert!((a.confidence - 0.6).abs() < 1e-6, "3 of 5 samples: {}", a.confidence);
    assert_eq!(a.samples, 5);
    assert_eq!(a.alternatives.len(), 1);
    assert_eq!((a.alternatives[0].votes, a.alternatives[0].rows), (2, 1));
    assert!(a.alternatives[0].sql.contains("'pending'"));
    assert!(e.warehouse().runs.borrow().len() <= 3, "at most max_executions distinct runs");
    assert!(stages.contains(&Stage::Voting { groups: 2, top: 3, total: 5 }), "{stages:?}");
}

#[test]
fn a_fragmented_vote_abstains_as_uncertain() {
    let mut cfg = EngineConfig::new("proj");
    cfg.min_confidence = 0.5;
    cfg.max_samples = 3;
    let e = Engine::new(
        ScriptModel::native(vec![SHIPPED, PENDING, ALL]),
        voting_warehouse(),
        catalog(),
        cfg,
    );
    let mut s = e.session();
    let r = block_on(s.ask("How many orders?", &mut ())).unwrap();
    let Outcome::Abstain(AbstainReason::Uncertain { alternatives }) = r.outcome else {
        panic!("{:?}", r.outcome)
    };
    assert_eq!(alternatives.len(), 3, "every reading offered: {alternatives:?}");
    assert!(alternatives.iter().all(|a| a.votes == 1));
}

#[test]
fn declines_outvoting_answers_abstain() {
    let e = Engine::new(
        ScriptModel::native(vec![
            "CANNOT_ANSWER: no weather data",
            SHIPPED,
            "CANNOT_ANSWER: no weather data",
            "CANNOT_ANSWER: nope",
            PENDING,
        ]),
        voting_warehouse(),
        catalog(),
        EngineConfig::new("proj"),
    );
    let mut s = e.session();
    let r = block_on(s.ask("Will it rain on shipped orders?", &mut ())).unwrap();
    assert!(
        matches!(r.outcome, Outcome::Abstain(AbstainReason::NotAnswerable { ref reason }) if reason == "no weather data"),
        "{:?}",
        r.outcome
    );
}

#[test]
fn verified_answers_are_served_from_the_cache_without_the_model() {
    let e = Engine::new(
        ScriptModel::native(vec![SHIPPED, SHIPPED_TOO]),
        voting_warehouse(),
        catalog(),
        EngineConfig::new("proj"),
    )
    .with_cache(crab_engine::MemoryCache::new(64));
    let mut s = e.session();
    s.set_today(Date::parse("2026-09-30").unwrap());
    let first = block_on(s.ask("How many shipped orders?", &mut ())).unwrap();
    let mut stages = Vec::new();
    let again = block_on(s.ask("  how many SHIPPED orders ", &mut |st| stages.push(st))).unwrap();
    let (Outcome::Answer(a), Outcome::Answer(b)) = (&first.outcome, &again.outcome) else {
        panic!()
    };
    assert!(b.cached && !a.cached);
    assert_eq!(
        (b.sql.as_str(), b.fingerprint, b.confidence, b.samples),
        (a.sql.as_str(), a.fingerprint, 1.0, 0)
    );
    assert_eq!(e.model().requests.borrow().len(), 1, "no model call the second time");
    assert_eq!((again.spend.input_tokens, again.spend.micro_usd), (0, 0));
    assert_eq!(stages.first(), Some(&Stage::CacheHit));
    assert_eq!(
        e.warehouse().dry_runs.borrow().len(),
        1,
        "no dry run either: the cap is remembered"
    );
    // Another day: relative dates may resolve differently, so ask afresh.
    s.set_today(Date::parse("2026-10-01").unwrap());
    e.model().replies.borrow_mut().extend([SHIPPED, SHIPPED]);
    let r = block_on(s.ask("How many shipped orders?", &mut ())).unwrap();
    assert!(matches!(r.outcome, Outcome::Answer(ref a) if !a.cached));
}

// ---- grammar-constrained decoding ----------------------------------------------

#[test]
fn grammar_capable_models_decode_under_the_schema_grammar() {
    for (lark, gbnf, expect) in
        [(true, false, "lark:"), (false, true, "gbnf:"), (true, true, "lark:")]
    {
        let mut m = ScriptModel::new(vec![GOOD]);
        m.caps.constraints.lark = lark;
        m.caps.constraints.gbnf = gbnf;
        let e = Engine::new(m, ScriptWarehouse::default(), catalog(), EngineConfig::new("proj"));
        let mut s = e.session();
        let r = block_on(s.ask("How many orders per status?", &mut ())).unwrap();
        assert!(matches!(r.outcome, Outcome::Answer(_)), "{:?}", r.outcome);
        let c = &e.model().constraints.borrow()[0];
        assert!(c.starts_with(expect), "{}", &c[..40]);
        assert!(
            c.contains("\"o.status\"") && c.contains("\"`proj.shop.orders`\""),
            "catalog identifiers are grammar literals"
        );
        assert!(!c.contains("statuz"));
        let prompts = e.model().prompts.borrow();
        assert!(
            prompts[0].0.contains("decoded under a grammar"),
            "grammar rules in the system instruction"
        );
    }
    // A model without grammar support, or `constrain: false`: free text.
    let mut cfg = EngineConfig::new("proj");
    cfg.constrain = false;
    let mut m = ScriptModel::new(vec![GOOD]);
    m.caps.constraints.lark = true;
    let e = Engine::new(m, ScriptWarehouse::default(), catalog(), cfg);
    block_on(e.session().ask("How many orders per status?", &mut ())).unwrap();
    assert_eq!(e.model().constraints.borrow()[0], "none");
    assert!(!e.model().prompts.borrow()[0].0.contains("decoded under a grammar"));
}

// ---- large schemas: linking ---------------------------------------------------

fn wide_catalog() -> Catalog {
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
    let table = |ds: &str, name: &str, desc: &str, cols: Vec<ColumnSpec>| TableSpec {
        dataset: ds.into(),
        name: name.into(),
        kind: TableKind::Table,
        description: Some(desc.into()),
        row_count: 1000,
        logical_bytes: 0,
        last_modified_ms: 0,
        require_partition_filter: false,
        columns: cols,
    };
    let mut b = CatalogBuilder::new("proj");
    b.add_dataset("shop", "US");
    b.add_dataset("bikes", "US");
    b.add_table(table(
        "shop",
        "orders",
        "One row per order",
        vec![col("order_id", "INT64", 1), col("user_id", "INT64", 2), col("status", "STRING", 3)],
    ));
    b.add_table(table(
        "shop",
        "users",
        "Customers",
        vec![col("id", "INT64", 1), col("country", "STRING", 2)],
    ));
    b.add_table(table(
        "shop",
        "products",
        "Catalog of products",
        vec![col("id", "INT64", 1), col("brand", "STRING", 2)],
    ));
    b.add_table(table(
        "bikes",
        "trips",
        "Bike share trips",
        vec![col("trip_id", "INT64", 1), col("duration_minutes", "INT64", 2)],
    ));
    Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
}

const BY_COUNTRY: &str = "SELECT u.country, COUNT(*) AS n FROM `proj.shop.orders` AS o JOIN `proj.shop.users` AS u ON o.user_id = u.id GROUP BY u.country";

#[test]
fn large_schemas_link_tables_under_an_enum_and_list_joins() {
    let mut cfg = EngineConfig::new("proj");
    cfg.schema_token_budget = 60; // the whole schema does not fit
    let mut m = ScriptModel::new(vec![r#"{"tables": ["shop.orders", "shop.users"]}"#, BY_COUNTRY]);
    m.caps.constraints.json_schema = true;
    let e = Engine::new(m, ScriptWarehouse::default(), wide_catalog(), cfg);
    let mut s = e.session();
    let mut stages = Vec::new();
    let r = block_on(s.ask("How many orders per customer country?", &mut |st| stages.push(st)))
        .unwrap();
    assert!(matches!(r.outcome, Outcome::Answer(_)), "{:?}", r.outcome);
    let prompts = e.model().prompts.borrow();
    let constraints = e.model().constraints.borrow();
    // 1. The linking call: cards of candidates, an enum of real names only.
    assert!(prompts[0].0.starts_with("You select the BigQuery tables"));
    let cards = &prompts[0].1[0].0;
    assert!(
        cards.contains("shop.orders (~1000 rows): order_id, user_id, status  -- One row per order"),
        "{cards}"
    );
    assert!(
        constraints[0].starts_with("json:")
            && constraints[0].contains(r#""enum":["#)
            && constraints[0].contains(r#""shop.users""#),
        "{}",
        constraints[0]
    );
    assert!(!constraints[0].contains("bogus"));
    // 2. The SQL call: only the linked tables, and how they join.
    let schema = &prompts[1].1[0].0;
    assert!(
        schema.contains("TABLE `proj.shop.orders` AS o")
            && schema.contains("TABLE `proj.shop.users` AS u"),
        "{schema}"
    );
    assert!(!schema.contains("trips") && !schema.contains("products"), "{schema}");
    assert!(schema.ends_with("Joins:\n  o.user_id = u.id\n"), "{schema}");
    assert!(
        stages.iter().any(|s| matches!(s, Stage::Linking { chosen: 2, model: true, .. })),
        "{stages:?}"
    );
    assert_eq!(r.spend.input_tokens, 2000, "both calls are accounted");
}

#[test]
fn lexical_linking_alone_needs_no_model_call() {
    let mut cfg = EngineConfig::new("proj");
    cfg.schema_token_budget = 60;
    cfg.link.model = false;
    let e = Engine::new(
        ScriptModel::new(vec![BY_COUNTRY]),
        ScriptWarehouse::default(),
        wide_catalog(),
        cfg,
    );
    let mut s = e.session();
    let mut stages = Vec::new();
    let r =
        block_on(s.ask("How many orders per user country?", &mut |st| stages.push(st))).unwrap();
    assert!(matches!(r.outcome, Outcome::Answer(_)), "{:?}", r.outcome);
    assert_eq!(e.model().prompts.borrow().len(), 1, "one call: the SQL");
    let schema = &e.model().prompts.borrow()[0].1[0].0;
    assert!(
        schema.contains("AS o") && schema.contains("AS u") && !schema.contains("trips"),
        "{schema}"
    );
    assert!(stages.iter().any(|s| matches!(s, Stage::Linking { model: false, .. })));
}
