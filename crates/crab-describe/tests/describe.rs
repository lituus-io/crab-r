// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// A whole describe run against scripted services: a staged table produced
// by CTAS from a raw table in another dataset. Pass-through and rename
// columns inherit the raw descriptions (no model call); a planted wrong
// description is contradicted by the model's own check and flagged with a
// correction; a derived column is generated and validated; checks run as
// one scan; the report, schema file and etag-guarded apply follow.

use crab_catalog::{CatalogBuilder, ColumnSpec, TableKind, TableSpec};
use crab_core::batch::{Batch, Cell, ColType};
use crab_describe::describe::{describe_discovered, Action, DescribeConfig, Discovery};
use crab_describe::producers::Producer;
use crab_describe::report::{self, Applied};
use crab_exec::{DryRun, QueryResult, RunOpts, TableMeta, TableMetadata, Warehouse};
use crab_lineage::NoLineage;
use crab_llm::{
    CacheSupport, Candidates, Caps, ConstraintSupport, Finish, GenRequest, Model, Price, Sink,
    Usage,
};
use objstore_r::Bytes;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::future::{ready, Future, Ready};

// ---- scripted model -----------------------------------------------------------

struct Script {
    caps: Caps,
    replies: RefCell<Vec<String>>,
    prompts: RefCell<Vec<(String, String, String)>>,
}

impl Model for Script {
    type Gen<'a, S>
        = Ready<crab_llm::Result<Usage>>
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
        let schema = match req.constraint {
            crab_llm::Constraint::JsonSchema(s) => s.to_owned(),
            _ => String::new(),
        };
        self.prompts.borrow_mut().push((
            req.prompt.system.to_owned(),
            req.prompt.parts[0].text.to_owned(),
            schema,
        ));
        let reply = self.replies.borrow_mut().remove(0);
        out.reset(1);
        out.push(0, &reply, sink);
        out.set_finish(0, Finish::Stop);
        ready(Ok(Usage {
            input_tokens: 2_000,
            cached_tokens: 0,
            output_tokens: 300,
            micro_usd: 1_350,
            estimated: false,
            wall_ms: 1,
        }))
    }
}

// ---- scripted warehouse + metadata ----------------------------------------------

#[derive(Default)]
struct Wh {
    runs: RefCell<Vec<String>>,
    metas: RefCell<HashMap<String, String>>,
    patches: RefCell<Vec<(String, String, String)>>,
    etag_conflict: bool,
}

impl Warehouse for Wh {
    type Dry<'a> = Ready<crab_exec::Result<DryRun>>;
    type Run<'a> = Ready<crab_exec::Result<QueryResult>>;
    fn dry_run<'a>(&'a self, _: &'a str) -> Self::Dry<'a> {
        ready(Ok(DryRun { bytes_processed: 1 << 20, schema: vec![] }))
    }
    fn run<'a>(&'a self, sql: &'a str, _: RunOpts) -> Self::Run<'a> {
        self.runs.borrow_mut().push(sql.to_owned());
        // Aggregates: c0 = NULL count of status (planted: 12), c1 = rows with
        // order_date after their timestamp (0). Query checks: one row.
        let n = sql.matches(" AS c").count().max(1);
        let names: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
        let schema: Vec<(&str, ColType)> =
            names.iter().map(|s| (s.as_str(), ColType::Int64)).collect();
        let mut b = Batch::new(&schema, 1);
        for (i, c) in b.columns.iter_mut().enumerate() {
            c.push(Cell::I64(if i == 0 { 12 } else { 0 })).unwrap();
        }
        ready(Ok(QueryResult {
            batch: b,
            total_rows: 1,
            truncated: false,
            bytes_processed: 1 << 20,
            bytes_billed: 10 << 20,
            cache_hit: false,
            wall_ms: 1,
        }))
    }
}

fn parse(raw: &str) -> TableMeta {
    // Reuse the client parser through a table_get round trip.
    struct One(String);
    impl objstore_r::transport::Transport for One {
        type Send<'a> = Ready<objstore_r::Result<objstore_r::transport::Response>>;
        type Body<'a> = NoBody;
        type Open<'a> = Ready<objstore_r::Result<(u16, NoBody)>>;
        fn send<'a>(&'a self, _: objstore_r::transport::Request<'a>) -> Self::Send<'a> {
            ready(Ok(objstore_r::transport::Response {
                status: 200,
                body: Bytes::from(self.0.clone()),
            }))
        }
        fn open<'a>(&'a self, _: objstore_r::transport::Request<'a>) -> Self::Open<'a> {
            unreachable!()
        }
    }
    struct NoBody;
    impl objstore_r::transport::BodyStream for NoBody {
        type Next<'a> = Ready<objstore_r::Result<Option<Bytes>>>;
        fn next_chunk(&mut self) -> Self::Next<'_> {
            ready(Ok(None))
        }
    }
    let bq = crab_exec::bigquery::BigQuery::new(
        One(raw.to_owned()),
        &crab_exec::bigquery::BigQueryConfig {
            project: "lab".into(),
            location: "US".into(),
            endpoint: None,
        },
    );
    block_on(bq.table_get("x", "y", "z")).unwrap().unwrap()
}

impl TableMetadata for Wh {
    type Get<'a> = Ready<crab_exec::Result<Option<TableMeta>>>;
    type Patch<'a> = Ready<crab_exec::Result<TableMeta>>;
    fn table_get<'a>(&'a self, p: &'a str, d: &'a str, t: &'a str) -> Self::Get<'a> {
        ready(Ok(self.metas.borrow().get(&format!("{p}.{d}.{t}")).map(|raw| parse(raw))))
    }
    fn table_patch<'a>(
        &'a self,
        p: &'a str,
        d: &'a str,
        t: &'a str,
        body: Bytes,
        etag: &'a str,
    ) -> Self::Patch<'a> {
        self.patches.borrow_mut().push((
            format!("{p}.{d}.{t}"),
            String::from_utf8(body.to_vec()).unwrap(),
            etag.to_owned(),
        ));
        if self.etag_conflict {
            return ready(Err(crab_exec::Error::PreconditionFailed));
        }
        ready(Ok(parse(r#"{"etag":"e2","type":"TABLE","schema":{"fields":[]}}"#)))
    }
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

// ---- fixture ----------------------------------------------------------------------

const STG_SCHEMA: &str = r#"{"etag":"e1","type":"TABLE","numRows":"1000","schema":{"fields":[{"name":"order_id","type":"INTEGER","policyTags":{"names":["pt"]}},{"name":"amount_usd","type":"NUMERIC"},{"name":"status","type":"STRING","description":"Always set: one of shipped, pending."},{"name":"order_date","type":"DATE"}]}}"#;
const RAW_SCHEMA: &str = r#"{"etag":"r1","type":"TABLE","numRows":"1200","schema":{"fields":[{"name":"order_id","type":"INTEGER","description":"Order key."},{"name":"amount","type":"NUMERIC","description":"Order amount in USD."},{"name":"status","type":"STRING"},{"name":"ts","type":"TIMESTAMP"}]}}"#;
const CTAS: &str = "CREATE OR REPLACE TABLE lab.stg.orders AS SELECT order_id, amount AS amount_usd, status, DATE(ts) AS order_date FROM lab.raw.orders WHERE status != 'TEST'";

fn discovery(wh: &Wh) -> Discovery {
    let col = |p: &str, t: &str, o: u32, d: Option<&str>| ColumnSpec {
        path: p.into(),
        data_type: t.into(),
        ordinal: o,
        nullable: true,
        partitioning: false,
        hidden: false,
        clustering: None,
        description: d.map(Into::into),
    };
    let mut b = CatalogBuilder::new("lab");
    b.add_table(TableSpec {
        dataset: "stg".into(),
        name: "orders".into(),
        kind: TableKind::Table,
        description: None,
        row_count: 1000,
        logical_bytes: 1 << 20,
        last_modified_ms: 0,
        require_partition_filter: false,
        columns: vec![
            col("order_id", "INT64", 1, None),
            col("amount_usd", "NUMERIC", 2, None),
            col("status", "STRING", 3, Some("Always set: one of shipped, pending.")),
            col("order_date", "DATE", 4, None),
        ],
    });
    wh.metas.borrow_mut().insert("lab.stg.orders".into(), STG_SCHEMA.into());
    wh.metas.borrow_mut().insert("lab.raw.orders".into(), RAW_SCHEMA.into());
    let mut metas = HashMap::new();
    metas.insert("lab.stg.orders".to_owned(), parse(STG_SCHEMA));
    let mut producers = BTreeMap::new();
    producers.insert(
        "lab.stg.orders".to_owned(),
        Producer {
            table: "lab.stg.orders".into(),
            statement_type: "CREATE_TABLE_AS_SELECT".into(),
            query: CTAS.into(),
            writes: 30,
            writers: 1,
            first_ms: 1,
            last_ms: 2,
            others: vec![],
        },
    );
    Discovery {
        builder: b,
        home: "lab".into(),
        in_scope: vec!["lab.stg.orders".into()],
        metas,
        producers,
        warnings: vec![],
        spend: Default::default(),
    }
}

const DRAFT: &str = r#"{"table_description": "One row per order, staged from raw orders excluding test orders.",
  "grain": ["order_id"],
  "table_checks": [{"claim": "one row per order_id", "sql": "SELECT COUNT(*) - COUNT(DISTINCT order_id) AS dup FROM lab.stg.orders", "expect": "0"}],
  "columns": [
    {"column": "status", "description": "Always set: one of shipped, pending.", "semantic_type": "category", "checks": [], "statements": [{"claim": "Always set", "expr": "COUNTIF(status IS NULL)", "expect": "0"}]},
    {"column": "order_date", "description": "Calendar date of the order timestamp.", "semantic_type": "date", "checks": [{"claim": "not in the future", "expr": "COUNTIF(order_date > CURRENT_DATE())", "expect": "0"}]}
  ]}"#;

const JUDGE: &str = r#"{"verdicts": [
    {"check": 1, "verdict": "supports", "reason": "0 duplicates"},
    {"check": 2, "verdict": "contradicts", "reason": "12 of 1000 rows are NULL"},
    {"check": 3, "verdict": "contradicts", "reason": "125348 future dates"}],
  "columns": [
    {"column": "status", "description": "Order status (shipped, pending); NULL for about 1% of rows.", "confidence": "high"},
    {"column": "order_date", "description": "Calendar date of the order timestamp.", "confidence": "high"}],
  "table_description": "One row per order, staged from raw orders excluding test orders.",
  "more_checks": []}"#;

fn model() -> Script {
    Script {
        caps: Caps {
            id: "script".into(),
            constraints: ConstraintSupport { lark: false, gbnf: false, json_schema: true },
            max_candidates: 1,
            max_stream_candidates: 1,
            cache: CacheSupport::None,
            context_tokens: 1_000_000,
            max_output_tokens: 8_192,
            streaming: true,
            price: Price { input: 300_000, cached_input: 30_000, output: 2_500_000 },
        },
        replies: RefCell::new(vec![DRAFT.into(), JUDGE.into()]),
        prompts: RefCell::new(vec![]),
    }
}

#[test]
fn a_dataset_is_described_validated_and_applied() {
    let wh = Wh::default();
    let m = model();
    let mut cfg = DescribeConfig::new(vec!["lab.stg".into()], "US", "lab");
    cfg.max_rounds = 2;
    let r = block_on(describe_discovered(&m, &wh, &NoLineage, &cfg, discovery(&wh)));
    assert_eq!(r.levels, vec![vec!["lab.stg.orders".to_owned()]]);
    let t = &r.tables[0];
    let col = |n: &str| t.columns.iter().find(|c| c.column == n).unwrap();

    // Inherited verbatim: pass-through and rename of documented raw columns.
    assert_eq!(
        (col("order_id").action, col("order_id").description.as_deref()),
        (Action::Propagate, Some("Order key."))
    );
    assert_eq!(
        (col("amount_usd").action, col("amount_usd").description.as_deref()),
        (Action::Propagate, Some("Order amount in USD."))
    );
    assert_eq!(col("amount_usd").transform, Some(crab_ir::Transform::Rename));
    // The planted wrong description is flagged with evidence and a fix.
    let status = col("status");
    assert_eq!(status.action, Action::Flag);
    assert_eq!(status.existing.as_deref(), Some("Always set: one of shipped, pending."));
    assert_eq!(
        status.description.as_deref(),
        Some("Order status (shipped, pending); NULL for about 1% of rows.")
    );
    // Generated and validated.
    let od = col("order_date");
    // Its only check is disputed (the judge misread it), so nothing settled
    // supports it: medium, not high.
    assert_eq!(
        (od.action, od.confidence.as_deref(), od.transform),
        (Action::Generate, Some("medium"), Some(crab_ir::Transform::Derived))
    );
    assert_eq!(od.sources, vec!["lab.raw.orders.ts"]);
    assert_eq!(
        t.description.as_deref(),
        Some("One row per order, staged from raw orders excluding test orders.")
    );
    assert_eq!(t.grain, vec!["order_id"]);

    // The judge read check #3 (result 0, expected 0) the opposite way: the
    // two readings disagree, so the check is disputed, never trusted alone.
    let c3 = t.checks.iter().find(|c| c.result.check.id == 3).unwrap();
    assert_eq!(
        (c3.verdict.as_deref(), c3.result.holds, c3.disputed),
        (Some("inconclusive"), Some(true), true)
    );
    assert!(t.warnings.iter().any(|w| w.contains("check #3: disputed")), "{:?}", t.warnings);
    // Costs: two model calls (draft, judge); aggregates in one scan.
    assert_eq!(t.model_calls, 2);
    let runs = wh.runs.borrow();
    assert_eq!(runs.iter().filter(|s| s.contains(" AS c0")).count(), 1, "{runs:?}");
    assert!(runs.iter().any(|s| s.contains("COUNT(DISTINCT order_id)")), "table check ran");
    drop(runs);
    // The draft saw the evidence and only describable columns.
    let prompts = m.prompts.borrow();
    let draft_prompt = &prompts[0].1;
    assert!(draft_prompt.contains("CREATE OR REPLACE TABLE lab.stg.orders"), "producer SQL shown");
    assert!(
        draft_prompt.contains("DESCRIBE: order_date") && draft_prompt.contains("AUDIT: status"),
        "{draft_prompt}"
    );
    assert!(
        draft_prompt.contains("from lab.raw.orders.amount: \"Order amount in USD.\""),
        "upstream descriptions shown"
    );
    assert!(
        prompts[0].2.contains(r#""enum":["order_date","status"]"#),
        "column names constrained: {}",
        prompts[0].2
    );
    assert!(prompts[1].1.contains("#2 [status] [tests existing] claim: \"Always set\" | sql: COUNTIF(status IS NULL) | expected 0 | result 12 -> VIOLATED"), "{}", prompts[1].1);
    drop(prompts);

    // Report and schema file.
    let json = report::to_json(&r);
    assert!(json.contains(r#""action":"flag""#) && json.contains(r#""verdict":"contradicts""#));
    let schema = report::schema_file(t, false).unwrap();
    assert!(schema.starts_with(r#"[{"name":"order_id","type":"INTEGER","policyTags":{"names":["pt"]},"description":"Order key."}"#), "{schema}");
    assert!(
        schema.contains(r#""description":"Always set: one of shipped, pending.""#),
        "flag not applied without --correct"
    );
    assert!(
        report::schema_file(t, true).unwrap().contains("NULL for about 1% of rows"),
        "applied with --correct"
    );

    // Apply: fill-empty under If-Match; the existing status description stays.
    assert_eq!(
        block_on(report::apply(&wh, t, false)),
        Applied::Written { columns: 3, table: true }
    );
    let (tbl, body, etag) = wh.patches.borrow()[0].clone();
    assert_eq!((tbl.as_str(), etag.as_str()), ("lab.stg.orders", "e1"));
    assert!(
        body.contains("Order key.")
            && body.contains("Always set: one of shipped, pending.")
            && body.contains(r#""description":"One row per order"#),
        "{body}"
    );
    // The engine's catalog takes the verified text: generated, propagated
    // and the evidence-backed correction of the flagged column.
    let mut b = discovery(&Wh::default()).builder;
    assert_eq!(report::overlay(&r, &mut b), 5);
    let cat = crab_catalog::Catalog::from_bytes(Bytes::from(b.build(1, 0).unwrap())).unwrap();
    let id = cat.find_table_in("lab", "stg", "orders").unwrap();
    let d = |p: &str| cat.columns(id).find(|c| c.path == p).unwrap().description.to_owned();
    assert_eq!(d("amount_usd"), "Order amount in USD.");
    assert_eq!(d("status"), "Order status (shipped, pending); NULL for about 1% of rows.");
    assert!(cat.table(id).unwrap().description.starts_with("One row per order"));

    let wh2 = Wh { etag_conflict: true, ..Wh::default() };
    wh2.metas.borrow_mut().insert("lab.stg.orders".into(), STG_SCHEMA.into());
    assert_eq!(block_on(report::apply(&wh2, t, false)), Applied::Conflict);
}

#[test]
fn unchanged_well_documented_tables_cost_nothing() {
    let wh = Wh::default();
    let m = model();
    let mut cfg = DescribeConfig::new(vec!["lab.stg".into()], "US", "lab");
    cfg.audit_existing = false;
    let mut d = discovery(&wh);
    // Every column documented already, and the table too.
    let col = |p: &str, o: u32| ColumnSpec {
        path: p.into(),
        data_type: "STRING".into(),
        ordinal: o,
        nullable: true,
        partitioning: false,
        hidden: false,
        clustering: None,
        description: Some(format!("{p} doc")),
    };
    d.builder = CatalogBuilder::new("lab");
    d.builder.add_table(TableSpec {
        dataset: "stg".into(),
        name: "orders".into(),
        kind: TableKind::Table,
        description: Some("Orders.".into()),
        row_count: 1,
        logical_bytes: 0,
        last_modified_ms: 0,
        require_partition_filter: false,
        columns: vec![
            col("order_id", 1),
            col("amount_usd", 2),
            col("status", 3),
            col("order_date", 4),
        ],
    });
    let r = block_on(describe_discovered(&m, &wh, &NoLineage, &cfg, d));
    assert_eq!(r.tables[0].model_calls, 0);
    assert!(r.tables[0].columns.iter().all(|c| c.action == Action::Keep));
    assert!(wh.runs.borrow().is_empty());
    assert_eq!(r.spend.micro_usd, 0);
}

#[test]
fn an_unchanged_rerun_is_free_and_changes_are_described_again() {
    let cfg0 = DescribeConfig::new(vec!["lab.stg".into()], "US", "lab");
    let wh = Wh::default();
    let first = block_on(describe_discovered(&model(), &wh, &NoLineage, &cfg0, discovery(&wh)));
    let t0 = &first.tables[0];
    assert!(t0.key.is_some() && !t0.reused);
    let state = report::to_json(&first);
    assert!(state.contains(r#""prekey":""#) && state.contains(r#""reused":false"#));
    let mut cfg = cfg0.clone();
    cfg.prior = crab_describe::incremental::Prior::from_json(&state);
    assert_eq!(cfg.prior.len(), 1);

    // Same warehouse: nothing is profiled, asked or queried.
    let wh = Wh::default();
    let idle = Script { replies: RefCell::new(vec![]), ..model() };
    let again = block_on(describe_discovered(&idle, &wh, &NoLineage, &cfg, discovery(&wh)));
    let t = &again.tables[0];
    assert!(t.reused && t.model_calls == 0 && wh.runs.borrow().is_empty());
    assert_eq!(again.spend.micro_usd, 0);
    assert_eq!(t.key, t0.key);
    let brief = |r: &crab_describe::describe::TableReport| {
        r.columns
            .iter()
            .map(|c| (c.column.clone(), c.action, c.description.clone(), c.confidence.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(brief(t), brief(t0));
    assert_eq!(
        (t.description.clone(), t.grain.clone()),
        (t0.description.clone(), t0.grain.clone())
    );
    // The flag's evidence survives into the new report, and it re-reads.
    let state2 = report::to_json(&again);
    assert!(state2.contains("result 12") || state2.contains(r#""result":"12""#), "{state2}");
    assert_eq!(crab_describe::incremental::Prior::from_json(&state2), cfg.prior);

    // After applying: the filled descriptions are the report's own.
    let wh = Wh::default();
    let mut d = discovery(&wh);
    let mut spec = d.builder.tables()[0].clone();
    for c in &t0.columns {
        if let (Some(s), Some(desc)) =
            (spec.columns.iter_mut().find(|s| s.path == c.column), &c.description)
        {
            if c.action != Action::Flag {
                s.description = Some(desc.clone());
            }
        }
    }
    spec.description.clone_from(&t0.description);
    d.builder = CatalogBuilder::new("lab");
    d.builder.add_table(spec);
    let applied = block_on(describe_discovered(&idle, &wh, &NoLineage, &cfg, d));
    assert!(applied.tables[0].reused, "{:?}", applied.tables[0].warnings);

    // New producer SQL: described again.
    let wh = Wh::default();
    let mut d = discovery(&wh);
    d.producers.get_mut("lab.stg.orders").unwrap().query.push_str(" AND amount > 0");
    let m = model();
    let changed = block_on(describe_discovered(&m, &wh, &NoLineage, &cfg, d));
    assert!(!changed.tables[0].reused && changed.tables[0].model_calls == 2);

    // Upstream text changed (data did not): described again, and profiled
    // now, since discovery skipped it as unchanged.
    let wh = Wh::default();
    let d = discovery(&wh);
    wh.metas.borrow_mut().insert(
        "lab.raw.orders".into(),
        RAW_SCHEMA.replace("Order amount in USD.", "Order amount in cents."),
    );
    let m = model();
    let up = block_on(describe_discovered(&m, &wh, &NoLineage, &cfg, d));
    assert!(!up.tables[0].reused && up.tables[0].model_calls == 2);
    assert!(
        wh.runs
            .borrow()
            .iter()
            .any(|s| s.contains("APPROX_COUNT_DISTINCT") && s.contains("stg.orders")),
        "{:?}",
        wh.runs.borrow()
    );
}

#[test]
fn an_unusable_reply_is_retried_once_then_reported() {
    let cfg = DescribeConfig::new(vec!["lab.stg".into()], "US", "lab");
    // A judge reply cut off at the token cap, then a usable one.
    let wh = Wh::default();
    let cut = r#"{"verdicts": [{"check": 1, "verdict": "supports", "reason": "0 dup"#;
    let m =
        Script { replies: RefCell::new(vec![DRAFT.into(), cut.into(), JUDGE.into()]), ..model() };
    let r = block_on(describe_discovered(&m, &wh, &NoLineage, &cfg, discovery(&wh)));
    let t = &r.tables[0];
    assert_eq!(t.model_calls, 3);
    assert!(t.key.is_some() && !t.warnings.iter().any(|w| w.contains("not usable")));
    assert_eq!(t.columns.iter().find(|c| c.column == "status").unwrap().action, Action::Flag);
    let judge: Vec<bool> =
        m.prompts.borrow().iter().map(|p| p.0.starts_with("You verify")).collect();
    assert_eq!(judge, vec![false, true, true]);

    // Unusable twice: reported, and left without a key so a rerun retries.
    let wh = Wh::default();
    let m = Script { replies: RefCell::new(vec!["{".into(), "not json".into()]), ..model() };
    let r = block_on(describe_discovered(&m, &wh, &NoLineage, &cfg, discovery(&wh)));
    let t = &r.tables[0];
    assert_eq!(t.model_calls, 2);
    assert!(t.key.is_none() && t.warnings.iter().any(|w| w == "draft reply was not usable JSON"));
}

/// A lineage service that knows where `lab.stg.orders` came from.
struct Lineage(RefCell<Vec<crab_lineage::Column>>);

impl crab_lineage::LineageSource for Lineage {
    type Search<'a> = Ready<crab_lineage::Result<crab_lineage::Graph>>;
    fn search<'a>(
        &'a self,
        roots: &'a [crab_lineage::Column],
        _: u32,
        dir: crab_lineage::Direction,
    ) -> Self::Search<'a> {
        if dir == crab_lineage::Direction::Downstream {
            // A dashboard outside the datasets reads amount_usd; an in-scope
            // dependant is not a consumer.
            let c = |t: &str, col: &str| crab_lineage::Column::new(t, col);
            let mut g = crab_lineage::Graph::new();
            g.add(c("other.bi.dash", "amt"), c("lab.stg.orders", "amount_usd"), false);
            g.add(c("other.bi.dash", "amt"), c("other.bi.fx", "rate"), false);
            g.add(c("lab.stg.orders", "order_date"), c("lab.stg.orders", "amount_usd"), false);
            return ready(Ok(g));
        }
        self.0.borrow_mut().extend(roots.iter().cloned());
        let c = |t: &str, col: &str| crab_lineage::Column::new(t, col);
        let mut g = crab_lineage::Graph::new();
        let (stg, raw) = ("lab.stg.orders", "lab.raw.orders");
        g.add(c(stg, "order_id"), c(raw, "order_id"), true);
        // Copied through a filter on status, as the service reports it.
        g.add(c(stg, "amount_usd"), c(raw, "amount"), true);
        g.add(c(stg, "amount_usd"), c(raw, "status"), false);
        g.add(c(stg, "order_date"), c(raw, "ts"), false);
        ready(Ok(g))
    }
}

#[test]
fn without_producing_sql_the_lineage_service_is_the_record() {
    let cfg = DescribeConfig::new(vec!["lab.stg".into()], "US", "lab");
    let wh = Wh::default();
    let mut d = discovery(&wh);
    d.producers.clear();
    let lin = Lineage(RefCell::new(vec![]));
    let m = model();
    let r = block_on(describe_discovered(&m, &wh, &lin, &cfg, d));
    let roots = lin.0.borrow();
    assert!(roots.iter().any(|c| c.table == "lab.stg.orders" && c.column == "order_date"));
    let t = &r.tables[0];
    let col = |n: &str| t.columns.iter().find(|c| c.column == n).unwrap();
    // Inherited from the upstream table the service named (its resource
    // read for the descriptions); the filter column is not a competing source.
    assert_eq!(
        (col("order_id").action, col("order_id").description.as_deref()),
        (Action::Propagate, Some("Order key."))
    );
    assert_eq!(
        (col("amount_usd").action, col("amount_usd").description.as_deref()),
        (Action::Propagate, Some("Order amount in USD.")),
        "a meaning, not a checkable claim: survives the filter"
    );
    assert_eq!(col("amount_usd").sources, vec!["lab.raw.orders.amount", "lab.raw.orders.status"]);
    // Computed from a column: generated, with that source as evidence.
    assert_eq!(col("order_date").action, Action::Generate);
    assert!(m.prompts.borrow()[0].1.contains("order_date DATE | from lab.raw.orders.ts"));
    assert!(t.warnings.iter().any(|w| w.contains("no producing SQL")));
}

#[test]
fn a_run_becomes_a_metadata_image() {
    use crab_meta::{MetaImage, Provenance, Transform, Walk};
    let cfg = DescribeConfig::new(vec!["lab.stg".into()], "US", "lab");
    let wh = Wh::default();
    let r = block_on(describe_discovered(&model(), &wh, &NoLineage, &cfg, discovery(&wh)));
    let img = MetaImage::from_bytes(Bytes::from(report::meta_image(&r).unwrap())).unwrap();
    let t = img.table("lab.stg.orders").unwrap();
    assert_eq!(t.statement_type(), "CREATE_TABLE_AS_SELECT");
    assert!(t.producer_sql().starts_with("CREATE OR REPLACE TABLE lab.stg.orders"));
    let amount = img.column("lab.stg.orders", "AMOUNT_USD").unwrap();
    assert_eq!(
        (amount.description(), amount.provenance(), amount.transform()),
        ("Order amount in USD.", Provenance::Propagated, Transform::Rename)
    );
    // The raw source is an external node, reached as an exact copy.
    let (p, exact) = amount.parent_edges().next().unwrap();
    let raw = img.column_at(p).unwrap();
    assert_eq!(
        (raw.table().unwrap().name(), raw.path(), exact),
        ("lab.raw.orders", "amount", true)
    );
    assert_eq!(raw.provenance(), Provenance::External);
    let status = img.column("lab.stg.orders", "status").unwrap();
    assert_eq!(status.provenance(), Provenance::Flagged);
    assert!(status.description().contains("NULL for about 1%"), "the evidence-backed correction");
    // Impact of the raw timestamp: the derived order_date.
    let mut w = Walk::new();
    let ts = img.column("lab.raw.orders", "ts").unwrap().id();
    let mut hit = Vec::new();
    img.downstream(ts, 5, &mut w, |h| {
        hit.push((img.column_at(h.column).unwrap().path(), h.exact_chain));
        true
    });
    assert_eq!(hit, vec![("order_date", false)]);
}

#[test]
fn consumers_outside_the_datasets_are_reported_and_imaged() {
    let mut cfg = DescribeConfig::new(vec!["lab.stg".into()], "US", "lab");
    cfg.downstream = true;
    let wh = Wh::default();
    let lin = Lineage(RefCell::new(vec![]));
    let r = block_on(describe_discovered(&model(), &wh, &lin, &cfg, discovery(&wh)));
    let t = &r.tables[0];
    let amount = t.columns.iter().find(|c| c.column == "amount_usd").unwrap();
    assert_eq!(amount.consumers, vec!["other.bi.dash.amt"]);
    assert!(t.columns.iter().filter(|c| c.column != "amount_usd").all(|c| c.consumers.is_empty()));
    assert!(report::to_json(&r).contains(r#""consumers":["other.bi.dash.amt"]"#));
    let img =
        crab_meta::MetaImage::from_bytes(Bytes::from(report::meta_image(&r).unwrap())).unwrap();
    let dash = img.column("other.bi.dash", "amt").unwrap();
    assert_eq!(dash.table().unwrap().kind(), "EXTERNAL");
    let src = img.column("lab.stg.orders", "amount_usd").unwrap();
    assert_eq!(src.children().map(|c| c.path()).collect::<Vec<_>>(), vec!["amt"]);
}
