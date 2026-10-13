// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Running an engine over a dataset.
//!
//! Each item's question goes through [`Session::ask`]; for answerable items
//! the gold SQL runs on the gold warehouse and the two results are compared
//! row-order-insensitively with normalised values (execution match). Items
//! run `concurrency` at a time — one session each, all borrowing the same
//! engine, joined on the caller's task. Gold results are served from
//! BigQuery's result cache on reruns.

use crate::base::date::Date;
use crate::engine::{AnswerCache, Engine, Outcome, Session};
use crate::eval::dataset::Item;
use crate::eval::score::{Record, Verdict};
use crate::exec::{RunOpts, Warehouse};
use crate::llm::Model;
use crate::verify::{hash_batch, Match};

/// Run options.
#[derive(Clone, Copy, Debug)]
pub struct RunConfig {
    /// Items in flight at once.
    pub concurrency: usize,
    /// "Today" for relative dates (fixed for reproducible reruns).
    pub today: Option<Date>,
    /// Append the item's evidence (external knowledge) to the question.
    pub use_evidence: bool,
    /// Row cap for gold execution.
    pub gold_max_rows: u32,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self { concurrency: 4, today: None, use_evidence: true, gold_max_rows: 10_000 }
    }
}

/// One item's full trace, for reports and error analysis.
#[derive(Clone, Debug)]
pub struct Trace {
    /// The scored record.
    pub record: Record,
    /// Canonical SQL the engine executed, if any.
    pub sql: Option<String>,
    /// Abstention or error detail.
    pub note: Option<String>,
}

/// The question as asked (with evidence, when present and enabled).
#[must_use]
pub fn question_text(item: &Item, use_evidence: bool) -> String {
    if use_evidence && !item.evidence.trim().is_empty() {
        format!("{}\nHint: {}", item.question.trim(), item.evidence.trim())
    } else {
        item.question.trim().to_owned()
    }
}

async fn one<M: Model, W: Warehouse, C: AnswerCache, G: Warehouse>(
    session: &mut Session<'_, M, W, C>,
    gold: &G,
    item: &Item,
    cfg: RunConfig,
) -> Trace {
    let q = question_text(item, cfg.use_evidence);
    let report = session.ask(&q, &mut ()).await;
    let answerable = item.answerable();
    let mut trace = Trace {
        record: Record {
            item_id: item.id.clone(),
            answerable,
            verdict: Verdict::Error,
            confidence: 0.0,
            spend: crate::base::Spend::default(),
            latency_ms: 0,
        },
        sql: None,
        note: None,
    };
    let report = match report {
        Ok(r) => r,
        Err(e) => {
            trace.note = Some(e.to_string());
            return trace;
        }
    };
    trace.record.spend = report.spend;
    trace.record.latency_ms = report.wall_ms;
    match report.outcome {
        Outcome::Abstain(reason) => {
            trace.record.verdict = Verdict::Abstained;
            trace.note = Some(format!("{reason:?}"));
        }
        Outcome::Answer(a) => {
            trace.record.confidence = a.confidence;
            trace.sql = Some(a.sql.clone());
            let Some(gold_sql) = item.gold_sql.as_deref() else {
                trace.record.verdict = Verdict::Wrong;
                trace.note = Some("answered an unanswerable question".into());
                return trace;
            };
            let opts = RunOpts { max_rows: cfg.gold_max_rows, ..RunOpts::default() };
            match gold.run(gold_sql, opts).await {
                Ok(g) => {
                    let same = hash_batch(&a.result.batch).compare(&hash_batch(&g.batch), false)
                        == Match::Same
                        && a.result.total_rows == g.total_rows;
                    trace.record.verdict = if same { Verdict::Correct } else { Verdict::Wrong };
                }
                Err(e) => {
                    trace.record.verdict = Verdict::Error;
                    trace.note = Some(format!("gold SQL failed: {e}"));
                }
            }
        }
    }
    trace
}

/// Evaluate `engine` on `items`, comparing against gold SQL run on `gold`.
pub async fn evaluate<M: Model, W: Warehouse, C: AnswerCache, G: Warehouse>(
    engine: &Engine<M, W, C>,
    gold: &G,
    items: &[Item],
    cfg: RunConfig,
) -> Vec<Trace> {
    let lanes = cfg.concurrency.max(1);
    let mut traces: Vec<Option<Trace>> = vec![None; items.len()];
    for chunk_start in (0..items.len()).step_by(lanes) {
        let end = (chunk_start + lanes).min(items.len());
        let futs = (chunk_start..end).map(|i| {
            let item = &items[i];
            async move {
                let mut s = engine.session();
                if let Some(d) = cfg.today {
                    s.set_today(d);
                }
                (i, one(&mut s, gold, item, cfg).await)
            }
        });
        for (i, t) in objstore_r::join::join_all(futs).await {
            traces[i] = Some(t);
        }
    }
    traces.into_iter().flatten().collect()
}
