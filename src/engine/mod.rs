// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! # `engine`
//!
//! Text-to-SQL over any [`Model`] and any [`Warehouse`].
//!
//! An [`Engine`] owns the model, the warehouse, the catalog and the
//! pre-rendered schema context; it is built once and shared by reference. A
//! [`Session`] borrows it (no `Arc`) and owns only reusable buffers, so a warm
//! session allocates little per question. [`Session::ask`]:
//!
//! 1. assembles the context — schema with profile hints (cached prefix),
//!    today's date, the stored values the question names (grounded through
//!    the value index: "Brazil" → `country = 'Brasil'`), the question;
//! 0. answers a question already answered with confidence (same day, same
//!    catalog) from the **verified-answer cache**: no model call at all;
//! 2. generates candidates **adaptively**: on backends with native
//!    n-sampling (one prompt charge for all samples) it draws a small first
//!    batch; a single sample is **streamed through a viability check** that
//!    aborts it the moment it can no longer be valid SQL;
//! 3. validates it — strict parse, then bind against the catalog — and on
//!    failure runs a **targeted repair** round carrying BigQuery-style
//!    diagnostics (the schema prefix stays cached, so repair pays for the
//!    delta). String literals compared with columns whose complete value
//!    set is known are checked too: `status = 'complete'` against
//!    `'Complete'` would silently return nothing, so it is repaired instead;
//! 4. deduplicates candidates by meaning-level fingerprint, **dry-runs**
//!    the distinct ones (free, concurrently) and abstains with the figure
//!    instead of spending money when the favourite would scan too much;
//! 5. executes the few most-voted distinct candidates concurrently under
//!    byte caps and **votes by result** — different SQL returning the same
//!    rows agrees. Only when candidates disagree are more samples drawn (one
//!    extra call); a fragmented vote abstains as uncertain instead of
//!    guessing. Every answer carries its confidence and the alternatives.
//!
//! On a model that decodes under a grammar (Lark or GBNF, per its caps),
//! every sample is generated under the **schema grammar** of the tables in
//! context (`grammar`): tables and columns cannot be invented, only
//! chosen.
//!
//! Every stage is reported to an [`Events`] sink for progressive UX.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

use crate::base::date::Date;
use crate::base::{Budget, Spend};
use crate::catalog::Catalog;
use crate::exec::{QueryResult, RunOpts, Warehouse};
use crate::ir::canon::emit_canonical;
use crate::ir::fingerprint::fingerprint;
use crate::ir::{BindOpts, Binder};
use crate::llm::{Candidates, Constraint, Finish, Flow, GenRequest, Model, Part, Prompt, Sink};
use crate::prompt::{
    context_line, extract_sql, literal_into, question as question_part, repair as repair_part,
    RenderOpts, SchemaContext, CANNOT_ANSWER, GRAMMAR_RULES, SYSTEM,
};
use crate::sql::ast::{BinOp, Expr, LitKind};
use crate::sql::{viability, Mode, Parser, Viability};
use crate::values::{LiteralCheck, ValueIndex};
use objstore_r::join::join_all;
use std::fmt;
use std::fmt::Write as _;
use std::time::Instant;
use vote::{Pool, State};

pub use crate::link::SchemaIndex;
pub use cache::{AnswerCache, CachedAnswer, MemoryCache, NoCache};

pub mod cache;
mod context;
mod vote;

/// Engine settings.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Project for unqualified table names.
    pub default_project: String,
    /// Dataset for one-part table names.
    pub default_dataset: Option<String>,
    /// Largest schema context sent, tokens (≈ 4 bytes each).
    pub schema_token_budget: u32,
    /// Output cap per sample, tokens.
    pub max_output_tokens: u32,
    /// Sampling temperature for the first candidate.
    pub temperature: f32,
    /// Repair rounds after a failed validation.
    pub repair_rounds: u8,
    /// Refuse queries that would scan more than this, bytes.
    pub max_scan_bytes: u64,
    /// Rows returned.
    pub max_rows: u32,
    /// Per-question spending limits.
    pub budget: Budget,
    /// Schema rendering.
    pub render: RenderOpts,
    /// Most grounded values added to a prompt (0 disables grounding).
    pub max_grounded: usize,
    /// Repair SQL that compares a column with a value it never holds.
    pub check_literals: bool,
    /// Samples in the first batch (clamped to the model's native
    /// n-sampling; a model without it draws one).
    pub samples: u8,
    /// Most samples per question, first batch included; more are drawn
    /// only when executed candidates disagree.
    pub max_samples: u8,
    /// Temperature when drawing more than one sample.
    pub sample_temperature: f32,
    /// Most distinct candidates executed per question.
    pub max_executions: u8,
    /// Below this share of votes the engine abstains as uncertain.
    pub min_confidence: f32,
    /// Answers at or above this confidence are cached.
    pub cache_confidence: f32,
    /// Decode under the schema grammar when the model accepts one (Lark
    /// or GBNF): no invented table or column can be generated.
    pub constrain: bool,
    /// Table linking for schemas over the token budget.
    pub link: LinkConfig,
}

/// Table linking for large schemas.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LinkConfig {
    /// Most tables linked (before join-path completion).
    pub max_tables: usize,
    /// Refine the lexical choice with one model call under an enum of
    /// table names (when the model accepts a JSON schema).
    pub model: bool,
    /// Candidate tables shown to that call when the whole catalog's cards
    /// do not fit `card_budget`.
    pub candidates: usize,
    /// When the cards of every table (one per table family) fit this many
    /// tokens, the linking call sees them all — best recall, and a
    /// byte-stable prefix the provider caches across questions.
    pub card_budget: u32,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self { max_tables: 6, model: true, candidates: 30, card_budget: 2_000 }
    }
}

impl EngineConfig {
    /// Defaults for `project`.
    #[must_use]
    pub fn new(project: impl Into<String>) -> Self {
        Self {
            default_project: project.into(),
            default_dataset: None,
            schema_token_budget: 12_000,
            max_output_tokens: 600,
            temperature: 0.0,
            repair_rounds: 1,
            max_scan_bytes: 10 << 30,
            max_rows: 1_000,
            budget: Budget::default(),
            render: RenderOpts::default(),
            max_grounded: 8,
            check_literals: true,
            samples: 2,
            max_samples: 5,
            sample_temperature: 0.5,
            max_executions: 3,
            min_confidence: 0.4,
            cache_confidence: 0.66,
            constrain: true,
            link: LinkConfig::default(),
        }
    }
}

/// Progress stages, for progressive UX.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// Context assembled: tables and approximate tokens.
    Context {
        /// Tables in context.
        tables: u32,
        /// Approximate prompt tokens.
        tokens: u32,
    },
    /// Generating (round 0 = first attempt).
    Generating {
        /// Round.
        round: u8,
    },
    /// A sample was aborted mid-stream as invalid.
    StreamAborted,
    /// Validating against the catalog.
    Validating,
    /// Repairing after a validation failure.
    Repairing {
        /// Round (1-based).
        round: u8,
    },
    /// Dry run done: bytes the query will scan.
    Estimated {
        /// Bytes.
        bytes: u64,
    },
    /// Executing distinct candidates.
    Executing {
        /// Candidates executed concurrently.
        candidates: u8,
    },
    /// Executed candidates were grouped by result.
    Voting {
        /// Distinct results.
        groups: u8,
        /// Votes for the leading result.
        top: u8,
        /// Votes cast.
        total: u8,
    },
    /// Answered from the verified-answer cache.
    CacheHit,
    /// Tables linked (large schemas only).
    Linking {
        /// Candidate tables considered.
        candidates: u32,
        /// Tables chosen (with join bridges).
        chosen: u32,
        /// Whether a model call refined the choice.
        model: bool,
    },
    /// Finished.
    Done,
}

/// Receives progress stages. `()` ignores them.
pub trait Events {
    /// A stage was reached.
    fn stage(&mut self, _s: Stage) {}
}

impl Events for () {}

impl<F: FnMut(Stage)> Events for F {
    fn stage(&mut self, s: Stage) {
        self(s);
    }
}

/// Why the engine did not answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AbstainReason {
    /// No valid SQL after the allowed repairs; `problems` says why.
    InvalidSql {
        /// The last diagnostics.
        problems: Vec<String>,
    },
    /// The query would scan more than allowed.
    TooExpensive {
        /// Bytes it would scan.
        bytes: u64,
        /// The limit.
        limit: u64,
    },
    /// The per-question budget ran out.
    BudgetExhausted,
    /// The schema cannot answer the question; `reason` explains why.
    NotAnswerable {
        /// The model's short explanation.
        reason: String,
    },
    /// Candidates disagreed too much to pick one; the competing readings,
    /// most votes first (a host can offer them as choices).
    Uncertain {
        /// Competing answers.
        alternatives: Vec<Alternative>,
    },
}

/// A competing reading of the question.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Alternative {
    /// Canonical SQL.
    pub sql: String,
    /// Samples that produced it (or an equivalent result).
    pub votes: u8,
    /// Rows it returned.
    pub rows: u64,
}

/// An answer.
#[derive(Clone, Debug)]
pub struct Answer {
    /// Canonical SQL that was executed.
    pub sql: String,
    /// The model's SQL as written.
    pub model_sql: String,
    /// The result.
    pub result: QueryResult,
    /// Meaning-level fingerprint of the query.
    pub fingerprint: u128,
    /// Repair rounds used.
    pub repairs: u8,
    /// Share of samples whose result agrees with this one (1.0 when all
    /// agree; cached answers keep the confidence they were cached with).
    pub confidence: f32,
    /// Samples drawn (0 when answered from the cache).
    pub samples: u8,
    /// Executed candidates with a different result, most votes first.
    pub alternatives: Vec<Alternative>,
    /// Answered from the verified-answer cache.
    pub cached: bool,
}

/// What [`Session::ask`] produced.
#[derive(Clone, Debug)]
pub enum Outcome {
    /// Answered.
    Answer(Box<Answer>),
    /// Declined, with a reason the user can act on.
    Abstain(AbstainReason),
}

/// A finished question: outcome plus everything it cost.
#[derive(Clone, Debug)]
pub struct Report {
    /// The outcome.
    pub outcome: Outcome,
    /// Tokens, bytes and money spent.
    pub spend: Spend,
    /// Wall time, milliseconds.
    pub wall_ms: u64,
}

/// Infrastructure failures (the question itself was not the problem).
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The model call failed.
    Model(crate::llm::Error),
    /// The warehouse call failed for a reason other than the SQL.
    Warehouse(crate::exec::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Model(e) => write!(f, "model: {e}"),
            Self::Warehouse(e) => write!(f, "warehouse: {e}"),
        }
    }
}

impl std::error::Error for Error {}

/// Result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The engine: model, warehouse, catalog, pre-rendered context and the
/// verified-answer cache.
#[derive(Debug)]
pub struct Engine<M: Model, W: Warehouse, C: AnswerCache = NoCache> {
    model: M,
    warehouse: W,
    catalog: Catalog,
    schema: SchemaContext,
    values: ValueIndex,
    cache: C,
    cfg: EngineConfig,
    /// System instruction (with grammar rules when constrained).
    system: String,
    /// Schema linking index (built only for schemas over the budget).
    links: crate::link::SchemaIndex,
    /// Cards of every table family for the linking call, when they fit.
    all_cards: Option<context::Cards>,
    /// Grammar dialect the model decodes under, if any.
    dialect: Option<Dialect>,
}

/// Grammar dialects a model may accept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dialect {
    Lark,
    Gbnf,
}

impl<M: Model, W: Warehouse> Engine<M, W> {
    /// Build an engine; renders the schema context once.
    #[must_use]
    pub fn new(model: M, warehouse: W, catalog: Catalog, mut cfg: EngineConfig) -> Self {
        if cfg.default_dataset.is_none() {
            let mut ds = catalog.datasets();
            if let (Some(only), None) = (ds.next(), ds.next()) {
                cfg.default_dataset = Some(only.name.to_owned());
            }
        }
        let schema = SchemaContext::build(&catalog, None, cfg.render);
        let values = ValueIndex::build(&catalog);
        let caps = model.caps().constraints;
        let dialect = if !cfg.constrain {
            None
        } else if caps.lark {
            Some(Dialect::Lark)
        } else if caps.gbnf {
            Some(Dialect::Gbnf)
        } else {
            None
        };
        let system =
            if dialect.is_some() { format!("{SYSTEM}{GRAMMAR_RULES}") } else { SYSTEM.to_owned() };
        let links = if schema.text().len()
            > graph_r::text::bytes_for_tokens(u64::from(cfg.schema_token_budget))
        {
            crate::link::SchemaIndex::build(&catalog)
        } else {
            crate::link::SchemaIndex::default()
        };
        let all_cards = if links.is_empty() {
            None
        } else {
            let reps: Vec<crate::catalog::TableId> = links.representatives().collect();
            Some(context::Cards::build(&catalog, &links, &reps)).filter(|c| {
                c.text.len() <= graph_r::text::bytes_for_tokens(u64::from(cfg.link.card_budget))
            })
        };
        Self {
            model,
            warehouse,
            catalog,
            schema,
            values,
            cache: NoCache,
            cfg,
            system,
            links,
            all_cards,
            dialect,
        }
    }

    /// Remember verified answers in `cache`.
    #[must_use]
    pub fn with_cache<C: AnswerCache>(self, cache: C) -> Engine<M, W, C> {
        let Self {
            model,
            warehouse,
            catalog,
            schema,
            values,
            cfg,
            system,
            links,
            all_cards,
            dialect,
            ..
        } = self;
        Engine {
            model,
            warehouse,
            catalog,
            schema,
            values,
            cache,
            cfg,
            system,
            links,
            all_cards,
            dialect,
        }
    }
}

impl<M: Model, W: Warehouse, C: AnswerCache> Engine<M, W, C> {
    /// A session (per user or conversation) borrowing this engine.
    #[must_use]
    pub fn session(&self) -> Session<'_, M, W, C> {
        Session {
            e: self,
            parser: Parser::new(),
            stream_parser: Parser::new(),
            binder: Binder::new(),
            cands: Candidates::new(),
            schema_buf: String::new(),
            ctx: Vec::new(),
            grounded: Vec::new(),
            grammar: None,
            sql: String::new(),
            today: Date::today(),
        }
    }

    /// The catalog.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// The rendered schema context.
    pub fn schema(&self) -> &SchemaContext {
        &self.schema
    }

    /// The value index (built from the catalog's column profiles).
    pub fn values(&self) -> &ValueIndex {
        &self.values
    }

    /// The model.
    pub fn model(&self) -> &M {
        &self.model
    }

    /// The warehouse.
    pub fn warehouse(&self) -> &W {
        &self.warehouse
    }

    /// Settings.
    pub fn config(&self) -> &EngineConfig {
        &self.cfg
    }

    /// The verified-answer cache.
    pub fn cache(&self) -> &C {
        &self.cache
    }
}

/// Aborts a streamed sample as soon as it can no longer become valid SQL.
struct StreamGuard<'p> {
    parser: &'p mut Parser,
    aborted: bool,
}

/// Minimum characters before judging a stream (short prefixes are always viable).
const MIN_JUDGE: usize = 12;

impl Sink for StreamGuard<'_> {
    fn delta(&mut self, _cand: u8, text: &str, _delta: &str) -> Flow {
        let t = text.trim_start();
        // A fenced answer: judge the body; a closing fence ends it (stop reading).
        let body = if let Some(rest) = t.strip_prefix("```") {
            match rest.find('\n') {
                Some(nl) => &rest[nl + 1..],
                None => return Flow::Continue,
            }
        } else {
            t
        };
        if body.contains("```") {
            return Flow::Abort; // complete; the rest is chatter we need not pay for
        }
        let body = body.strip_prefix("SQL:").unwrap_or(body).trim_start();
        // A declared non-answer is a valid, short reply: let it finish.
        if CANNOT_ANSWER.starts_with(&body[..body.len().min(CANNOT_ANSWER.len())]) {
            return Flow::Continue;
        }
        if body.len() < MIN_JUDGE {
            return Flow::Continue;
        }
        match viability(self.parser, body, Mode::Strict) {
            Viability::Invalid(_) => {
                self.aborted = true;
                Flow::Abort
            }
            _ => Flow::Continue,
        }
    }
}

/// A session: borrows the engine, owns reusable buffers.
pub struct Session<'e, M: Model, W: Warehouse, C: AnswerCache = NoCache> {
    e: &'e Engine<M, W, C>,
    parser: Parser,
    stream_parser: Parser,
    binder: Binder,
    cands: Candidates,
    schema_buf: String,
    /// Tables in context, canonical order.
    ctx: Vec<crate::catalog::TableId>,
    /// Stored values the current question names.
    grounded: Vec<crate::values::Grounding>,
    /// The schema grammar for the tables in `ctx`, when constrained.
    grammar: Option<(Vec<crate::catalog::TableId>, String)>,
    sql: String,
    today: Date,
}

impl<M: Model, W: Warehouse, C: AnswerCache> fmt::Debug for Session<'_, M, W, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session").field("today", &self.today).finish_non_exhaustive()
    }
}

/// Validation result of one candidate.
enum Checked {
    /// Canonical SQL, fingerprint, how many tables it reads, and literals
    /// that can never match their column.
    Ok { canonical: String, fp: u128, tables: u64, literals: Vec<String> },
    /// What is wrong, as BigQuery-style messages.
    Invalid(Vec<String>),
}

impl<M: Model, W: Warehouse, C: AnswerCache> Session<'_, M, W, C> {
    /// Use this date as "today" (the session clock for relative dates).
    pub fn set_today(&mut self, d: Date) {
        self.today = d;
    }

    /// Answer `question`, reporting stages to `events`.
    pub async fn ask<E: Events>(&mut self, question: &str, events: &mut E) -> Result<Report> {
        let started = Instant::now();
        let mut budget = self.e.cfg.budget;
        let outcome = self.run(question, events, &mut budget).await?;
        events.stage(Stage::Done);
        Ok(Report { outcome, spend: budget.spent(), wall_ms: started.elapsed().as_millis() as u64 })
    }

    /// Draw `n` samples into `self.cands`. One sample streams through the
    /// viability guard; several are drawn in one call (one prompt charge).
    async fn sample<E: Events>(
        &mut self,
        prompt_tail: &str,
        n: u8,
        round: u8,
        events: &mut E,
        budget: &mut Budget,
    ) -> Result<()> {
        events.stage(Stage::Generating { round });
        let today = context_line(self.today);
        let parts = [Part::cached(&self.schema_buf), Part::text(&today), Part::text(prompt_tail)];
        let cfg = &self.e.cfg;
        let constraint = match (self.e.dialect, &self.grammar) {
            (Some(Dialect::Lark), Some((_, g))) => Constraint::Lark(g),
            (Some(Dialect::Gbnf), Some((_, g))) => Constraint::Gbnf(g),
            _ => Constraint::None,
        };
        let req = GenRequest {
            constraint,
            n,
            temperature: if n > 1 { cfg.sample_temperature } else { cfg.temperature },
            max_output_tokens: cfg.max_output_tokens,
            independent: n == 1,
            ..GenRequest::new(Prompt { system: &self.e.system, parts: &parts })
        };
        let mut guard = StreamGuard { parser: &mut self.stream_parser, aborted: false };
        let usage = if n == 1 {
            self.e.model.generate(&req, &mut self.cands, &mut guard).await
        } else {
            self.e.model.generate(&req, &mut self.cands, &mut ()).await
        }
        .map_err(Error::Model)?;
        let aborted = guard.aborted;
        let _ = budget.charge(usage.spend());
        if aborted {
            events.stage(Stage::StreamAborted);
        }
        Ok(())
    }

    /// Validate the samples in `self.cands` into `pool`. Returns the
    /// problems of the first invalid sample and its SQL (for repair).
    /// Samples comparing a column with a value it never holds count as
    /// invalid unless `accept_literals` (the last round: profiles can be
    /// stale).
    fn absorb(&mut self, pool: &mut Pool, accept_literals: bool) -> Option<(Vec<String>, String)> {
        let mut first_problem: Option<(Vec<String>, String)> = None;
        for i in 0..self.cands.len() {
            pool.drawn += 1;
            let text = self.cands.text(i);
            if text.trim().is_empty() || self.cands.finish(i) == Finish::Filtered {
                first_problem.get_or_insert_with(|| {
                    (vec!["The model returned no SQL.".into()], String::new())
                });
                continue;
            }
            let sql = extract_sql(text).to_owned();
            if let Some(reason) = sql.strip_prefix(CANNOT_ANSWER) {
                pool.decline(reason.trim_start_matches([':', ' ']).trim());
                continue;
            }
            match self.check(&sql) {
                Checked::Ok { literals, .. } if !literals.is_empty() && !accept_literals => {
                    first_problem.get_or_insert((literals, sql));
                }
                Checked::Ok { canonical, fp, tables, .. } => pool.vote(canonical, &sql, fp, tables),
                Checked::Invalid(p) => {
                    first_problem.get_or_insert((p, sql));
                }
            }
        }
        first_problem
    }

    /// Dry-run and execute the most-voted pending candidates, concurrently.
    /// Returns an abstention when the favourite is too expensive or the
    /// budget is spent.
    async fn execute<E: Events>(
        &mut self,
        pool: &mut Pool,
        events: &mut E,
        budget: &mut Budget,
    ) -> Result<Option<AbstainReason>> {
        let e = self.e;
        let idx = pool.to_execute(usize::from(e.cfg.max_executions.max(1)));
        if idx.is_empty() {
            return Ok(None);
        }
        let favourite = idx[0] == top_candidate(pool);
        // Free dry runs: warehouse-side validation and the cost guardrail.
        let drys =
            join_all(idx.iter().map(|&i| e.warehouse.dry_run(&pool.cands[i].canonical))).await;
        let mut runs: Vec<(usize, u64)> = Vec::with_capacity(idx.len());
        let mut remaining = budget.remaining_warehouse_bytes();
        for (k, (&i, dry)) in idx.iter().zip(drys).enumerate() {
            let dry = match dry {
                Ok(d) => d,
                Err(err) if err.is_sql_error() => {
                    pool.cands[i].state = State::Failed(err.to_string());
                    continue;
                }
                Err(err) => return Err(Error::Warehouse(err)),
            };
            if k == 0 {
                events.stage(Stage::Estimated { bytes: dry.bytes_processed });
            }
            if dry.bytes_processed > e.cfg.max_scan_bytes {
                if k == 0 && favourite {
                    return Ok(Some(AbstainReason::TooExpensive {
                        bytes: dry.bytes_processed,
                        limit: e.cfg.max_scan_bytes,
                    }));
                }
                pool.cands[i].state = State::Failed(format!(
                    "Query would scan {} bytes, over the limit",
                    dry.bytes_processed
                ));
                continue;
            }
            // BigQuery bills at least 10 MiB per referenced table, rounded
            // up per table: allow the estimate plus headroom over that
            // floor, never beyond the remaining budget.
            let floor = pool.cands[i].tables * (10 << 20);
            let cap = (dry.bytes_processed + dry.bytes_processed / 10 + (1 << 20))
                .max(floor + (1 << 20))
                .min(remaining);
            if cap == 0 {
                if runs.is_empty() && !pool.has_result() {
                    return Ok(Some(AbstainReason::BudgetExhausted));
                }
                continue;
            }
            remaining -= cap;
            runs.push((i, cap));
        }
        if runs.is_empty() {
            return Ok(None);
        }
        events.stage(Stage::Executing { candidates: runs.len() as u8 });
        let max_rows = e.cfg.max_rows;
        let results = join_all(runs.iter().map(|&(i, cap)| {
            e.warehouse.run(
                &pool.cands[i].canonical,
                RunOpts { max_bytes_billed: Some(cap), max_rows, ..RunOpts::default() },
            )
        }))
        .await;
        for (&(i, cap), res) in runs.iter().zip(results) {
            match res {
                Ok(r) => {
                    let _ = budget
                        .charge(Spend { warehouse_bytes: r.bytes_billed, ..Spend::default() });
                    let h = crate::verify::hash_batch(&r.batch);
                    // Column order and names do not matter; truncation does.
                    let key = (h.loose as u64)
                        ^ ((h.loose >> 64) as u64).rotate_left(29)
                        ^ h.rows
                        ^ u64::from(r.truncated);
                    pool.cands[i].state = State::Done { result: Box::new(r), key, bytes: cap };
                }
                Err(crate::exec::Error::BytesLimit { .. }) => {
                    if i == top_candidate(pool) && !pool.has_result() {
                        return Ok(Some(AbstainReason::TooExpensive { bytes: cap, limit: cap }));
                    }
                    pool.cands[i].state = State::Failed("Query exceeded its byte cap".into());
                }
                Err(err) if err.is_sql_error() => {
                    pool.cands[i].state = State::Failed(err.to_string());
                }
                Err(err) => return Err(Error::Warehouse(err)),
            }
        }
        Ok(None)
    }

    fn check(&mut self, sql: &str) -> Checked {
        let opts = BindOpts {
            default_project: &self.e.cfg.default_project,
            default_dataset: self.e.cfg.default_dataset.as_deref(),
            mode: Mode::Strict,
        };
        let parsed = match self.parser.parse(sql, Mode::Strict) {
            Ok(p) => p,
            Err(e) => {
                let near: String =
                    sql.get(e.sp.start as usize..).unwrap_or("").chars().take(24).collect();
                return Checked::Invalid(vec![format!("Syntax error: {} near \"{near}\"", e.msg)]);
            }
        };
        let bound = self.binder.bind(&parsed, &self.e.catalog, opts);
        if !bound.ok() {
            return Checked::Invalid(
                bound
                    .diags
                    .iter()
                    .filter(|d| d.severity == crate::ir::Severity::Error)
                    .map(|d| d.message(sql))
                    .collect(),
            );
        }
        let mut canonical = String::with_capacity(sql.len() + 64);
        emit_canonical(&parsed, bound, &self.e.catalog, &mut canonical);
        let literals = if self.e.cfg.check_literals {
            literal_problems(self.e, &parsed, bound)
        } else {
            Vec::new()
        };
        Checked::Ok {
            canonical,
            fp: fingerprint(&parsed, bound, &self.e.catalog),
            tables: bound.tables.len().max(1) as u64,
            literals,
        }
    }

    /// Answer from the verified-answer cache, if it holds this question.
    async fn answer_cached<E: Events>(
        &mut self,
        key: u128,
        events: &mut E,
        budget: &mut Budget,
    ) -> Result<Option<Outcome>> {
        let Some(hit) = self.e.cache.get(key).await else { return Ok(None) };
        events.stage(Stage::CacheHit);
        let cap = hit.max_bytes_billed.min(budget.remaining_warehouse_bytes());
        events.stage(Stage::Executing { candidates: 1 });
        let opts = RunOpts {
            max_bytes_billed: Some(cap),
            max_rows: self.e.cfg.max_rows,
            ..RunOpts::default()
        };
        match self.e.warehouse.run(&hit.sql, opts).await {
            Ok(result) => {
                let _ = budget
                    .charge(Spend { warehouse_bytes: result.bytes_billed, ..Spend::default() });
                Ok(Some(Outcome::Answer(Box::new(Answer {
                    model_sql: hit.sql.clone(),
                    sql: hit.sql,
                    result,
                    fingerprint: hit.fingerprint,
                    repairs: 0,
                    confidence: hit.confidence,
                    samples: 0,
                    alternatives: Vec::new(),
                    cached: true,
                }))))
            }
            // The data outgrew the cap or the schema moved on: answer afresh.
            Err(err)
                if err.is_sql_error() || matches!(err, crate::exec::Error::BytesLimit { .. }) =>
            {
                Ok(None)
            }
            Err(err) => Err(Error::Warehouse(err)),
        }
    }

    async fn run<E: Events>(
        &mut self,
        question: &str,
        events: &mut E,
        budget: &mut Budget,
    ) -> Result<Outcome> {
        let key = cache::key(self.e.catalog.generation(), self.today.days(), question);
        if let Some(hit) = self.answer_cached(key, events, budget).await? {
            return Ok(hit);
        }
        let (tables, tokens) = self.context(question, events, budget).await?;
        self.ensure_grammar();
        events.stage(Stage::Context { tables, tokens });
        let cfg = &self.e.cfg;
        let mut q = String::new();
        self.grounding_text(&mut q);
        q.push_str(&question_part(question));

        // Samples share one prompt charge only on native n-sampling.
        let native = self.e.model.caps().max_candidates.max(1);
        let max_samples = cfg.max_samples.max(1);
        let first = cfg.samples.clamp(1, max_samples).min(native);
        let rounds = cfg.repair_rounds;
        let mut pool = Pool::default();
        let mut problems: Vec<String> = Vec::new();
        let mut round = 0u8;
        // 1. A valid, executed candidate — repairing while rounds remain.
        loop {
            if budget.check(Spend::default()).is_err() {
                return Ok(Outcome::Abstain(AbstainReason::BudgetExhausted));
            }
            let (n, tail) = if round == 0 {
                (first, q.clone())
            } else {
                (1, repair_part(question, &self.sql, &problems))
            };
            self.sample(&tail, n, round, events, budget).await?;
            events.stage(Stage::Validating);
            if let Some((p, sql)) = self.absorb(&mut pool, round == rounds) {
                problems = p;
                self.sql = sql;
            }
            if pool.viable() == 0 && pool.declines * 2 >= pool.drawn {
                let reason = pool.decline_reason.take().unwrap_or_default();
                return Ok(Outcome::Abstain(AbstainReason::NotAnswerable { reason }));
            }
            if let Some(r) = self.execute(&mut pool, events, budget).await? {
                return Ok(Outcome::Abstain(r));
            }
            if pool.has_result() {
                break;
            }
            let failures = pool.failures();
            if let Some(last) =
                pool.cands.iter().rev().find(|c| matches!(c.state, State::Failed(_)))
            {
                if !failures.is_empty() {
                    self.sql.clone_from(&last.model_sql);
                    problems = vec![failures[failures.len() - 1].clone()];
                }
            }
            if round == rounds {
                return Ok(Outcome::Abstain(AbstainReason::InvalidSql { problems }));
            }
            round += 1;
            events.stage(Stage::Repairing { round });
        }

        // 2. Disagreement: draw the rest of the samples once, execute the
        //    new distinct candidates, vote again.
        let agreed =
            |pool: &Pool| pool.groups().first().is_some_and(|g| g.votes == pool.total_votes());
        if !agreed(&pool)
            && native > 1
            && u32::from(max_samples) > pool.drawn
            && budget.check(Spend::default()).is_ok()
        {
            let n = (u32::from(max_samples) - pool.drawn).min(u32::from(native)) as u8;
            self.sample(&q, n, round, events, budget).await?;
            self.absorb(&mut pool, true);
            if let Some(r) = self.execute(&mut pool, events, budget).await? {
                if !pool.has_result() {
                    return Ok(Outcome::Abstain(r));
                }
            }
        }

        self.decide(pool, round, key, events).await
    }

    /// Pick the answer from the vote (or abstain) and cache it when
    /// confident.
    async fn decide<E: Events>(
        &mut self,
        mut pool: Pool,
        round: u8,
        key: u128,
        events: &mut E,
    ) -> Result<Outcome> {
        let groups = pool.groups();
        let total = pool.total_votes().max(1);
        let top = &groups[0];
        events.stage(Stage::Voting {
            groups: groups.len() as u8,
            top: top.votes as u8,
            total: total as u8,
        });
        if pool.declines > top.votes {
            let reason = pool.decline_reason.take().unwrap_or_default();
            return Ok(Outcome::Abstain(AbstainReason::NotAnswerable { reason }));
        }
        let confidence = top.votes as f32 / total as f32;
        let alternatives: Vec<Alternative> = groups
            .iter()
            .map(|g| {
                let c = &pool.cands[g.members[0]];
                let rows = match &c.state {
                    State::Done { result, .. } => result.total_rows,
                    _ => 0,
                };
                Alternative { sql: c.canonical.clone(), votes: g.votes as u8, rows }
            })
            .collect();
        let cfg = &self.e.cfg;
        if confidence < cfg.min_confidence {
            return Ok(Outcome::Abstain(AbstainReason::Uncertain { alternatives }));
        }
        let c = pool.cands.swap_remove(top.members[0]);
        let State::Done { result, bytes, .. } = c.state else {
            unreachable!("groups hold executed candidates")
        };
        if confidence >= cfg.cache_confidence {
            self.e
                .cache
                .put(
                    key,
                    CachedAnswer {
                        sql: c.canonical.clone(),
                        fingerprint: c.fp,
                        confidence,
                        max_bytes_billed: bytes,
                    },
                )
                .await;
        }
        Ok(Outcome::Answer(Box::new(Answer {
            sql: c.canonical,
            model_sql: c.model_sql,
            result: *result,
            fingerprint: c.fp,
            repairs: round,
            confidence,
            samples: pool.drawn as u8,
            alternatives: alternatives.into_iter().skip(1).collect(),
            cached: false,
        })))
    }
}

/// The candidate with the most votes (first seen on ties), failed or not.
fn top_candidate(pool: &Pool) -> usize {
    let mut best = 0;
    for (i, c) in pool.cands.iter().enumerate() {
        if c.votes > pool.cands[best].votes {
            best = i;
        }
    }
    best
}

/// String literals compared (`=`, `!=`, `IN`) with a column whose complete
/// value set is known and which the column never holds, as repair messages.
fn literal_problems<M: Model, W: Warehouse, C: AnswerCache>(
    e: &Engine<M, W, C>,
    parsed: &crate::sql::Parsed<'_>,
    bound: &crate::ir::Bound,
) -> Vec<String> {
    let ast = parsed.ast;
    let mut out: Vec<String> = Vec::new();
    let column = |x: crate::sql::ast::ExprId| match bound.refs.get(x.raw() as usize) {
        Some(&crate::ir::Ref::Column { col, .. }) => Some(col),
        _ => None,
    };
    let literal = |x: crate::sql::ast::ExprId| match ast.exprs[x] {
        Expr::Lit { kind: LitKind::Str, sp } => crate::sql::lexer::string_value(parsed.text(sp)),
        _ => None,
    };
    let mut check = |col: crate::catalog::ColId, lit: &str| {
        let LiteralCheck::Absent { closest } = e.values.check(col, lit) else { return };
        let Some(c) = e.catalog.column(col) else { return };
        let alias = e.schema.alias(c.table).unwrap_or("");
        let mut m = String::new();
        let _ = write!(m, "Value ");
        literal_into(lit, true, &mut m);
        let _ = write!(m, " never occurs in column {alias}.{}", c.path);
        if let Some(v) = closest {
            m.push_str("; did you mean ");
            literal_into(&v, true, &mut m);
            m.push('?');
        } else if let Some(p) = e.catalog.profile(col) {
            m.push_str("; its values are ");
            for (i, (v, _)) in p.values().take(12).enumerate() {
                if i > 0 {
                    m.push_str(", ");
                }
                literal_into(v, true, &mut m);
            }
        }
        if !out.contains(&m) {
            out.push(m);
        }
    };
    for (_, x) in ast.exprs.iter() {
        match *x {
            Expr::Binary { op: BinOp::Eq | BinOp::Ne, l, r } => {
                for (a, b) in [(l, r), (r, l)] {
                    if let (Some(col), Some(lit)) = (column(a), literal(b)) {
                        check(col, &lit);
                    }
                }
            }
            Expr::InList { e: subject, list, .. } => {
                if let Some(col) = column(subject) {
                    for &item in ast.expr_lists.slice(list) {
                        if let Some(lit) = literal(item) {
                            check(col, &lit);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// Execution match of two results (row order ignored).
#[must_use]
pub fn same_result(a: &QueryResult, b: &QueryResult) -> bool {
    let (ha, hb) = (crate::verify::hash_batch(&a.batch), crate::verify::hash_batch(&b.batch));
    ha.compare(&hb, false) == crate::verify::Match::Same && a.truncated == b.truncated
}
