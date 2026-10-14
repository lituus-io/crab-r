// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Running model-written validation checks safely and cheaply.
//!
//! The model decides what to check and writes the SQL; this module decides
//! whether that SQL may run, and runs it for as little as possible:
//!
//! - **Aggregate checks** are expressions over the table's rows
//!   (`COUNTIF(amount < 0)`, `COUNT(*) - COUNT(DISTINCT order_id)`). Each is
//!   accepted only if, alone in `SELECT (expr) AS v FROM table`, it parses
//!   in strict mode, keeps that exact shape (one item, no CTE, no set
//!   operation, FROM exactly the table — so an expression cannot smuggle in
//!   another query or table), binds cleanly with the table as the only one
//!   read, and aggregates. All accepted aggregates of a table then run as
//!   **one** SELECT — one scan per table per round — sampled above a size
//!   threshold, dry-run cost-guarded and capped by `maximumBytesBilled`.
//! - **Query checks** are whole read-only SELECTs (foreign-key containment,
//!   duplicates by key): strict parse, clean bind, dry-run guard, byte cap,
//!   a few rows back.
//!
//! A rejected or failed check returns its reason, so the next judging round
//! can repair it.

use crate::base::batch::{Batch, Cell, ColType};
use crate::catalog::{Catalog, TableId};
use crate::exec::{RunOpts, Warehouse};
use crate::ir::{BindOpts, Binder};
use crate::sql::ast::{From as FromItem, SelectItem, SetExpr};
use crate::sql::{Mode, Parser};

/// A check's SQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckSql {
    /// An aggregate expression over the table's rows.
    Aggregate(String),
    /// A whole SELECT.
    Query(String),
}

/// A validation check written by the model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Check {
    /// Identifier (stable within a table's run).
    pub id: u32,
    /// The column it is about (`None`: the table).
    pub column: Option<String>,
    /// What it tests, in words.
    pub claim: String,
    /// How.
    pub sql: CheckSql,
    /// The result that confirms the claim, as the model wrote it.
    pub expect: Option<String>,
    /// The claim comes from the column's existing description (an audit).
    pub tests_existing: bool,
}

/// What running a check produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The aggregate's value.
    Value(String),
    /// A query check's first rows (column names first).
    Rows(Vec<Vec<String>>),
    /// Refused before running, with the reason.
    Rejected(String),
    /// The warehouse refused or failed it.
    Failed(String),
}

/// A check with its outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckResult {
    /// The check.
    pub check: Check,
    /// The outcome.
    pub outcome: Outcome,
    /// Computed on a sample of the table's rows.
    pub sampled: bool,
    /// The result compared mechanically with `expect`: `Some(true)` holds,
    /// `Some(false)` violated, `None` when either is not comparable.
    pub holds: Option<bool>,
}

/// Compare a result with an expectation (`0`, `= 0`, `!= 0`, `> 0`,
/// `>= 1`, `< 5`, `<= 5`, `between 1 and 4`, `true`, `false`).
#[must_use]
pub fn evaluate(result: &str, expect: &str) -> Option<bool> {
    let e = expect.trim().trim_end_matches('.').to_ascii_lowercase();
    let r = result.trim().to_ascii_lowercase();
    if matches!(e.as_str(), "true" | "false") {
        return match r.as_str() {
            "true" | "false" => Some(r == e),
            _ => None,
        };
    }
    let num = |s: &str| s.trim().replace(',', "").parse::<f64>().ok();
    let v = num(&r)?;
    if let Some(rest) = e.strip_prefix("between ") {
        let (lo, hi) = rest.split_once(" and ")?;
        return Some(v >= num(lo)? && v <= num(hi)?);
    }
    for (op, f) in [
        (">=", (|a: f64, b: f64| a >= b) as fn(f64, f64) -> bool),
        ("<=", |a, b| a <= b),
        ("!=", |a, b| (a - b).abs() > f64::EPSILON),
        ("==", |a, b| (a - b).abs() <= f64::EPSILON),
        (">", |a, b| a > b),
        ("<", |a, b| a < b),
        ("=", |a, b| (a - b).abs() <= f64::EPSILON),
    ] {
        if let Some(rest) = e.strip_prefix(op) {
            return Some(f(v, num(rest)?));
        }
    }
    Some((v - num(&e)?).abs() <= f64::EPSILON)
}

/// Limits.
#[derive(Clone, Copy, Debug)]
pub struct RunnerConfig {
    /// Byte cap per query (merged aggregate query or query check).
    pub max_bytes: u64,
    /// Tables larger than this are sampled for aggregate checks.
    pub sample_above_bytes: u64,
    /// Query checks allowed per table per round.
    pub max_query_checks: usize,
    /// Rows returned by a query check.
    pub max_rows: u32,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self { max_bytes: 2 << 30, sample_above_bytes: 1 << 30, max_query_checks: 3, max_rows: 20 }
    }
}

/// The table under check.
#[derive(Clone, Copy, Debug)]
pub struct Target<'a> {
    /// Catalog id.
    pub id: TableId,
    /// `project.dataset.table`.
    pub fqn: &'a str,
    /// Logical bytes (for sampling).
    pub bytes: u64,
    /// For a virtual table (the result of a query that writes no table):
    /// the query it stands for. Checks then run over `FROM (query)`.
    pub source: Option<&'a str>,
}

impl Target<'_> {
    /// The FROM item that reads this table's rows.
    fn read_item(&self) -> String {
        match self.source {
            None => format!("`{}`", self.fqn),
            Some(q) => format!("({q})"),
        }
    }

    /// A validated query check (which names the table) as it runs.
    fn runnable(&self, sql: &str) -> String {
        match self.source {
            None => sql.to_owned(),
            Some(q) => sql.replace(&format!("`{}`", self.fqn), &format!("({q})")),
        }
    }
}

/// Check text that could hide statements or comments is refused before
/// parsing.
fn suspicious(s: &str) -> Option<&'static str> {
    if s.contains(';') {
        return Some("contains ';' (one expression or statement only)");
    }
    if s.contains("--") || s.contains("/*") || s.contains('#') {
        return Some("contains a comment marker");
    }
    None
}

fn opts(fqn: &str) -> BindOpts<'_> {
    let parts = crate::base::key::split_fqn(fqn);
    BindOpts {
        default_project: parts.map_or("", |(p, _, _)| p),
        default_dataset: parts.map(|(_, d, _)| d),
        mode: Mode::Strict,
    }
}

/// Validate one aggregate expression; returns it normalised (whitespace
/// collapsed) or the reason it may not run.
fn guard_aggregate(
    expr: &str,
    t: Target<'_>,
    cat: &Catalog,
    p: &mut Parser,
    b: &mut Binder,
) -> Result<String, String> {
    if let Some(why) = suspicious(expr) {
        return Err(why.into());
    }
    let norm = expr.split_whitespace().collect::<Vec<_>>().join(" ");
    if norm.is_empty() {
        return Err("empty expression".into());
    }
    let sql = format!("SELECT ({norm}) AS v FROM `{}`", t.fqn);
    let parsed = p.parse(&sql, Mode::Strict).map_err(|e| format!("does not parse: {}", e.msg))?;
    let q = parsed.ast.queries[parsed.root];
    let shape_ok = q.ctes.is_empty()
        && q.order.is_empty()
        && q.limit.is_none()
        && match parsed.ast.set_exprs[q.body] {
            SetExpr::Select(sel) => {
                let s = parsed.ast.selects[sel];
                parsed.ast.select_items.slice(s.items).len() == 1
                    && matches!(parsed.ast.select_items.slice(s.items)[0], SelectItem::Expr { .. })
                    && s.where_.is_none()
                    && matches!(s.from.map(|f| parsed.ast.froms[f]), Some(FromItem::Table { .. }))
            }
            _ => false,
        };
    if !shape_ok {
        return Err("must be a single expression over the table's rows".into());
    }
    let bound = b.bind(&parsed, cat, opts(t.fqn));
    if let Some(d) = bound.diags.iter().find(|d| d.severity == crate::ir::Severity::Error) {
        return Err(d.message(&sql));
    }
    if bound.tables != [t.id] {
        return Err("may read only the table itself (write joins as a query check)".into());
    }
    if !bound.aggregates {
        return Err(
            "must aggregate over all rows (e.g. COUNTIF(...), COUNT(DISTINCT ...), MIN/MAX)".into(),
        );
    }
    Ok(norm)
}

/// Validate a query check.
fn guard_query(
    sql: &str,
    t: Target<'_>,
    cat: &Catalog,
    p: &mut Parser,
    b: &mut Binder,
) -> Result<String, String> {
    if let Some(why) = suspicious(sql) {
        return Err(why.into());
    }
    let parsed = p.parse(sql, Mode::Strict).map_err(|e| format!("does not parse: {}", e.msg))?;
    let bound = b.bind(&parsed, cat, opts(t.fqn));
    if let Some(d) = bound.diags.iter().find(|d| d.severity == crate::ir::Severity::Error) {
        return Err(d.message(sql));
    }
    if bound.tables.is_empty() {
        return Err("reads no table".into());
    }
    Ok(sql.trim().to_owned())
}

/// Render a result cell for the judge.
fn render(b: &Batch, col: usize, row: usize) -> String {
    let c = &b.columns[col];
    match c.get(row) {
        Cell::Null => "NULL".into(),
        Cell::Bool(v) => v.to_string(),
        Cell::I64(v) => match c.ty {
            ColType::Timestamp | ColType::DateTime => {
                let days = v.div_euclid(86_400_000_000);
                let secs = v.rem_euclid(86_400_000_000) / 1_000_000;
                let date = crate::base::date::Date::from_days(days as i32)
                    .map_or_else(|_| days.to_string(), |d| d.to_string());
                format!("{date} {:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60)
            }
            _ => v.to_string(),
        },
        Cell::I32(d) => {
            crate::base::date::Date::from_days(d).map_or_else(|_| d.to_string(), |d| d.to_string())
        }
        Cell::F64(v) => format!("{v}"),
        Cell::I128(v) => {
            let s = crate::base::batch::NUMERIC_SCALE;
            let (i, f) = (v / s, (v % s).abs());
            if f == 0 {
                i.to_string()
            } else {
                format!("{i}.{}", format!("{f:09}").trim_end_matches('0'))
            }
        }
        Cell::Str(s) => s.chars().take(200).collect(),
        Cell::Bytes(b) => format!("<{} bytes>", b.len()),
    }
}

/// Dry-run guard, then run under a byte cap.
/// Table references in `sql` (CTE references included: an upper bound).
fn tables_named(sql: &str) -> u64 {
    let mut p = Parser::new();
    p.parse_script(sql, Mode::Full).map_or(1, |s| {
        s.ast.froms.iter().filter(|(_, f)| matches!(f, FromItem::Table { .. })).count() as u64
    })
}

async fn guarded_run<W: Warehouse>(
    w: &W,
    sql: &str,
    cfg: RunnerConfig,
    max_rows: u32,
) -> Result<crate::exec::QueryResult, String> {
    let dry = w.dry_run(sql).await.map_err(|e| e.to_string())?;
    if dry.bytes_processed > cfg.max_bytes {
        return Err(format!(
            "would scan {} bytes, over the {} byte cap",
            dry.bytes_processed, cfg.max_bytes
        ));
    }
    // BigQuery bills at least 10 MiB per table read: the cap allows that
    // floor for every table the SQL names (a ceiling; billing is actual).
    let floor = (10u64 << 20) * tables_named(sql).max(1) + (1 << 20);
    let cap = (dry.bytes_processed + dry.bytes_processed / 10 + (1 << 20))
        .max(floor)
        .min(cfg.max_bytes.max(floor));
    w.run(sql, RunOpts { max_bytes_billed: Some(cap), max_rows, ..RunOpts::default() })
        .await
        .map_err(|e| e.to_string())
}

/// Run `checks` against table `t`. Returns the results (in check order) and
/// the bytes billed.
pub async fn run<W: Warehouse>(
    w: &W,
    cat: &Catalog,
    t: Target<'_>,
    checks: &[Check],
    cfg: RunnerConfig,
) -> (Vec<CheckResult>, u64) {
    let mut p = Parser::new();
    let mut b = Binder::new();
    // A query's result cannot be sampled (TABLESAMPLE reads tables only).
    let sampled =
        t.source.is_none() && t.bytes > cfg.sample_above_bytes && cfg.sample_above_bytes > 0;
    let mut results: Vec<CheckResult> = checks
        .iter()
        .map(|c| CheckResult {
            check: c.clone(),
            outcome: Outcome::Rejected(String::new()),
            sampled: false,
            holds: None,
        })
        .collect();
    let mut billed = 0u64;

    // 1. Guard every check; dedup identical aggregates.
    let mut aggs: Vec<(String, Vec<usize>)> = Vec::new();
    let mut queries: Vec<(usize, String)> = Vec::new();
    for (i, c) in checks.iter().enumerate() {
        match &c.sql {
            CheckSql::Aggregate(e) => match guard_aggregate(e, t, cat, &mut p, &mut b) {
                Ok(norm) => match aggs.iter_mut().find(|(x, _)| x.eq_ignore_ascii_case(&norm)) {
                    Some((_, ids)) => ids.push(i),
                    None => aggs.push((norm, vec![i])),
                },
                Err(why) => results[i].outcome = Outcome::Rejected(why),
            },
            CheckSql::Query(q) => {
                if queries.len() >= cfg.max_query_checks {
                    results[i].outcome = Outcome::Rejected(format!(
                        "more than {} query checks for one table",
                        cfg.max_query_checks
                    ));
                    continue;
                }
                match guard_query(q, t, cat, &mut p, &mut b) {
                    Ok(sql) => queries.push((i, t.runnable(&sql))),
                    Err(why) => results[i].outcome = Outcome::Rejected(why),
                }
            }
        }
    }

    // 2. One merged query for every aggregate.
    let sample = if sampled {
        format!(
            " TABLESAMPLE SYSTEM ({} PERCENT)",
            ((cfg.sample_above_bytes as f64 / t.bytes as f64) * 100.0).clamp(0.1, 100.0)
        )
    } else {
        String::new()
    };
    let tail = format!(" FROM {}{sample}", t.read_item());
    let merged = (!aggs.is_empty()).then(|| {
        let items: Vec<String> =
            aggs.iter().enumerate().map(|(k, (e, _))| format!("({e}) AS c{k}")).collect();
        format!("SELECT {}{tail}", items.join(", "))
    });
    let tail = tail.as_str();
    // A merged query fails as a whole when one expression is invalid at run
    // time (a type BigQuery refuses): find the culprits by dry-running each
    // expression alone (free, concurrent) and run the rest again.
    let agg_run = async {
        let sql = merged.as_deref()?;
        let first = guarded_run(w, sql, cfg, 1).await;
        if first.is_ok() || aggs.len() < 2 {
            return Some((first, Vec::new()));
        }
        let singles = objstore_r::join::join_all(aggs.iter().map(|(e, _)| async move {
            let one = format!("SELECT ({e}) AS c0 FROM {}", t.read_item());
            w.dry_run(&one).await.err().map(|err| err.to_string())
        }))
        .await;
        if singles.iter().all(Option::is_none) {
            return Some((first, Vec::new()));
        }
        let keep: Vec<usize> = (0..aggs.len()).filter(|&k| singles[k].is_none()).collect();
        let bad: Vec<(usize, String)> =
            singles.into_iter().enumerate().filter_map(|(k, e)| e.map(|e| (k, e))).collect();
        if keep.is_empty() {
            return Some((Err(String::new()), bad));
        }
        let items: Vec<String> =
            keep.iter().enumerate().map(|(j, &k)| format!("({}) AS c{j}", aggs[k].0)).collect();
        let retry = format!("SELECT {}{tail}", items.join(", "));
        let r = guarded_run(w, &retry, cfg, 1).await.map(|mut r| {
            // Re-align result columns with `aggs` (dropped ones become NULL).
            let mut cols = Vec::with_capacity(aggs.len());
            let mut names = Vec::with_capacity(aggs.len());
            let mut it = r.batch.columns.drain(..);
            for k in 0..aggs.len() {
                names.push(format!("c{k}"));
                if keep.contains(&k) {
                    cols.push(
                        it.next()
                            .unwrap_or_else(|| crate::base::batch::Column::new(ColType::String, 0)),
                    );
                } else {
                    cols.push(crate::base::batch::Column::new(ColType::String, 0));
                }
            }
            drop(it);
            r.batch.columns = cols;
            r.batch.names = names;
            r
        });
        Some((r, bad))
    };
    let query_runs = objstore_r::join::join_all(
        queries.iter().map(|(_, sql)| guarded_run(w, sql, cfg, cfg.max_rows)),
    );
    let (agg, qs) = std::future::join!(agg_run, query_runs).await;

    // 3. Scatter results back.
    if let Some((res, bad)) = agg {
        for (k, why) in &bad {
            for &i in &aggs[*k].1 {
                results[i].outcome = Outcome::Failed(why.clone());
            }
        }
        let failed: Vec<usize> = bad.iter().map(|(k, _)| *k).collect();
        match res {
            Ok(r) => {
                billed += r.bytes_billed;
                for (k, (_, ids)) in aggs.iter().enumerate() {
                    if failed.contains(&k) {
                        continue;
                    }
                    let v = if r.batch.rows() > 0 && k < r.batch.columns.len() {
                        render(&r.batch, k, 0)
                    } else {
                        "NULL".into()
                    };
                    for &i in ids {
                        results[i].outcome = Outcome::Value(v.clone());
                        results[i].sampled = sampled;
                    }
                }
            }
            Err(e) => {
                for (k, (_, ids)) in aggs.iter().enumerate() {
                    if failed.contains(&k) {
                        continue;
                    }
                    for &i in ids {
                        results[i].outcome = Outcome::Failed(e.clone());
                    }
                }
            }
        }
    }
    for ((i, _), r) in queries.iter().zip(qs) {
        results[*i].outcome = match r {
            Ok(r) => {
                billed += r.bytes_billed;
                let mut rows = vec![r.batch.names.clone()];
                for row in 0..r.batch.rows() {
                    rows.push(
                        (0..r.batch.columns.len()).map(|c| render(&r.batch, c, row)).collect(),
                    );
                }
                Outcome::Rows(rows)
            }
            Err(e) => Outcome::Failed(e),
        };
    }
    for r in &mut results {
        let value = match &r.outcome {
            Outcome::Value(v) => Some(v.as_str()),
            // A query check with one row and one column is a value too.
            Outcome::Rows(rows) if rows.len() == 2 && rows[1].len() == 1 => {
                Some(rows[1][0].as_str())
            }
            _ => None,
        };
        r.holds = match (value, r.check.expect.as_deref()) {
            // A sampled uniqueness or count cannot prove a table-wide claim.
            (Some(v), Some(e)) if !r.sampled => evaluate(v, e),
            _ => None,
        };
    }
    (results, billed)
}

#[cfg(test)]
mod tests {
    use super::evaluate;

    #[test]
    fn expectations_are_compared_mechanically() {
        assert_eq!(evaluate("0", "0"), Some(true));
        assert_eq!(evaluate("12", "0"), Some(false));
        assert_eq!(evaluate("12", "> 0"), Some(true));
        assert_eq!(evaluate("3", "between 1 and 4"), Some(true));
        assert_eq!(evaluate("5", "between 1 and 4"), Some(false));
        assert_eq!(evaluate("0.5", "<= 1"), Some(true));
        assert_eq!(evaluate("true", "TRUE"), Some(true));
        assert_eq!(evaluate("1,024", ">= 1000"), Some(true));
        assert_eq!(evaluate("NULL", "0"), None);
        assert_eq!(evaluate("7", "about seven"), None);
    }
}
