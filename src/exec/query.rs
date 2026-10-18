// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! `query`: one read-only query under a [`MetadataGuard`] (feature `http`).
//!
//! The guard's `check_query` is the only gate. It is asked before a
//! credential is touched, so a refused statement costs no token and no
//! byte on the wire, and the transport (objstore-r's client, under the same
//! guard) asks it again for the body it actually sends. Anything reading a
//! table outside `INFORMATION_SCHEMA`, `__TABLES__` and the guard's graph
//! datasets is [`Failure::RefusedByGuard`].
//!
//! The run is [`Warehouse::run`] with its `maximumBytesBilled`, row cap and
//! timeout; the results of the one job it issues are readable (and no
//! other job's) through a [`Tracked`] transport. Rows come back as
//! [`Rows`], bounded by the row cap and, when given, a token budget, with
//! `truncated` set whenever either cut something. A failure is one
//! [`QueryError`] with a typed [`Failure`]; its message never holds the
//! credential.

use crate::base::batch::{Batch, Cell, ColType, Column, NUMERIC_SCALE};
use crate::base::date::{Date, MICROS_PER_DAY};
use crate::exec::bigquery::{BigQuery, BigQueryConfig};
use crate::exec::guard::{MetadataGuard, OwnJobs, Tracked};
use crate::exec::{Error, QueryResult, RunOpts, Warehouse};
use graph_r::TokenBudget;
use objstore_r::policy::RequestPolicy;
use objstore_r::token::TokenProvider;
use std::fmt::{self, Write as _};
use std::time::Duration;

/// Why a query failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    /// The caller may not read a table the query reads, or run jobs (403).
    NoAccess,
    /// A table, dataset or project the query names does not exist, or is
    /// not visible to the caller.
    NotFound,
    /// No credential was supplied or discovered.
    NoCredentials,
    /// The credential was refused as expired or invalid (401).
    Expired,
    /// BigQuery could not be reached, or answered with a server error, a
    /// throttle, or something unreadable.
    Unreachable,
    /// The guard refused the statement; nothing was sent.
    RefusedByGuard,
    /// The query would bill more than its `maximumBytesBilled`.
    BytesLimit,
    /// BigQuery found the SQL invalid.
    InvalidQuery,
    /// The query did not finish within its timeout.
    Timeout,
}

impl Failure {
    /// The failure's wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoAccess => "no_access",
            Self::NotFound => "not_found",
            Self::NoCredentials => "no_credentials",
            Self::Expired => "expired",
            Self::Unreachable => "unreachable",
            Self::RefusedByGuard => "refused_by_guard",
            Self::BytesLimit => "bytes_limit",
            Self::InvalidQuery => "invalid_query",
            Self::Timeout => "timeout",
        }
    }

    /// The failure of a transport error.
    #[must_use]
    pub fn of_transport(e: &objstore_r::Error) -> Self {
        match e {
            objstore_r::Error::Policy(_) => Self::RefusedByGuard,
            objstore_r::Error::NoCredentials { .. } => Self::NoCredentials,
            objstore_r::Error::Auth { .. } => Self::Expired,
            _ => Self::Unreachable,
        }
    }

    /// The failure of a warehouse error.
    #[must_use]
    pub fn of_error(e: &Error) -> Self {
        match e {
            Error::Transport(t) => Self::of_transport(t),
            Error::Query { reason, .. } if reason == "notFound" => Self::NotFound,
            Error::Query { .. } => Self::InvalidQuery,
            Error::BytesLimit { .. } => Self::BytesLimit,
            Error::Denied { .. } => Self::NoAccess,
            Error::Unauthenticated { .. } => Self::Expired,
            Error::Timeout => Self::Timeout,
            Error::Api { status: 404, .. } => Self::NotFound,
            _ => Self::Unreachable,
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A failed query: what kind of failure, and a message that never holds
/// the credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryError {
    /// What kind.
    pub failure: Failure,
    /// What happened.
    pub message: String,
}

impl QueryError {
    fn new(failure: Failure, message: impl Into<String>) -> Self {
        Self { failure, message: message.into() }
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.failure, self.message)
    }
}

impl std::error::Error for QueryError {}

/// Default `maximumBytesBilled`: 1 GiB.
pub const DEFAULT_MAX_BYTES_BILLED: u64 = 1 << 30;
/// Default row cap.
pub const DEFAULT_MAX_ROWS: u32 = 1_000;
/// Default timeout, milliseconds.
pub const DEFAULT_TIMEOUT_MS: u32 = 30_000;

/// How to run one query.
#[derive(Clone, Copy, Debug)]
pub struct QueryOptions<'a> {
    /// Where the job runs; `None` lets BigQuery place it with the data.
    pub location: Option<&'a str>,
    /// Bytes billed, rows returned and time waited, at most.
    pub run: RunOpts,
    /// Rows returned fit this many tokens (about four bytes each, measured
    /// on each row as compact JSON); whole rows, in order.
    pub budget: Option<TokenBudget>,
    /// The API origin (default `https://bigquery.googleapis.com`; the guard
    /// decides whether another is reachable).
    pub endpoint: Option<&'a str>,
}

impl Default for QueryOptions<'_> {
    fn default() -> Self {
        Self {
            location: None,
            run: RunOpts {
                max_bytes_billed: Some(DEFAULT_MAX_BYTES_BILLED),
                max_rows: DEFAULT_MAX_ROWS,
                timeout_ms: DEFAULT_TIMEOUT_MS,
                use_cache: true,
            },
            budget: None,
            endpoint: None,
        }
    }
}

/// A query's rows: the first [`len`](Rows::len) rows of `batch`.
#[derive(Clone, Debug, PartialEq)]
pub struct Rows {
    /// The decoded rows (at most the row cap; more than `len` when the
    /// budget cut).
    pub batch: Batch,
    kept: usize,
    /// Rows exist beyond those returned (the row cap or the budget cut).
    pub truncated: bool,
    /// Rows the query produced.
    pub total_rows: u64,
    /// Bytes scanned.
    pub bytes_processed: u64,
    /// Bytes billed (0 for a cache hit).
    pub bytes_billed: u64,
    /// Served from BigQuery's result cache.
    pub cache_hit: bool,
}

impl Rows {
    /// The rows of `r` that fit `budget`.
    #[must_use]
    pub fn new(r: QueryResult, budget: Option<TokenBudget>) -> Self {
        let all = r.batch.rows();
        let kept = budget.map_or(all, |b| fit(&r.batch, all, b));
        Self {
            truncated: r.truncated || kept < all,
            kept,
            batch: r.batch,
            total_rows: r.total_rows,
            bytes_processed: r.bytes_processed,
            bytes_billed: r.bytes_billed,
            cache_hit: r.cache_hit,
        }
    }

    /// Rows returned.
    #[must_use]
    pub fn len(&self) -> usize {
        self.kept
    }

    /// No rows returned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.kept == 0
    }
}

/// One value as a plain scalar.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Scalar<'a> {
    /// SQL NULL.
    Null,
    /// `BOOL`.
    Bool(bool),
    /// `INT64`.
    Int(i64),
    /// `FLOAT64`.
    Float(f64),
    /// Text: `STRING`; `TIMESTAMP` as ISO 8601 UTC (`...Z`), `DATETIME`,
    /// `DATE` and `TIME` as ISO 8601; `NUMERIC` as an exact decimal;
    /// `BYTES` as base64; `STRUCT`, `ARRAY`, `JSON` and the rest as compact
    /// JSON text.
    Str(&'a str),
}

/// Row `i` of `col` as a [`Scalar`]; `buf` holds the text of a value
/// rendered here (dates, times, decimals, bytes).
#[must_use]
pub fn scalar<'a>(col: &'a Column, i: usize, buf: &'a mut String) -> Scalar<'a> {
    buf.clear();
    match (col.ty, col.get(i)) {
        (_, Cell::Null) => Scalar::Null,
        (_, Cell::Bool(b)) => Scalar::Bool(b),
        (ColType::Timestamp, Cell::I64(v)) => {
            write_datetime(v, buf);
            buf.push('Z');
            Scalar::Str(buf)
        }
        (ColType::DateTime, Cell::I64(v)) => {
            write_datetime(v, buf);
            Scalar::Str(buf)
        }
        (ColType::Time, Cell::I64(v)) => {
            write_time(v, buf);
            Scalar::Str(buf)
        }
        (_, Cell::I64(v)) => Scalar::Int(v),
        (_, Cell::I32(d)) => {
            write_date(d, buf);
            Scalar::Str(buf)
        }
        (_, Cell::F64(f)) => Scalar::Float(f),
        (_, Cell::I128(n)) => {
            write_numeric(n, buf);
            Scalar::Str(buf)
        }
        (_, Cell::Str(s)) => Scalar::Str(s),
        (_, Cell::Bytes(b)) => {
            write_base64(b, buf);
            Scalar::Str(buf)
        }
    }
}

fn write_date(days: i32, out: &mut String) {
    match Date::from_days(days) {
        Ok(d) => {
            let _ = write!(out, "{d}");
        }
        Err(_) => {
            let _ = write!(out, "{days}");
        }
    }
}

fn write_time(micros: i64, out: &mut String) {
    let (secs, frac) = (micros.div_euclid(1_000_000), micros.rem_euclid(1_000_000));
    let _ = write!(out, "{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60);
    if frac > 0 {
        let _ = write!(out, ".{frac:06}");
    }
}

fn write_datetime(micros: i64, out: &mut String) {
    let days = micros.div_euclid(MICROS_PER_DAY);
    match i32::try_from(days) {
        Ok(d) if Date::from_days(d).is_ok() => {
            write_date(d, out);
            out.push('T');
            write_time(micros.rem_euclid(MICROS_PER_DAY), out);
        }
        _ => {
            let _ = write!(out, "{micros}");
        }
    }
}

fn write_numeric(n: i128, out: &mut String) {
    let scale = NUMERIC_SCALE.unsigned_abs();
    let a = n.unsigned_abs();
    if n < 0 {
        out.push('-');
    }
    let _ = write!(out, "{}", a / scale);
    let frac = a % scale;
    if frac > 0 {
        let at = out.len();
        let _ = write!(out, ".{frac:09}");
        let keep = out[at..].trim_end_matches('0').len();
        out.truncate(at + keep);
    }
}

fn write_base64(b: &[u8], out: &mut String) {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for c in b.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for k in 0..4 {
            if k <= c.len() {
                out.push(A[(n >> (18 - 6 * k) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
}

/// How many of the first `rows` rows of `batch` fit `budget`, each
/// measured as its compact JSON object (`{"name":value,...}`).
#[must_use]
pub fn fit(batch: &Batch, rows: usize, budget: TokenBudget) -> usize {
    let allowance = u64::from(budget.0);
    let (mut spent, mut buf, mut num) = (0u64, String::new(), String::new());
    for i in 0..rows {
        let mut bytes = 1usize;
        for (name, col) in batch.names.iter().zip(&batch.columns) {
            num.clear();
            bytes += name.len() + 4;
            bytes += match scalar(col, i, &mut buf) {
                Scalar::Null | Scalar::Bool(true) => 4,
                Scalar::Bool(false) => 5,
                Scalar::Int(v) => {
                    let _ = write!(num, "{v}");
                    num.len()
                }
                Scalar::Float(v) => {
                    let _ = write!(num, "{v}");
                    num.len()
                }
                Scalar::Str(s) => s.len() + 2,
            };
        }
        spent += graph_r::text::estimate_tokens(bytes);
        if spent > allowance {
            return i;
        }
    }
    rows
}

/// The response-header wait: BigQuery's longest long poll, and margin.
const HEADER_WAIT: Duration = Duration::from_secs(25);

/// Run `sql` in `project` with the credential `tokens` (a discovery's
/// result, so a missing credential is a typed failure) under `guard`.
///
/// The guard checks `sql` first; nothing else happens when it refuses.
pub async fn query<T: TokenProvider>(
    tokens: objstore_r::Result<T>,
    guard: MetadataGuard<'_>,
    project: &str,
    sql: &str,
    opts: &QueryOptions<'_>,
) -> Result<Rows, QueryError> {
    guard
        .check_query(sql)
        .map_err(|e| QueryError::new(Failure::RefusedByGuard, format!("refused: {e}")))?;
    let tokens = tokens.map_err(|e| QueryError::new(Failure::of_transport(&e), e.to_string()))?;
    let jobs = OwnJobs::new();
    let cfg = objstore_r::http::HttpConfig {
        // `jobs.query` carries a requestId: a repeat never runs twice.
        max_retries: 2,
        timeout: HEADER_WAIT,
        ..objstore_r::http::HttpConfig::default()
    };
    let client = objstore_r::http::Client::with_policy(tokens, guard.with_own_jobs(&jobs), cfg)
        .map_err(|e| QueryError::new(Failure::Unreachable, e.to_string()))?;
    let bq = BigQuery::new(
        Tracked::new(client, &jobs),
        &BigQueryConfig {
            project: project.to_owned(),
            location: opts.location.unwrap_or_default().to_owned(),
            endpoint: opts.endpoint.map(str::to_owned),
        },
    );
    match bq.run(sql, opts.run).await {
        Ok(r) => Ok(Rows::new(r, opts.budget)),
        Err(e) => {
            let mut message = e.to_string();
            // A message BigQuery wrote: the credential it was sent never
            // stays in it.
            if !matches!(e, Error::Transport(_) | Error::Timeout) {
                if let Ok(t) = bq.transport().inner().tokens().token().await {
                    message = redact(&message, t.secret());
                }
            }
            Err(QueryError::new(Failure::of_error(&e), message))
        }
    }
}

/// `message` with every occurrence of `secret` replaced.
#[must_use]
pub fn redact(message: &str, secret: &str) -> String {
    if secret.is_empty() {
        message.to_owned()
    } else {
        message.replace(secret, "<redacted>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch() -> Batch {
        let mut b = Batch::new(
            &[
                ("s", ColType::String),
                ("i", ColType::Int64),
                ("f", ColType::Float64),
                ("b", ColType::Bool),
                ("ts", ColType::Timestamp),
                ("dt", ColType::DateTime),
                ("d", ColType::Date),
                ("t", ColType::Time),
                ("n", ColType::Numeric),
                ("y", ColType::Bytes),
                ("j", ColType::Text),
            ],
            2,
        );
        let cells = [
            Cell::Str("a\"b"),
            Cell::I64(-7),
            Cell::F64(2.5),
            Cell::Bool(true),
            Cell::I64(1_700_000_000_123_456),
            Cell::I64(86_400_000_000 + 3_600_000_000),
            Cell::I32(20_000),
            Cell::I64(45_296_000_001),
            Cell::I128(-12_500_000_000),
            Cell::Bytes(b"\x01\x02\x03\x04"),
            Cell::Str(r#"{"a":1}"#),
        ];
        for (c, v) in b.columns.iter_mut().zip(cells) {
            c.push(v).unwrap();
            c.push_null();
        }
        b
    }

    fn row(b: &Batch, i: usize) -> Vec<String> {
        let mut buf = String::new();
        b.columns.iter().map(|c| format!("{:?}", scalar(c, i, &mut buf))).collect()
    }

    #[test]
    fn every_type_decodes_to_a_plain_scalar() {
        let b = batch();
        assert_eq!(
            row(&b, 0),
            [
                r#"Str("a\"b")"#,
                "Int(-7)",
                "Float(2.5)",
                "Bool(true)",
                r#"Str("2023-11-14T22:13:20.123456Z")"#,
                r#"Str("1970-01-02T01:00:00")"#,
                r#"Str("2024-10-04")"#,
                r#"Str("12:34:56.000001")"#,
                r#"Str("-12.5")"#,
                r#"Str("AQIDBA==")"#,
                r#"Str("{\"a\":1}")"#,
            ]
        );
        assert!(row(&b, 1).iter().all(|c| c == "Null"));
    }

    #[test]
    fn decimals_and_bytes_render_exactly() {
        let mut s = String::new();
        for (n, want) in [
            (0, "0"),
            (NUMERIC_SCALE, "1"),
            (1, "0.000000001"),
            (-1, "-0.000000001"),
            (123_450_000_000, "123.45"),
            (i128::MIN, "-170141183460469231731687303715.884105728"),
        ] {
            s.clear();
            write_numeric(n, &mut s);
            assert_eq!(s, want, "{n}");
        }
        for (b, want) in [(&b""[..], ""), (b"f", "Zg=="), (b"fo", "Zm8="), (b"foo", "Zm9v")] {
            s.clear();
            write_base64(b, &mut s);
            assert_eq!(s, want);
        }
        s.clear();
        write_datetime(-1, &mut s);
        assert_eq!(s, "1969-12-31T23:59:59.999999");
        s.clear();
        write_datetime(i64::MAX, &mut s);
        assert_eq!(s, i64::MAX.to_string(), "out of range: the raw value");
    }

    #[test]
    fn a_budget_keeps_whole_rows_in_order() {
        let b = batch();
        assert_eq!(fit(&b, 2, TokenBudget(0)), 0);
        assert_eq!(fit(&b, 2, TokenBudget(u32::MAX)), 2);
        let one = fit(&b, 2, TokenBudget(60));
        assert_eq!(one, 1, "the first row is about 50 tokens");
        let r = Rows::new(
            QueryResult {
                batch: b,
                total_rows: 2,
                truncated: false,
                bytes_processed: 0,
                bytes_billed: 0,
                cache_hit: false,
                wall_ms: 0,
            },
            Some(TokenBudget(60)),
        );
        assert_eq!((r.len(), r.truncated), (1, true));
    }

    #[test]
    fn every_error_maps_to_one_failure() {
        let api = |status, body: &str| crate::exec::decode::api_error(status, body.as_bytes());
        let cases = [
            (api(403, "{}"), Failure::NoAccess),
            (api(400, r#"{"error":{"errors":[{"reason":"accessDenied"}]}}"#), Failure::NoAccess),
            (api(401, "{}"), Failure::Expired),
            (api(404, r#"{"error":{"errors":[{"reason":"notFound"}]}}"#), Failure::NotFound),
            (api(404, "{}"), Failure::NotFound),
            (
                api(400, r#"{"error":{"errors":[{"reason":"bytesBilledLimitExceeded"}]}}"#),
                Failure::BytesLimit,
            ),
            (
                api(400, r#"{"error":{"errors":[{"reason":"invalidQuery"}]}}"#),
                Failure::InvalidQuery,
            ),
            (api(500, "{}"), Failure::Unreachable),
            (api(429, "{}"), Failure::Unreachable),
            (Error::Timeout, Failure::Timeout),
            (Error::Decode(crate::base::Error::json("x")), Failure::Unreachable),
            (Error::PreconditionFailed, Failure::Unreachable),
            (
                Error::Transport(objstore_r::Error::NoCredentials { reason: "x" }),
                Failure::NoCredentials,
            ),
            (Error::Transport(objstore_r::Error::Auth { message: "x".into() }), Failure::Expired),
            (
                Error::Transport(objstore_r::Error::Policy(objstore_r::PolicyError::Endpoint {
                    reason: "x",
                })),
                Failure::RefusedByGuard,
            ),
            (
                Error::Transport(objstore_r::Error::Unavailable {
                    message: "x".into(),
                    retriable: true,
                }),
                Failure::Unreachable,
            ),
        ];
        for (e, want) in cases {
            assert_eq!(Failure::of_error(&e), want, "{e}");
        }
        let names: Vec<&str> = [
            Failure::NoAccess,
            Failure::NotFound,
            Failure::NoCredentials,
            Failure::Expired,
            Failure::Unreachable,
            Failure::RefusedByGuard,
            Failure::BytesLimit,
            Failure::InvalidQuery,
            Failure::Timeout,
        ]
        .map(Failure::as_str)
        .to_vec();
        let mut uniq = names.clone();
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(uniq.len(), names.len());
    }

    #[test]
    fn redaction_removes_every_copy() {
        assert_eq!(redact("a tok b tok", "tok"), "a <redacted> b <redacted>");
        assert_eq!(redact("abc", ""), "abc");
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
    }

    #[test]
    fn a_refused_statement_touches_no_credential() {
        // A token provider that panics if asked: the guard answers first.
        struct Never;
        impl TokenProvider for Never {
            type Token<'a> = std::future::Ready<objstore_r::Result<objstore_r::token::AccessToken>>;
            fn token(&self) -> Self::Token<'_> {
                unreachable!("a refused statement asked for a token")
            }
        }
        let opts = QueryOptions::default();
        let run =
            query(Ok(Never), MetadataGuard::default(), "p", "SELECT * FROM p.mart.orders", &opts);
        let e = rt().block_on(run).unwrap_err();
        assert_eq!(e.failure, Failure::RefusedByGuard);
        let missing = query::<Never>(
            Err(objstore_r::Error::NoCredentials { reason: "none" }),
            MetadataGuard::default(),
            "p",
            "SELECT * FROM p.mart.orders",
            &opts,
        );
        assert_eq!(
            rt().block_on(missing).unwrap_err().failure,
            Failure::RefusedByGuard,
            "refused before the credential is even looked at"
        );
    }
}
