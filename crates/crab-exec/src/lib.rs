// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # crab-exec
//!
//! The warehouse layer. The engine talks to any warehouse through
//! [`Warehouse`]: a free **dry run** that validates SQL against the real
//! catalog and reports the bytes it would scan (the cost guardrail), and a
//! **capped run** that bills at most `max_bytes_billed`, returns at most
//! `max_rows` rows as a columnar [`Batch`], and reports what it cost.
//!
//! [`bigquery::BigQuery`] implements it over any objstore-r
//! [`objstore_r::transport::Transport`] (in production the reqwest/rustls
//! client with ADC tokens, feature `http`), so crab-exec itself carries no
//! HTTP stack.

#![feature(impl_trait_in_assoc_type)]
#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::doc_markdown
)]

pub mod bigquery;
mod decode;
pub mod metadata;

pub use crab_core::batch::{Batch, ColType};
pub use metadata::{TableMeta, TableMetadata};

use std::fmt;
use std::future::Future;

/// Options for one execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunOpts {
    /// Refuse to bill more than this many bytes (the query fails instead).
    pub max_bytes_billed: Option<u64>,
    /// Return at most this many rows (the result says whether more existed).
    pub max_rows: u32,
    /// Give up waiting after this long, milliseconds.
    pub timeout_ms: u32,
    /// Allow a cached result (free, and the default).
    pub use_cache: bool,
}

impl Default for RunOpts {
    fn default() -> Self {
        Self {
            max_bytes_billed: Some(10 << 30),
            max_rows: 10_000,
            timeout_ms: 60_000,
            use_cache: true,
        }
    }
}

/// A completed query.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryResult {
    /// Up to `max_rows` rows.
    pub batch: Batch,
    /// Total rows the query produced.
    pub total_rows: u64,
    /// More rows existed than were returned.
    pub truncated: bool,
    /// Bytes scanned.
    pub bytes_processed: u64,
    /// Bytes billed (0 for cache hits).
    pub bytes_billed: u64,
    /// Served from the warehouse's result cache (free).
    pub cache_hit: bool,
    /// Wall time, milliseconds.
    pub wall_ms: u64,
}

/// A dry run: validity, cost and output shape, without executing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DryRun {
    /// Bytes the query would scan.
    pub bytes_processed: u64,
    /// Output columns.
    pub schema: Vec<(String, ColType)>,
}

/// A 1-based source position reported by the warehouse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqlPos {
    /// Line.
    pub line: u32,
    /// Column.
    pub col: u32,
}

/// What a warehouse call can report.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Transport failure (network, auth, limits).
    Transport(objstore_r::Error),
    /// The SQL is invalid or cannot run (feeds targeted repair).
    Query {
        /// Machine reason (e.g. `invalidQuery`).
        reason: String,
        /// Warehouse message (sanitised).
        message: String,
        /// Position in the SQL, when reported.
        pos: Option<SqlPos>,
    },
    /// The query would bill more than `max_bytes_billed`.
    BytesLimit {
        /// Warehouse message.
        message: String,
    },
    /// Access denied to a table or project.
    Denied {
        /// Warehouse message.
        message: String,
    },
    /// The job did not finish within the timeout.
    Timeout,
    /// An API failure not attributable to the SQL.
    Api {
        /// HTTP status.
        status: u16,
        /// Message.
        message: String,
    },
    /// The response could not be decoded.
    Decode(crab_core::Error),
    /// A conditional write lost a race: the resource changed since it was
    /// read (`If-Match` etag mismatch). Re-read and retry.
    PreconditionFailed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "transport: {e}"),
            Self::Query { reason, message, pos } => match pos {
                Some(p) => write!(f, "{reason} at [{}:{}]: {message}", p.line, p.col),
                None => write!(f, "{reason}: {message}"),
            },
            Self::BytesLimit { message } => write!(f, "bytes limit: {message}"),
            Self::Denied { message } => write!(f, "access denied: {message}"),
            Self::Timeout => f.write_str("query timed out"),
            Self::Api { status, message } => write!(f, "api {status}: {message}"),
            Self::Decode(e) => write!(f, "decode: {e}"),
            Self::PreconditionFailed => {
                write!(f, "the resource changed since it was read (etag mismatch)")
            }
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// Worth retrying unchanged (transient transport or server errors).
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Transport(e) => e.is_retriable(),
            Self::Api { status, .. } => matches!(status, 429 | 500 | 502 | 503 | 504),
            _ => false,
        }
    }

    /// The SQL itself is at fault (the repair loop should see it).
    #[must_use]
    pub fn is_sql_error(&self) -> bool {
        matches!(self, Self::Query { .. })
    }
}

impl From<objstore_r::Error> for Error {
    fn from(e: objstore_r::Error) -> Self {
        Self::Transport(e)
    }
}

impl From<crab_core::Error> for Error {
    fn from(e: crab_core::Error) -> Self {
        Self::Decode(e)
    }
}

/// Result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A SQL warehouse.
pub trait Warehouse {
    /// Future of [`dry_run`](Warehouse::dry_run).
    type Dry<'a>: Future<Output = Result<DryRun>> + 'a
    where
        Self: 'a;
    /// Future of [`run`](Warehouse::run).
    type Run<'a>: Future<Output = Result<QueryResult>> + 'a
    where
        Self: 'a;

    /// Validate `sql` and estimate its cost without running it.
    fn dry_run<'a>(&'a self, sql: &'a str) -> Self::Dry<'a>;

    /// Run `sql` under `opts`.
    fn run<'a>(&'a self, sql: &'a str, opts: RunOpts) -> Self::Run<'a>;
}
