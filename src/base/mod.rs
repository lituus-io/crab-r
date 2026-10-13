// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! # `base`
//!
//! Foundations shared by every crab-r crate. Nothing here depends on a
//! third-party crate: the only dependency is graph-r, for the entity-key
//! function shared with graph-r and link-r.
//!
//! - [`key`]: [`EntityKey`], the durable identity of a warehouse entity.
//! - [`pool`]: [`Pool`], a typed index arena. IR nodes are `u32` handles into
//!   contiguous storage, reused across requests with `clear()`: no `unsafe`,
//!   no per-node allocation after warm-up.
//! - [`json`]: a zero-copy pull scanner and a streaming writer, for model and
//!   warehouse wire formats.
//! - [`batch`]: columnar result batches with validity bitmaps.
//! - [`date`]: proleptic-Gregorian civil-date arithmetic.
//! - [`prng`]: deterministic SplitMix64 / xoshiro256** generators.
//! - [`budget`]: per-request spending limits.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    // Product names (BigQuery, GoogleSQL) are prose, not code items.
    clippy::doc_markdown
)]

pub mod batch;
pub mod budget;
pub mod date;
pub mod error;
pub mod json;
pub mod key;
pub mod pool;
pub mod prng;
pub mod text;

pub use budget::{Budget, Spend};
pub use error::{Error, Result};
pub use key::EntityKey;
pub use pool::{Idx, Pool, Span};
