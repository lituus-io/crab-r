// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # crab-eval
//!
//! The harness every crab-r change is judged by. Leaderboard accuracy alone
//! hides what matters in production, so a [`Scorecard`] reports jointly:
//!
//! - **accuracy** (execution match against verified gold),
//! - **false-success rate**: answered, and wrong — the costly failure,
//! - **selective risk and coverage**, and the **risk–coverage curve** (AURC)
//!   that calibrated abstention trades along,
//! - **abstention quality** on questions that have no correct SQL,
//! - **cost** (tokens, cached share, warehouse bytes, dollars) and
//!   **latency** percentiles,
//! - **pass^k** run-to-run consistency.
//!
//! Changes are compared with a **paired bootstrap** over the same items
//! ([`stats::paired_bootstrap`]), so a claimed gain comes with a confidence
//! interval rather than a single number.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::doc_markdown
)]

pub mod dataset;
pub mod run;
pub mod score;
pub mod stats;

pub use dataset::{Dataset, Item};
pub use score::{Record, Scorecard, Verdict};
