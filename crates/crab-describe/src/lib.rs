// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # crab-describe
//!
//! Evidence-driven metadata for a whole dataset. For every table and column
//! it discovers the SQL that produces them, gathers evidence (lineage,
//! INFORMATION_SCHEMA, profiles, upstream descriptions), drafts
//! descriptions, has the model **write its own validation checks**, runs
//! those checks safely (strict parse, bind to real columns, read-only,
//! dry-run cost guard, byte cap, merged into one query per table), and lets
//! the model judge the results and revise — converging on metadata the data
//! and the SQL actually support. Existing descriptions are audited the same
//! way; contradicted ones are flagged with evidence, never silently
//! overwritten.

#![feature(future_join)]
#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::too_many_lines
)]

pub mod checks;
pub mod describe;
pub mod generate;
pub mod incremental;
pub mod producers;
pub mod report;
pub mod splice;
pub mod trace;
