// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! # crab-r
//!
//! Generative technical metadata for BigQuery: name a project, a dataset, a
//! table, or hand over SQL, and [`describe`] writes column and table
//! descriptions from the catalog, the producing queries, lineage and the
//! values themselves — each checked against the data before it is trusted.
//!
//! | Module | Role |
//! |---|---|
//! | [`base`] | Keys, typed arenas, JSON scanner/writer, batches, dates, budgets |
//! | [`sql`] | GoogleSQL lexer, parser and emitter |
//! | [`ir`] | Binder, diagnostics, column lineage and row conditions |
//! | [`catalog`] | Zero-copy catalog image, INFORMATION_SCHEMA sync, value profiles |
//! | [`exec`] | Warehouse trait; BigQuery client with dry-run cost guards |
//! | [`lineage`] | Column lineage graph; Dataplex lineage client |
//! | [`llm`] | Model trait; Vertex AI, OpenAI-compatible and replay backends |
//! | [`describe`] | Describe: discovery, tracing, drafting, checks, judging, state |
//! | [`engine`], [`prompt`], [`link`], [`values`], [`grammar`], [`verify`], [`eval`] | Text-to-SQL |
//!
//! Features: `http` (authorised HTTPS transport), `vertex`, `openai`,
//! `dataplex`, and `cli` (the `crab-describe` command and [`describe::app`]).
//! Futures are named (GATs and `impl Trait` in associated types), so the
//! crate builds on the pinned nightly toolchain in `rust-toolchain.toml`.

#![feature(impl_trait_in_assoc_type)]
#![feature(future_join)]
#![forbid(unsafe_code)]

pub mod base;
pub mod catalog;
pub mod describe;
pub mod engine;
pub mod eval;
pub mod exec;
pub mod grammar;
pub mod ir;
pub mod lineage;
pub mod link;
pub mod llm;
pub mod prompt;
pub mod sql;
pub mod values;
pub mod verify;
