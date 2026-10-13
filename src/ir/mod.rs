// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! # `ir`
//!
//! The semantic layer over the GoogleSQL tree.
//!
//! - [`bind`]: resolve every name against the catalog, type every
//!   expression, and enforce GoogleSQL's rules (grouping, aggregate and
//!   window placement, partition filters) — catching what BigQuery would
//!   reject before any warehouse round trip.
//! - [`diag`]: diagnostics with spans and suggestions, phrased like
//!   BigQuery's own errors, for targeted repair.
//! - [`fingerprint()`]: meaning-level hashes, so equivalent candidates are
//!   executed once.
//! - [`canon`]: canonical, fully-qualified GoogleSQL for execution: equivalent
//!   candidates emit byte-identical text, which BigQuery's free result cache
//!   recognises.
//! - [`funcs`], [`ty`]: the function table and type model.
//!
//! The IR is the parsed tree plus side tables (types, resolutions) indexed by
//! node id — one tree, one pass, buffers reused across statements.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::cast_possible_truncation,
    clippy::doc_markdown,
    clippy::wildcard_imports,
    clippy::enum_glob_use,
    // Expression-walking code names nodes e/x/r/t by convention.
    clippy::many_single_char_names,
    clippy::single_match_else
)]

pub mod bind;
pub mod canon;
pub mod diag;
pub mod fingerprint;
pub mod funcs;
pub mod lineage;
pub mod ty;
mod walk;

pub use bind::{BindOpts, Binder, Bound, FromBind, OutCol, Ref, Rel};
pub use diag::{Diag, DiagKind, Severity};
pub use fingerprint::{fingerprint, template_fingerprint};
pub use lineage::{conditions, lineage, ColumnLineage, Hop, Transform};
pub use ty::Ty;
pub use walk::{span, span_in, visit};
