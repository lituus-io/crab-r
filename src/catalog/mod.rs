// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! # `catalog`
//!
//! What the engine knows about the warehouse, as one immutable image.
//!
//! - [`image`]: the zero-copy, checksummed catalog image — datasets, tables
//!   and every nested column field with types, descriptions, partitioning,
//!   clustering, sizes and freshness. Served from a memory map (shared page
//!   cache across processes) or from bytes fetched from an object store,
//!   validated once at open, then read by offset arithmetic. Keys are
//!   `base` [`crate::base::EntityKey`]s, shared with graph-r and link-r.
//! - [`sync()`]: build it from BigQuery INFORMATION_SCHEMA with six concurrent
//!   region-wide queries.
//! - [`types`]: BigQuery type classification.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::doc_markdown,
    clippy::cast_precision_loss
)]

pub mod image;
pub mod profile;
pub mod sync;
pub mod types;

pub use image::{
    Catalog, CatalogBuilder, ColId, ColumnRef, ColumnSpec, DatasetRef, Error, Profile, ProfileSpec,
    Result, TableId, TableKind, TableRef, TableSpec,
};
pub use profile::{profile, ProfileConfig, ProfileStats};
pub use sync::{sync, SyncConfig, SyncStats};
pub use types::TypeCode;
