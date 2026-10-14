// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # `sql`
//!
//! The GoogleSQL front end.
//!
//! - [`lexer`]: zero-copy tokenizer with compile-time keyword hashing.
//! - [`ast`]: a pooled syntax tree of spans and handles (no lifetimes), reused
//!   across statements.
//! - [`parser`]: recursive descent + Pratt expressions; [`parser::Mode::Full`]
//!   for any GoogleSQL query, [`parser::Mode::Strict`] for CrabSQL (what
//!   models write). [`parser::viability`] classifies a streamed prefix as
//!   complete, still-viable or already invalid — the stream-abort signal.
//! - [`emit`]: canonical GoogleSQL text from the tree.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::cast_possible_truncation,
    clippy::doc_markdown,
    clippy::enum_glob_use,
    // The AST vocabulary is imported whole where it is walked.
    clippy::wildcard_imports
)]

pub mod ast;
pub mod emit;
pub mod lexer;
pub mod parser;

pub use parser::{viability, Mode, ParseError, Parsed, Parser, Script, Viability};

/// A byte span into the source text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Sp {
    /// Start (inclusive).
    pub start: u32,
    /// End (exclusive).
    pub end: u32,
}

impl Sp {
    /// The spanned text (empty for an out-of-range span).
    #[must_use]
    pub fn text(self, src: &str) -> &str {
        src.get(self.start as usize..self.end as usize).unwrap_or("")
    }
}
