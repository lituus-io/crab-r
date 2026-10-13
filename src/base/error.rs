// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! The crate error type. Decoders report the byte offset of the failure so
//! callers (and fuzz targets) get a typed error, never a panic.

use std::fmt;

/// Result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Everything `base` can report.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// Malformed JSON at byte `at`.
    Json {
        /// Byte offset of the offending input.
        at: usize,
        /// What was wrong.
        msg: &'static str,
    },
    /// A value could not be interpreted as the requested type.
    Type {
        /// What was expected.
        expected: &'static str,
    },
    /// A date or time outside the supported range, or malformed.
    Date(&'static str),
    /// A configured limit was reached.
    Limit(&'static str),
}

impl Error {
    /// A JSON shape error found while walking a document (no byte offset).
    #[must_use]
    pub fn json(msg: &'static str) -> Self {
        Self::Json { at: 0, msg }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json { at, msg } => write!(f, "json: {msg} at byte {at}"),
            Self::Type { expected } => write!(f, "type: expected {expected}"),
            Self::Date(msg) => write!(f, "date: {msg}"),
            Self::Limit(what) => write!(f, "limit reached: {what}"),
        }
    }
}

impl std::error::Error for Error {}
