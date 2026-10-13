// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Model-layer errors. Messages never contain credentials or prompt text.

use std::fmt;

/// Result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What a model call can report.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The transport failed (network, auth, limits); see its `is_retriable`.
    Transport(objstore_r::Error),
    /// The provider rejected or failed the request.
    Api {
        /// HTTP status (0 when reported inside a stream).
        status: u16,
        /// Provider message excerpt.
        message: String,
    },
    /// The provider's response could not be decoded.
    Decode(crate::base::Error),
    /// The request asks for something the model cannot do.
    Unsupported(&'static str),
    /// Replay mode: no recording for this request.
    ReplayMiss {
        /// The request key that was looked up.
        key: u128,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "transport: {e}"),
            Self::Api { status, message } => write!(f, "model api {status}: {message}"),
            Self::Decode(e) => write!(f, "decode: {e}"),
            Self::Unsupported(what) => write!(f, "unsupported: {what}"),
            Self::ReplayMiss { key } => write!(f, "no recording for request {key:032x}"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// Worth retrying at a higher level (transient transport or 429/5xx).
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Transport(e) => e.is_retriable(),
            Self::Api { status, .. } => matches!(status, 408 | 429 | 500 | 502 | 503 | 504),
            _ => false,
        }
    }
}

impl Error {
    /// A provider's error envelope (an HTTP error body, or an error event in
    /// a stream with `status` 0).
    #[cfg(any(feature = "vertex", feature = "openai"))]
    pub(crate) fn api(status: u16, body: &[u8]) -> Self {
        let e = objstore_r::transport::ApiError::parse(status, body);
        Self::Api { status: e.status, message: e.message }
    }
}

impl From<objstore_r::Error> for Error {
    fn from(e: objstore_r::Error) -> Self {
        Self::Transport(e)
    }
}

impl From<crate::base::Error> for Error {
    fn from(e: crate::base::Error) -> Self {
        Self::Decode(e)
    }
}
