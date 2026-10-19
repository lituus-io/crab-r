// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # `llm`
//!
//! The model layer. The engine never names a vendor: it talks to a [`Model`]
//! and adapts its strategy to the model's [`Caps`] — whether it accepts a
//! grammar or a JSON schema, how many candidates one request can return,
//! whether prefixes are cached, and what tokens cost.
//!
//! Generation **streams**. Every text delta goes to a [`Sink`] the caller
//! supplies (the engine plugs its incremental SQL validator in here) and the
//! sink may answer [`Flow::Abort`] to stop a candidate mid-generation. A
//! backend then stops reading — for HTTP backends it drops the connection,
//! which stops billing for output nobody will use.
//!
//! Backends:
//! - [`replay::Replay`]: deterministic, free, network-less. It replays recorded
//!   completions (or records a live model's) so tests and harness reruns cost
//!   nothing.
//! - [`vertex::Vertex`] (feature `vertex`): Gemini on Vertex AI through
//!   objstore-r's authorised HTTPS transport.
//! - [`openai::OpenAi`] (feature `openai`): any OpenAI-compatible
//!   chat-completions server (vLLM, llama.cpp, hosted APIs, Vertex AI's
//!   compatible endpoint), including grammar-constrained decoding. Its JSON
//!   schemas go out through [`strict::write_strict`], so every schema meets
//!   OpenAI's strict structured-output rules whoever built it.
//!
//! Hosts add any other provider by implementing [`Model`].

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

pub mod caps;
pub mod error;
#[cfg(feature = "openai")]
pub mod openai;
pub mod replay;
pub mod request;
pub mod strict;
#[cfg(feature = "vertex")]
pub mod vertex;

pub use caps::{CacheSupport, Caps, ConstraintSupport, Price};
pub use error::{Error, Result};
pub use request::{Candidates, Constraint, Finish, Flow, GenRequest, Part, Prompt, Sink, Usage};

use std::future::Future;

/// A text-generation model.
///
/// `generate` fills `out` with `req.n` candidates (reusing its buffers) and
/// streams every delta through `sink`. The returned [`Usage`] is what the
/// call cost, including aborted candidates.
pub trait Model {
    /// Future of [`generate`](Model::generate).
    type Gen<'a, S>: Future<Output = Result<Usage>> + 'a
    where
        Self: 'a,
        S: Sink + 'a;

    /// What this model supports and costs.
    fn caps(&self) -> &Caps;

    /// Generate `req.n` candidates into `out`, streaming deltas to `sink`.
    fn generate<'a, S: Sink + 'a>(
        &'a self,
        req: &'a GenRequest<'a>,
        out: &'a mut Candidates,
        sink: &'a mut S,
    ) -> Self::Gen<'a, S>;
}
