// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Requests, streamed output and usage.
//!
//! A [`Prompt`] is a system text plus ordered [`Part`]s, all borrowed from the
//! caller's context buffer — nothing is copied until the backend serialises
//! the wire request. Parts carry a `cache_break` marker: the text before and
//! including a marked part is byte-stable across requests (schema, dialect
//! card), which is what prefix caches key on.

use graph_r::hash::Hasher;

/// One contiguous piece of the user prompt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Part<'a> {
    /// The text.
    pub text: &'a str,
    /// Everything up to and including this part is a stable, cacheable prefix.
    pub cache_break: bool,
}

impl<'a> Part<'a> {
    /// A part that is not a cache boundary.
    #[must_use]
    pub fn text(text: &'a str) -> Self {
        Self { text, cache_break: false }
    }

    /// A part that ends a stable, cacheable prefix.
    #[must_use]
    pub fn cached(text: &'a str) -> Self {
        Self { text, cache_break: true }
    }
}

/// System instructions plus the ordered user parts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Prompt<'a> {
    /// System instructions (the most stable text; first in cache order).
    pub system: &'a str,
    /// User content, stable-first.
    pub parts: &'a [Part<'a>],
}

impl Prompt<'_> {
    /// Total bytes of prompt text (a cheap proxy for input size).
    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.system.len() + self.parts.iter().map(|p| p.text.len()).sum::<usize>()
    }
}

/// An output constraint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Constraint<'a> {
    /// Free text.
    None,
    /// A JSON schema (OpenAPI-subset JSON text) the output must satisfy.
    JsonSchema(&'a str),
    /// A Lark context-free grammar the output must satisfy.
    Lark(&'a str),
    /// A GBNF grammar (llama.cpp dialect) the output must satisfy.
    Gbnf(&'a str),
}

/// A generation request. Everything is borrowed.
#[derive(Clone, Copy, Debug)]
pub struct GenRequest<'a> {
    /// The prompt.
    pub prompt: Prompt<'a>,
    /// Output constraint (must be supported by the model's caps).
    pub constraint: Constraint<'a>,
    /// Number of candidates.
    pub n: u8,
    /// Sampling temperature.
    pub temperature: f32,
    /// Output token cap per candidate.
    pub max_output_tokens: u32,
    /// Stop sequences.
    pub stop: &'a [&'a str],
    /// Sampling seed, for reproducibility where the provider honours it.
    pub seed: Option<u64>,
    /// Stream every candidate independently so each can be aborted on its
    /// own (costs one prefill per candidate unless the prefix is cached).
    /// When false, a model that supports multi-candidate requests returns all
    /// candidates from one call (one prefill), and an abort only stops
    /// accepting that candidate's text.
    pub independent: bool,
}

impl<'a> GenRequest<'a> {
    /// A single-candidate, unconstrained request.
    #[must_use]
    pub fn new(prompt: Prompt<'a>) -> Self {
        Self {
            prompt,
            constraint: Constraint::None,
            n: 1,
            temperature: 0.0,
            max_output_tokens: 1024,
            stop: &[],
            seed: None,
            independent: false,
        }
    }

    /// A stable 128-bit fingerprint of everything that determines the output
    /// (for a given model id): the replay-cache key.
    #[must_use]
    pub fn fingerprint(&self, model_id: &str) -> u128 {
        let mut h = Hasher::new();
        let mut field = |b: &[u8]| {
            h.update(&(b.len() as u64).to_le_bytes());
            h.update(b);
        };
        field(model_id.as_bytes());
        field(self.prompt.system.as_bytes());
        for p in self.prompt.parts {
            field(p.text.as_bytes());
        }
        match self.constraint {
            Constraint::None => field(b"none"),
            Constraint::JsonSchema(s) => {
                field(b"json");
                field(s.as_bytes());
            }
            Constraint::Lark(s) => {
                field(b"lark");
                field(s.as_bytes());
            }
            Constraint::Gbnf(s) => {
                field(b"gbnf");
                field(s.as_bytes());
            }
        }
        field(&[self.n]);
        field(&self.temperature.to_bits().to_le_bytes());
        field(&self.max_output_tokens.to_le_bytes());
        for s in self.stop {
            field(s.as_bytes());
        }
        field(&self.seed.unwrap_or(u64::MAX).to_le_bytes());
        h.digest128()
    }
}

/// What the sink wants after a delta.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flow {
    /// Keep generating.
    Continue,
    /// Stop this candidate: its output is already known to be unusable.
    Abort,
}

/// Receives streamed output.
pub trait Sink {
    /// `cand` produced `delta`; `text` is its whole output so far (including
    /// `delta`), so an incremental validator needs no buffer of its own.
    fn delta(&mut self, cand: u8, text: &str, delta: &str) -> Flow;
}

impl Sink for () {
    fn delta(&mut self, _: u8, _: &str, _: &str) -> Flow {
        Flow::Continue
    }
}

impl<F: FnMut(u8, &str, &str) -> Flow> Sink for F {
    fn delta(&mut self, cand: u8, text: &str, delta: &str) -> Flow {
        self(cand, text, delta)
    }
}

/// How a candidate ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Finish {
    /// Still streaming / not produced.
    #[default]
    Pending,
    /// Natural end or stop sequence.
    Stop,
    /// Output token cap reached.
    Length,
    /// The sink aborted it.
    Aborted,
    /// The provider withheld it (safety, recitation, ...).
    Filtered,
    /// Any other provider reason.
    Other,
}

/// Candidate outputs. Owned by the caller and reused across requests: the
/// strings keep their capacity, so a warm session allocates nothing here.
#[derive(Debug, Default)]
pub struct Candidates {
    texts: Vec<String>,
    finish: Vec<Finish>,
    len: usize,
}

impl Candidates {
    /// Empty buffers.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Prepare for `n` candidates, keeping allocations.
    pub fn reset(&mut self, n: usize) {
        if self.texts.len() < n {
            self.texts.resize_with(n, String::new);
            self.finish.resize(n, Finish::Pending);
        }
        for t in &mut self.texts[..n] {
            t.clear();
        }
        self.finish[..n].fill(Finish::Pending);
        self.len = n;
    }

    /// Number of candidates of the current request.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// True when there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Output of candidate `i`.
    #[must_use]
    pub fn text(&self, i: usize) -> &str {
        self.texts.get(i).filter(|_| i < self.len).map_or("", String::as_str)
    }

    /// How candidate `i` ended.
    #[must_use]
    pub fn finish(&self, i: usize) -> Finish {
        if i < self.len {
            self.finish[i]
        } else {
            Finish::Pending
        }
    }

    /// Iterate `(text, finish)` for the current request.
    pub fn iter(&self) -> impl Iterator<Item = (&str, Finish)> {
        self.texts[..self.len]
            .iter()
            .map(String::as_str)
            .zip(self.finish[..self.len].iter().copied())
    }

    /// Append `delta` to candidate `i` and let `sink` decide. Deltas for an
    /// already-finished (e.g. aborted) candidate are dropped. Returns the
    /// sink's verdict (or `Abort` for a finished candidate).
    pub fn push<S: Sink + ?Sized>(&mut self, i: usize, delta: &str, sink: &mut S) -> Flow {
        if i >= self.len || self.finish[i] != Finish::Pending {
            return Flow::Abort;
        }
        self.texts[i].push_str(delta);
        let flow = sink.delta(i as u8, &self.texts[i], delta);
        if flow == Flow::Abort {
            self.finish[i] = Finish::Aborted;
        }
        flow
    }

    /// Record how candidate `i` ended (an abort is never overwritten).
    pub fn set_finish(&mut self, i: usize, f: Finish) {
        if i < self.len && self.finish[i] != Finish::Aborted {
            self.finish[i] = f;
        }
    }

    /// Settle candidate `i` as `f` only if it has not ended yet (a stream
    /// that closed without a reason ended naturally; a reported `Length` or
    /// `Filtered` must never be overwritten by that default).
    pub fn settle(&mut self, i: usize, f: Finish) {
        if i < self.len && self.finish[i] == Finish::Pending {
            self.finish[i] = f;
        }
    }

    /// Replace candidate `i`'s text wholesale (replay).
    pub(crate) fn set_text(&mut self, i: usize, text: &str) {
        if i < self.len {
            self.texts[i].clear();
            self.texts[i].push_str(text);
        }
    }

    /// True when every candidate has finished.
    #[must_use]
    pub fn all_finished(&self) -> bool {
        self.finish[..self.len].iter().all(|f| *f != Finish::Pending)
    }
}

/// What a call consumed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    /// Uncached input tokens.
    pub input_tokens: u64,
    /// Input tokens served from a prefix cache.
    pub cached_tokens: u64,
    /// Output tokens, including reasoning tokens billed as output.
    pub output_tokens: u64,
    /// Micro-dollars, from the model's prices.
    pub micro_usd: u64,
    /// True when some counts are estimates (a stream aborted before the
    /// provider reported usage).
    pub estimated: bool,
    /// Wall time, milliseconds.
    pub wall_ms: u64,
}

impl Usage {
    /// Sum of two usages (parallel streams of one request).
    #[must_use]
    pub fn plus(self, o: Self) -> Self {
        Self {
            input_tokens: self.input_tokens + o.input_tokens,
            cached_tokens: self.cached_tokens + o.cached_tokens,
            output_tokens: self.output_tokens + o.output_tokens,
            micro_usd: self.micro_usd.saturating_add(o.micro_usd),
            estimated: self.estimated || o.estimated,
            wall_ms: self.wall_ms.max(o.wall_ms),
        }
    }

    /// As a budget spend.
    #[must_use]
    pub fn spend(&self) -> crab_core::Spend {
        crab_core::Spend {
            input_tokens: self.input_tokens,
            cached_tokens: self.cached_tokens,
            output_tokens: self.output_tokens,
            warehouse_bytes: 0,
            micro_usd: self.micro_usd,
        }
    }
}

/// Rough token estimate (≈ 4 bytes per token) for accounting when a provider
/// never reported usage (e.g. an aborted stream).
#[must_use]
pub fn estimate_tokens(bytes: usize) -> u64 {
    (bytes as u64).div_ceil(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req<'a>(parts: &'a [Part<'a>]) -> GenRequest<'a> {
        GenRequest::new(Prompt { system: "sys", parts })
    }

    #[test]
    fn fingerprint_is_stable_and_sensitive() {
        let parts = [Part::cached("schema"), Part::text("q")];
        let a = req(&parts);
        assert_eq!(a.fingerprint("m"), a.fingerprint("m"));
        assert_ne!(a.fingerprint("m"), a.fingerprint("m2"));
        let mut b = a;
        b.temperature = 0.7;
        assert_ne!(a.fingerprint("m"), b.fingerprint("m"));
        let parts2 = [Part::cached("schem"), Part::text("aq")];
        assert_ne!(
            a.fingerprint("m"),
            req(&parts2).fingerprint("m"),
            "length-prefixed: no boundary ambiguity"
        );
    }

    #[test]
    fn candidates_reuse_capacity() {
        let mut c = Candidates::new();
        c.reset(2);
        c.push(0, "SELECT 1", &mut ());
        let cap = c.texts[0].capacity();
        c.reset(1);
        assert_eq!(c.text(0), "");
        assert_eq!(c.texts[0].capacity(), cap);
        assert_eq!(c.len(), 1);
        assert_eq!(c.text(1), "", "out of range reads as empty");
    }

    #[test]
    fn abort_is_sticky() {
        let mut c = Candidates::new();
        c.reset(2);
        let mut sink = |_: u8, text: &str, _: &str| {
            if text.contains("DROP") {
                Flow::Abort
            } else {
                Flow::Continue
            }
        };
        assert_eq!(c.push(0, "SELECT", &mut sink), Flow::Continue);
        assert_eq!(c.push(1, "DROP", &mut sink), Flow::Abort);
        assert_eq!(c.push(1, " TABLE", &mut sink), Flow::Abort, "later deltas are dropped");
        assert_eq!(c.text(1), "DROP");
        c.set_finish(1, Finish::Stop);
        assert_eq!(c.finish(1), Finish::Aborted);
        c.set_finish(0, Finish::Stop);
        assert!(c.all_finished());
    }

    #[test]
    fn usage_sums() {
        let a = Usage {
            input_tokens: 10,
            output_tokens: 5,
            micro_usd: 3,
            wall_ms: 100,
            ..Usage::default()
        };
        let b = Usage {
            input_tokens: 1,
            cached_tokens: 9,
            estimated: true,
            wall_ms: 50,
            ..Usage::default()
        };
        let s = a.plus(b);
        assert_eq!((s.input_tokens, s.cached_tokens, s.output_tokens, s.wall_ms), (11, 9, 5, 100));
        assert!(s.estimated);
    }

    #[test]
    fn token_estimate_rounds_up() {
        assert_eq!(estimate_tokens(0), 0);
        assert_eq!(estimate_tokens(1), 1);
        assert_eq!(estimate_tokens(8), 2);
    }
}
