// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Deterministic, zero-cost model calls.
//!
//! [`Replay`] answers from a [`Cassette`] of recorded completions keyed by
//! [`GenRequest::fingerprint`]. In [`Mode::Record`] it forwards misses to a
//! live model and records the result, so the first harness run pays and every
//! rerun of an unchanged pipeline is free, instant and bit-identical. Replayed
//! text still streams through the caller's [`Sink`], so stream-abort logic is
//! exercised exactly as live.
//!
//! Cassettes serialise to JSON lines (one recording per line, sorted by key:
//! byte-reproducible files that diff cleanly) and can live on disk or in an
//! object store.

use crate::base::json::{self, Event, Scanner, Writer};
use crate::llm::request::{Candidates, Finish, GenRequest, Sink, Usage};
use crate::llm::{Caps, Error, Model, Result};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::future::Future;
use std::sync::Mutex;

/// One recorded completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recording {
    /// Candidate texts.
    pub texts: Vec<String>,
    /// How each ended.
    pub finish: Vec<Finish>,
    /// What the live call consumed.
    pub usage: Usage,
}

/// Recorded completions by request fingerprint.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Cassette {
    map: HashMap<u128, Recording>,
}

fn finish_tag(f: Finish) -> &'static str {
    match f {
        Finish::Pending => "pending",
        Finish::Stop => "stop",
        Finish::Length => "length",
        Finish::Aborted => "aborted",
        Finish::Filtered => "filtered",
        Finish::Other => "other",
    }
}

fn finish_from(s: &str) -> Finish {
    match s {
        "stop" => Finish::Stop,
        "length" => Finish::Length,
        "aborted" => Finish::Aborted,
        "filtered" => Finish::Filtered,
        "pending" => Finish::Pending,
        _ => Finish::Other,
    }
}

impl Cassette {
    /// Empty cassette.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of recordings.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// The recording for `key`.
    #[must_use]
    pub fn get(&self, key: u128) -> Option<&Recording> {
        self.map.get(&key)
    }

    /// Store a recording.
    pub fn insert(&mut self, key: u128, rec: Recording) {
        self.map.insert(key, rec);
    }

    /// Serialise as sorted JSON lines.
    #[must_use]
    pub fn to_jsonl(&self) -> String {
        let mut keys: Vec<u128> = self.map.keys().copied().collect();
        keys.sort_unstable();
        let mut out = String::new();
        let mut hex = String::with_capacity(32);
        for k in keys {
            let r = &self.map[&k];
            hex.clear();
            let _ = write!(hex, "{k:032x}");
            let mut w = Writer::new(&mut out);
            w.begin_object().key("key").string(&hex).key("texts").begin_array();
            for t in &r.texts {
                w.string(t);
            }
            w.end_array().key("finish").begin_array();
            for f in &r.finish {
                w.string(finish_tag(*f));
            }
            w.end_array()
                .key("in")
                .uint(r.usage.input_tokens)
                .key("cached")
                .uint(r.usage.cached_tokens)
                .key("out")
                .uint(r.usage.output_tokens)
                .key("usd_micro")
                .uint(r.usage.micro_usd)
                .key("ms")
                .uint(r.usage.wall_ms)
                .key("estimated")
                .bool(r.usage.estimated)
                .end_object();
            out.push('\n');
        }
        out
    }

    /// Parse JSON lines written by [`Cassette::to_jsonl`].
    pub fn from_jsonl(src: &str) -> Result<Self> {
        let mut c = Self::new();
        let mut buf = String::new();
        for line in src.lines().filter(|l| !l.trim().is_empty()) {
            let (key, rec) = parse_line(line, &mut buf)?;
            c.map.insert(key, rec);
        }
        Ok(c)
    }
}

fn parse_line(line: &str, buf: &mut String) -> Result<(u128, Recording)> {
    let bad = |msg| Error::Decode(crate::base::Error::Json { at: 0, msg });
    let mut s = Scanner::new(line);
    if s.expect()? != Event::ObjectStart {
        return Err(bad("cassette line is not an object"));
    }
    let mut key = None;
    let mut rec = Recording { texts: Vec::new(), finish: Vec::new(), usage: Usage::default() };
    loop {
        match s.expect()? {
            Event::ObjectEnd => break,
            Event::Key(k) => {
                let name = k.as_plain().unwrap_or("");
                let v = s.expect()?;
                match (name, v) {
                    ("key", Event::Str(h)) => {
                        key = Some(u128::from_str_radix(h.raw(), 16).map_err(|_| bad("bad key"))?);
                    }
                    ("texts" | "finish", Event::ArrayStart) => loop {
                        match s.expect()? {
                            Event::ArrayEnd => break,
                            Event::Str(t) => {
                                let t = t.decode(buf)?;
                                if name == "texts" {
                                    rec.texts.push(t.to_owned());
                                } else {
                                    rec.finish.push(finish_from(t));
                                }
                            }
                            _ => return Err(bad("expected string")),
                        }
                    },
                    ("in" | "cached" | "out" | "usd_micro" | "ms", Event::Num(n)) => {
                        let n = json::parse_u64(n)?;
                        match name {
                            "in" => rec.usage.input_tokens = n,
                            "cached" => rec.usage.cached_tokens = n,
                            "out" => rec.usage.output_tokens = n,
                            "usd_micro" => rec.usage.micro_usd = n,
                            _ => rec.usage.wall_ms = n,
                        }
                    }
                    ("estimated", Event::Bool(b)) => rec.usage.estimated = b,
                    (_, other) => s.skip(other)?,
                }
            }
            _ => return Err(bad("expected key")),
        }
    }
    if rec.texts.len() != rec.finish.len() {
        return Err(bad("texts and finish differ in length"));
    }
    Ok((key.ok_or_else(|| bad("missing key"))?, rec))
}

/// Replay or record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Recordings only; a miss is [`Error::ReplayMiss`].
    Replay,
    /// Answer from recordings; forward misses to the live model and record.
    Record,
}

/// A model backed by a cassette, optionally in front of a live model.
#[derive(Debug)]
pub struct Replay<M = NoModel> {
    live: Option<M>,
    caps: Caps,
    mode: Mode,
    cassette: Mutex<Cassette>,
}

/// Placeholder live model for pure replay.
#[derive(Debug)]
pub struct NoModel;

impl Model for NoModel {
    type Gen<'a, S>
        = std::future::Ready<Result<Usage>>
    where
        Self: 'a,
        S: Sink + 'a;

    fn caps(&self) -> &Caps {
        unreachable!("NoModel is never consulted")
    }

    fn generate<'a, S: Sink + 'a>(
        &'a self,
        _: &'a GenRequest<'a>,
        _: &'a mut Candidates,
        _: &'a mut S,
    ) -> Self::Gen<'a, S> {
        std::future::ready(Err(Error::Unsupported("no live model")))
    }
}

impl Replay<NoModel> {
    /// Pure replay with the given (recorded) model capabilities.
    #[must_use]
    pub fn new(caps: Caps, cassette: Cassette) -> Self {
        Self { live: None, caps, mode: Mode::Replay, cassette: Mutex::new(cassette) }
    }
}

impl<M: Model> Replay<M> {
    /// Record mode in front of `live`.
    pub fn recording(live: M, cassette: Cassette) -> Self {
        let caps = live.caps().clone();
        Self { live: Some(live), caps, mode: Mode::Record, cassette: Mutex::new(cassette) }
    }

    /// A copy of the cassette (e.g. to persist after a harness run).
    pub fn cassette(&self) -> Cassette {
        self.cassette.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    fn lookup(&self, key: u128) -> Option<Recording> {
        self.cassette.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get(key).cloned()
    }
}

impl<M: Model> Model for Replay<M> {
    type Gen<'a, S>
        = impl Future<Output = Result<Usage>> + 'a
    where
        Self: 'a,
        S: Sink + 'a;

    fn caps(&self) -> &Caps {
        &self.caps
    }

    fn generate<'a, S: Sink + 'a>(
        &'a self,
        req: &'a GenRequest<'a>,
        out: &'a mut Candidates,
        sink: &'a mut S,
    ) -> Self::Gen<'a, S> {
        async move {
            let key = req.fingerprint(&self.caps.id);
            if let Some(rec) = self.lookup(key) {
                out.reset(rec.texts.len());
                for (i, (t, f)) in rec.texts.iter().zip(&rec.finish).enumerate() {
                    // One delta per candidate: the sink sees the same final
                    // text it would have seen live, and may still abort.
                    if out.push(i, t, sink) == crate::llm::Flow::Continue {
                        out.set_finish(i, *f);
                    } else {
                        out.set_text(i, t);
                    }
                }
                return Ok(Usage { wall_ms: 0, ..rec.usage });
            }
            match (&self.live, self.mode) {
                (Some(live), Mode::Record) => {
                    let usage = live.generate(req, out, sink).await?;
                    let rec = Recording {
                        texts: out.iter().map(|(t, _)| t.to_owned()).collect(),
                        finish: out.iter().map(|(_, f)| f).collect(),
                        usage,
                    };
                    self.cassette
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(key, rec);
                    Ok(usage)
                }
                _ => Err(Error::ReplayMiss { key }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::request::{Part, Prompt};
    use crate::llm::{CacheSupport, ConstraintSupport, Flow, Price};
    use objstore_r::join::spin_on as block_on;

    pub(crate) fn caps() -> Caps {
        Caps {
            id: "test-model".into(),
            constraints: ConstraintSupport { lark: false, gbnf: false, json_schema: true },
            max_candidates: 8,
            max_stream_candidates: 1,
            cache: CacheSupport::Implicit,
            context_tokens: 1_000_000,
            max_output_tokens: 8192,
            streaming: true,
            price: Price { input: 300_000, cached_input: 75_000, output: 2_500_000 },
        }
    }

    fn recorded(req: &GenRequest<'_>) -> Cassette {
        let mut c = Cassette::new();
        c.insert(
            req.fingerprint("test-model"),
            Recording {
                texts: vec!["SELECT 1".into(), "DROP TABLE x".into()],
                finish: vec![Finish::Stop, Finish::Stop],
                usage: Usage {
                    input_tokens: 100,
                    output_tokens: 8,
                    micro_usd: 50,
                    wall_ms: 900,
                    ..Usage::default()
                },
            },
        );
        c
    }

    #[test]
    fn replays_recorded_candidates_through_the_sink() {
        let parts = [Part::cached("schema"), Part::text("q")];
        let req = GenRequest { n: 2, ..GenRequest::new(Prompt { system: "s", parts: &parts }) };
        let m = Replay::new(caps(), recorded(&req));
        let mut out = Candidates::new();
        let mut sink = |_: u8, t: &str, _: &str| {
            if t.starts_with("DROP") {
                Flow::Abort
            } else {
                Flow::Continue
            }
        };
        let usage = block_on(m.generate(&req, &mut out, &mut sink)).unwrap();
        assert_eq!(out.text(0), "SELECT 1");
        assert_eq!(out.finish(0), Finish::Stop);
        assert_eq!(out.finish(1), Finish::Aborted, "sink verdicts apply in replay too");
        assert_eq!((usage.input_tokens, usage.wall_ms), (100, 0), "recorded usage, zero wall time");
    }

    #[test]
    fn a_miss_is_an_error_in_replay_mode() {
        let parts = [Part::text("unseen")];
        let req = GenRequest::new(Prompt { system: "s", parts: &parts });
        let m = Replay::new(caps(), Cassette::new());
        let r = block_on(m.generate(&req, &mut Candidates::new(), &mut ()));
        assert!(matches!(r, Err(Error::ReplayMiss { .. })));
    }

    #[test]
    fn record_mode_forwards_misses_and_then_replays() {
        let parts = [Part::text("q")];
        let req = GenRequest::new(Prompt { system: "s", parts: &parts });
        let live = Replay::new(caps(), recorded(&req)); // stands in for a live model
        let rec = Replay::recording(live, Cassette::new());
        let mut out = Candidates::new();
        block_on(rec.generate(&req, &mut out, &mut ())).unwrap();
        assert_eq!(rec.cassette().len(), 1);
        let replay = Replay::new(caps(), rec.cassette());
        block_on(replay.generate(&req, &mut out, &mut ())).unwrap();
        assert_eq!(out.text(1), "DROP TABLE x");
    }

    #[test]
    fn jsonl_round_trip_is_byte_stable() {
        let parts = [Part::text("q")];
        let req = GenRequest::new(Prompt { system: "s", parts: &parts });
        let mut c = recorded(&req);
        c.insert(
            7,
            Recording {
                texts: vec!["line\n\"quoted\" é".into()],
                finish: vec![Finish::Length],
                usage: Usage::default(),
            },
        );
        let text = c.to_jsonl();
        let back = Cassette::from_jsonl(&text).unwrap();
        assert_eq!(back, c);
        assert_eq!(back.to_jsonl(), text, "sorted, byte-reproducible");
    }

    #[test]
    fn malformed_cassettes_are_errors() {
        for bad in ["[1]", r#"{"texts":["a"],"finish":[]}"#, r#"{"key":"zz"}"#, "{"] {
            assert!(Cassette::from_jsonl(bad).is_err(), "{bad}");
        }
    }
}
