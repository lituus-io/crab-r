// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Any OpenAI-compatible chat-completions server (feature `openai`): vLLM,
//! llama.cpp, SGLang, Ollama, hosted APIs, and Vertex AI's OpenAI-compatible
//! endpoint — over any objstore-r [`Transport`] (bearer tokens come from the
//! transport's token provider: ADC, or a static API key).
//!
//! - **n candidates, one prefill.** The protocol streams several
//!   candidates in one request (`n`), deltas interleaved by `index`; a
//!   server that multi-samples only without streaming
//!   ([`Caps::max_stream_candidates`]) gets one unary request instead.
//!   `independent` requests get one stream per candidate so each can be
//!   cancelled on its own.
//! - **Grammar-constrained decoding** where the server has it, declared by
//!   the model's [`Caps`]: a Lark grammar goes out as `guided_grammar`
//!   (vLLM / llguidance), a GBNF grammar as `grammar` (llama.cpp). A JSON
//!   schema maps to `response_format`.
//! - Provider-specific switches (reasoning effort, safety settings, ...) go
//!   in [`OpenAiConfig::extra`], raw JSON members appended to every body, so
//!   no provider is special-cased here.

use crate::base::json::{self, Event, Scanner, Writer};
use crate::llm::request::{
    estimate_tokens, Candidates, Constraint, Finish, Flow, GenRequest, Sink, Usage,
};
use crate::llm::{Caps, Error, Model, Result};
use objstore_r::transport::{BodyStream, Request, Transport};
use objstore_r::Bytes;
use std::cell::RefCell;
use std::future::Future;
use std::time::Instant;

/// Largest error body read from a failed call, bytes.
const MAX_ERROR_BODY: usize = 64 << 10;

/// Settings.
#[derive(Clone, Debug)]
pub struct OpenAiConfig {
    /// The chat-completions URL (`https://host/v1/chat/completions`).
    pub url: String,
    /// Model name as the server knows it.
    pub model: String,
    /// Capabilities and prices to report (and to cap requests by).
    pub caps: Caps,
    /// Raw JSON object members appended to every request body, e.g.
    /// `"reasoning_effort":"none"`. Must be valid JSON members (no braces).
    pub extra: Option<String>,
}

impl OpenAiConfig {
    /// [`extra`](Self::extra) for Vertex AI that disables reasoning (the
    /// cheapest, fastest setting for well-grounded SQL generation).
    pub const VERTEX_NO_THINKING: &'static str =
        r#""extra_body":{"google":{"thinking_config":{"thinking_budget":0}}}"#;

    /// Vertex AI's OpenAI-compatible endpoint for a publisher model
    /// (`gemini-2.5-flash` → `google/gemini-2.5-flash`).
    #[must_use]
    pub fn vertex(project: &str, location: &str, model: &str, caps: Caps) -> Self {
        let origin = if location == "global" {
            "https://aiplatform.googleapis.com".to_owned()
        } else {
            format!("https://{location}-aiplatform.googleapis.com")
        };
        let mut url = origin;
        url.push_str("/v1/projects/");
        objstore_r::transport::encode_component(project, &mut url);
        url.push_str("/locations/");
        objstore_r::transport::encode_component(location, &mut url);
        url.push_str("/endpoints/openapi/chat/completions");
        Self { url, model: format!("google/{model}"), caps, extra: None }
    }
}

/// An OpenAI-compatible chat model.
#[derive(Debug)]
pub struct OpenAi<T: Transport> {
    transport: T,
    cfg: OpenAiConfig,
}

impl<T: Transport> OpenAi<T> {
    /// A model reached through `transport`.
    #[must_use]
    pub fn new(transport: T, cfg: OpenAiConfig) -> Self {
        Self { transport, cfg }
    }

    /// The transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Serialise the wire request for `count` candidates.
    fn body(&self, req: &GenRequest<'_>, count: u8, stream: bool) -> Result<Bytes> {
        let caps = &self.cfg.caps;
        let mut out = String::with_capacity(req.prompt.byte_len() + 512);
        let mut w = Writer::new(&mut out);
        w.begin_object();
        w.key("model").string(&self.cfg.model);
        w.key("messages").begin_array();
        if !req.prompt.system.is_empty() {
            w.begin_object()
                .key("role")
                .string("system")
                .key("content")
                .string(req.prompt.system)
                .end_object();
        }
        // Parts stay separate (and in order), so the byte-stable schema
        // prefix is what a prefix cache sees first.
        w.begin_object().key("role").string("user").key("content").begin_array();
        for p in req.prompt.parts {
            w.begin_object().key("type").string("text").key("text").string(p.text).end_object();
        }
        w.end_array().end_object().end_array();
        w.key("temperature").float(f64::from(req.temperature));
        w.key("max_tokens").uint(u64::from(req.max_output_tokens.min(caps.max_output_tokens)));
        if count > 1 {
            w.key("n").uint(u64::from(count));
        }
        if !req.stop.is_empty() {
            w.key("stop").begin_array();
            for s in req.stop {
                w.string(s);
            }
            w.end_array();
        }
        if let Some(seed) = req.seed {
            w.key("seed").uint(seed & 0x7FFF_FFFF);
        }
        match req.constraint {
            Constraint::None => {}
            Constraint::JsonSchema(schema) => {
                if !caps.constraints.json_schema {
                    return Err(Error::Unsupported("json schema"));
                }
                w.key("response_format").begin_object().key("type").string("json_schema");
                w.key("json_schema")
                    .begin_object()
                    .key("name")
                    .string("output")
                    .key("strict")
                    .bool(true)
                    .key("schema")
                    .raw(schema)
                    .end_object();
                w.end_object();
            }
            Constraint::Lark(g) => {
                if !caps.constraints.lark {
                    return Err(Error::Unsupported("lark grammar"));
                }
                w.key("guided_grammar").string(g);
            }
            Constraint::Gbnf(g) => {
                if !caps.constraints.gbnf {
                    return Err(Error::Unsupported("gbnf grammar"));
                }
                w.key("grammar").string(g);
            }
        }
        if stream {
            w.key("stream").bool(true);
            w.key("stream_options").begin_object().key("include_usage").bool(true).end_object();
        }
        if let Some(extra) = self.cfg.extra.as_deref().filter(|e| !e.trim().is_empty()) {
            w.raw_members(extra);
        }
        w.end_object();
        Ok(Bytes::from(out))
    }

    fn usage(&self, r: Reported, estimated: bool, wall_ms: u64) -> Usage {
        let input = r.prompt.saturating_sub(r.cached);
        Usage {
            input_tokens: input,
            cached_tokens: r.cached,
            output_tokens: r.completion,
            micro_usd: self.cfg.caps.price.cost(input, r.cached, r.completion),
            estimated,
            wall_ms,
        }
    }

    /// A non-streaming call (servers without streaming).
    async fn unary<S: Sink>(
        &self,
        body: Bytes,
        base: usize,
        count: usize,
        shared: &RefCell<(&mut Candidates, &mut S)>,
    ) -> Result<Usage> {
        let started = Instant::now();
        let resp = self.transport.send(Request::post_json(&self.cfg.url, body)).await?;
        if !resp.is_success() {
            return Err(Error::api(resp.status, &resp.body));
        }
        let text = std::str::from_utf8(&resp.body)
            .map_err(|_| Error::from(crate::base::Error::json("response is not UTF-8")))?;
        let mut scratch = Scratch::default();
        let info = parse_event(text, &mut scratch)?;
        let mut guard = shared.borrow_mut();
        let (cands, sink) = &mut *guard;
        for (idx, t) in scratch.texts.drain(..) {
            cands.push(base + idx.min(count - 1), &t, *sink);
        }
        for &(idx, f) in &info.finishes {
            cands.set_finish(base + idx.min(count - 1), f);
        }
        for i in base..base + count {
            cands.settle(i, Finish::Stop);
        }
        Ok(self.usage(
            info.usage.unwrap_or_default(),
            info.usage.is_none(),
            started.elapsed().as_millis() as u64,
        ))
    }

    /// One stream serving candidate slots `base..base + count`.
    async fn stream<S: Sink>(
        &self,
        body: Bytes,
        base: usize,
        count: usize,
        shared: &RefCell<(&mut Candidates, &mut S)>,
        prompt_bytes: usize,
    ) -> Result<Usage> {
        let started = Instant::now();
        let (status, mut stream) =
            self.transport.open(Request::post_json(&self.cfg.url, body)).await?;
        if !(200..300).contains(&status) {
            let mut msg = Vec::new();
            while let Some(c) = stream.next_chunk().await? {
                let room = MAX_ERROR_BODY.saturating_sub(msg.len());
                msg.extend_from_slice(&c[..c.len().min(room)]);
                if msg.len() >= MAX_ERROR_BODY {
                    break;
                }
            }
            return Err(Error::api(status, &msg));
        }
        let mut sse = objstore_r::sse::SseDecoder::new();
        let mut scratch = Scratch::default();
        let mut reported: Option<Reported> = None;
        let mut aborted_any = false;
        'read: while let Some(chunk) = stream.next_chunk().await? {
            sse.push(&chunk)?;
            while let Some(data) = sse.next_data()? {
                if data.trim() == "[DONE]" {
                    break 'read;
                }
                let info = parse_event(data, &mut scratch)?;
                if info.usage.is_some() {
                    reported = info.usage;
                }
                let mut guard = shared.borrow_mut();
                let (cands, sink) = &mut *guard;
                for (idx, text) in scratch.texts.drain(..) {
                    if cands.push(base + idx.min(count - 1), &text, *sink) == Flow::Abort {
                        aborted_any = true;
                    }
                }
                for &(idx, f) in &info.finishes {
                    cands.set_finish(base + idx.min(count - 1), f);
                }
                // Every candidate aborted or finished after an abort: stop
                // reading (dropping the stream cancels it, ending billing).
                let settled = (base..base + count).all(|i| cands.finish(i) != Finish::Pending);
                drop(guard);
                if settled && aborted_any {
                    break 'read;
                }
            }
        }
        let wall_ms = started.elapsed().as_millis() as u64;
        let mut guard = shared.borrow_mut();
        for i in base..base + count {
            guard.0.settle(i, Finish::Stop);
        }
        Ok(match reported {
            Some(r) if !aborted_any => self.usage(r, false, wall_ms),
            _ => {
                // Cancelled before the final usage event: estimate from bytes,
                // never below what was reported.
                let out_bytes: usize = (base..base + count).map(|i| guard.0.text(i).len()).sum();
                let r = Reported {
                    prompt: reported
                        .filter(|r| r.prompt > 0)
                        .map_or(estimate_tokens(prompt_bytes), |r| r.prompt),
                    cached: reported.map_or(0, |r| r.cached),
                    completion: reported
                        .map_or(0, |r| r.completion)
                        .max(estimate_tokens(out_bytes)),
                };
                self.usage(r, true, wall_ms)
            }
        })
    }
}

impl<T: Transport> Model for OpenAi<T> {
    type Gen<'a, S>
        = impl Future<Output = Result<Usage>> + 'a
    where
        Self: 'a,
        S: Sink + 'a;

    fn caps(&self) -> &Caps {
        &self.cfg.caps
    }

    fn generate<'a, S: Sink + 'a>(
        &'a self,
        req: &'a GenRequest<'a>,
        out: &'a mut Candidates,
        sink: &'a mut S,
    ) -> Self::Gen<'a, S> {
        async move {
            let n = usize::from(req.n.max(1));
            out.reset(n);
            let prompt_bytes = req.prompt.byte_len();
            let shared = RefCell::new((out, sink));
            let caps = &self.cfg.caps;
            let native = usize::from(caps.max_candidates.max(1));
            // One request for all candidates (one prefill) unless each must
            // be cancellable on its own or the server caps `n`; streamed when
            // the server streams that many candidates, else one unary call.
            let (per, calls) = if !req.independent && n <= native { (n, 1) } else { (1, n) };
            let streaming = caps.streaming && per <= usize::from(caps.max_stream_candidates.max(1));
            let body = self.body(req, per as u8, streaming)?;
            let results = objstore_r::join::join_all((0..calls).map(|c| {
                let body = body.clone();
                let shared = &shared;
                async move {
                    if streaming {
                        self.stream(body, c * per, per, shared, prompt_bytes).await
                    } else {
                        self.unary(body, c * per, per, shared).await
                    }
                }
            }))
            .await;
            let mut total = Usage::default();
            let mut first_err = None;
            let mut ok = 0;
            for r in results {
                match r {
                    Ok(u) => {
                        total = total.plus(u);
                        ok += 1;
                    }
                    Err(e) => {
                        first_err.get_or_insert(e);
                    }
                }
            }
            match first_err {
                Some(e) if ok == 0 => Err(e),
                _ => Ok(total),
            }
        }
    }
}

/// Usage as reported by the server.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Reported {
    prompt: u64,
    cached: u64,
    completion: u64,
}

#[derive(Debug, Default)]
struct Scratch {
    /// (candidate index, text) of the current event, in order.
    texts: Vec<(usize, String)>,
    unescape: String,
}

#[derive(Debug, Default)]
struct EventInfo {
    usage: Option<Reported>,
    finishes: Vec<(usize, Finish)>,
}

fn finish_from(reason: &str) -> Finish {
    match reason {
        "stop" | "eos" => Finish::Stop,
        "length" => Finish::Length,
        "content_filter" => Finish::Filtered,
        _ => Finish::Other,
    }
}

/// Parse one chunk (`chat.completion.chunk`) or a whole unary response
/// (`chat.completion`): choices' `delta`/`message` content, finish reasons
/// and usage.
fn parse_event(data: &str, scratch: &mut Scratch) -> Result<EventInfo> {
    let mut info = EventInfo::default();
    let mut s = Scanner::new(data);
    if s.expect()? != Event::ObjectStart {
        return Err(Error::from(crate::base::Error::json("event is not an object")));
    }
    loop {
        match s.expect()? {
            Event::ObjectEnd => break,
            Event::Key(k) if k.eq_plain("choices") => {
                if s.expect()? != Event::ArrayStart {
                    return Err(Error::from(crate::base::Error::json("choices is not an array")));
                }
                loop {
                    match s.expect()? {
                        Event::ArrayEnd => break,
                        Event::ObjectStart => parse_choice(&mut s, scratch, &mut info)?,
                        other => s.skip(other)?,
                    }
                }
            }
            Event::Key(k) if k.eq_plain("usage") => info.usage = parse_usage(&mut s)?,
            Event::Key(k) if k.eq_plain("error") => return Err(Error::api(0, data.as_bytes())),
            Event::Key(_) => s.skip_value()?,
            _ => return Err(Error::from(crate::base::Error::json("expected key"))),
        }
    }
    Ok(info)
}

fn parse_choice(s: &mut Scanner<'_>, scratch: &mut Scratch, info: &mut EventInfo) -> Result<()> {
    let mut index = 0usize;
    let mut text = String::new();
    let mut finish = None;
    loop {
        match s.expect()? {
            Event::ObjectEnd => break,
            Event::Key(k) if k.eq_plain("index") => {
                if let Event::Num(n) = s.expect()? {
                    index = json::parse_u64(n)? as usize;
                }
            }
            Event::Key(k) if k.eq_plain("finish_reason") => {
                if let Event::Str(r) = s.expect()? {
                    finish = Some(finish_from(r.decode(&mut scratch.unescape)?));
                }
            }
            Event::Key(k) if k.eq_plain("delta") || k.eq_plain("message") => {
                let first = s.expect()?;
                if first != Event::ObjectStart {
                    s.skip(first)?;
                    continue;
                }
                loop {
                    match s.expect()? {
                        Event::ObjectEnd => break,
                        // Reasoning text (`reasoning_content`) is never surfaced.
                        Event::Key(k) if k.eq_plain("content") => {
                            if let Event::Str(t) = s.expect()? {
                                text.push_str(t.decode(&mut scratch.unescape)?);
                            }
                        }
                        Event::Key(_) => s.skip_value()?,
                        _ => return Err(Error::from(crate::base::Error::json("expected key"))),
                    }
                }
            }
            Event::Key(_) => s.skip_value()?,
            _ => return Err(Error::from(crate::base::Error::json("expected key"))),
        }
    }
    if !text.is_empty() {
        scratch.texts.push((index, text));
    }
    if let Some(f) = finish {
        info.finishes.push((index, f));
    }
    Ok(())
}

fn parse_usage(s: &mut Scanner<'_>) -> Result<Option<Reported>> {
    let first = s.expect()?;
    if first != Event::ObjectStart {
        s.skip(first)?;
        return Ok(None);
    }
    let mut r = Reported::default();
    loop {
        match s.expect()? {
            Event::ObjectEnd => return Ok(Some(r)),
            Event::Key(k) => match (k.as_plain().unwrap_or(""), s.expect()?) {
                ("prompt_tokens", Event::Num(n)) => r.prompt = json::parse_u64(n)?,
                ("completion_tokens", Event::Num(n)) => r.completion = json::parse_u64(n)?,
                ("prompt_tokens_details", Event::ObjectStart) => loop {
                    match s.expect()? {
                        Event::ObjectEnd => break,
                        Event::Key(k) if k.eq_plain("cached_tokens") => {
                            if let Event::Num(n) = s.expect()? {
                                r.cached = json::parse_u64(n)?;
                            }
                        }
                        Event::Key(_) => s.skip_value()?,
                        _ => return Err(Error::from(crate::base::Error::json("expected key"))),
                    }
                },
                (_, v) => s.skip(v)?,
            },
            _ => return Err(Error::from(crate::base::Error::json("expected key"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api_message(body: &[u8]) -> String {
        match Error::api(400, body) {
            Error::Api { message, .. } => message,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parses_interleaved_candidates_finishes_and_usage() {
        let mut sc = Scratch::default();
        let ev = r#"{"id":"x","choices":[{"index":1,"delta":{"role":"assistant","content":"SELECT \"b\""},"finish_reason":null},{"delta":{"content":"SEL","reasoning_content":"hmm"},"index":0}]}"#;
        let info = parse_event(ev, &mut sc).unwrap();
        assert_eq!(sc.texts, vec![(1, "SELECT \"b\"".to_owned()), (0, "SEL".to_owned())]);
        assert!(info.finishes.is_empty() && info.usage.is_none());
        sc.texts.clear();
        let last = r#"{"choices":[{"index":0,"delta":{},"finish_reason":"length"}],"usage":{"prompt_tokens":900,"completion_tokens":40,"prompt_tokens_details":{"cached_tokens":512},"total_tokens":940}}"#;
        let info = parse_event(last, &mut sc).unwrap();
        assert_eq!(info.finishes, vec![(0, Finish::Length)]);
        assert_eq!(info.usage, Some(Reported { prompt: 900, cached: 512, completion: 40 }));
    }

    #[test]
    fn unary_responses_and_errors() {
        let mut sc = Scratch::default();
        let r = r#"{"choices":[{"index":0,"message":{"role":"assistant","content":"SELECT 1"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":3}}"#;
        let info = parse_event(r, &mut sc).unwrap();
        assert_eq!(sc.texts, vec![(0, "SELECT 1".to_owned())]);
        assert_eq!(info.finishes, vec![(0, Finish::Stop)]);
        assert!(
            matches!(parse_event(r#"{"error":{"message":"bad\nkey","code":401}}"#, &mut sc), Err(Error::Api { status: 401, ref message }) if message == "badkey")
        );
        assert_eq!(api_message(br#"[{"error":{"code":400,"message":"Invalid n"}}]"#), "Invalid n");
    }
}
