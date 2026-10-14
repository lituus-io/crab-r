// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Gemini on Vertex AI (feature `vertex`), over any objstore-r
//! [`Transport`] — in production [`objstore_r::http::Client`] with ADC tokens.
//!
//! Two paths, chosen per request for cost:
//!
//! - **One unary request, n candidates** (`:generateContent`,
//!   `candidateCount = n`) when `n > 1` and the request is not
//!   `independent`: one prompt prefill for all samples — the cheapest way to
//!   multi-sample. (Vertex streaming accepts only one candidate per request.)
//! - **Streams** (`:streamGenerateContent?alt=sse`, one candidate each),
//!   driven concurrently on the caller's task, for single samples and for
//!   `independent` requests. An aborted candidate's connection is dropped
//!   immediately, which stops its output billing. Byte-stable prompts keep
//!   repeated prefill cheap through Vertex's implicit prefix cache.
//!
//! Prompt order is preserved exactly (system instruction, then parts in
//! order), so the stable schema prefix is what the cache sees first.
//!
//! **Explicit context caching** ([`ExplicitCache`], optional): the prompt
//! prefix up to the last part marked cacheable is counted per distinct
//! prefix; once it is hot (seen `min_uses` times within the TTL) and large
//! enough (Vertex requires 1 024 tokens), the backend creates a
//! `cachedContents` resource and sends only the rest of each request with a
//! reference to it — cached input is billed at a tenth of the input price,
//! deterministically, where the implicit cache is best-effort. The creation
//! prefill and the TTL's storage are charged to the request that triggered
//! it, so budgets see the true cost. A failed creation backs off for a TTL
//! rather than retrying on every request.
//! Reasoning ("thought") parts are never surfaced; their tokens are counted as
//! output because that is how they are billed. Grammar constraints are not
//! supported by this API; JSON-schema constraints map to `responseSchema`.

use crate::base::json::{self, Event, Scanner, Writer};
use crate::llm::request::{
    estimate_tokens, Candidates, Constraint, Finish, Flow, GenRequest, Sink, Usage,
};
use crate::llm::{Caps, Error, Model, Result};
use graph_r::fsio::now_ms;
use objstore_r::transport::{BodyStream, Request, Transport};
use objstore_r::Bytes;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

/// Largest error body read from a failed call, bytes.
const MAX_ERROR_BODY: usize = 64 << 10;

/// Settings.
#[derive(Clone, Debug)]
pub struct VertexConfig {
    /// GCP project id.
    pub project: String,
    /// Location (`us-central1`, `global`, ...).
    pub location: String,
    /// Publisher model id (e.g. `gemini-2.5-flash`).
    pub model: String,
    /// Capabilities and prices to report (and to cap requests by).
    pub caps: Caps,
    /// Reasoning-token budget (`thinkingConfig.thinkingBudget`); `Some(0)`
    /// disables reasoning where the model allows it — the cheapest, fastest
    /// setting for well-grounded SQL generation. `None` keeps the default.
    pub thinking_budget: Option<i32>,
    /// Override the API origin (tests, private endpoints).
    pub endpoint: Option<String>,
    /// Explicit context caching of hot prompt prefixes (`None`: implicit
    /// caching only).
    pub cache: Option<ExplicitCache>,
}

/// When and how to cache prompt prefixes explicitly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExplicitCache {
    /// Cache lifetime, seconds (storage is billed for all of it).
    pub ttl_secs: u32,
    /// Sightings of a prefix within the TTL before it is cached (2: the
    /// second use creates the cache, from the third on it is served).
    pub min_uses: u32,
    /// Smallest prefix cached, tokens (Vertex rejects fewer than 1 024).
    pub min_tokens: u32,
    /// Storage price, micro-dollars per million tokens per hour.
    pub storage_price: u64,
}

impl Default for ExplicitCache {
    /// Ten minutes, cached from the second use, Gemini 2.5 storage price.
    fn default() -> Self {
        Self { ttl_secs: 600, min_uses: 2, min_tokens: 1_024, storage_price: 1_000_000 }
    }
}

/// One prompt prefix's cache state.
#[derive(Debug, Default)]
struct Prefix {
    /// Sightings since `window_ms`.
    uses: u32,
    window_ms: u64,
    /// Cache resource name and its expiry.
    cache: Option<(String, u64)>,
    creating: bool,
    /// No creation attempt before this time (after a failure).
    retry_ms: u64,
}

/// A Vertex AI Gemini model.
#[derive(Debug)]
pub struct Vertex<T: Transport> {
    transport: T,
    url: String,
    unary_url: String,
    caps: Caps,
    thinking_budget: Option<i32>,
    /// `.../cachedContents` endpoint and the model's resource name.
    caches_url: String,
    model_name: String,
    cache: Option<ExplicitCache>,
    prefixes: Mutex<HashMap<u128, Prefix>>,
}

impl<T: Transport> Vertex<T> {
    /// A model reached through `transport`.
    #[must_use]
    pub fn new(transport: T, cfg: VertexConfig) -> Self {
        let origin = cfg.endpoint.clone().unwrap_or_else(|| {
            if cfg.location == "global" {
                "https://aiplatform.googleapis.com".to_owned()
            } else {
                format!("https://{}-aiplatform.googleapis.com", cfg.location)
            }
        });
        let mut url = String::with_capacity(160);
        url.push_str(origin.trim_end_matches('/'));
        url.push_str("/v1/projects/");
        objstore_r::transport::encode_component(&cfg.project, &mut url);
        url.push_str("/locations/");
        objstore_r::transport::encode_component(&cfg.location, &mut url);
        let caches_url = format!("{url}/cachedContents");
        let model_name = format!(
            "projects/{}/locations/{}/publishers/google/models/{}",
            cfg.project, cfg.location, cfg.model
        );
        url.push_str("/publishers/google/models/");
        objstore_r::transport::encode_component(&cfg.model, &mut url);
        let unary_url = format!("{url}:generateContent");
        url.push_str(":streamGenerateContent?alt=sse");
        Self {
            transport,
            url,
            unary_url,
            caps: cfg.caps,
            thinking_budget: cfg.thinking_budget,
            caches_url,
            model_name,
            cache: cfg.cache,
            prefixes: Mutex::new(HashMap::new()),
        }
    }

    /// The streaming endpoint URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// The explicit cache covering this request's prefix, if any: its
    /// name and how many parts it holds — creating it when the prefix has
    /// become hot. Returns the creation's cost as usage.
    async fn prefix_cache(&self, req: &GenRequest<'_>) -> Result<(Option<(String, usize)>, Usage)> {
        let none = Ok((None, Usage::default()));
        let Some(pol) = self.cache else { return none };
        let Some(last) = req.prompt.parts.iter().rposition(|p| p.cache_break) else { return none };
        let prefix = &req.prompt.parts[..=last];
        let bytes = req.prompt.system.len() + prefix.iter().map(|p| p.text.len()).sum::<usize>();
        if estimate_tokens(bytes) < u64::from(pol.min_tokens) {
            return none;
        }
        let mut h = graph_r::hash::Hasher::new();
        h.update(req.prompt.system.as_bytes());
        for p in prefix {
            h.field(p.text.as_bytes());
        }
        let key = h.digest128();
        let now = now_ms();
        let ttl_ms = u64::from(pol.ttl_secs) * 1000;
        {
            let mut map = self.prefixes.lock().unwrap_or_else(PoisonError::into_inner);
            let e = map.entry(key).or_default();
            // Served while comfortably before expiry (a request in flight
            // must not outlive its cache).
            if let Some((name, exp)) = &e.cache {
                if *exp > now + 30_000 {
                    return Ok((Some((name.clone(), last + 1)), Usage::default()));
                }
                e.cache = None;
            }
            if now.saturating_sub(e.window_ms) > ttl_ms {
                e.window_ms = now;
                e.uses = 0;
            }
            e.uses += 1;
            if e.uses < pol.min_uses || e.creating || now < e.retry_ms {
                return none;
            }
            e.creating = true;
        }
        let created = self.create_cache(req.prompt.system, prefix, pol).await;
        let mut map = self.prefixes.lock().unwrap_or_else(PoisonError::into_inner);
        let e = map.entry(key).or_default();
        e.creating = false;
        let Ok((name, tokens, wall_ms)) = created else {
            // Too small, quota, permissions: fall back to implicit caching
            // and try again after a TTL.
            e.retry_ms = now + ttl_ms;
            return none;
        };
        e.cache = Some((name.clone(), now + ttl_ms));
        // Creation prefill at the input price, plus storage for the TTL.
        let storage =
            (u128::from(tokens) * u128::from(pol.storage_price) * u128::from(pol.ttl_secs))
                .div_ceil(3_600 * 1_000_000);
        let usage = Usage {
            input_tokens: tokens,
            cached_tokens: 0,
            output_tokens: 0,
            micro_usd: self
                .caps
                .price
                .cost(tokens, 0, 0)
                .saturating_add(u64::try_from(storage).unwrap_or(u64::MAX)),
            estimated: false,
            wall_ms,
        };
        Ok((Some((name, last + 1)), usage))
    }

    /// Create a `cachedContents` resource for `system` + `parts`: (name,
    /// tokens, wall ms).
    async fn create_cache(
        &self,
        system: &str,
        parts: &[crate::llm::Part<'_>],
        pol: ExplicitCache,
    ) -> Result<(String, u64, u64)> {
        let started = Instant::now();
        let mut out = String::with_capacity(
            system.len() + parts.iter().map(|p| p.text.len()).sum::<usize>() + 256,
        );
        let mut w = Writer::new(&mut out);
        w.begin_object().key("model").string(&self.model_name);
        if !system.is_empty() {
            w.key("systemInstruction").begin_object().key("parts").begin_array();
            w.begin_object().key("text").string(system).end_object();
            w.end_array().end_object();
        }
        w.key("contents")
            .begin_array()
            .begin_object()
            .key("role")
            .string("user")
            .key("parts")
            .begin_array();
        for p in parts {
            w.begin_object().key("text").string(p.text).end_object();
        }
        w.end_array().end_object().end_array();
        w.key("ttl").string(&format!("{}s", pol.ttl_secs));
        w.end_object();
        let resp =
            self.transport.send(Request::post_json(&self.caches_url, Bytes::from(out))).await?;
        if !resp.is_success() {
            return Err(Error::api(resp.status, &resp.body));
        }
        let text = std::str::from_utf8(&resp.body)
            .map_err(|_| Error::from(crate::base::Error::json("response is not UTF-8")))?;
        let (mut name, mut tokens) = (None, 0u64);
        let mut buf = String::new();
        let mut s = Scanner::new(text);
        if s.expect()? != Event::ObjectStart {
            return Err(Error::from(crate::base::Error::json("cache response is not an object")));
        }
        loop {
            match s.expect()? {
                Event::ObjectEnd => break,
                Event::Key(k) if k.eq_plain("name") => {
                    if let Event::Str(v) = s.expect()? {
                        name = Some(v.decode(&mut buf)?.to_owned());
                    }
                }
                Event::Key(k) if k.eq_plain("usageMetadata") => {
                    if s.expect()? == Event::ObjectStart {
                        loop {
                            match s.expect()? {
                                Event::ObjectEnd => break,
                                Event::Key(k) if k.eq_plain("totalTokenCount") => {
                                    if let Event::Num(n) = s.expect()? {
                                        tokens = json::parse_u64(n)?;
                                    }
                                }
                                Event::Key(_) => {
                                    let v = s.expect()?;
                                    s.skip(v)?;
                                }
                                _ => {
                                    return Err(Error::from(crate::base::Error::json(
                                        "expected key",
                                    )))
                                }
                            }
                        }
                    }
                }
                Event::Key(_) => {
                    let v = s.expect()?;
                    s.skip(v)?;
                }
                _ => return Err(Error::from(crate::base::Error::json("expected key"))),
            }
        }
        let name = name
            .ok_or_else(|| Error::from(crate::base::Error::json("cache response has no name")))?;
        Ok((name, tokens, started.elapsed().as_millis() as u64))
    }

    /// Serialise the wire request for `count` candidates; with `cached`,
    /// the cache replaces the system instruction and the first parts.
    fn body(
        &self,
        req: &GenRequest<'_>,
        count: u8,
        cached: Option<&(String, usize)>,
    ) -> Result<Bytes> {
        let mut out = String::with_capacity(req.prompt.byte_len() + 512);
        let mut w = Writer::new(&mut out);
        w.begin_object();
        let skip = cached.map_or(0, |c| c.1);
        if let Some((name, _)) = cached {
            w.key("cachedContent").string(name);
        } else if !req.prompt.system.is_empty() {
            w.key("systemInstruction").begin_object().key("parts").begin_array();
            w.begin_object().key("text").string(req.prompt.system).end_object();
            w.end_array().end_object();
        }
        w.key("contents")
            .begin_array()
            .begin_object()
            .key("role")
            .string("user")
            .key("parts")
            .begin_array();
        for p in &req.prompt.parts[skip..] {
            w.begin_object().key("text").string(p.text).end_object();
        }
        w.end_array().end_object().end_array();
        w.key("generationConfig").begin_object();
        w.key("temperature").float(f64::from(req.temperature));
        w.key("maxOutputTokens")
            .uint(u64::from(req.max_output_tokens.min(self.caps.max_output_tokens)));
        if count > 1 {
            w.key("candidateCount").uint(u64::from(count));
        }
        if !req.stop.is_empty() {
            w.key("stopSequences").begin_array();
            for s in req.stop {
                w.string(s);
            }
            w.end_array();
        }
        if let Some(seed) = req.seed {
            // The API takes an int32 seed.
            w.key("seed").int(i64::from((seed & 0x7FFF_FFFF) as i32));
        }
        match req.constraint {
            Constraint::None => {}
            Constraint::JsonSchema(schema) => {
                if !self.caps.constraints.json_schema {
                    return Err(Error::Unsupported("json schema"));
                }
                w.key("responseMimeType")
                    .string("application/json")
                    .key("responseSchema")
                    .raw(schema);
            }
            Constraint::Lark(_) | Constraint::Gbnf(_) => {
                return Err(Error::Unsupported("grammar constraints on Vertex AI"));
            }
        }
        if let Some(b) = self.thinking_budget {
            w.key("thinkingConfig")
                .begin_object()
                .key("thinkingBudget")
                .int(i64::from(b))
                .end_object();
        }
        w.end_object().end_object();
        Ok(Bytes::from(out))
    }

    /// One unary call returning `n` candidates (one prefill for all).
    async fn unary<S: Sink>(
        &self,
        body: Bytes,
        n: usize,
        shared: &RefCell<(&mut Candidates, &mut S)>,
    ) -> Result<Usage> {
        let started = Instant::now();
        let resp = self.transport.send(Request::post_json(&self.unary_url, body)).await?;
        if !resp.is_success() {
            return Err(Error::api(resp.status, &resp.body));
        }
        let text = std::str::from_utf8(&resp.body)
            .map_err(|_| Error::from(crate::base::Error::json("response is not UTF-8")))?;
        let mut scratch = ChunkScratch::default();
        let info = parse_chunk(text, &mut scratch)?;
        {
            let mut guard = shared.borrow_mut();
            let (cands, sink) = &mut *guard;
            for (idx, t) in scratch.texts.drain(..) {
                cands.push(idx.min(n - 1), &t, *sink);
            }
            for &(idx, f) in &info.finishes {
                cands.set_finish(idx.min(n - 1), f);
            }
            for i in 0..n {
                cands.settle(i, Finish::Stop);
            }
        }
        let r = info.usage.unwrap_or_default();
        let input = r.prompt.saturating_sub(r.cached);
        let output = r.candidates + r.thoughts;
        Ok(Usage {
            input_tokens: input,
            cached_tokens: r.cached,
            output_tokens: output,
            micro_usd: self.caps.price.cost(input, r.cached, output),
            estimated: info.usage.is_none(),
            wall_ms: started.elapsed().as_millis() as u64,
        })
    }

    /// Run one stream serving candidate slots `base..base + count`.
    async fn stream<S: Sink>(
        &self,
        body: Bytes,
        base: usize,
        count: usize,
        shared: &RefCell<(&mut Candidates, &mut S)>,
        prompt_bytes: usize,
    ) -> Result<Usage> {
        let started = Instant::now();
        let req = Request::post_json(&self.url, body);
        let (status, mut stream) = self.transport.open(req).await?;
        if !(200..300).contains(&status) {
            let mut msg = Vec::new();
            while let Some(c) = stream.next_chunk().await? {
                msg.extend_from_slice(
                    &c[..c.len().min(MAX_ERROR_BODY - msg.len().min(MAX_ERROR_BODY))],
                );
                if msg.len() >= MAX_ERROR_BODY {
                    break;
                }
            }
            return Err(Error::api(status, &msg));
        }
        let mut sse = objstore_r::sse::SseDecoder::new();
        let mut scratch = ChunkScratch::default();
        let mut reported: Option<Reported> = None;
        let mut aborted_any = false;
        'read: while let Some(chunk) = stream.next_chunk().await? {
            sse.push(&chunk)?;
            while let Some(data) = sse.next_data()? {
                let info = parse_chunk(data, &mut scratch)?;
                if let Some(r) = info.usage {
                    reported = Some(r);
                }
                let mut guard = shared.borrow_mut();
                let (cands, sink) = &mut *guard;
                for (idx, text) in scratch.texts.drain(..) {
                    let slot = base + idx.min(count.saturating_sub(1));
                    if cands.push(slot, &text, *sink) == Flow::Abort {
                        aborted_any = true;
                    }
                }
                for &(idx, f) in &info.finishes {
                    cands.set_finish(base + idx.min(count.saturating_sub(1)), f);
                }
                let mine_done = (base..base + count).all(|i| cands.finish(i) != Finish::Pending);
                drop(guard);
                if mine_done && aborted_any {
                    // Every candidate of this stream is settled and at least one
                    // was aborted: stop reading (dropping the stream cancels it).
                    break 'read;
                }
            }
        }
        let wall_ms = started.elapsed().as_millis() as u64;
        // Candidates the stream ended without settling ended naturally.
        {
            let mut guard = shared.borrow_mut();
            for i in base..base + count {
                guard.0.settle(i, Finish::Stop);
            }
        }
        Ok(match reported {
            Some(r) if !aborted_any => {
                let input = r.prompt.saturating_sub(r.cached);
                let output = r.candidates + r.thoughts;
                Usage {
                    input_tokens: input,
                    cached_tokens: r.cached,
                    output_tokens: output,
                    micro_usd: self.caps.price.cost(input, r.cached, output),
                    estimated: false,
                    wall_ms,
                }
            }
            _ => {
                // Aborted before final usage: estimate conservatively from
                // bytes (reasoning tokens, if any, are not visible here).
                let guard = shared.borrow();
                let out_bytes: usize = (base..base + count).map(|i| guard.0.text(i).len()).sum();
                // Early chunks may omit the prompt count: never under-report.
                let input = reported
                    .filter(|r| r.prompt > 0)
                    .map_or(estimate_tokens(prompt_bytes), |r| r.prompt.saturating_sub(r.cached));
                let cached = reported.map_or(0, |r| r.cached);
                let output = reported
                    .map_or(0, |r| r.candidates + r.thoughts)
                    .max(estimate_tokens(out_bytes));
                Usage {
                    input_tokens: input,
                    cached_tokens: cached,
                    output_tokens: output,
                    micro_usd: self.caps.price.cost(input, cached, output),
                    estimated: true,
                    wall_ms,
                }
            }
        })
    }
}

impl<T: Transport> Model for Vertex<T> {
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
            let n = usize::from(req.n.max(1));
            out.reset(n);
            let prompt_bytes = req.prompt.byte_len();
            let shared = RefCell::new((out, sink));
            let (cached, cache_cost) = self.prefix_cache(req).await?;
            if n > 1 && !req.independent && n <= usize::from(self.caps.max_candidates) {
                let body = self.body(req, n as u8, cached.as_ref())?;
                return Ok(self.unary(body, n, &shared).await?.plus(cache_cost));
            }
            // Independent streams: one body, shared by reference count.
            let body = self.body(req, 1, cached.as_ref())?;
            let streams = (0..n).map(|i| self.stream(body.clone(), i, 1, &shared, prompt_bytes));
            let results = objstore_r::join::join_all(streams).await;
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
            // Partial success is still useful output; fail only if all failed.
            match first_err {
                Some(e) if ok == 0 => Err(e),
                _ => Ok(total.plus(cache_cost)),
            }
        }
    }
}

/// Final usage as reported by the provider (cumulative per stream).
#[derive(Clone, Copy, Debug, Default)]
struct Reported {
    prompt: u64,
    cached: u64,
    candidates: u64,
    thoughts: u64,
}

/// Reusable per-stream parse buffers.
#[derive(Debug, Default)]
struct ChunkScratch {
    /// (candidate index, text) produced by the current event, in order.
    texts: Vec<(usize, String)>,
    unescape: String,
}

#[derive(Debug, Default)]
struct ChunkInfo {
    usage: Option<Reported>,
    finishes: Vec<(usize, Finish)>,
}

fn finish_from(reason: &str) -> Option<Finish> {
    Some(match reason {
        "STOP" => Finish::Stop,
        "MAX_TOKENS" => Finish::Length,
        "SAFETY" | "RECITATION" | "BLOCKLIST" | "PROHIBITED_CONTENT" | "SPII" | "IMAGE_SAFETY" => {
            Finish::Filtered
        }
        "FINISH_REASON_UNSPECIFIED" | "" => return None,
        _ => Finish::Other,
    })
}

/// Parse one streamed `GenerateContentResponse` event. Text goes to
/// `scratch.texts`; finishes and usage are returned.
fn parse_chunk(data: &str, scratch: &mut ChunkScratch) -> Result<ChunkInfo> {
    let mut info = ChunkInfo::default();
    let mut s = Scanner::new(data);
    if s.expect()? != Event::ObjectStart {
        return Err(Error::from(crate::base::Error::json("event is not an object")));
    }
    loop {
        match s.expect()? {
            Event::ObjectEnd => break,
            Event::Key(k) if k.eq_plain("candidates") => {
                if s.expect()? != Event::ArrayStart {
                    return Err(Error::from(crate::base::Error::json(
                        "candidates is not an array",
                    )));
                }
                loop {
                    match s.expect()? {
                        Event::ArrayEnd => break,
                        Event::ObjectStart => parse_candidate(&mut s, scratch, &mut info)?,
                        other => s.skip(other)?,
                    }
                }
            }
            Event::Key(k) if k.eq_plain("usageMetadata") => info.usage = Some(parse_usage(&mut s)?),
            Event::Key(k) if k.eq_plain("error") => return Err(Error::api(0, data.as_bytes())),
            Event::Key(_) => {
                s.skip_value()?;
            }
            _ => return Err(Error::from(crate::base::Error::json("expected key"))),
        }
    }
    Ok(info)
}

fn parse_candidate(
    s: &mut Scanner<'_>,
    scratch: &mut ChunkScratch,
    info: &mut ChunkInfo,
) -> Result<()> {
    // "index" may follow "content": buffer this candidate's text until the end.
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
            Event::Key(k) if k.eq_plain("finishReason") => {
                if let Event::Str(r) = s.expect()? {
                    finish = finish_from(r.decode(&mut scratch.unescape)?);
                }
            }
            Event::Key(k) if k.eq_plain("content") => {
                parse_content(s, &mut scratch.unescape, &mut text)?;
            }
            Event::Key(_) => {
                s.skip_value()?;
            }
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

fn parse_content(s: &mut Scanner<'_>, unescape: &mut String, text: &mut String) -> Result<()> {
    let first = s.expect()?;
    if first != Event::ObjectStart {
        return s.skip(first).map_err(Error::from);
    }
    loop {
        match s.expect()? {
            Event::ObjectEnd => return Ok(()),
            Event::Key(k) if k.eq_plain("parts") => {
                if s.expect()? != Event::ArrayStart {
                    return Err(Error::from(crate::base::Error::json("parts is not an array")));
                }
                loop {
                    match s.expect()? {
                        Event::ArrayEnd => break,
                        Event::ObjectStart => {
                            let mut part = None;
                            let mut thought = false;
                            loop {
                                match s.expect()? {
                                    Event::ObjectEnd => break,
                                    Event::Key(k) if k.eq_plain("text") => {
                                        if let Event::Str(t) = s.expect()? {
                                            part = Some(t);
                                        }
                                    }
                                    Event::Key(k) if k.eq_plain("thought") => {
                                        thought = s.expect()? == Event::Bool(true);
                                    }
                                    Event::Key(_) => {
                                        let v = s.expect()?;
                                        s.skip(v)?;
                                    }
                                    _ => {
                                        return Err(Error::from(crate::base::Error::json(
                                            "expected key",
                                        )))
                                    }
                                }
                            }
                            if let (Some(t), false) = (part, thought) {
                                text.push_str(t.decode(unescape)?);
                            }
                        }
                        other => s.skip(other)?,
                    }
                }
            }
            Event::Key(_) => {
                s.skip_value()?;
            }
            _ => return Err(Error::from(crate::base::Error::json("expected key"))),
        }
    }
}

fn parse_usage(s: &mut Scanner<'_>) -> Result<Reported> {
    let mut r = Reported::default();
    let first = s.expect()?;
    if first != Event::ObjectStart {
        s.skip(first)?;
        return Ok(r);
    }
    loop {
        match s.expect()? {
            Event::ObjectEnd => return Ok(r),
            Event::Key(k) => {
                let name = k.as_plain().unwrap_or("");
                match (name, s.expect()?) {
                    ("promptTokenCount", Event::Num(n)) => r.prompt = json::parse_u64(n)?,
                    ("cachedContentTokenCount", Event::Num(n)) => r.cached = json::parse_u64(n)?,
                    ("candidatesTokenCount", Event::Num(n)) => r.candidates = json::parse_u64(n)?,
                    ("thoughtsTokenCount", Event::Num(n)) => r.thoughts = json::parse_u64(n)?,
                    (_, v) => s.skip(v)?,
                }
            }
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
    fn parses_text_finish_and_usage_regardless_of_key_order() {
        let mut scratch = ChunkScratch::default();
        let ev = r#"{"candidates":[{"finishReason":"STOP","content":{"role":"model","parts":[{"text":"SELECT \"a\""},{"text":"secret","thought":true}]},"index":1}],"usageMetadata":{"promptTokenCount":120,"cachedContentTokenCount":100,"candidatesTokenCount":7,"thoughtsTokenCount":3}}"#;
        let info = parse_chunk(ev, &mut scratch).unwrap();
        assert_eq!(
            scratch.texts,
            vec![(1, "SELECT \"a\"".to_owned())],
            "thought parts are dropped"
        );
        assert_eq!(info.finishes, vec![(1, Finish::Stop)]);
        let u = info.usage.unwrap();
        assert_eq!((u.prompt, u.cached, u.candidates, u.thoughts), (120, 100, 7, 3));
    }

    #[test]
    fn stream_errors_are_typed() {
        let mut scratch = ChunkScratch::default();
        let r =
            parse_chunk(r#"{"error":{"code":429,"message":"Resource exhausted\n"}}"#, &mut scratch);
        assert!(
            matches!(r, Err(Error::Api { status: 429, ref message }) if message == "Resource exhausted")
        );
        assert!(Error::Api { status: 429, message: String::new() }.is_retriable());
    }

    #[test]
    fn error_bodies_prefer_the_json_message() {
        assert_eq!(
            api_message(br#"{"error":{"code":403,"message":"Permission denied"}}"#),
            "Permission denied"
        );
        assert_eq!(api_message(b"plain\x1b text"), "plain text");
    }

    #[test]
    fn finish_reasons_map() {
        assert_eq!(finish_from("MAX_TOKENS"), Some(Finish::Length));
        assert_eq!(finish_from("SAFETY"), Some(Finish::Filtered));
        assert_eq!(finish_from("FINISH_REASON_UNSPECIFIED"), None);
        assert_eq!(finish_from("NEW_REASON"), Some(Finish::Other));
    }
}
