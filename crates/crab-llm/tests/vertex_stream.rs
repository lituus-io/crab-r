// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// The Vertex backend's streaming path against a scripted, in-process
// Transport: request shape, multi-candidate vs independent streams, abort
// cancelling a stream, error mapping and usage accounting.

#![cfg(feature = "vertex")]

use crab_llm::request::{Part, Prompt};
use crab_llm::vertex::{ExplicitCache, Vertex, VertexConfig};
use crab_llm::{
    CacheSupport, Candidates, Caps, Constraint, ConstraintSupport, Error, Finish, Flow, GenRequest,
    Model, Price,
};
use objstore_r::transport::{BodyStream, Request, Response, Transport};
use objstore_r::Bytes;
use std::cell::{Cell, RefCell};
use std::future::{ready, Future, Ready};

/// One scripted response: status and body chunks.
struct Scripted {
    status: u16,
    chunks: Vec<&'static str>,
}

#[derive(Default)]
struct Mock {
    script: RefCell<Vec<Scripted>>,
    bodies: RefCell<Vec<String>>,
    urls: RefCell<Vec<String>>,
    chunks_read: Cell<usize>,
    unary: String,
    sends: RefCell<Vec<(String, String)>>,
    /// Reply to `cachedContents` creation: (status, body).
    cache_reply: (u16, String),
}

struct MockBody<'a> {
    chunks: std::vec::IntoIter<&'static str>,
    read: &'a Cell<usize>,
}

impl BodyStream for MockBody<'_> {
    type Next<'b>
        = Ready<objstore_r::Result<Option<Bytes>>>
    where
        Self: 'b;
    fn next_chunk(&mut self) -> Self::Next<'_> {
        let c = self.chunks.next().map(|c| {
            self.read.set(self.read.get() + 1);
            Bytes::from_static(c.as_bytes())
        });
        ready(Ok(c))
    }
}

impl Transport for Mock {
    type Send<'a> = Ready<objstore_r::Result<Response>>;
    type Body<'a> = MockBody<'a>;
    type Open<'a> = Ready<objstore_r::Result<(u16, MockBody<'a>)>>;

    fn send<'a>(&'a self, req: Request<'a>) -> Self::Send<'a> {
        self.sends
            .borrow_mut()
            .push((req.url.to_owned(), String::from_utf8(req.body.to_vec()).unwrap()));
        if req.url.ends_with("/cachedContents") {
            let (status, body) = &self.cache_reply;
            return ready(Ok(Response { status: *status, body: Bytes::from(body.clone()) }));
        }
        ready(Ok(Response { status: 200, body: Bytes::from(self.unary.clone()) }))
    }

    fn open<'a>(&'a self, req: Request<'a>) -> Self::Open<'a> {
        self.urls.borrow_mut().push(req.url.to_owned());
        self.bodies.borrow_mut().push(String::from_utf8(req.body.to_vec()).unwrap());
        let s = self.script.borrow_mut().remove(0);
        ready(Ok((s.status, MockBody { chunks: s.chunks.into_iter(), read: &self.chunks_read })))
    }
}

fn caps(max_candidates: u8) -> Caps {
    Caps {
        id: "gemini-test".into(),
        constraints: ConstraintSupport { lark: false, gbnf: false, json_schema: true },
        max_candidates,
        max_stream_candidates: 1,
        cache: CacheSupport::Implicit,
        context_tokens: 1_000_000,
        max_output_tokens: 1024,
        streaming: true,
        price: Price { input: 300_000, cached_input: 75_000, output: 2_500_000 },
    }
}

fn model(script: Vec<Scripted>, max_candidates: u8) -> Vertex<Mock> {
    model_unary(script, String::new(), max_candidates)
}

fn model_unary(script: Vec<Scripted>, unary: String, max_candidates: u8) -> Vertex<Mock> {
    let mock = Mock { script: RefCell::new(script), unary, ..Mock::default() };
    Vertex::new(
        mock,
        VertexConfig {
            project: "my-project".into(),
            location: "us-central1".into(),
            model: "gemini-2.5-flash".into(),
            caps: caps(max_candidates),
            thinking_budget: Some(0),
            endpoint: None,
            cache: None,
        },
    )
}

fn block_on<F: Future>(f: F) -> F::Output {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let mut f = std::pin::pin!(f);
    loop {
        if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

const USAGE: &str = r#""usageMetadata":{"promptTokenCount":1000,"cachedContentTokenCount":800,"candidatesTokenCount":20,"thoughtsTokenCount":0}"#;

#[test]
fn one_unary_request_serves_n_candidates_with_exact_usage() {
    let unary = format!(
        "{{\"candidates\":[{{\"content\":{{\"parts\":[{{\"text\":\"SELECT 1\"}}]}},\"finishReason\":\"STOP\",\"index\":0}},{{\"content\":{{\"parts\":[{{\"text\":\"SELECT 2\\\"\"}}]}},\"finishReason\":\"MAX_TOKENS\",\"index\":1}}],{USAGE}}}"
    );
    let m = model_unary(vec![Scripted { status: 200, chunks: vec![] }], unary, 8);
    let parts = [Part::cached("SCHEMA"), Part::text("question")];
    let req = GenRequest {
        n: 2,
        temperature: 0.7,
        seed: Some(42),
        ..GenRequest::new(Prompt { system: "rules", parts: &parts })
    };
    let mut out = Candidates::new();
    let usage = block_on(m.generate(&req, &mut out, &mut ())).unwrap();

    assert_eq!(out.text(0), "SELECT 1");
    assert_eq!(out.text(1), "SELECT 2\"");
    assert_eq!((out.finish(0), out.finish(1)), (Finish::Stop, Finish::Length));
    assert_eq!((usage.input_tokens, usage.cached_tokens, usage.output_tokens), (200, 800, 20));
    assert!(!usage.estimated);
    assert_eq!(usage.micro_usd, m.caps().price.cost(200, 800, 20));

    let t = m.transport();
    assert_eq!(t.sends.borrow().len(), 1, "one prefill for both candidates");
    assert!(t.bodies.borrow().is_empty(), "no stream opened");
    let (url, body) = &t.sends.borrow()[0];
    assert!(url.ends_with("/publishers/google/models/gemini-2.5-flash:generateContent"), "{url}");
    assert!(
        body.contains(r#""candidateCount":2"#)
            && body.contains(r#""thinkingBudget":0"#)
            && body.contains(r#""seed":42"#)
    );
    let (sys, schema, q) =
        (body.find("rules").unwrap(), body.find("SCHEMA").unwrap(), body.find("question").unwrap());
    assert!(sys < schema && schema < q, "stable prefix first, in order");
}

#[test]
fn single_samples_stream_without_candidate_count() {
    let script = vec![Scripted {
        status: 200,
        chunks: vec![
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"SELECT \"}]}}]}\n\n",
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"1\\\"\"}]},\"finishReason\":\"STOP\"}],",
            USAGE,
            "}\n\n",
        ],
    }];
    let m = model(script, 8);
    let parts = [Part::text("q")];
    let req = GenRequest::new(Prompt { system: "", parts: &parts });
    let mut out = Candidates::new();
    block_on(m.generate(&req, &mut out, &mut ())).unwrap();
    assert_eq!(out.text(0), "SELECT 1\"");
    let body = &m.transport().bodies.borrow()[0];
    assert!(!body.contains("candidateCount"), "streaming requests carry one candidate");
    assert_eq!(
        m.transport().urls.borrow()[0],
        "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/us-central1/publishers/google/models/gemini-2.5-flash:streamGenerateContent?alt=sse"
    );
}

#[test]
fn independent_streams_abort_and_cancel_individually() {
    let good = Scripted {
        status: 200,
        chunks: vec![
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"SELECT o.id\"}]},\"finishReason\":\"STOP\"}],",
            USAGE,
            "}\n\n",
        ],
    };
    let bad = Scripted {
        status: 200,
        chunks: vec![
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"DELETE\"}]}}]}\n\n",
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\" FROM t\"}]}}]}\n\n",
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\" never read\"}]}}]}\n\n",
        ],
    };
    let m = model(vec![good, bad], 8);
    let parts = [Part::text("q")];
    let req = GenRequest {
        n: 2,
        independent: true,
        ..GenRequest::new(Prompt { system: "", parts: &parts })
    };
    let mut out = Candidates::new();
    let mut validator = |_: u8, text: &str, _: &str| {
        if text.starts_with("DELETE") {
            Flow::Abort
        } else {
            Flow::Continue
        }
    };
    let usage = block_on(m.generate(&req, &mut out, &mut validator)).unwrap();

    assert_eq!(out.text(0), "SELECT o.id");
    assert_eq!(out.finish(1), Finish::Aborted);
    assert_eq!(out.text(1), "DELETE", "nothing after the abort is accepted");
    assert_eq!(
        m.transport().chunks_read.get(),
        3 + 1,
        "the aborted stream was dropped after its first chunk"
    );
    assert!(usage.estimated, "an aborted stream's usage is an honest estimate");
    let body = &m.transport().bodies.borrow()[0];
    assert!(!body.contains("candidateCount"), "one candidate per independent stream");
}

#[test]
fn too_many_candidates_fall_back_to_parallel_streams() {
    let one = || {
        Scripted {
        status: 200,
        chunks: vec!["data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"x\"}]},\"finishReason\":\"STOP\"}]}\n\n"],
    }
    };
    let m = model(vec![one(), one(), one()], 1);
    let parts = [Part::text("q")];
    let req = GenRequest { n: 3, ..GenRequest::new(Prompt { system: "", parts: &parts }) };
    let mut out = Candidates::new();
    block_on(m.generate(&req, &mut out, &mut ())).unwrap();
    assert_eq!(m.transport().bodies.borrow().len(), 3);
    assert!(out.iter().all(|(t, f)| t == "x" && f == Finish::Stop));
}

#[test]
fn http_errors_map_to_api_errors() {
    let script = vec![Scripted {
        status: 403,
        chunks: vec![r#"{"error":{"code":403,"message":"Vertex AI API has not been used"}}"#],
    }];
    let m = model(script, 8);
    let parts = [Part::text("q")];
    let req = GenRequest::new(Prompt { system: "", parts: &parts });
    let r = block_on(m.generate(&req, &mut Candidates::new(), &mut ()));
    assert!(
        matches!(r, Err(Error::Api { status: 403, ref message }) if message.contains("not been used")),
        "{r:?}"
    );
}

#[test]
fn grammar_constraints_are_refused_before_any_call() {
    let m = model(vec![], 8);
    let parts = [Part::text("q")];
    let req = GenRequest {
        constraint: Constraint::Lark("start: \"x\""),
        ..GenRequest::new(Prompt { system: "", parts: &parts })
    };
    let r = block_on(m.generate(&req, &mut Candidates::new(), &mut ()));
    assert!(matches!(r, Err(Error::Unsupported(_))));
    assert!(m.transport().bodies.borrow().is_empty());
}

// ---- explicit context caching ---------------------------------------------

const DONE: &str = "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"SELECT 1\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":1010,\"cachedContentTokenCount\":1000,\"candidatesTokenCount\":3}}\n\n";

fn cached_model(cache_reply: (u16, String), calls: usize) -> Vertex<Mock> {
    let script = (0..calls).map(|_| Scripted { status: 200, chunks: vec![DONE] }).collect();
    let mock = Mock { script: RefCell::new(script), cache_reply, ..Mock::default() };
    Vertex::new(
        mock,
        VertexConfig {
            project: "my-project".into(),
            location: "us-central1".into(),
            model: "gemini-2.5-flash".into(),
            caps: caps(8),
            thinking_budget: Some(0),
            endpoint: None,
            cache: Some(ExplicitCache {
                ttl_secs: 600,
                min_uses: 2,
                min_tokens: 100,
                storage_price: 1_000_000,
            }),
        },
    )
}

#[test]
fn hot_prefixes_are_cached_explicitly_and_costed() {
    let created = r#"{"name":"projects/1/locations/us-central1/cachedContents/42","usageMetadata":{"totalTokenCount":1000}}"#;
    let m = cached_model((200, created.into()), 3);
    let schema = "TABLE x AS x  -- rows\n".repeat(40);
    let mut out = Candidates::new();
    let mut usages = Vec::new();
    for q in ["q1", "q2", "q3"] {
        let parts = [Part::cached(&schema), Part::text("today"), Part::text(q)];
        let req = GenRequest::new(Prompt { system: "rules", parts: &parts });
        usages.push(block_on(m.generate(&req, &mut out, &mut ())).unwrap());
    }
    let bodies = m.transport().bodies.borrow();
    // 1st sighting: sent whole.
    assert!(bodies[0].contains("systemInstruction") && bodies[0].contains("TABLE x"));
    // 2nd: cache created (model resource, system, prefix, ttl), then referenced.
    let sends = m.transport().sends.borrow();
    assert_eq!(sends.len(), 1, "created once");
    assert!(sends[0]
        .0
        .ends_with("/v1/projects/my-project/locations/us-central1/cachedContents"));
    let c = &sends[0].1;
    assert!(c.starts_with(r#"{"model":"projects/my-project/locations/us-central1/publishers/google/models/gemini-2.5-flash","systemInstruction""#), "{c}");
    assert!(
        c.contains("TABLE x") && !c.contains("today") && c.ends_with(r#""ttl":"600s"}"#),
        "{c}"
    );
    for b in &bodies[1..] {
        assert!(b.starts_with(r#"{"cachedContent":"projects/1/locations/us-central1/cachedContents/42","contents":[{"role":"user","parts":[{"text":"today"}"#), "{b}");
        assert!(
            !b.contains("systemInstruction") && !b.contains("TABLE x"),
            "prefix not resent: {b}"
        );
    }
    // Creation cost lands on the request that triggered it: 1000 tokens at
    // $0.30/M plus 10 minutes of storage at $1/M/h.
    let base = usages[2].micro_usd;
    assert_eq!(usages[1].micro_usd, base + 300 + 167, "{usages:?}");
    assert_eq!((usages[2].cached_tokens, usages[2].input_tokens), (1000, 10));
}

#[test]
fn failed_cache_creation_falls_back_and_backs_off() {
    let m = cached_model(
        (400, r#"{"error":{"code":400,"message":"minimum token count is 1024"}}"#.into()),
        4,
    );
    let schema = "TABLE x AS x  -- rows\n".repeat(40);
    let mut out = Candidates::new();
    for q in ["q1", "q2", "q3", "q4"] {
        let parts = [Part::cached(&schema), Part::text(q)];
        let req = GenRequest::new(Prompt { system: "rules", parts: &parts });
        block_on(m.generate(&req, &mut out, &mut ())).unwrap();
    }
    assert_eq!(m.transport().sends.borrow().len(), 1, "one attempt, then backs off for a TTL");
    assert!(m
        .transport()
        .bodies
        .borrow()
        .iter()
        .all(|b| b.contains("systemInstruction") && b.contains("TABLE x")));
}
