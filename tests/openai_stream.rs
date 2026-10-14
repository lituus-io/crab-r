// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// The OpenAI-compatible backend against a scripted, in-process Transport:
// request shape (parts kept in order, grammar pass-through, extra members),
// n candidates interleaved in one stream (one prefill), abort cancelling
// the stream, and exact usage accounting.

#![cfg(feature = "openai")]

use crab_r::llm::openai::{OpenAi, OpenAiConfig};
use crab_r::llm::request::{Part, Prompt};
use crab_r::llm::{
    CacheSupport, Candidates, Caps, Constraint, ConstraintSupport, Error, Finish, Flow, GenRequest,
    Model, Price, Sink,
};
use objstore_r::join::spin_on as block_on;
use objstore_r::transport::{BodyStream, Request, Response, Transport};
use objstore_r::Bytes;
use std::cell::{Cell, RefCell};
use std::future::{ready, Ready};

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
        ready(Ok(Response { status: 200, body: Bytes::from(self.unary.clone()) }))
    }

    fn open<'a>(&'a self, req: Request<'a>) -> Self::Open<'a> {
        self.urls.borrow_mut().push(req.url.to_owned());
        self.bodies.borrow_mut().push(String::from_utf8(req.body.to_vec()).unwrap());
        let s = self.script.borrow_mut().remove(0);
        ready(Ok((s.status, MockBody { chunks: s.chunks.into_iter(), read: &self.chunks_read })))
    }
}

fn caps(lark: bool, gbnf: bool) -> Caps {
    Caps {
        id: "local".into(),
        constraints: ConstraintSupport { lark, gbnf, json_schema: true },
        max_candidates: 8,
        max_stream_candidates: 8,
        cache: CacheSupport::Implicit,
        context_tokens: 32_000,
        max_output_tokens: 1024,
        streaming: true,
        price: Price { input: 100_000, cached_input: 25_000, output: 400_000 },
    }
}

fn model(script: Vec<Scripted>, lark: bool, gbnf: bool) -> OpenAi<Mock> {
    let mock = Mock { script: RefCell::new(script), ..Mock::default() };
    OpenAi::new(
        mock,
        OpenAiConfig {
            url: "http://127.0.0.1:8000/v1/chat/completions".into(),
            model: "qwen".into(),
            caps: caps(lark, gbnf),
            extra: Some(r#""reasoning_effort":"none""#.into()),
        },
    )
}

const USAGE: &str = r#"data: {"choices":[],"usage":{"prompt_tokens":1000,"completion_tokens":14,"prompt_tokens_details":{"cached_tokens":768}}}

"#;

#[test]
fn one_stream_serves_n_interleaved_candidates() {
    let m = model(
        vec![Scripted {
            status: 200,
            chunks: vec![
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"SELECT \"}},{\"index\":1,\"delta\":{\"content\":\"SELECT 2\"}}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"1\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[{\"index\":1,\"delta\":{},\"finish_reason\":\"length\"}]}\n\n",
                USAGE,
                "data: [DONE]\n\n",
            ],
        }],
        true,
        false,
    );
    let parts = [Part::cached("SCHEMA"), Part::text("today"), Part::text("question")];
    let req = GenRequest {
        n: 2,
        temperature: 0.5,
        seed: Some(7),
        constraint: Constraint::Lark("start: \"SELECT\""),
        ..GenRequest::new(Prompt { system: "rules", parts: &parts })
    };
    let mut out = Candidates::new();
    let usage = block_on(m.generate(&req, &mut out, &mut ())).unwrap();
    assert_eq!((out.text(0), out.text(1)), ("SELECT 1", "SELECT 2"));
    assert_eq!((out.finish(0), out.finish(1)), (Finish::Stop, Finish::Length));
    assert_eq!(
        (usage.input_tokens, usage.cached_tokens, usage.output_tokens, usage.estimated),
        (232, 768, 14, false)
    );
    assert_eq!(
        usage.micro_usd,
        232 * 100_000 / 1_000_000 + 768 * 25_000 / 1_000_000 + 14 * 400_000 / 1_000_000 + 1
    );
    let bodies = m.transport().bodies.borrow();
    assert_eq!(bodies.len(), 1, "one request, one prefill for both candidates");
    let b = &bodies[0];
    assert!(b.starts_with(r#"{"model":"qwen","messages":[{"role":"system","content":"rules"},{"role":"user","content":[{"type":"text","text":"SCHEMA"},{"type":"text","text":"today"},{"type":"text","text":"question"}]}]"#), "{b}");
    assert!(
        b.contains(r#""n":2"#)
            && b.contains(r#""seed":7"#)
            && b.contains(r#""guided_grammar":"start: \"SELECT\"""#),
        "{b}"
    );
    assert!(
        b.ends_with(
            r#""stream":true,"stream_options":{"include_usage":true},"reasoning_effort":"none"}"#
        ),
        "{b}"
    );
}

struct AbortAfter(usize);

impl Sink for AbortAfter {
    fn delta(&mut self, _: u8, text: &str, _: &str) -> Flow {
        if text.len() >= self.0 {
            Flow::Abort
        } else {
            Flow::Continue
        }
    }
}

#[test]
fn an_aborted_stream_is_dropped_and_estimated() {
    let m = model(
        vec![Scripted {
            status: 200,
            chunks: vec![
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Sure! Here is\"}}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\" a long story\"}}]}\n\n",
                USAGE,
            ],
        }],
        false,
        true,
    );
    let parts = [Part::text("q")];
    let req = GenRequest {
        constraint: Constraint::Gbnf("root ::= \"x\""),
        ..GenRequest::new(Prompt { system: "", parts: &parts })
    };
    let mut out = Candidates::new();
    let usage = block_on(m.generate(&req, &mut out, &mut AbortAfter(5))).unwrap();
    assert_eq!(out.finish(0), Finish::Aborted);
    assert_eq!(m.transport().chunks_read.get(), 1, "stopped reading after the abort");
    assert!(usage.estimated && usage.output_tokens >= 1);
    assert!(m.transport().bodies.borrow()[0].contains(r#""grammar":"root ::= \"x\"""#));
}

#[test]
fn unsupported_constraints_and_api_errors_are_typed() {
    let m = model(
        vec![Scripted {
            status: 429,
            chunks: vec![r#"{"error":{"code":429,"message":"Too many requests"}}"#],
        }],
        false,
        false,
    );
    let parts = [Part::text("q")];
    let mut out = Candidates::new();
    let lark = GenRequest {
        constraint: Constraint::Lark("start: \"x\""),
        ..GenRequest::new(Prompt { system: "", parts: &parts })
    };
    assert!(matches!(block_on(m.generate(&lark, &mut out, &mut ())), Err(Error::Unsupported(_))));
    let plain = GenRequest::new(Prompt { system: "", parts: &parts });
    assert!(
        matches!(block_on(m.generate(&plain, &mut out, &mut ())), Err(Error::Api { status: 429, ref message }) if message == "Too many requests")
    );
}

#[test]
fn servers_that_only_multi_sample_unary_get_one_unary_request() {
    let unary = r#"{"choices":[{"index":0,"message":{"content":"SELECT 1"},"finish_reason":"stop"},{"index":1,"message":{"content":"SELECT 2"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1000,"completion_tokens":8}}"#;
    let mock = Mock { unary: unary.into(), ..Mock::default() };
    let mut cfg = OpenAiConfig {
        url: "http://h/v1/chat/completions".into(),
        model: "m".into(),
        caps: caps(false, false),
        extra: None,
    };
    cfg.caps.max_stream_candidates = 1;
    let m = OpenAi::new(mock, cfg);
    let parts = [Part::text("q")];
    let req = GenRequest { n: 2, ..GenRequest::new(Prompt { system: "", parts: &parts }) };
    let mut out = Candidates::new();
    let usage = block_on(m.generate(&req, &mut out, &mut ())).unwrap();
    assert_eq!((out.text(0), out.text(1)), ("SELECT 1", "SELECT 2"));
    assert_eq!((usage.input_tokens, usage.output_tokens), (1000, 8));
    let sends = m.transport().sends.borrow();
    assert_eq!(sends.len(), 1, "one request, one prefill");
    assert!(sends[0].1.contains(r#""n":2"#) && !sends[0].1.contains("stream"), "{}", sends[0].1);
    assert!(m.transport().bodies.borrow().is_empty(), "nothing streamed");
}
