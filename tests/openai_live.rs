// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Live check of an OpenAI-compatible server through the describe options
// (costs real money, fractions of a cent). Skipped unless all three are set:
//
//   CRAB_OPENAI_URL=https://host/v1 CRAB_OPENAI_MODEL=gpt-4o \
//   CRAB_OPENAI_KEY_ENV=NAME_OF_THE_VARIABLE_HOLDING_THE_KEY \
//     cargo test --features cli --test openai_live -- --nocapture
//
// One structured request for three candidates, unary (the default): three
// schema-valid answers from one request. Nothing about the server, the URL
// or the key is printed.

#![cfg(feature = "cli")]

use crab_r::base::json::{Event, Scanner};
use crab_r::describe::app;
use crab_r::llm::request::{Part, Prompt};
use crab_r::llm::{Candidates, Constraint, Finish, GenRequest, Model};

const SCHEMA: &str = r#"{"type":"object","properties":{"description":{"type":"string"}},"required":["description"],"additionalProperties":false}"#;

/// `text` is `{"description": "<non-empty string>"}` and nothing else.
fn schema_valid(text: &str) -> bool {
    let mut s = Scanner::new(text);
    let mut buf = String::new();
    let ok = matches!(s.next_event(), Ok(Some(Event::ObjectStart)))
        && matches!(s.next_event(), Ok(Some(Event::Key(k))) if k.eq_plain("description"))
        && matches!(s.next_event(), Ok(Some(Event::Str(v))) if v.decode(&mut buf).is_ok_and(|d| !d.trim().is_empty()))
        && matches!(s.next_event(), Ok(Some(Event::ObjectEnd)));
    ok && matches!(s.next_event(), Ok(None))
}

#[test]
fn live_structured_candidates_in_one_unary_request() {
    let (Ok(url), Ok(model), Ok(key_env)) = (
        std::env::var("CRAB_OPENAI_URL"),
        std::env::var("CRAB_OPENAI_MODEL"),
        std::env::var("CRAB_OPENAI_KEY_ENV"),
    ) else {
        eprintln!("CRAB_OPENAI_URL / CRAB_OPENAI_MODEL / CRAB_OPENAI_KEY_ENV not set; skipping");
        return;
    };
    if std::env::var_os(&key_env).is_none() {
        eprintln!("the variable CRAB_OPENAI_KEY_ENV names is not set; skipping");
        return;
    }
    let argv =
        ["p.d", "--openai-url", &url, "--openai-model", &model, "--openai-key-env", &key_env];
    let opts = app::parse(argv.iter().map(|s| (*s).to_owned())).expect("options");
    let m = opts.openai().expect("openai").connect().expect("key");
    let parts = [Part::text(
        "In one sentence, describe a BigQuery column `order_id INT64` of table `shop.orders` whose values are unique per row.",
    )];
    let req = GenRequest {
        n: 3,
        temperature: 0.7,
        max_output_tokens: 200,
        constraint: Constraint::JsonSchema(SCHEMA),
        ..GenRequest::new(Prompt {
            system:
                "You write column descriptions for a data catalog. Answer in the JSON schema given.",
            parts: &parts,
        })
    };
    let mut out = Candidates::new();
    let t = std::time::Instant::now();
    let usage = objstore_r::http::block_on(m.generate(&req, &mut out, &mut ()))
        .expect("runtime")
        .expect("generate");
    assert_eq!(out.len(), 3, "three candidates");
    for (i, (text, finish)) in out.iter().enumerate() {
        eprintln!("candidate {i} {finish:?}: {text}");
        assert_eq!(finish, Finish::Stop);
        assert!(schema_valid(text), "candidate {i} is not schema-valid: {text}");
    }
    eprintln!(
        "usage: {} input, {} cached, {} output tokens, {} micro-USD, estimated {}, in {:?}",
        usage.input_tokens,
        usage.cached_tokens,
        usage.output_tokens,
        usage.micro_usd,
        usage.estimated,
        t.elapsed()
    );
    assert!(!usage.estimated && usage.input_tokens + usage.cached_tokens > 0);
    assert!(usage.output_tokens > 0 && usage.micro_usd > 0);
}
