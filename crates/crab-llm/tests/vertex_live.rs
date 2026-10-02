// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Live Vertex AI check (costs real money, fractions of a cent): ignored by
// default. Run with
//
//   CRAB_VERTEX_PROJECT=my-project \
//     cargo test -p crab-llm --features vertex --test vertex_live -- --ignored --nocapture
//
// Credentials come from Application Default Credentials.

#![cfg(feature = "vertex")]

use crab_llm::request::{Part, Prompt};
use crab_llm::vertex::{Vertex, VertexConfig};
use crab_llm::{
    CacheSupport, Candidates, Caps, ConstraintSupport, Finish, Flow, GenRequest, Model, Price,
};
use objstore_r::http::{Client, HttpConfig};
use objstore_r::token::Adc;

fn live() -> Option<Vertex<Client<Adc>>> {
    let project = std::env::var("CRAB_VERTEX_PROJECT").ok()?;
    let model = std::env::var("CRAB_VERTEX_MODEL").unwrap_or_else(|_| "gemini-2.5-flash".into());
    let location = std::env::var("CRAB_VERTEX_LOCATION").unwrap_or_else(|_| "us-central1".into());
    let client = Client::new(Adc::discover().expect("ADC"), HttpConfig::default()).expect("client");
    Some(Vertex::new(
        client,
        VertexConfig {
            project,
            location,
            model: model.clone(),
            caps: Caps {
                id: model,
                constraints: ConstraintSupport { lark: false, gbnf: false, json_schema: true },
                max_candidates: 8,
                max_stream_candidates: 1,
                cache: CacheSupport::Implicit,
                context_tokens: 1_048_576,
                max_output_tokens: 8192,
                streaming: true,
                // Published list prices, micro-USD per million tokens.
                price: Price { input: 300_000, cached_input: 75_000, output: 2_500_000 },
            },
            thinking_budget: Some(0),
            endpoint: None,
            cache: None,
        },
    ))
}

const SYSTEM: &str = "You write one BigQuery GoogleSQL SELECT statement. Output only the SQL, no prose, no code fences.";
const SCHEMA: &str = "Table `shop.orders` AS o: o.order_id INT64, o.customer_id INT64, o.status STRING enum(shipped|pending|cancelled), o.order_ts TIMESTAMP";

#[tokio::test(flavor = "current_thread")]
#[ignore = "calls the live Vertex AI API"]
async fn live_multi_candidate_and_stream_abort() {
    let Some(m) = live() else {
        eprintln!("CRAB_VERTEX_PROJECT not set; skipping");
        return;
    };
    let parts = [Part::cached(SCHEMA), Part::text("How many shipped orders are there?")];
    let prompt = Prompt { system: SYSTEM, parts: &parts };

    // 1. Two candidates, one unary request (one prefill).
    let req =
        GenRequest { n: 2, temperature: 0.7, max_output_tokens: 200, ..GenRequest::new(prompt) };
    let mut out = Candidates::new();
    let t = std::time::Instant::now();
    let usage = m.generate(&req, &mut out, &mut ()).await.expect("generate");
    for (i, (text, f)) in out.iter().enumerate() {
        eprintln!("cand {i} {f:?}: {text}");
        assert_eq!(f, Finish::Stop);
        assert!(text.to_ascii_uppercase().contains("SELECT"), "{text}");
    }
    eprintln!("multi: {usage:?} in {:?}", t.elapsed());
    assert!(
        !usage.estimated && usage.input_tokens + usage.cached_tokens > 0 && usage.output_tokens > 0
    );

    // 2. Independent streams; abort as soon as "FROM" appears (simulates
    //    the validator rejecting a candidate mid-generation).
    let req = GenRequest {
        n: 2,
        independent: true,
        temperature: 0.7,
        max_output_tokens: 200,
        ..GenRequest::new(prompt)
    };
    let mut sink = |_: u8, text: &str, _: &str| {
        if text.to_ascii_uppercase().contains("FROM") {
            Flow::Abort
        } else {
            Flow::Continue
        }
    };
    let t = std::time::Instant::now();
    let usage = m.generate(&req, &mut out, &mut sink).await.expect("generate");
    for (i, (text, f)) in out.iter().enumerate() {
        eprintln!("indep {i} {f:?}: {text}");
    }
    eprintln!("independent+abort: {usage:?} in {:?}", t.elapsed());
    assert!(
        out.iter().any(|(_, f)| f == Finish::Aborted),
        "the validator stopped at least one stream"
    );
}
