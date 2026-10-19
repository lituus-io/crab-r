// Author: spicyzhug@gmail.com
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
// schema-valid answers from one request. Then the real draft and judge
// schemas `describe` sends, which a server enforcing strict structured
// output accepts only as rewritten strict, and their replies parse. Nothing
// about the server, the URL or the key is printed.

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

/// The draft and judge schemas `describe` sends, as built, reach the server
/// strict-compliant (a strict server answers 400 to anything less) and the
/// replies parse. One unary request each.
#[test]
fn live_describe_schemas_are_accepted_and_their_replies_parse() {
    use crab_r::describe::generate::{
        draft_schema, judge_schema, parse_draft, parse_judgement, DRAFT_SYSTEM, JUDGE_SYSTEM,
    };
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
    let columns: Vec<String> = ["order_id", "amount_usd", "status"].map(String::from).to_vec();
    let ask = |system: &str, user: &str, schema: &str| -> String {
        let parts = [Part::text(user)];
        let req = GenRequest {
            temperature: 0.2,
            max_output_tokens: 1500,
            constraint: Constraint::JsonSchema(schema),
            ..GenRequest::new(Prompt { system, parts: &parts })
        };
        let mut out = Candidates::new();
        objstore_r::http::block_on(m.generate(&req, &mut out, &mut ()))
            .expect("runtime")
            .unwrap_or_else(|e| panic!("refused: {e}"));
        assert_eq!(out.iter().next().map(|(_, f)| f), Some(Finish::Stop));
        out.text(0).to_owned()
    };
    let draft = ask(
        DRAFT_SYSTEM,
        "Table shop.stg.orders, produced by: SELECT order_id, amount AS amount_usd, status FROM shop.raw.orders WHERE status != 'TEST'\n\
         Upstream: order_id 'Order key.'; amount 'Order amount in USD.'\n\
         DESCRIBE: order_id, amount_usd, status",
        &draft_schema(&columns, &columns),
    );
    eprintln!("draft: {draft}");
    let d = parse_draft(&draft, &columns, &columns, &[]).expect("the draft reply parses");
    assert!(d.table_description.is_some() && !d.columns.is_empty(), "{d:?}");
    let judge = ask(
        JUDGE_SYSTEM,
        "Check 1 [order_id] claim: unique per row; expr COUNT(*) - COUNT(DISTINCT order_id); expect 0; result 0: HOLDS.\n\
         Columns: order_id draft 'Order key, unique per row.'",
        &judge_schema(&columns, &[1]),
    );
    eprintln!("judge: {judge}");
    let j = parse_judgement(&judge, &columns, &[1]).expect("the judge reply parses");
    assert!(!j.verdicts.is_empty() || !j.columns.is_empty(), "{j:?}");
}
