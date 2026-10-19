// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// `describe` on an OpenAI-compatible server (`--openai-url`), through the
// real HTTPS client against a local chat-completions server: requests are
// unary by default (a gateway may refuse streamed structured output), `n`
// candidates share one request, the bearer key comes from the named
// variable, `--openai-stream` streams, and the key appears in no argv,
// error, report or store.

#![cfg(feature = "cli")]

use crab_r::base::batch::{Batch, Cell, ColType};
use crab_r::describe::app;
use crab_r::describe::describe::{describe, DescribeConfig, Target};
use crab_r::describe::generate::Value;
use crab_r::exec::{DryRun, QueryResult, RunOpts, TableMeta, TableMetadata, Warehouse};
use crab_r::lineage::NoLineage;
use crab_r::llm::request::{Part, Prompt};
use crab_r::llm::{Candidates, Constraint, Finish, GenRequest, Model};
use objstore_r::http::block_on;
use objstore_r::Bytes;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::{ready, Ready};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, Once};

/// The key every test authorises with. Set once, before any thread reads the
/// environment (each test passes through [`key_env`] first).
const SECRET: &str = "sk-crab-test-7f3a9c2e5b1d4086";
const KEY_ENV: &str = "CRAB_GATEWAY_TEST_KEY";

fn key_env() -> &'static str {
    static SET: Once = Once::new();
    SET.call_once(|| std::env::set_var(KEY_ENV, SECRET));
    KEY_ENV
}

/// One request as the server saw it.
#[derive(Clone, Debug)]
struct Seen {
    head: String,
    body: String,
}

/// A local chat-completions server: answers each request with the next
/// scripted reply (the last repeats) — unary with `n` choices, or as a
/// stream when the request asks for one; `status` other than 200 answers an
/// error envelope instead.
struct Server {
    url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

fn serve(replies: Vec<String>, status: u16) -> Server {
    serve_with(replies, status, false)
}

/// [`serve`], but refusing a structured request whose schema breaks
/// OpenAI's strict structured-output rules (`strict: true`) with the 400 an
/// OpenAI-compatible gateway answers, as the hosted API does.
fn serve_strict(replies: Vec<String>) -> Server {
    serve_with(replies, 200, true)
}

fn serve_with(replies: Vec<String>, status: u16, strict: bool) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://127.0.0.1:{}/v1", listener.local_addr().unwrap().port());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    std::thread::spawn(move || {
        let mut next = 0usize;
        for sock in listener.incoming() {
            let Ok(mut sock) = sock else { continue };
            let Some(req) = read_request(&mut sock) else { continue };
            let refusal = if strict { strict_refusal(&req.body) } else { None };
            let reply = &replies[next.min(replies.len() - 1)];
            if refusal.is_none() {
                next += 1;
            }
            let resp = if let Some(msg) = refusal {
                let body = format!(
                    r#"{{"error":{{"message":{},"type":"invalid_request_error","param":"response_format","code":null}}}}"#,
                    quoted(&msg)
                );
                format!(
                    "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
            } else if status != 200 {
                let body = r#"{"error":{"message":"invalid api key","code":401}}"#;
                format!(
                    "HTTP/1.1 {status} Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
            } else if req.body.contains(r#""stream":true"#) {
                let body = format!(
                    "data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":{}}},\"finish_reason\":\"stop\"}}]}}\n\ndata: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":40,\"completion_tokens\":9}}}}\n\ndata: [DONE]\n\n",
                    quoted(reply)
                );
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
            } else {
                let n = candidates(&req.body);
                let choices: Vec<String> = (0..n)
                    .map(|i| {
                        format!(
                            r#"{{"index":{i},"message":{{"role":"assistant","content":{}}},"finish_reason":"stop"}}"#,
                            quoted(reply)
                        )
                    })
                    .collect();
                let body = format!(
                    r#"{{"choices":[{}],"usage":{{"prompt_tokens":48,"completion_tokens":{}}}}}"#,
                    choices.join(","),
                    12 * n
                );
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
            };
            log.lock().unwrap().push(req);
            let _ = sock.write_all(resp.as_bytes());
        }
    });
    Server { url, seen }
}

fn read_request(sock: &mut std::net::TcpStream) -> Option<Seen> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16 << 10];
    loop {
        let r = sock.read(&mut chunk).ok()?;
        if r == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..r]);
        let text = String::from_utf8_lossy(&buf);
        if let Some(end) = text.find("\r\n\r\n") {
            let len = text[..end]
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|v| v.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            if buf.len() >= end + 4 + len {
                return Some(Seen {
                    head: text[..end].to_owned(),
                    body: text[end + 4..end + 4 + len].to_owned(),
                });
            }
        }
    }
}

/// `"n"` of a request body (1 when absent).
fn candidates(body: &str) -> usize {
    body.split_once(r#""n":"#).map_or(1, |(_, rest)| {
        rest.chars().take_while(char::is_ascii_digit).collect::<String>().parse().unwrap_or(1)
    })
}

/// The 400 message for a request whose `response_format` schema breaks the
/// strict rules, `None` when it complies (or asks for no schema). Checked
/// independently of the crate's own normaliser: under `strict: true` every
/// object schema at every depth (`type` "object", or carrying `properties`)
/// must set `additionalProperties` to `false` and list every property in
/// `required`.
fn strict_refusal(body: &str) -> Option<String> {
    let v = Value::parse(body).expect("request body is JSON");
    let js = v.get("response_format")?.get("json_schema")?;
    assert_eq!(js.get("strict"), Some(&Value::Bool(true)), "strict mode is asked for");
    let schema = js.get("schema").expect("a schema");
    let mut path = Vec::new();
    strict_violation(schema, &mut path).map(|why| {
        format!(
            "Invalid schema for response_format 'output': In context=({}), {why}.",
            path.iter().map(|p| format!("'{p}'")).collect::<Vec<_>>().join(", ")
        )
    })
}

fn strict_violation(node: &Value, path: &mut Vec<String>) -> Option<String> {
    let Value::Obj(members) = node else { return None };
    let ty = node.get("type");
    let is_object = node.get("properties").is_some()
        || matches!(ty, Some(Value::Str(t)) if t == "object")
        || matches!(ty, Some(Value::Arr(ts)) if ts.contains(&Value::Str("object".into())));
    if is_object {
        if node.get("additionalProperties") != Some(&Value::Bool(false)) {
            return Some(
                "'additionalProperties' is required to be supplied and to be false".into(),
            );
        }
        let keys: Vec<&str> = match node.get("properties") {
            Some(Value::Obj(p)) => p.iter().map(|(k, _)| k.as_str()).collect(),
            _ => Vec::new(),
        };
        let required: Vec<&str> = node
            .arr("required")
            .iter()
            .filter_map(|r| if let Value::Str(s) = r { Some(s.as_str()) } else { None })
            .collect();
        if let Some(k) = keys.iter().find(|k| !required.contains(k)) {
            return Some(format!(
                "'required' is required to be supplied and to be an array including every key in properties. Missing '{k}'"
            ));
        }
        if let Some(k) = required.iter().find(|k| !keys.contains(k)) {
            return Some(format!("'required' names '{k}', which is not in properties"));
        }
    }
    for (key, child) in members {
        match (key.as_str(), child) {
            ("properties" | "$defs" | "definitions", Value::Obj(map)) => {
                for (name, sub) in map {
                    path.push(key.clone());
                    path.push(name.clone());
                    if let Some(why) = strict_violation(sub, path) {
                        return Some(why);
                    }
                    path.truncate(path.len() - 2);
                }
            }
            ("items" | "not", sub) => {
                path.push(key.clone());
                if let Some(why) = strict_violation(sub, path) {
                    return Some(why);
                }
                path.pop();
            }
            ("anyOf" | "oneOf" | "allOf", Value::Arr(subs)) => {
                for (i, sub) in subs.iter().enumerate() {
                    path.push(format!("{key}{i}"));
                    if let Some(why) = strict_violation(sub, path) {
                        return Some(why);
                    }
                    path.pop();
                }
            }
            _ => {}
        }
    }
    None
}

fn quoted(s: &str) -> String {
    let mut out = String::new();
    crab_r::base::json::Writer::new(&mut out).string(s);
    out
}

fn options(url: &str, extra: &[&str]) -> (Vec<String>, app::Options) {
    let argv: Vec<String> =
        ["lab.stg", "--openai-url", url, "--openai-model", "gpt-4o", "--openai-key-env", key_env()]
            .iter()
            .chain(extra)
            .map(|s| (*s).to_owned())
            .collect();
    let opts = app::parse(argv.clone()).unwrap();
    (argv, opts)
}

const SCHEMA: &str = r#"{"type":"object","properties":{"description":{"type":"string"}},"required":["description"],"additionalProperties":false}"#;

const QUESTION: &str = "Describe the column order_id of shop.orders in one sentence.";

#[test]
fn structured_candidates_are_one_unary_request_with_the_bearer_key() {
    let server = serve(vec![r#"{"description":"Order key."}"#.into()], 200);
    let (argv, opts) = options(&server.url, &[]);
    assert!(argv.iter().all(|a| !a.contains(SECRET)), "the key is never in argv");
    let model = opts.openai().unwrap().connect().unwrap();
    let parts = [Part::text(QUESTION)];
    let req = GenRequest {
        n: 3,
        constraint: Constraint::JsonSchema(SCHEMA),
        ..GenRequest::new(Prompt { system: "Answer in JSON.", parts: &parts })
    };
    let mut out = Candidates::new();
    let usage = block_on(model.generate(&req, &mut out, &mut ())).unwrap().unwrap();
    assert_eq!(out.len(), 3);
    for (text, finish) in out.iter() {
        assert_eq!((text, finish), (r#"{"description":"Order key."}"#, Finish::Stop));
    }
    let seen = server.seen.lock().unwrap();
    // Counted: n candidates, one prefill, one request.
    assert_eq!(seen.len(), 1, "one request for three candidates");
    let r = &seen[0];
    assert!(r.head.starts_with("POST /v1/chat/completions "), "{}", r.head);
    assert!(
        r.head.lines().any(|l| l.eq_ignore_ascii_case(&format!("authorization: Bearer {SECRET}"))),
        "the key from {KEY_ENV} is the bearer token"
    );
    // Unary: the body never asks for a stream (OpenAI's default is false).
    assert!(!r.body.contains("\"stream\""), "{}", r.body);
    assert!(r.body.contains(r#""model":"gpt-4o""#) && r.body.contains(r#""n":3"#), "{}", r.body);
    assert!(
        r.body.contains(r#""response_format":{"type":"json_schema","json_schema":{"name":"output","strict":true,"schema":{"type":"object""#),
        "{}",
        r.body
    );
    // Price: gpt-4o's list price, 48 input and 36 output tokens.
    assert_eq!((usage.input_tokens, usage.output_tokens, usage.estimated), (48, 36, false));
    assert_eq!(usage.micro_usd, (48 * 2_500_000 + 36 * 10_000_000_u64).div_ceil(1_000_000));
}

#[test]
fn openai_stream_streams_a_single_candidate_and_never_several() {
    let server = serve(vec![r#"{"description":"Order key."}"#.into()], 200);
    let (_, opts) = options(&server.url, &["--openai-stream"]);
    let model = opts.openai().unwrap().connect().unwrap();
    let parts = [Part::text(QUESTION)];
    let one = GenRequest {
        constraint: Constraint::JsonSchema(SCHEMA),
        ..GenRequest::new(Prompt { system: "", parts: &parts })
    };
    let mut out = Candidates::new();
    let usage = block_on(model.generate(&one, &mut out, &mut ())).unwrap().unwrap();
    assert_eq!(out.text(0), r#"{"description":"Order key."}"#);
    assert_eq!((usage.input_tokens, usage.output_tokens), (40, 9));
    let three = GenRequest { n: 3, ..one };
    block_on(model.generate(&three, &mut out, &mut ())).unwrap().unwrap();
    assert_eq!(out.len(), 3);
    let seen = server.seen.lock().unwrap();
    assert_eq!(seen.len(), 2, "one request per prefill");
    assert!(
        seen[0].body.contains(r#""stream":true,"stream_options":{"include_usage":true}"#),
        "{}",
        seen[0].body
    );
    assert!(!seen[1].body.contains("\"stream\"") && seen[1].body.contains(r#""n":3"#));
}

#[test]
fn a_refused_key_is_an_api_error_that_does_not_carry_the_key() {
    let server = serve(vec![String::new()], 401);
    let (_, opts) = options(&server.url, &[]);
    let model = opts.openai().unwrap().connect().unwrap();
    let parts = [Part::text(QUESTION)];
    let req = GenRequest::new(Prompt { system: "", parts: &parts });
    let e = block_on(model.generate(&req, &mut Candidates::new(), &mut ())).unwrap().unwrap_err();
    let text = format!("{e} {e:?}");
    assert!(text.contains("invalid api key") && !text.contains(SECRET), "{text}");
}

// ---- a whole describe through the gateway -------------------------------------

/// A warehouse holding one source table; every query answers one row of zeros.
#[derive(Default)]
struct Wh {
    runs: RefCell<Vec<String>>,
    metas: HashMap<String, String>,
}

const RAW: &str = r#"{"etag":"r1","type":"TABLE","numRows":"1200","schema":{"fields":[{"name":"order_id","type":"INTEGER","description":"Order key."},{"name":"amount","type":"NUMERIC","description":"Order amount in USD."},{"name":"status","type":"STRING"}]}}"#;

impl Warehouse for Wh {
    type Dry<'a> = Ready<crab_r::exec::Result<DryRun>>;
    type Run<'a> = Ready<crab_r::exec::Result<QueryResult>>;
    fn dry_run<'a>(&'a self, _: &'a str) -> Self::Dry<'a> {
        let col = |name: &str, ty: ColType, sql_type: &str| crab_r::exec::OutputColumn {
            name: name.into(),
            ty,
            sql_type: sql_type.into(),
        };
        ready(Ok(DryRun {
            bytes_processed: 1 << 20,
            schema: vec![
                col("order_id", ColType::Int64, "INT64"),
                col("amount_usd", ColType::Numeric, "NUMERIC"),
            ],
        }))
    }
    fn run<'a>(&'a self, sql: &'a str, _: RunOpts) -> Self::Run<'a> {
        self.runs.borrow_mut().push(sql.to_owned());
        let n = sql.matches(" AS c").count().max(1);
        let names: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
        let schema: Vec<(&str, ColType)> =
            names.iter().map(|s| (s.as_str(), ColType::Int64)).collect();
        let mut b = Batch::new(&schema, 1);
        for c in &mut b.columns {
            c.push(Cell::I64(0)).unwrap();
        }
        ready(Ok(QueryResult {
            batch: b,
            total_rows: 1,
            truncated: false,
            bytes_processed: 1 << 20,
            bytes_billed: 10 << 20,
            cache_hit: false,
            wall_ms: 1,
        }))
    }
}

impl TableMetadata for Wh {
    type Get<'a> = Ready<crab_r::exec::Result<Option<TableMeta>>>;
    type Patch<'a> = Ready<crab_r::exec::Result<TableMeta>>;
    fn table_get<'a>(&'a self, p: &'a str, d: &'a str, t: &'a str) -> Self::Get<'a> {
        ready(Ok(self.metas.get(&format!("{p}.{d}.{t}")).map(|raw| meta(raw))))
    }
    fn table_patch<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a str,
        _: Bytes,
        _: &'a str,
    ) -> Self::Patch<'a> {
        unreachable!("describe never writes without --apply")
    }
}

/// A table resource parsed by the BigQuery client's own parser.
fn meta(raw: &str) -> TableMeta {
    struct One(String);
    struct NoBody;
    impl objstore_r::transport::BodyStream for NoBody {
        type Next<'a> = Ready<objstore_r::Result<Option<Bytes>>>;
        fn next_chunk(&mut self) -> Self::Next<'_> {
            ready(Ok(None))
        }
    }
    impl objstore_r::transport::Transport for One {
        type Send<'a> = Ready<objstore_r::Result<objstore_r::transport::Response>>;
        type Body<'a> = NoBody;
        type Open<'a> = Ready<objstore_r::Result<(u16, NoBody)>>;
        fn send<'a>(&'a self, _: objstore_r::transport::Request<'a>) -> Self::Send<'a> {
            ready(Ok(objstore_r::transport::Response {
                status: 200,
                body: Bytes::from(self.0.clone()),
            }))
        }
        fn open<'a>(&'a self, _: objstore_r::transport::Request<'a>) -> Self::Open<'a> {
            unreachable!()
        }
    }
    let bq = crab_r::exec::bigquery::BigQuery::new(
        One(raw.to_owned()),
        &crab_r::exec::bigquery::BigQueryConfig {
            project: "lab".into(),
            location: "US".into(),
            endpoint: None,
        },
    );
    objstore_r::join::spin_on(bq.table_get("x", "y", "z")).unwrap().unwrap()
}

fn files_under(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            files_under(&p, out);
        } else {
            out.push(p);
        }
    }
}

#[test]
fn describe_runs_unary_through_the_gateway_and_the_key_stays_out_of_its_outputs() {
    let sql = "SELECT order_id, amount AS amount_usd FROM lab.raw.orders WHERE status != 'TEST'";
    let name = format!("lab.stg._query_{:08x}", graph_r::hash::xxh3_64(sql.as_bytes()) as u32);
    let draft = format!(
        r#"{{"table_description": "Orders excluding test orders.", "grain": ["order_id"],
        "table_checks": [{{"claim": "one row per order", "sql": "SELECT COUNT(*) - COUNT(DISTINCT order_id) AS dup FROM `{name}`", "expect": "0"}}],
        "columns": []}}"#
    );
    let judge = r#"{"verdicts": [{"check": 1, "verdict": "supports", "reason": "0"}], "columns": [], "table_description": "Orders excluding test orders.", "more_checks": []}"#;
    let server = serve(vec![draft, judge.into()], 200);
    let (argv, opts) = options(&server.url, &[]);
    let o = opts.openai().unwrap();
    let model = o.connect().unwrap();
    let wh = Wh { metas: [("lab.raw.orders".to_owned(), RAW.to_owned())].into(), ..Wh::default() };
    let target =
        Target::Sql { sql: sql.into(), target: None, default_dataset: Some("lab.stg".into()) };
    let cfg = DescribeConfig::new(vec![target], "US", "lab");
    let report = block_on(describe(&model, &wh, &NoLineage, &cfg)).unwrap();
    assert_eq!(report.tables.len(), 1, "{:?}", report.warnings);
    assert_eq!(
        report.tables[0].description.as_deref(),
        Some("Orders excluding test orders."),
        "{:?}",
        report.warnings
    );
    {
        let seen = server.seen.lock().unwrap();
        // Counted: draft and judge, one request each, both unary and
        // structured, both authorised by the key.
        assert_eq!(seen.len(), 2, "{seen:?}");
        for r in seen.iter() {
            assert!(!r.body.contains("\"stream\""), "{}", r.body);
            assert!(r.body.contains(r#""response_format":{"type":"json_schema""#), "{}", r.body);
            assert!(r.head.contains(SECRET), "authorised");
        }
    }
    // The model's spend is charged at gpt-4o's list price.
    assert!(report.spend.micro_usd > 0, "{:?}", report.spend);

    // The key is in no argv, option dump, run-log line, report or store file.
    let dir = std::env::temp_dir().join(format!("crab-openai-store-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = graph_r::Store::create(&dir, graph_r::Config::default()).unwrap();
    crab_r::describe::state::save(&store, &report, graph_r::fsio::now_ms()).unwrap();
    drop(store);
    let mut files = Vec::new();
    files_under(&dir, &mut files);
    assert!(!files.is_empty());
    let needle = SECRET.as_bytes();
    for f in &files {
        let bytes = std::fs::read(f).unwrap();
        assert!(!bytes.windows(needle.len()).any(|w| w == needle), "{}", f.display());
    }
    let json = crab_r::describe::report::to_json(&report);
    for text in [argv.join(" "), format!("{opts:?}"), o.summary(), json, app::USAGE.to_owned()] {
        assert!(!text.contains(SECRET), "{text}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The draft and judge schemas `describe` really sends pass a gateway that
/// enforces OpenAI's strict structured-output rules (a non-compliant schema
/// is refused with a 400 before any model runs), and the replies a strict
/// model gives — `null` for every field the reply may leave out — parse.
#[test]
fn describe_schemas_pass_a_strict_gateway_and_null_optionals_parse() {
    let sql = "SELECT order_id, amount AS amount_usd FROM lab.raw.orders WHERE status != 'TEST'";
    let draft = r#"{"table_description": "Orders excluding test orders.", "grain": null,
        "table_checks": null,
        "columns": [{"column": "amount_usd", "description": "Order amount in USD.", "semantic_type": "currency_amount",
                     "unit": null, "checks": [{"claim": "non-negative", "expr": "COUNTIF(amount_usd < 0)", "expect": "0"}],
                     "statements": null}]}"#;
    let judge = r#"{"verdicts": [{"check": "1", "verdict": "supports", "reason": "0"}],
        "columns": [{"column": "amount_usd", "description": "Order amount in USD.", "confidence": "high"}],
        "table_description": null, "more_checks": null}"#;
    let server = serve_strict(vec![draft.into(), judge.into()]);
    let (_, opts) = options(&server.url, &[]);
    let model = opts.openai().unwrap().connect().unwrap();
    let wh = Wh { metas: [("lab.raw.orders".to_owned(), RAW.to_owned())].into(), ..Wh::default() };
    let target =
        Target::Sql { sql: sql.into(), target: None, default_dataset: Some("lab.stg".into()) };
    let cfg = DescribeConfig::new(vec![target], "US", "lab");
    let report = block_on(describe(&model, &wh, &NoLineage, &cfg)).unwrap();
    assert_eq!(report.tables.len(), 1, "{:?}", report.warnings);
    let t = &report.tables[0];
    let warnings = format!("{:?} {:?}", report.warnings, t.warnings);
    assert!(!warnings.contains("400"), "a strict gateway refused a schema: {warnings}");
    assert_eq!(t.description.as_deref(), Some("Orders excluding test orders."), "{warnings}");
    let amount = t.columns.iter().find(|c| c.column == "amount_usd").expect("amount_usd");
    assert_eq!(amount.description.as_deref(), Some("Order amount in USD."), "{warnings}");
    let seen = server.seen.lock().unwrap();
    // Counted: draft and judge, each structured, each accepted first time.
    assert_eq!(seen.len(), 2, "{seen:?}");
    for r in seen.iter() {
        assert!(!r.body.contains("\"stream\""), "unary: {}", r.body);
        assert_eq!(strict_refusal(&r.body), None);
    }
}
