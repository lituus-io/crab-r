// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// describe_offline: drafts from supplied table metadata with no Google
// client. The model's transport panics on any Google host; the store an
// earlier run left supplies the upstream descriptions (pass-through and
// renamed columns inherit them, no draft); nothing is queried; a decoy
// instance-metadata server is never contacted; and an offline run refuses
// every option that would reach Google.

#![cfg(feature = "cli")]

use crab_r::describe::app;
use crab_r::describe::describe::{Action, ColumnReport, DescribeConfig, Report, TableReport};
use crab_r::describe::offline::{describe_offline, parse_catalog};
use crab_r::describe::state;
use crab_r::llm::openai::{OpenAi, OpenAiConfig};
use crab_r::llm::{CacheSupport, Caps, ConstraintSupport, Price};
use objstore_r::join::spin_on as block_on;
use objstore_r::transport::{BodyStream, Request, Response, Transport};
use objstore_r::Bytes;
use std::cell::RefCell;
use std::future::{ready, Ready};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A chat-completions gateway in-process: answers the scripted replies in
/// order, and panics on any request to a Google host.
struct Gateway {
    replies: RefCell<Vec<&'static str>>,
    hosts: RefCell<Vec<String>>,
}

struct NoBody;
impl BodyStream for NoBody {
    type Next<'a> = Ready<objstore_r::Result<Option<Bytes>>>;
    fn next_chunk(&mut self) -> Self::Next<'_> {
        ready(Ok(None))
    }
}

fn quoted(s: &str) -> String {
    let mut out = String::new();
    crab_r::base::json::escape_into(s, &mut out);
    out
}

impl Transport for Gateway {
    type Send<'a> = Ready<objstore_r::Result<Response>>;
    type Body<'a> = NoBody;
    type Open<'a> = Ready<objstore_r::Result<(u16, NoBody)>>;
    fn send<'a>(&'a self, req: Request<'a>) -> Self::Send<'a> {
        let host = req.url.split("://").nth(1).unwrap_or("").split('/').next().unwrap_or("");
        assert!(
            !crab_r::describe::offline::is_google_host(host),
            "an offline run reached a Google host: {host}"
        );
        self.hosts.borrow_mut().push(host.to_owned());
        let reply = self.replies.borrow_mut().remove(0);
        let body = format!(
            r#"{{"choices":[{{"index":0,"message":{{"role":"assistant","content":{}}},"finish_reason":"stop"}}],"usage":{{"prompt_tokens":40,"completion_tokens":9}}}}"#,
            quoted(reply)
        );
        ready(Ok(Response { status: 200, body: Bytes::from(body) }))
    }
    fn open<'a>(&'a self, req: Request<'a>) -> Self::Open<'a> {
        panic!("no streaming expected: {}", req.url)
    }
}

fn caps() -> Caps {
    Caps {
        id: "gateway-model".into(),
        constraints: ConstraintSupport { lark: false, gbnf: false, json_schema: true },
        max_candidates: 1,
        max_stream_candidates: 1,
        cache: CacheSupport::None,
        context_tokens: 128_000,
        max_output_tokens: 8_192,
        streaming: false,
        price: Price { input: 300_000, cached_input: 30_000, output: 2_500_000 },
    }
}

const CATALOG: &str = r#"[{"table":"lab.stg.orders","available":true,"reason":"ok","type":"VIEW",
  "schema":{"fields":[{"name":"order_id","type":"INTEGER","policyTags":{"names":["pt"]}},{"name":"amount_usd","type":"NUMERIC"},
  {"name":"status","type":"STRING","description":"Always set: one of shipped, pending."},{"name":"order_date","type":"DATE"}]},
  "view_sql":"SELECT order_id, amount AS amount_usd, status, DATE(ts) AS order_date FROM lab.raw.orders WHERE status != 'TEST'"},
  {"table":"lab.stg.hidden","available":false,"reason":"no_access","message":"BigQuery answered 403"}]"#;

const DRAFT: &str = r#"{"table_description": "One row per order, staged from raw orders excluding test orders.",
  "grain": ["order_id"], "table_checks": [],
  "columns": [
    {"column": "status", "description": "Always set: one of shipped, pending.", "semantic_type": "category", "checks": []},
    {"column": "order_date", "description": "Calendar date of the order timestamp.", "semantic_type": "date", "checks": []}
  ]}"#;
const JUDGE: &str = r#"{"verdicts": [], "columns": [
    {"column": "status", "description": "Order status: shipped or pending.", "confidence": "medium"},
    {"column": "order_date", "description": "Calendar date of the order timestamp.", "confidence": "medium"}],
  "table_description": "One row per order, staged from raw orders excluding test orders.", "more_checks": []}"#;

fn col(name: &str, ty: &str, desc: &str) -> ColumnReport {
    ColumnReport {
        column: name.into(),
        data_type: ty.into(),
        action: Action::Generate,
        existing: None,
        description: Some(desc.into()),
        semantic_type: None,
        unit: None,
        transform: None,
        expression: None,
        sources: vec![],
        hops: vec![],
        confidence: None,
        checks: vec![],
        consumers: vec![],
    }
}

/// What an earlier run recorded about the upstream raw table.
fn earlier() -> Report {
    Report {
        tables: vec![TableReport {
            table: "lab.raw.orders".into(),
            kind: "TABLE".into(),
            rows: 1200,
            bytes: 100,
            last_modified_ms: 0,
            producer: None,
            existing: None,
            description: Some("Raw orders.".into()),
            grain: vec![],
            filters: vec![],
            staged: vec![],
            conditions: vec![],
            columns: vec![
                col("order_id", "INT64", "Order key."),
                col("amount", "NUMERIC", "Order amount in USD."),
                col("status", "STRING", "Order status."),
                col("ts", "TIMESTAMP", "When the order was placed."),
            ],
            checks: vec![],
            model_calls: 1,
            warnings: vec![],
            pending: false,
            spend: Default::default(),
            meta: None,
            prekey: 1,
            key: Some(2),
            reused: false,
            carried_checks: None,
        }],
        ..Report::default()
    }
}

fn dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("crab-offline-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn drafts_from_supplied_metadata_inheriting_from_the_store_and_reaching_no_google_host() {
    let d = dir("drafts");
    let store = graph_r::Store::open_or_create(&d, graph_r::Config::default()).unwrap();
    state::save(&store, &earlier(), 1_000).unwrap();

    let supplied = parse_catalog(CATALOG, 16).unwrap();
    assert_eq!(supplied.len(), 1, "an unavailable table is not drafted");
    let gw = Gateway { replies: RefCell::new(vec![DRAFT, JUDGE]), hosts: RefCell::new(vec![]) };
    let model = OpenAi::new(
        gw,
        OpenAiConfig {
            url: "https://gateway.example/v1/chat/completions".into(),
            model: "gateway-model".into(),
            caps: caps(),
            extra: None,
        },
    );
    let mut cfg = DescribeConfig::new(vec![], "US", "");
    cfg.prior = state::load(&store);
    let report = block_on(describe_offline(&model, &supplied, Some(&store), &cfg));
    let t = report.tables.iter().find(|t| t.table == "lab.stg.orders").expect("drafted");
    let by = |c: &str| t.columns.iter().find(|x| x.column == c).unwrap();
    assert_eq!(by("order_id").action, Action::Propagate, "{:?}", by("order_id"));
    assert_eq!(by("order_id").description.as_deref(), Some("Order key."));
    assert_eq!(by("amount_usd").action, Action::Propagate);
    assert_eq!(by("amount_usd").description.as_deref(), Some("Order amount in USD."));
    assert_eq!(
        by("order_date").description.as_deref(),
        Some("Calendar date of the order timestamp.")
    );
    assert_eq!(report.spend.warehouse_bytes, 0, "nothing queried");
    let hosts = model.transport().hosts.borrow().clone();
    assert!(!hosts.is_empty() && hosts.iter().all(|h| h == "gateway.example"), "{hosts:?}");
    assert!(report.warnings.iter().any(|w| w.starts_with("offline:")));

    // Recorded, then reused: a second run drafts nothing.
    state::save(&store, &report, 2_000).unwrap();
    cfg.prior = state::load(&store);
    let idle = OpenAi::new(
        Gateway { replies: RefCell::new(vec![]), hosts: RefCell::new(vec![]) },
        OpenAiConfig {
            url: "https://gateway.example/v1/chat/completions".into(),
            model: "gateway-model".into(),
            caps: caps(),
            extra: None,
        },
    );
    let again = block_on(describe_offline(&idle, &supplied, Some(&store), &cfg));
    let t = again.tables.iter().find(|t| t.table == "lab.stg.orders").unwrap();
    assert!(t.reused && t.model_calls == 0, "{t:?}");
}

/// A local listener that counts connections: the instance metadata
/// server's stand-in.
fn decoy() -> (String, Arc<AtomicUsize>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let hits = Arc::new(AtomicUsize::new(0));
    let h = Arc::clone(&hits);
    std::thread::spawn(move || {
        for s in l.incoming() {
            h.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut s) = s {
                let mut b = [0u8; 1024];
                let _ = s.read(&mut b);
                let _ = s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}");
            }
        }
    });
    (addr, hits)
}

#[test]
fn an_offline_run_never_asks_the_metadata_server_and_refuses_google() {
    let (addr, hits) = decoy();
    std::env::set_var("GCE_METADATA_HOST", &addr);
    std::env::set_var("CRAB_OFFLINE_TEST_KEY", "sk-offline-test");
    // Google hosts as the model, and options that reach Google, are refused
    // before anything is sent.
    for (extra, why) in [
        (vec!["--openai-url", "https://us-central1-aiplatform.googleapis.com/v1"], "Google host"),
        (vec!["--openai-url", "http://127.0.0.1:9/v1", "--apply"], "refused"),
        (vec!["--openai-url", "http://127.0.0.1:9/v1", "--publish", "gs://b/p"], "refused"),
        (vec!["--openai-url", "http://127.0.0.1:9/v1", "--lineage-location", "us"], "refused"),
    ] {
        let mut argv = vec![
            "lab.stg.orders",
            "--openai-model",
            "m",
            "--openai-key-env",
            "CRAB_OFFLINE_TEST_KEY",
            "--price-input-usd-per-mtok",
            "1",
            "--price-output-usd-per-mtok",
            "1",
        ];
        argv.extend(extra);
        let opts = app::parse(argv.into_iter().map(String::from)).unwrap();
        let e = block_on(app::run_offline(&opts, CATALOG, &mut |_| {})).unwrap_err();
        assert!(e.contains(why), "{e}");
    }
    // A table read and a drafting run with the caller's token and the
    // metadata server forbidden: the decoy is never contacted.
    let creds = app::Credentials::new(
        Some("ya29.caller".into()),
        objstore_r::token::MetadataPolicy::Forbid,
    );
    assert!(creds.adc().is_ok());
    let none = app::Credentials::new(None, objstore_r::token::MetadataPolicy::Forbid);
    std::env::set_var("CLOUDSDK_CONFIG", dir("no-gcloud"));
    std::env::remove_var("GOOGLE_APPLICATION_CREDENTIALS");
    assert!(none.adc().unwrap_err().contains("no credentials"));
    assert_eq!(hits.load(Ordering::SeqCst), 0, "the metadata server was asked");
}
