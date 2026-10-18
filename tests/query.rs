// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// exec::query against a stub BigQuery (jobs.query and getQueryResults),
// through the real HTTP client under the MetadataGuard with a caller's
// token: typed rows, every failure typed, a forbidden table never on the
// wire (also for generated SQL), the row cap and the token budget both
// truncating visibly, only the rows still needed paged, cancellation
// hanging up the request, the metadata server never asked, and the token
// never in an error.

#![cfg(feature = "testing")]

use crab_r::base::json::{Event, Scanner};
use crab_r::exec::guard::MetadataGuard;
use crab_r::exec::query::{query, scalar, Failure, QueryOptions, Rows, Scalar};
use crab_r::exec::RunOpts;
use crab_r::sql::refs::{walk_script, DatasetRef, RefKind};
use crab_r::sql::{Mode, Parser};
use objstore_r::token::Adc;
use proptest::prelude::*;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TOKEN: &str = "ya29.caller-token-for-query-tests";

/// One request the stub saw.
#[derive(Clone, Debug)]
struct Seen {
    line: String,
    auth: String,
    /// The SQL of a jobs.query body.
    sql: Option<String>,
    body: String,
}

struct Stub {
    origin: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    aborted: Arc<AtomicUsize>,
}

impl Stub {
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
    fn queries(&self) -> usize {
        self.seen().iter().filter(|s| s.sql.is_some()).count()
    }
}

const SCHEMA: &str = r#""schema":{"fields":[{"name":"resource","type":"STRING"},{"name":"failures","type":"INTEGER"},{"name":"ratio","type":"FLOAT"},{"name":"healthy","type":"BOOLEAN"},{"name":"last_run","type":"TIMESTAMP"},{"name":"day","type":"DATE"},{"name":"cost","type":"NUMERIC"},{"name":"meta","type":"RECORD","fields":[{"name":"k","type":"STRING"}]},{"name":"tags","type":"STRING","mode":"REPEATED"}]}"#;

fn row(i: usize) -> String {
    format!(
        r#"{{"f":[{{"v":"res-{i}"}},{{"v":"{i}"}},{{"v":"0.5"}},{{"v":"true"}},{{"v":"1700000000123456"}},{{"v":"2026-10-18"}},{{"v":"12.50"}},{{"v":{{"f":[{{"v":"x"}}]}}}},{{"v":[{{"v":"a"}}]}}]}}"#
    )
}

fn rows(from: usize, n: usize) -> String {
    (from..from + n).map(row).collect::<Vec<_>>().join(",")
}

fn complete(rows_json: &str, total: usize, extra: &str) -> String {
    format!(
        r#"{{"jobComplete":true,{SCHEMA},"rows":[{rows_json}],"totalRows":"{total}","totalBytesProcessed":"2048","totalBytesBilled":"10485760","cacheHit":false{extra}}}"#
    )
}

fn error(code: u16, reason: &str, message: &str) -> String {
    format!(
        r#"{{"error":{{"code":{code},"message":"{message}","errors":[{{"reason":"{reason}","message":"{message}"}}]}}}}"#
    )
}

/// The SQL of a jobs.query body (its first member).
fn sql_of(body: &str) -> Option<String> {
    let mut s = Scanner::new(body);
    if s.expect().ok()? != Event::ObjectStart {
        return None;
    }
    match (s.expect().ok()?, s.expect().ok()?) {
        (Event::Key(k), Event::Str(v)) if k.eq_plain("query") => {
            let mut buf = String::new();
            v.decode(&mut buf).ok().map(str::to_owned)
        }
        _ => None,
    }
}

fn read_request(s: &mut TcpStream) -> Option<(String, String)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = s.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(at) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..at]).into_owned();
            let len = head
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.eq_ignore_ascii_case("content-length").then(|| v.trim().parse().ok())?
                })
                .unwrap_or(0usize);
            while buf.len() < at + 4 + len {
                let n = s.read(&mut chunk).ok()?;
                if n == 0 {
                    return None;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            let body = String::from_utf8_lossy(&buf[at + 4..at + 4 + len]).into_owned();
            return Some((head, body));
        }
    }
}

/// The client hung up within `wait`.
fn hung_up(s: &mut TcpStream, wait: Duration) -> bool {
    s.set_read_timeout(Some(Duration::from_millis(20))).unwrap();
    let end = Instant::now() + wait;
    let mut b = [0u8; 1];
    while Instant::now() < end {
        match s.peek(&mut b) {
            Ok(0) => return true,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return true,
            _ => {}
        }
    }
    false
}

/// A jobs.query body's SQL picks the answer by a marker literal in it.
fn answer(sql: &str, auth: &str) -> (u16, String) {
    let has = |m: &str| sql.contains(&format!("'{m}'"));
    if has("pending") {
        (200, r#"{"jobComplete":false,"jobReference":{"jobId":"job_wait","location":"US"}}"#.into())
    } else if has("paged") {
        (200, complete(&rows(0, 2), 7, r#","pageToken":"t2","jobReference":{"jobId":"job_paged"}"#))
    } else if has("e403") {
        (403, error(403, "accessDenied", "Access Denied: Table p:ops.x"))
    } else if has("e401") {
        (401, error(401, "authError", "Request had invalid authentication credentials"))
    } else if has("e404") {
        (404, error(404, "notFound", "Not found: Table p:ops.gone was not found in location US"))
    } else if has("ebytes") {
        (400, error(400, "bytesBilledLimitExceeded", "Query exceeded limit for bytes billed"))
    } else if has("e400") {
        (400, error(400, "invalidQuery", "Unrecognized name: nope at [1:8]"))
    } else if has("e500") {
        (500, error(500, "backendError", "boom"))
    } else if has("echo") {
        // A hostile server repeating the credential back.
        (403, error(403, "accessDenied", &format!("you sent {auth}")))
    } else {
        (200, complete(&rows(0, 3), 3, ""))
    }
}

fn stub() -> Stub {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://127.0.0.1:{}", l.local_addr().unwrap().port());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let aborted = Arc::new(AtomicUsize::new(0));
    let (log, ab) = (Arc::clone(&seen), Arc::clone(&aborted));
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { continue };
            let (log, ab) = (Arc::clone(&log), Arc::clone(&ab));
            std::thread::spawn(move || {
                // Keep-alive: answer every request on the connection.
                while let Some((head, body)) = read_request(&mut s) {
                    let line = head.lines().next().unwrap_or("").to_owned();
                    let auth = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("authorization").then(|| v.trim().to_owned())
                        })
                        .unwrap_or_default();
                    let sql = line.starts_with("POST ").then(|| sql_of(&body)).flatten();
                    log.lock().unwrap().push(Seen {
                        line: line.clone(),
                        auth: auth.clone(),
                        sql: sql.clone(),
                        body,
                    });
                    let path = line.split(' ').nth(1).unwrap_or("");
                    let (status, reply) = if let Some(sql) = &sql {
                        if sql.contains("'slow'") && hung_up(&mut s, Duration::from_secs(5)) {
                            ab.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                        answer(sql, &auth)
                    } else if path.contains("/queries/job_wait?") {
                        (200, complete(&rows(0, 1), 1, r#","jobReference":{"jobId":"job_wait"}"#))
                    } else if path.contains("/queries/job_paged?") {
                        let want: usize = path
                            .split("maxResults=")
                            .nth(1)
                            .and_then(|v| v.split('&').next())
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0);
                        (200, complete(&rows(2, want.min(5)), 7, r#","pageToken":"t3""#))
                    } else {
                        (404, error(404, "notFound", "no such path"))
                    };
                    let resp = format!(
                        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{reply}",
                        reply.len()
                    );
                    if s.write_all(resp.as_bytes()).is_err() {
                        return;
                    }
                }
            });
        }
    });
    Stub { origin, seen, aborted }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

fn guard() -> MetadataGuard<'static> {
    MetadataGuard::default().with_loopback_http()
}

fn opts(stub: &Stub) -> QueryOptions<'_> {
    QueryOptions { endpoint: Some(&stub.origin), ..QueryOptions::default() }
}

fn run(sql: &str, o: &QueryOptions<'_>) -> Result<Rows, crab_r::exec::query::QueryError> {
    rt().block_on(query(Adc::from_access_token(TOKEN), guard(), "p", sql, o))
}

const OK_SQL: &str = "SELECT * FROM `p.ops.resource_health`";

fn values(r: &Rows, i: usize) -> Vec<String> {
    let mut buf = String::new();
    r.batch
        .columns
        .iter()
        .map(|c| match scalar(c, i, &mut buf) {
            Scalar::Str(s) => format!("s:{s}"),
            other => format!("{other:?}"),
        })
        .collect()
}

#[test]
fn rows_arrive_as_typed_scalars_in_one_round_trip() {
    let stub = stub();
    let r = run(OK_SQL, &opts(&stub)).unwrap();
    assert_eq!((r.len(), r.truncated, r.total_rows), (3, false, 3));
    assert_eq!(r.batch.names[0], "resource");
    assert_eq!(
        values(&r, 1),
        [
            "s:res-1",
            "Int(1)",
            "Float(0.5)",
            "Bool(true)",
            "s:2023-11-14T22:13:20.123456Z",
            "s:2026-10-18",
            "s:12.5",
            r#"s:{"k":"x"}"#,
            r#"s:["a"]"#,
        ]
    );
    let seen = stub.seen();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(seen[0].line.starts_with("POST /bigquery/v2/projects/p/queries "));
    assert_eq!(seen[0].auth, format!("Bearer {TOKEN}"));
    for needle in [r#""maximumBytesBilled":"1073741824""#, r#""maxResults":1000"#] {
        assert!(seen[0].body.contains(needle), "{needle} in {}", seen[0].body);
    }
    assert!(!seen[0].body.contains("\"location\""), "no location: BigQuery places the job");
}

#[test]
fn a_pending_job_is_long_polled_with_its_location() {
    let stub = stub();
    let o = QueryOptions { location: Some("US"), ..opts(&stub) };
    let r = run("SELECT 'pending' FROM ops.resource_health", &o).unwrap();
    assert_eq!(r.len(), 1);
    let seen = stub.seen();
    assert_eq!(seen.len(), 2);
    assert!(seen[0].body.contains(r#""location":"US""#));
    assert!(seen[1].line.starts_with("GET /bigquery/v2/projects/p/queries/job_wait?location=US&"));
}

#[test]
fn every_failure_is_typed() {
    let stub = stub();
    for (marker, want) in [
        ("e403", Failure::NoAccess),
        ("e401", Failure::Expired),
        ("e404", Failure::NotFound),
        ("ebytes", Failure::BytesLimit),
        ("e400", Failure::InvalidQuery),
        ("e500", Failure::Unreachable),
    ] {
        let sql = format!("SELECT '{marker}' FROM ops.resource_health");
        let e = run(&sql, &opts(&stub)).unwrap_err();
        assert_eq!(e.failure, want, "{marker}: {e}");
    }
    // 4xx answers are not repeated; the 500 is retried twice.
    assert_eq!(stub.queries(), 5 + 3);
    let refused = run("SELECT * FROM p.mart.orders", &opts(&stub)).unwrap_err();
    assert_eq!(refused.failure, Failure::RefusedByGuard);
    let gone = rt().block_on(query::<Adc>(
        Err(objstore_r::Error::NoCredentials { reason: "none" }),
        guard(),
        "p",
        OK_SQL,
        &opts(&stub),
    ));
    assert_eq!(gone.unwrap_err().failure, Failure::NoCredentials);
    let down = QueryOptions { endpoint: Some("http://127.0.0.1:9"), ..opts(&stub) };
    assert_eq!(run(OK_SQL, &down).unwrap_err().failure, Failure::Unreachable);
}

#[test]
fn a_forbidden_table_never_reaches_the_server() {
    let stub = stub();
    for sql in [
        "SELECT * FROM p.mart.orders",
        "SELECT * FROM ops.resource_health, secret.t",
        "SELECT * FROM ops.resource_health WHERE EXISTS (SELECT 1 FROM p.d.t)",
        "SELECT 1; SELECT * FROM p.d.t",
        "DELETE FROM ops.resource_health WHERE TRUE",
        "SELECT * FROM EXTERNAL_QUERY('c', 'SELECT 1')",
        "SELECT * FROM orders",
    ] {
        let e = run(sql, &opts(&stub)).unwrap_err();
        assert_eq!(e.failure, Failure::RefusedByGuard, "{sql}");
        assert!(!e.message.contains("mart") && !e.message.contains("secret"), "{}", e.message);
    }
    assert_eq!(stub.seen().len(), 0, "a refused statement reached the server");
    // A graph dataset scoped to a project refuses the same dataset elsewhere.
    let ds = [DatasetRef { project: Some("graph-p"), dataset: "ops" }];
    let scoped = MetadataGuard::new(&ds, None).with_loopback_http();
    let e = rt()
        .block_on(query(
            Adc::from_access_token(TOKEN),
            scoped,
            "p",
            "SELECT * FROM other.ops.resource_health",
            &opts(&stub),
        ))
        .unwrap_err();
    assert_eq!(e.failure, Failure::RefusedByGuard);
    assert_eq!(stub.seen().len(), 0);
}

#[test]
fn the_row_cap_pages_only_what_is_still_needed() {
    let stub = stub();
    let o = QueryOptions { run: RunOpts { max_rows: 5, ..opts(&stub).run }, ..opts(&stub) };
    let r = run("SELECT 'paged' FROM ops.resource_health", &o).unwrap();
    assert_eq!((r.len(), r.truncated, r.total_rows), (5, true, 7));
    assert_eq!(values(&r, 4)[0], "s:res-4");
    let seen = stub.seen();
    assert_eq!(seen.len(), 2, "one page beyond the first: {seen:?}");
    assert!(seen[0].body.contains(r#""maxResults":5"#));
    assert!(
        seen[1].line.contains("/queries/job_paged?") && seen[1].line.contains("maxResults=3&"),
        "asks for the 3 missing rows only: {}",
        seen[1].line
    );
}

#[test]
fn the_token_budget_cuts_whole_rows_and_says_so() {
    let stub = stub();
    let full = run(OK_SQL, &opts(&stub)).unwrap();
    assert!(!full.truncated);
    let o = QueryOptions { budget: Some(graph_r::TokenBudget(100)), ..opts(&stub) };
    let cut = run(OK_SQL, &o).unwrap();
    assert!(cut.len() < full.len() && !cut.is_empty(), "{}", cut.len());
    assert!(cut.truncated);
    assert_eq!(values(&cut, 0), values(&full, 0), "whole rows, in order");
    let none = QueryOptions { budget: Some(graph_r::TokenBudget(1)), ..opts(&stub) };
    let r = run(OK_SQL, &none).unwrap();
    assert_eq!((r.len(), r.truncated), (0, true));
}

#[test]
fn dropping_the_future_hangs_up_the_request() {
    let stub = stub();
    let rt = rt();
    let o = opts(&stub);
    let start = Instant::now();
    let out = rt.block_on(async {
        tokio::time::timeout(
            Duration::from_millis(300),
            query(
                Adc::from_access_token(TOKEN),
                guard(),
                "p",
                "SELECT 'slow' FROM ops.resource_health",
                &o,
            ),
        )
        .await
    });
    assert!(out.is_err(), "the query was not cancelled");
    // The runtime keeps running (as the shared one does): the dropped
    // request's connection is closed, not left waiting.
    let end = Instant::now() + Duration::from_secs(3);
    rt.block_on(async {
        while stub.aborted.load(Ordering::SeqCst) == 0 && Instant::now() < end {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    assert_eq!(stub.aborted.load(Ordering::SeqCst), 1, "the request was not dropped");
    assert!(start.elapsed() < Duration::from_secs(3));
}

#[test]
fn the_token_is_never_in_an_error_and_the_metadata_server_never_asked() {
    let decoy = TcpListener::bind("127.0.0.1:0").unwrap();
    decoy.set_nonblocking(true).unwrap();
    std::env::set_var("GCE_METADATA_HOST", decoy.local_addr().unwrap().to_string());
    std::env::set_var("CLOUDSDK_CONFIG", std::env::temp_dir().join("crab-no-gcloud-config-q"));
    std::env::remove_var("GOOGLE_APPLICATION_CREDENTIALS");
    let stub = stub();
    let e = run("SELECT 'echo' FROM ops.resource_health", &opts(&stub)).unwrap_err();
    assert_eq!(e.failure, Failure::NoAccess);
    assert!(stub.seen()[0].auth.contains(TOKEN), "the stub did see it");
    let shown = format!("{e} {e:?}");
    assert!(!shown.contains(TOKEN) && shown.contains("<redacted>"), "{shown}");
    // Discovery with the metadata server forbidden: a typed failure, and
    // the decoy sees nothing.
    let adc = Adc::discover_with(objstore_r::token::MetadataPolicy::Forbid);
    let r = rt().block_on(query(adc, guard(), "p", OK_SQL, &opts(&stub)));
    assert_eq!(r.unwrap_err().failure, Failure::NoCredentials);
    run(OK_SQL, &opts(&stub)).unwrap();
    assert!(decoy.accept().is_err(), "the metadata server was contacted");
}

/// What generated SQL starts with.
const STARTS: [&str; 5] = [
    "SELECT * FROM ops.resource_health",
    "SELECT * FROM infra.nodes",
    "SELECT 1",
    "SELECT * FROM `p.region-us.INFORMATION_SCHEMA.JOBS`",
    "",
];

/// Fragments the generated SQL is spliced from, so tricks come quickly.
const PIECES: [&str; 23] = [
    " WHERE TRUE",
    " LIMIT 5",
    ", infra.nodes",
    " UNION ALL SELECT * FROM infra.nodes",
    ", p.mart.orders",
    "SELECT * FROM ",
    "ops.resource_health",
    "`p.mart.orders`",
    "infra.nodes",
    "`region-us`.INFORMATION_SCHEMA.JOBS",
    " /* ; */ ",
    "; DROP TABLE ops.runs",
    " -- \n",
    "WITH s AS (SELECT 1) ",
    " UNION ALL SELECT * FROM ",
    "EXTERNAL_QUERY('c','q')",
    " WHERE EXISTS (SELECT 1 FROM ",
    ")",
    "`ops`.`runs`",
    "secret.t",
    "r, r.items",
    " JOIN ",
    " s ",
];

/// An independent, cruder reading of an accepted SQL text: one statement,
/// every read an INFORMATION_SCHEMA view, a meta-table, or in `infra`/`ops`.
fn reads_only_allowed(sql: &str) -> bool {
    let mut p = Parser::new();
    let Ok(script) = p.parse_script(sql, Mode::Full) else { return false };
    let refs = walk_script(&script, None);
    script.stmts.len() == 1
        && refs.complete
        && refs.tables.iter().all(|r| match r.kind {
            RefKind::InformationSchema | RefKind::MetaTable => true,
            RefKind::Table => matches!(r.dataset.as_deref(), Some("infra" | "ops")),
            RefKind::Path => false,
        })
}

#[test]
fn generated_sql_reaches_the_server_only_when_it_reads_allowed_tables() {
    let stub = stub();
    let rt = rt();
    let o = opts(&stub);
    let piece = prop_oneof![
        3 => (0..PIECES.len()).prop_map(|i| PIECES[i].to_owned()),
        1 => "[ -~]{0,6}",
    ];
    // A valid statement, then what is spliced after it: most tails are
    // tricks, some keep it valid.
    let start = (0..STARTS.len()).prop_map(|i| STARTS[i].to_owned());
    let strategy =
        (start, prop::collection::vec(piece, 0..5)).prop_map(|(s, v)| format!("{s}{}", v.concat()));
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 400,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    let (sent, refused) = (std::cell::Cell::new(0usize), std::cell::Cell::new(0usize));
    runner
        .run(&strategy, |sql| {
            let before = stub.queries();
            let r = rt.block_on(query(Adc::from_access_token(TOKEN), guard(), "p", &sql, &o));
            let reached = stub.queries() - before;
            match &r {
                Err(e) if e.failure == Failure::RefusedByGuard => {
                    refused.set(refused.get() + 1);
                    prop_assert_eq!(reached, 0, "refused but sent: {:?}", sql);
                }
                _ => {
                    sent.set(sent.get() + 1);
                    prop_assert!(reached >= 1, "accepted but not sent: {:?}", sql);
                }
            }
            Ok(())
        })
        .unwrap();
    // Everything that reached the server reads only allowed tables, by an
    // independent reading of the SQL exactly as received.
    for s in stub.seen() {
        let sql = s.sql.expect("only jobs.query bodies reach this stub");
        assert!(reads_only_allowed(&sql), "a forbidden read reached the server: {sql:?}");
    }
    let (sent, refused) = (sent.get(), refused.get());
    assert!(sent > 10 && refused > 100, "sent {sent}, refused {refused}");
}
