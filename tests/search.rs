// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// search against a stub catalog, through the real HTTPS client under the
// MetadataGuard with a caller's token: one POST with no body to the global
// searchEntries method, the text scoped to BigQuery tables and views, the
// token sent as given and in the authorization header only, every failure
// typed with a message that holds neither the token nor the response body,
// a refused argument sending nothing, and the instance metadata server (a
// decoy) never contacted.

#![cfg(feature = "testing")]

use crab_r::exec::guard::MetadataGuard;
use crab_r::exec::search::{search, Failure, SearchOptions};
use objstore_r::token::Adc;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

const TOKEN: &str = "ya29.caller-token-for-search-tests";

struct Stub {
    origin: String,
    /// Every request as it arrived: head and body together.
    seen: Arc<Mutex<Vec<String>>>,
}

/// Answers `POST …:searchEntries`: a marker word in the query picks the
/// status (`s400`, `s401`, `s403`, `s404`, `s500`, `echo`, `junk`, `slow`),
/// anything else two entries and a next page.
fn stub() -> Stub {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://127.0.0.1:{}", l.local_addr().unwrap().port());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { continue };
            let log = Arc::clone(&log);
            std::thread::spawn(move || {
                let mut buf = vec![0u8; 16 << 10];
                let n = s.read(&mut buf).unwrap_or(0);
                let head = String::from_utf8_lossy(&buf[..n]).into_owned();
                log.lock().unwrap().push(head.clone());
                let line = head.lines().next().unwrap_or("").to_owned();
                let auth = head
                    .lines()
                    .find_map(|l| {
                        l.strip_prefix("authorization: ")
                            .or_else(|| l.strip_prefix("Authorization: "))
                    })
                    .unwrap_or("")
                    .to_owned();
                let marker = |m: &str| line.contains(&format!("query={m}%20"));
                let err = |code: u16, msg: &str| {
                    (code, format!(r#"{{"error":{{"code":{code},"message":"{msg}"}}}}"#))
                };
                let (status, body) = if marker("s400") {
                    err(400, "bad query")
                } else if marker("s401") {
                    err(401, "expired")
                } else if marker("s403") {
                    err(403, "denied")
                } else if marker("s404") {
                    err(404, "no project")
                } else if marker("s500") {
                    err(500, "boom")
                } else if marker("echo") {
                    // A catalog that repeats what it was sent.
                    err(403, &format!("denied for {auth} BODY-MARKER"))
                } else if marker("junk") {
                    (200, "<html>not json</html>".to_owned())
                } else {
                    if marker("slow") {
                        std::thread::sleep(std::time::Duration::from_millis(1500));
                    }
                    (
                        200,
                        r#"{"results":[
                          {"dataplexEntry":{"entryType":"projects/1/locations/global/entryTypes/bigquery-table",
                            "fullyQualifiedName":"bigquery:p1.mart.orders",
                            "aspects":{"x":{"data":{"hidden":"ASPECT-MARKER"}}},
                            "entrySource":{"system":"BIGQUERY","description":"Orders.","location":"us"}}},
                          {"dataplexEntry":{"entryType":"projects/1/locations/global/entryTypes/bigquery-view",
                            "fullyQualifiedName":"bigquery:p2.mart.v_orders","entrySource":{"system":"BIGQUERY"}}}
                        ],"nextPageToken":"more"}"#
                            .to_owned(),
                    )
                };
                let resp = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes());
            });
        }
    });
    Stub { origin, seen }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

fn run(
    stub: &Stub,
    project: &str,
    text: &str,
    opts: SearchOptions<'_>,
) -> Result<crab_r::exec::search::Found, crab_r::exec::search::SearchError> {
    let opts = SearchOptions { endpoint: Some(&stub.origin), ..opts };
    rt().block_on(search(
        Adc::from_access_token(TOKEN),
        MetadataGuard::default().with_loopback_http(),
        project,
        text,
        &opts,
    ))
}

#[test]
fn one_bodiless_post_scoped_to_tables_with_the_callers_token() {
    let stub = stub();
    let found = run(&stub, "home-proj", "monthly orders", SearchOptions::default()).unwrap();
    assert_eq!((found.count, found.truncated), (2, true));
    assert!(found
        .json
        .contains(r#""name":"p1.mart.orders","project":"p1","type":"bigquery-table""#));
    assert!(found.json.contains(r#""name":"p2.mart.v_orders""#));
    assert!(!found.json.contains("ASPECT-MARKER"), "aspects are dropped: {}", found.json);

    let seen = stub.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "{seen:?}");
    let req = &seen[0];
    let line = req.lines().next().unwrap();
    assert_eq!(
        line,
        "POST /v1/projects/home-proj/locations/global:searchEntries?query=monthly%20orders%20system%3Dbigquery%20%28type%3Dtable%20OR%20type%3Dview%29&pageSize=20&orderBy=relevance HTTP/1.1"
    );
    // The token is in the authorization header and nowhere else.
    assert_eq!(req.matches(TOKEN).count(), 1, "{req}");
    assert!(req
        .to_ascii_lowercase()
        .contains(&format!("authorization: bearer {}", TOKEN.to_ascii_lowercase())));
    // No body: the head ends the request.
    let (_, body) = req.split_once("\r\n\r\n").unwrap();
    assert!(body.is_empty(), "{body:?}");
    // A front end refuses a POST that does not state its length.
    assert!(req.to_ascii_lowercase().contains("content-length: 0"), "{req}");
}

#[test]
fn every_failure_is_typed_and_says_nothing_it_was_sent() {
    let stub = stub();
    let fail = |text: &str| run(&stub, "p", text, SearchOptions::default()).unwrap_err();
    assert_eq!(fail("s400 x").failure, Failure::InvalidQuery);
    assert_eq!(fail("s401 x").failure, Failure::Expired);
    assert_eq!(fail("s403 x").failure, Failure::NoAccess);
    assert_eq!(fail("s404 x").failure, Failure::NotFound);
    assert_eq!(fail("s500 x").failure, Failure::Unreachable);
    assert_eq!(fail("junk x").failure, Failure::Unreachable);
    let echoed = fail("echo x");
    assert_eq!(echoed.failure, Failure::NoAccess);
    let text = echoed.to_string();
    assert!(!text.contains(TOKEN) && !text.contains("BODY-MARKER"), "{text}");
    // A 4xx is asked once.
    let asked = |m: &str| {
        stub.seen.lock().unwrap().iter().filter(|r| r.contains(&format!("query={m}%20"))).count()
    };
    assert_eq!((asked("s400"), asked("s403"), asked("s404")), (1, 1, 1));
}

#[test]
fn a_slow_catalog_is_a_timeout() {
    let stub = stub();
    let opts = SearchOptions { timeout_ms: 200, ..SearchOptions::default() };
    let e = run(&stub, "p", "slow x", opts).unwrap_err();
    assert_eq!(e.failure, Failure::Timeout, "{e}");
}

#[test]
fn unusable_arguments_and_a_missing_credential_send_nothing() {
    let stub = stub();
    for (project, text, limit) in
        [("Not/A-Project", "orders", 5), ("p", "", 5), ("p", "a\nb", 5), ("p", "orders", 0)]
    {
        let opts = SearchOptions { limit, ..SearchOptions::default() };
        let e = run(&stub, project, text, opts).unwrap_err();
        assert_eq!(e.failure, Failure::InvalidQuery, "{project:?} {text:?} {limit}");
    }
    // Without the loopback allowance the stub is not a listed host.
    let opts = SearchOptions { endpoint: Some(&stub.origin), ..SearchOptions::default() };
    let e = rt()
        .block_on(search(
            Adc::from_access_token(TOKEN),
            MetadataGuard::default(),
            "p",
            "orders",
            &opts,
        ))
        .unwrap_err();
    assert_eq!(e.failure, Failure::RefusedByGuard, "{e}");

    // No credential: typed, and the metadata server (a decoy) is not asked.
    let decoy = TcpListener::bind("127.0.0.1:0").unwrap();
    decoy.set_nonblocking(true).unwrap();
    std::env::set_var("GCE_METADATA_HOST", decoy.local_addr().unwrap().to_string());
    std::env::set_var("CLOUDSDK_CONFIG", std::env::temp_dir().join("crab-no-gcloud-config"));
    std::env::remove_var("GOOGLE_APPLICATION_CREDENTIALS");
    let none = Adc::discover_with(objstore_r::token::MetadataPolicy::Forbid);
    let e = rt()
        .block_on(search(none, MetadataGuard::default().with_loopback_http(), "p", "orders", &opts))
        .unwrap_err();
    assert_eq!(e.failure, Failure::NoCredentials, "{e}");
    assert!(decoy.accept().is_err(), "the metadata server was contacted");
    assert!(stub.seen.lock().unwrap().is_empty(), "nothing was sent");
}
