// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// table_meta against a stub BigQuery, through the real HTTPS client under
// the MetadataGuard with a caller's token: twenty tables in about one
// round trip, every outcome typed, no 4xx repeated, only tables.get with
// view=BASIC on the wire, the token sent as given, and the instance
// metadata server (a decoy) never contacted.

#![cfg(feature = "testing")]

use crab_r::exec::guard::MetadataGuard;
use crab_r::exec::table_meta::{answers_json, Limits, MetaClient, MetaOptions, Outcome};
use objstore_r::token::Adc;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TOKEN: &str = "ya29.caller-token-for-tests";
const DELAY: Duration = Duration::from_millis(150);

struct Stub {
    origin: String,
    seen: Arc<Mutex<Vec<(String, String)>>>,
}

/// Answers `GET .../tables/{t}`: `t` names its status (`s403`, `s404`,
/// `s401`, `s500`, `big`), anything else a table resource, each after
/// [`DELAY`], one thread per connection.
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
                let line = head.lines().next().unwrap_or("").to_owned();
                let auth = head
                    .lines()
                    .find_map(|l| {
                        l.strip_prefix("authorization: ")
                            .or_else(|| l.strip_prefix("Authorization: "))
                    })
                    .unwrap_or("")
                    .to_owned();
                log.lock().unwrap().push((line.clone(), auth));
                std::thread::sleep(DELAY);
                let path = line.split(' ').nth(1).unwrap_or("");
                let table =
                    path.split("/tables/").nth(1).unwrap_or("").split('?').next().unwrap_or("");
                let (status, body) = match table {
                    "s403" => (403, r#"{"error":{"code":403,"message":"denied"}}"#.to_owned()),
                    "s404" => (404, r#"{"error":{"code":404,"message":"nope"}}"#.to_owned()),
                    "s401" => (401, r#"{"error":{"code":401,"message":"expired"}}"#.to_owned()),
                    "s500" => (500, r#"{"error":{"code":500,"message":"boom"}}"#.to_owned()),
                    "big" => (
                        200,
                        format!(r#"{{"type":"TABLE","description":"{}"}}"#, "x".repeat(64 << 10)),
                    ),
                    t => (
                        200,
                        format!(
                            r#"{{"etag":"e","type":"TABLE","location":"US","schema":{{"fields":[{{"name":"id","type":"INT64","policyTags":{{"names":["pt"]}}}},{{"name":"secret","type":"STRING"}}]}},"description":"Table {t}.","numRows":"9"}}"#
                        ),
                    ),
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

fn client(stub: &Stub, limits: &Limits) -> MetaClient<'static, Adc> {
    MetaClient::new(
        Adc::from_access_token(TOKEN).unwrap(),
        MetadataGuard::default().with_loopback_http(),
        Some(&stub.origin),
        limits,
    )
    .unwrap()
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

#[test]
fn a_twenty_table_batch_is_about_one_round_trip() {
    let stub = stub();
    let c = client(&stub, &Limits::default());
    let names: Vec<String> = (0..20).map(|i| format!("p.d.t{i}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let rt = rt();
    // Warm the connection pool's TLS-free path once.
    rt.block_on(c.table_meta(&["p.d.warm"], &MetaOptions::default()));
    let start = Instant::now();
    let answers = rt.block_on(c.table_meta(&names, &MetaOptions::default()));
    let took = start.elapsed();
    assert!(answers.iter().all(|a| a.outcome == Outcome::Ok), "{answers:?}");
    assert!(took < DELAY * 3, "20 tables took {took:?}; one round trip is {DELAY:?}");
    // Order kept.
    assert!(answers.iter().zip(&names).all(|(a, n)| a.table == *n));
}

#[test]
fn every_outcome_is_typed_and_no_4xx_is_repeated() {
    let stub = stub();
    let c = client(&stub, &Limits { max_resource_bytes: 32 << 10, ..Limits::default() });
    let tables = [
        "p.d.ok",
        "p.d.s403",
        "p.d.s404",
        "p.d.s401",
        "p.d.s500",
        "p.d.big",
        "not a name",
        "p.d",
        "My-Proj.d.ok2",
    ];
    let opts = MetaOptions { omit_columns: &["SECRET"], ..MetaOptions::default() };
    let answers = rt().block_on(c.table_meta(&tables, &opts));
    let got: HashMap<&str, Outcome> =
        answers.iter().map(|a| (a.table.as_str(), a.outcome)).collect();
    assert_eq!(got["p.d.ok"], Outcome::Ok);
    assert_eq!(got["p.d.s403"], Outcome::NoAccess);
    assert_eq!(got["p.d.s404"], Outcome::NotFound);
    assert_eq!(got["p.d.s401"], Outcome::Expired);
    assert_eq!(got["p.d.s500"], Outcome::Unreachable);
    assert_eq!(got["p.d.big"], Outcome::Unreachable);
    assert_eq!(got["not a name"], Outcome::InvalidName);
    assert_eq!(got["p.d"], Outcome::InvalidName);
    assert_eq!(got["My-Proj.d.ok2"], Outcome::Ok);
    let ok = &answers[0].detail;
    assert!(ok.contains(r#""policyTags":{"names":["pt"]}"#) && !ok.contains("secret"), "{ok}");
    assert!(ok.contains(r#""omitted":1"#) && !ok.contains("numRows"));
    let json = answers_json(&answers);
    assert!(json.starts_with('[') && json.contains(r#""reason":"no_access""#));

    let seen = stub.seen.lock().unwrap().clone();
    // Seven requests (two names never sent), each once: no retry.
    assert_eq!(seen.len(), 7, "{seen:?}");
    for (line, auth) in &seen {
        assert!(
            line.starts_with("GET /bigquery/v2/projects/") && line.contains("?view=BASIC "),
            "{line}"
        );
        assert_eq!(auth, &format!("Bearer {TOKEN}"));
    }
    assert!(seen.iter().any(|(l, _)| l.contains("/projects/my-proj/")), "project canonical");
}

#[test]
fn no_credential_is_a_typed_outcome_and_the_metadata_server_is_never_asked() {
    let decoy = TcpListener::bind("127.0.0.1:0").unwrap();
    decoy.set_nonblocking(true).unwrap();
    std::env::set_var("GCE_METADATA_HOST", decoy.local_addr().unwrap().to_string());
    std::env::set_var("CLOUDSDK_CONFIG", std::env::temp_dir().join("crab-no-gcloud-config"));
    std::env::remove_var("GOOGLE_APPLICATION_CREDENTIALS");
    let e = Adc::discover_with(objstore_r::token::MetadataPolicy::Forbid).unwrap_err();
    let a = crab_r::exec::table_meta::all_failed(&["p.d.t"], Outcome::of_error(&e), "x");
    assert_eq!(a[0].outcome, Outcome::NoCredentials);
    assert!(Adc::from_access_token("  ").is_err());
    // A batch with the caller's token: the decoy sees nothing.
    let stub = stub();
    rt().block_on(
        client(&stub, &Limits::default()).table_meta(&["p.d.t"], &MetaOptions::default()),
    );
    assert!(decoy.accept().is_err(), "the metadata server was contacted");
}
