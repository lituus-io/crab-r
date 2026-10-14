// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// The BigQuery client against a scripted, in-process Transport: request
// shape (short-query mode, requestId, byte cap), long-poll to completion,
// paging only as many rows as needed, dry runs, and error mapping.

use crab_r::base::batch::Cell;
use crab_r::exec::bigquery::{BigQuery, BigQueryConfig};
use crab_r::exec::{Error, RunOpts, TableMetadata, Warehouse};
use objstore_r::join::spin_on as block_on;
use objstore_r::transport::{BodyStream, Method, Request, Response, Transport};
use objstore_r::Bytes;
use std::cell::RefCell;
use std::future::{ready, Ready};

#[derive(Default)]
struct Mock {
    script: RefCell<Vec<(u16, String)>>,
    seen: RefCell<Vec<(Method, String, String)>>,
    headers: RefCell<Vec<Vec<(String, String)>>>,
}

struct NoBody;
impl BodyStream for NoBody {
    type Next<'a> = Ready<objstore_r::Result<Option<Bytes>>>;
    fn next_chunk(&mut self) -> Self::Next<'_> {
        ready(Ok(None))
    }
}

impl Transport for Mock {
    type Send<'a> = Ready<objstore_r::Result<Response>>;
    type Body<'a> = NoBody;
    type Open<'a> = Ready<objstore_r::Result<(u16, NoBody)>>;

    fn send<'a>(&'a self, req: Request<'a>) -> Self::Send<'a> {
        self.seen.borrow_mut().push((
            req.method,
            req.url.to_owned(),
            String::from_utf8(req.body.to_vec()).unwrap(),
        ));
        self.headers
            .borrow_mut()
            .push(req.headers.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect());
        let (status, body) = self.script.borrow_mut().remove(0);
        ready(Ok(Response { status, body: Bytes::from(body) }))
    }

    fn open<'a>(&'a self, _: Request<'a>) -> Self::Open<'a> {
        unreachable!()
    }
}

fn bq(script: Vec<(u16, &str)>) -> BigQuery<Mock> {
    let mock = Mock {
        script: RefCell::new(script.into_iter().map(|(s, b)| (s, b.to_owned())).collect()),
        ..Mock::default()
    };
    BigQuery::new(
        mock,
        &BigQueryConfig { project: "my-project".into(), location: "US".into(), endpoint: None },
    )
}

const SCHEMA: &str =
    r#""schema":{"fields":[{"name":"status","type":"STRING"},{"name":"n","type":"INTEGER"}]}"#;

#[test]
fn completes_in_one_round_trip_when_fast() {
    let resp = format!(
        r#"{{"jobComplete":true,{SCHEMA},"rows":[{{"f":[{{"v":"shipped"}},{{"v":"61"}}]}}],"totalRows":"1","totalBytesProcessed":"2048","totalBytesBilled":"10485760","cacheHit":false}}"#
    );
    let w = bq(vec![(200, &resp)]);
    let r = block_on(w.run(
        "SELECT status, COUNT(*) n FROM t GROUP BY 1",
        RunOpts { max_bytes_billed: Some(1 << 30), ..RunOpts::default() },
    ))
    .unwrap();
    assert_eq!(r.batch.rows(), 1);
    assert_eq!(r.batch.columns[0].get(0), Cell::Str("shipped"));
    assert_eq!(r.batch.columns[1].get(0), Cell::I64(61));
    assert_eq!((r.bytes_processed, r.bytes_billed, r.truncated), (2048, 10_485_760, false));

    let seen = w.transport().seen.borrow();
    assert_eq!(seen.len(), 1, "one round trip");
    let (m, url, body) = &seen[0];
    assert_eq!(*m, Method::Post);
    assert_eq!(url, "https://bigquery.googleapis.com/bigquery/v2/projects/my-project/queries");
    let rid_at = body.find(r#""requestId":""#).unwrap() + 13;
    let rid = &body[rid_at..rid_at + 36];
    assert!(
        rid.len() == 36
            && rid.as_bytes()[14] == b'4'
            && rid.bytes().all(|c| c.is_ascii_hexdigit() || c == b'-'),
        "UUID v4: {rid}"
    );
    for needle in [
        r#""useLegacySql":false"#,
        r#""jobCreationMode":"JOB_CREATION_OPTIONAL""#,
        r#""maximumBytesBilled":"1073741824""#,
        r#""useInt64Timestamp":true"#,
        r#""requestId":""#,
        r#""location":"US""#,
    ] {
        assert!(body.contains(needle), "{needle} missing from {body}");
    }
}

#[test]
fn long_polls_then_pages_only_what_is_needed() {
    let pending = r#"{"jobComplete":false,"jobReference":{"jobId":"job_9","location":"US"}}"#;
    let page1 = format!(
        r#"{{"jobComplete":true,{SCHEMA},"rows":[{{"f":[{{"v":"a"}},{{"v":"1"}}]}},{{"f":[{{"v":"b"}},{{"v":"2"}}]}}],"totalRows":"5","pageToken":"tok 1","jobReference":{{"jobId":"job_9"}},"totalBytesProcessed":"10"}}"#
    );
    let page2 = r#"{"jobComplete":true,"rows":[{"f":[{"v":"c"},{"v":"3"}]}],"totalRows":"5","pageToken":"tok2"}"#;
    let w = bq(vec![(200, pending), (200, &page1), (200, page2)]);
    let r = block_on(w.run("SELECT 1", RunOpts { max_rows: 3, ..RunOpts::default() })).unwrap();
    assert_eq!(r.batch.rows(), 3);
    assert!(r.truncated, "5 rows exist, 3 returned");
    let seen = w.transport().seen.borrow();
    assert_eq!(seen.len(), 3, "no fetch beyond the row cap");
    assert!(seen[1].1.contains("/queries/job_9?location=US&timeoutMs="), "{}", seen[1].1);
    assert!(
        seen[2].1.contains("maxResults=1&") && seen[2].1.ends_with("pageToken=tok%201"),
        "asks only for the missing row: {}",
        seen[2].1
    );
}

#[test]
fn dry_run_reports_cost_and_shape() {
    let resp = format!(r#"{{"jobComplete":true,{SCHEMA},"totalBytesProcessed":"123456789"}}"#);
    let w = bq(vec![(200, &resp)]);
    let d = block_on(w.dry_run("SELECT status, n FROM t")).unwrap();
    assert_eq!(d.bytes_processed, 123_456_789);
    let types: Vec<&str> = d.schema.iter().map(|c| c.sql_type.as_str()).collect();
    assert_eq!(types.len(), 2);
    assert!(types.iter().all(|t| !t.is_empty()), "{types:?}");
    let body = &w.transport().seen.borrow()[0].2;
    assert!(body.contains(r#""dryRun":true"#) && body.contains(r#""useQueryCache":false"#));
    assert!(!body.contains("maximumBytesBilled"));
}

#[test]
fn sql_errors_carry_positions_for_repair() {
    let err = r#"{"error":{"code":400,"message":"Unrecognized name: statuz; Did you mean status? at [1:8]","errors":[{"reason":"invalidQuery"}]}}"#;
    let w = bq(vec![(400, err)]);
    let e = block_on(w.run("SELECT statuz FROM t", RunOpts::default())).unwrap_err();
    assert!(e.is_sql_error());
    match e {
        Error::Query { pos: Some(p), message, .. } => {
            assert_eq!((p.line, p.col), (1, 8));
            assert!(message.contains("Did you mean status?"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_poll_past_the_deadline_times_out() {
    let pending = r#"{"jobComplete":false,"jobReference":{"jobId":"job_1"}}"#;
    let w = bq(vec![(200, pending), (200, pending), (200, pending), (200, pending)]);
    // A zero timeout means the first incomplete answer is already too late.
    let e =
        block_on(w.run("SELECT 1", RunOpts { timeout_ms: 0, ..RunOpts::default() })).unwrap_err();
    assert!(matches!(e, Error::Timeout));
}

// ---- table metadata ---------------------------------------------------------

const TABLE: &str = r#"{"etag":"e1","type":"TABLE","description":"","schema":{"fields":[{"name":"id","type":"INT64","policyTags":{"names":["pt"]}}]},"numRows":"7","lastModifiedTime":"1790000000000"}"#;

#[test]
fn table_get_reads_metadata_for_free_and_handles_absence() {
    let w = bq(vec![(200, TABLE), (404, r#"{"error":{"code":404,"message":"Not found: Table"}}"#)]);
    let m = block_on(w.table_get("p", "d", "t")).unwrap().unwrap();
    assert_eq!(
        (m.etag.as_str(), m.kind.as_str(), m.description.as_deref(), m.num_rows),
        ("e1", "TABLE", None, 7)
    );
    assert!(m.schema_json().contains("policyTags"));
    assert!(block_on(w.table_get("p", "d", "missing")).unwrap().is_none(), "not deployed yet");
    let seen = w.transport().seen.borrow();
    assert_eq!(
        (seen[0].0, seen[0].1.as_str()),
        (Method::Get, "https://bigquery.googleapis.com/bigquery/v2/projects/p/datasets/d/tables/t")
    );
}

#[test]
fn table_patch_is_guarded_by_the_etag() {
    let w = bq(vec![
        (200, TABLE),
        (412, r#"{"error":{"code":412,"message":"Precondition check failed."}}"#),
    ]);
    let body = Bytes::from_static(br#"{"schema":{"fields":[]}}"#);
    let m = block_on(w.table_patch("p", "d", "t", body.clone(), "e0")).unwrap();
    assert_eq!(m.etag, "e1");
    assert!(matches!(
        block_on(w.table_patch("p", "d", "t", body, "stale")),
        Err(Error::PreconditionFailed)
    ));
    let seen = w.transport().seen.borrow();
    assert_eq!(seen[0].0, Method::Patch);
    assert_eq!(
        w.transport().headers.borrow()[1],
        vec![("If-Match".to_owned(), "stale".to_owned())]
    );
}
