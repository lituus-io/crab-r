// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! BigQuery over the REST API, on any objstore-r [`Transport`].
//!
//! - `jobs.query` in **short-query mode** (`jobCreationMode:
//!   JOB_CREATION_OPTIONAL`): fast queries complete in one round trip with no
//!   job created.
//! - Every request carries a `requestId`, so a transport retry of the same
//!   body is de-duplicated server-side and can never bill twice.
//! - Every run sets `maximumBytesBilled`; dry runs are free and report the
//!   bytes a query would scan, which is the engine's cost guardrail.
//! - Waiting for completion is a server-side long poll
//!   (`getQueryResults?timeoutMs`), never a client sleep loop.
//! - Result pages request exactly the rows still needed, never more.
//! - `TIMESTAMP`s are requested as int64 microseconds (exact, cheap to parse).

use crate::base::json::Writer;
use crate::exec::decode::{api_error, batch_for, decode_rows, parse_page, Field, Page, Scratch};
use crate::exec::{DryRun, Error, QueryResult, Result, RunOpts, Warehouse};
use objstore_r::transport::{encode_component, Request, Transport};
use objstore_r::Bytes;
use std::fmt::Write as _;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Longest single server-side wait, milliseconds.
const MAX_POLL_MS: u32 = 10_000;
/// Largest page requested, rows.
const MAX_PAGE_ROWS: u32 = 50_000;

/// Settings.
#[derive(Clone, Debug)]
pub struct BigQueryConfig {
    /// Project that runs (and pays for) the jobs.
    pub project: String,
    /// Location (`US`, `EU`, `us-central1`, ...).
    pub location: String,
    /// Override the API origin (tests, private endpoints).
    pub endpoint: Option<String>,
}

/// A BigQuery warehouse.
#[derive(Debug)]
pub struct BigQuery<T: Transport> {
    transport: T,
    location: String,
    /// API origin (`https://bigquery.googleapis.com`).
    origin: String,
    queries_url: String,
    seq: AtomicU64,
    salt: u64,
}

impl<T: Transport> BigQuery<T> {
    /// A warehouse reached through `transport`.
    #[must_use]
    pub fn new(transport: T, cfg: &BigQueryConfig) -> Self {
        let origin = cfg
            .endpoint
            .as_deref()
            .unwrap_or("https://bigquery.googleapis.com")
            .trim_end_matches('/');
        let mut queries_url = format!("{origin}/bigquery/v2/projects/");
        encode_component(&cfg.project, &mut queries_url);
        queries_url.push_str("/queries");
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64)
            ^ u64::from(std::process::id()).rotate_left(32);
        Self {
            transport,
            location: cfg.location.clone(),
            origin: origin.to_owned(),
            queries_url,
            seq: AtomicU64::new(0),
            salt,
        }
    }

    /// The transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// A unique, retry-stable request id: a version-4 UUID (36 characters,
    /// BigQuery's limit).
    fn request_id(&self) -> String {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        let mut g =
            crate::base::prng::SplitMix64::new(self.salt ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let (a, b) = (g.next_u64(), g.next_u64());
        let (a, b) = ((a & !0xF000) | 0x4000, (b & !(0b11 << 62)) | (0b10 << 62));
        format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            a >> 32,
            (a >> 16) & 0xFFFF,
            a & 0xFFFF,
            b >> 48,
            b & 0xFFFF_FFFF_FFFF
        )
    }

    fn query_body(&self, sql: &str, dry: bool, opts: &RunOpts) -> Bytes {
        let mut out = String::with_capacity(sql.len() + 320);
        let mut w = Writer::new(&mut out);
        w.begin_object()
            .key("query")
            .string(sql)
            .key("useLegacySql")
            .bool(false)
            .key("location")
            .string(&self.location)
            .key("requestId")
            .string(&self.request_id())
            .key("formatOptions")
            .begin_object()
            .key("useInt64Timestamp")
            .bool(true)
            .end_object();
        if dry {
            // Estimate as if uncached: the guardrail must see real scan bytes.
            w.key("dryRun").bool(true).key("useQueryCache").bool(false);
        } else {
            w.key("jobCreationMode")
                .string("JOB_CREATION_OPTIONAL")
                .key("useQueryCache")
                .bool(opts.use_cache)
                .key("timeoutMs")
                .uint(u64::from(opts.timeout_ms.min(MAX_POLL_MS)))
                .key("maxResults")
                .uint(u64::from(opts.max_rows.clamp(1, MAX_PAGE_ROWS)));
            if let Some(max) = opts.max_bytes_billed {
                // int64 fields travel as strings in the REST API.
                w.key("maximumBytesBilled").string(&max.to_string());
            }
        }
        w.end_object();
        Bytes::from(out)
    }

    fn results_url(
        &self,
        job_id: &str,
        location: &str,
        timeout_ms: u32,
        max_rows: u32,
        token: Option<&str>,
    ) -> String {
        let mut u = String::with_capacity(self.queries_url.len() + 160);
        u.push_str(&self.queries_url);
        u.push('/');
        encode_component(job_id, &mut u);
        u.push_str("?location=");
        encode_component(location, &mut u);
        let _ = write!(
            u,
            "&timeoutMs={timeout_ms}&maxResults={max_rows}&formatOptions.useInt64Timestamp=true"
        );
        if let Some(t) = token {
            u.push_str("&pageToken=");
            encode_component(t, &mut u);
        }
        u
    }

    async fn call(&self, req: Request<'_>) -> Result<Bytes> {
        let resp = self.transport.send(req).await?;
        if resp.is_success() {
            Ok(resp.body)
        } else {
            Err(api_error(resp.status, &resp.body))
        }
    }

    async fn run_inner(&self, sql: &str, opts: RunOpts) -> Result<QueryResult> {
        let started = Instant::now();
        let deadline_ms = u64::from(opts.timeout_ms);
        let mut body = self
            .call(Request::post_json(&self.queries_url, self.query_body(sql, false, &opts)))
            .await?;
        let mut page = parse_page(&body)?;
        let location = page.location.clone().unwrap_or_else(|| self.location.clone());

        // Wait for completion with server-side long polls.
        while !page.complete {
            let elapsed = started.elapsed().as_millis() as u64;
            if elapsed >= deadline_ms {
                return Err(Error::Timeout);
            }
            let job = page.job_id.clone().ok_or(Error::Decode(crate::base::Error::Type {
                expected: "jobReference for an incomplete query",
            }))?;
            let wait = ((deadline_ms - elapsed) as u32).min(MAX_POLL_MS);
            let url = self.results_url(
                &job,
                &location,
                wait,
                opts.max_rows.clamp(1, MAX_PAGE_ROWS),
                None,
            );
            body = self.call(Request::get(&url)).await?;
            let next = parse_page(&body)?;
            page = Page { job_id: next.job_id.clone().or(page.job_id), ..next };
        }

        let schema: Vec<Field> = page.schema.clone().unwrap_or_default();
        let want = opts.max_rows as usize;
        let mut batch = batch_for(&schema, want.min(page.total_rows as usize));
        let mut scratch = Scratch::default();
        let mut got = decode_rows(&body, &schema, &mut batch, want, &mut scratch)?;
        let (bytes_processed, bytes_billed, cache_hit, total_rows) =
            (page.bytes_processed, page.bytes_billed, page.cache_hit, page.total_rows);

        // Further pages, only as many rows as still wanted.
        let mut token = page.next_token.clone();
        while got < want {
            let Some(t) = token.take() else { break };
            let Some(job) = page.job_id.clone() else { break };
            let remaining = (want - got).min(MAX_PAGE_ROWS as usize) as u32;
            let url = self.results_url(&job, &location, MAX_POLL_MS, remaining, Some(&t));
            let b = self.call(Request::get(&url)).await?;
            let p = parse_page(&b)?;
            got += decode_rows(&b, &schema, &mut batch, want - got, &mut scratch)?;
            token = p.next_token;
        }

        Ok(QueryResult {
            batch,
            total_rows,
            truncated: total_rows > got as u64,
            bytes_processed,
            bytes_billed,
            cache_hit,
            wall_ms: started.elapsed().as_millis() as u64,
        })
    }

    async fn dry_inner(&self, sql: &str) -> Result<DryRun> {
        let body = self
            .call(Request::post_json(
                &self.queries_url,
                self.query_body(sql, true, &RunOpts::default()),
            ))
            .await?;
        let page = parse_page(&body)?;
        let schema = page.schema.unwrap_or_default().into_iter().map(|f| {
            crate::exec::OutputColumn { ty: f.col_type(), sql_type: f.sql_type(), name: f.name }
        });
        Ok(DryRun { bytes_processed: page.bytes_processed, schema: schema.collect() })
    }
}

impl<T: Transport> Warehouse for BigQuery<T> {
    type Dry<'a>
        = impl Future<Output = Result<DryRun>> + 'a
    where
        Self: 'a;
    type Run<'a>
        = impl Future<Output = Result<QueryResult>> + 'a
    where
        Self: 'a;

    fn dry_run<'a>(&'a self, sql: &'a str) -> Self::Dry<'a> {
        self.dry_inner(sql)
    }

    fn run<'a>(&'a self, sql: &'a str, opts: RunOpts) -> Self::Run<'a> {
        self.run_inner(sql, opts)
    }
}

impl<T: Transport> crate::exec::metadata::TableMetadata for BigQuery<T> {
    type Get<'a>
        = impl Future<Output = Result<Option<crate::exec::metadata::TableMeta>>> + 'a
    where
        Self: 'a;
    type Patch<'a>
        = impl Future<Output = Result<crate::exec::metadata::TableMeta>> + 'a
    where
        Self: 'a;

    fn table_get<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table: &'a str,
    ) -> Self::Get<'a> {
        async move {
            let url = crate::exec::metadata::table_url(&self.origin, project, dataset, table);
            let resp = self.transport.send(Request::get(&url)).await?;
            match resp.status {
                404 => Ok(None),
                s if (200..300).contains(&s) => {
                    crate::exec::metadata::parse_table(resp.body).map(Some)
                }
                s => Err(crate::exec::decode::api_error(s, &resp.body)),
            }
        }
    }

    fn table_patch<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table: &'a str,
        body: Bytes,
        etag: &'a str,
    ) -> Self::Patch<'a> {
        async move {
            let url = crate::exec::metadata::table_url(&self.origin, project, dataset, table);
            let headers = [("If-Match", etag)];
            let resp =
                self.transport.send(Request::patch_json(&url, body).headers(&headers)).await?;
            match resp.status {
                412 => Err(Error::PreconditionFailed),
                s if (200..300).contains(&s) => crate::exec::metadata::parse_table(resp.body),
                s => Err(crate::exec::decode::api_error(s, &resp.body)),
            }
        }
    }
}
