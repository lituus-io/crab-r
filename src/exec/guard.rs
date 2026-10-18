// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! [`MetadataGuard`]: the request policy under which crab-r reads metadata
//! and never data (feature `http`).
//!
//! **Endpoints** ([`RequestPolicy::check`]): only
//!
//! - BigQuery `tables.get?view=BASIC` (schema and partitioning; no
//!   statistics, no rows) and `datasets.get`;
//! - `jobs.query`, and `jobs.getQueryResults` for a job this process issued
//!   through a [`Tracked`] transport ([`OwnJobs`]);
//! - Dataplex Data Lineage searches and process reads.
//!
//! **Queries** ([`RequestPolicy::check_query`], which objstore-r calls on
//! every `jobs.query` and `jobs.insert` body under every policy): exactly one
//! query statement, parsed by crab-r's own GoogleSQL parser, every one of
//! whose reads ([`crate::sql::refs`]) is an `INFORMATION_SCHEMA` view, a
//! dataset's `__TABLES__` meta-table, or a table in one of the graph
//! datasets (default `infra` and `ops`, the tables the graph itself writes).
//! Refused: DML, DDL, scripts, more than one statement, `EXTERNAL_QUERY`,
//! functions outside the built-in namespaces (a persistent or remote
//! function, a model), an unqualified name (it would resolve against a
//! request's `defaultDataset`, which the guard does not see), an array path
//! (only reads already checked may be scanned, and the guard does not rely
//! on scoping for that), a read the walk could not place, and anything the
//! parser cannot read.
//!
//! Refusal reasons are fixed text: never a name, the SQL or the URL.

use crate::sql::ast::StmtKind;
use crate::sql::refs::{walk_script, DatasetRef, RefKind, TableRef};
use crate::sql::{Mode, Parser};
use objstore_r::policy::{AllowList, RequestPolicy, Rule, Url};
use objstore_r::transport::{Method, Request, Response, Transport};
use objstore_r::PolicyError;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Mutex;

/// The datasets the graph writes, read by default.
pub const DEFAULT_GRAPH_DATASETS: [DatasetRef<'static>; 2] =
    [DatasetRef::any("infra"), DatasetRef::any("ops")];

const BQ: &str = "bigquery.googleapis.com";
const LINEAGE: &str = "datalineage.googleapis.com";
const TABLES_GET: &str = "/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}";
const RESULTS: &str = "/bigquery/v2/projects/{project}/queries/{job}";

/// The endpoints a [`MetadataGuard`] allows.
pub const RULES: &[Rule<'static>] = &[
    Rule::new(Method::Get, BQ, TABLES_GET),
    Rule::new(Method::Get, BQ, "/bigquery/v2/projects/{project}/datasets/{dataset}"),
    Rule::new(Method::Post, BQ, "/bigquery/v2/projects/{project}/queries"),
    Rule::new(Method::Get, BQ, RESULTS),
    Rule::new(Method::Post, LINEAGE, "/v1/projects/{project}/locations/{location}:searchLinks"),
    Rule::new(
        Method::Post,
        LINEAGE,
        "/v1/projects/{project}/locations/{location}:searchLineageStreaming",
    ),
    Rule::new(
        Method::Post,
        LINEAGE,
        "/v1/projects/{project}/locations/{location}:batchSearchLinkProcesses",
    ),
    Rule::new(
        Method::Get,
        LINEAGE,
        "/v1/projects/{project}/locations/{location}/processes/{process}",
    ),
];

/// Job ids this process's [`Tracked`] transports were handed by
/// `jobs.query`: the only jobs whose results a [`MetadataGuard`] lets a
/// client read. Bounded: the oldest are forgotten first.
#[derive(Debug, Default)]
pub struct OwnJobs(Mutex<VecDeque<Box<str>>>);

/// Job ids remembered by an [`OwnJobs`].
pub const OWN_JOBS_KEPT: usize = 1024;

impl OwnJobs {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Box<str>>> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Remember `job`.
    pub fn record(&self, job: &str) {
        let mut ids = self.lock();
        if ids.iter().any(|j| &**j == job) {
            return;
        }
        if ids.len() == OWN_JOBS_KEPT {
            ids.pop_front();
        }
        ids.push_back(job.into());
    }

    /// `job` was issued here.
    #[must_use]
    pub fn contains(&self, job: &str) -> bool {
        self.lock().iter().any(|j| &**j == job)
    }
}

/// The request policy for metadata reads. See the module docs.
#[derive(Debug, Clone, Copy)]
pub struct MetadataGuard<'a> {
    graph_datasets: &'a [DatasetRef<'a>],
    own_jobs: Option<&'a OwnJobs>,
    list: AllowList<'static>,
}

impl Default for MetadataGuard<'static> {
    fn default() -> Self {
        Self::new(&DEFAULT_GRAPH_DATASETS, None)
    }
}

impl<'a> MetadataGuard<'a> {
    /// A guard reading the `graph_datasets` beside `INFORMATION_SCHEMA`; job
    /// results only for the jobs in `own_jobs` (none without it).
    #[must_use]
    pub fn new(graph_datasets: &'a [DatasetRef<'a>], own_jobs: Option<&'a OwnJobs>) -> Self {
        // The rules are constants, validated by a test.
        let list = AllowList::new(RULES).unwrap_or_else(|_| unreachable!("valid rules"));
        Self { graph_datasets, own_jobs, list }
    }

    /// Plain `http` to a loopback host stands in for every listed host
    /// (stub servers in tests).
    #[cfg(feature = "testing")]
    #[must_use]
    pub fn with_loopback_http(mut self) -> Self {
        self.list = self.list.with_loopback_http();
        self
    }

    /// The graph datasets.
    #[must_use]
    pub fn graph_datasets(&self) -> &'a [DatasetRef<'a>] {
        self.graph_datasets
    }

    fn in_graph(&self, r: &TableRef) -> bool {
        let Some(d) = r.dataset.as_deref() else { return false };
        self.graph_datasets.iter().any(|g| {
            g.dataset == d
                && match (g.project, r.project.as_deref()) {
                    (None, _) => true,
                    (Some(gp), Some(rp)) => gp.eq_ignore_ascii_case(rp),
                    (Some(_), None) => false,
                }
        })
    }

    /// Accept or refuse one query body (what [`RequestPolicy::check_query`]
    /// answers).
    pub fn check_sql(&self, sql: &str) -> Result<(), PolicyError> {
        thread_local! {
            static PARSER: RefCell<Parser> = RefCell::new(Parser::new());
        }
        PARSER.with(|p| {
            // A re-entrant call (none today) gets its own parser.
            match p.try_borrow_mut() {
                Ok(mut p) => self.check_parsed(&mut p, sql),
                Err(_) => self.check_parsed(&mut Parser::new(), sql),
            }
        })
    }

    fn check_parsed(&self, p: &mut Parser, sql: &str) -> Result<(), PolicyError> {
        let script = p.parse_script(sql, Mode::Full).map_err(|_| refuse("unparseable SQL"))?;
        match script.stmts {
            [s] if s.kind == StmtKind::Query => {}
            [_] => return Err(refuse("not a query: DML, DDL and scripts are refused")),
            [] => return Err(refuse("no statement")),
            _ => return Err(refuse("more than one statement")),
        }
        for (_, c) in script.ast.calls.iter() {
            let names = script.ast.names.slice(c.name);
            let part = |i: usize| names.get(i).map(|n| n.sp.text(sql).trim_matches('`'));
            let allowed = match (part(0), part(1)) {
                (Some(f), None) => !f.eq_ignore_ascii_case("EXTERNAL_QUERY"),
                (Some(ns), Some(f)) if names.len() == 2 => {
                    BUILTIN_NAMESPACES.iter().any(|b| ns.eq_ignore_ascii_case(b))
                        && !f.eq_ignore_ascii_case("EXTERNAL_QUERY")
                }
                _ => false,
            };
            if !allowed {
                return Err(refuse("EXTERNAL_QUERY or a function outside the built-in namespaces"));
            }
        }
        let refs = walk_script(&script, None);
        if !refs.complete {
            return Err(refuse("a read the guard could not place"));
        }
        for r in &refs.tables {
            let ok = match r.kind {
                RefKind::InformationSchema | RefKind::MetaTable => true,
                RefKind::Table => self.in_graph(r),
                RefKind::Path => false,
            };
            if !ok {
                return Err(refuse(
                    "reads a table outside INFORMATION_SCHEMA and the graph datasets",
                ));
            }
        }
        Ok(())
    }
}

/// Function namespaces that are built in and read no table: `SAFE.`,
/// `NET.`, `HLL_COUNT.`, `KEYS.`, `AEAD.`.
const BUILTIN_NAMESPACES: [&str; 5] = ["SAFE", "NET", "HLL_COUNT", "KEYS", "AEAD"];

fn refuse(reason: &'static str) -> PolicyError {
    PolicyError::Query { reason: Cow::Borrowed(reason) }
}

fn endpoint(reason: &'static str) -> PolicyError {
    PolicyError::Endpoint { reason }
}

impl RequestPolicy for MetadataGuard<'_> {
    fn check(&self, method: Method, url: &Url) -> Result<(), PolicyError> {
        self.list.check(method, url)?;
        let path = url.path();
        let segs: Vec<&str> = path.split('/').collect();
        let bq = segs.get(1) == Some(&"bigquery");
        // /bigquery/v2/projects/{p}/datasets/{d}/tables/{t}
        if bq && segs.len() == 9 && segs[7] == "tables" {
            return if url.query() == Some("view=BASIC") {
                Ok(())
            } else {
                Err(endpoint("tables.get only with view=BASIC"))
            };
        }
        // /bigquery/v2/projects/{p}/queries/{job}
        if bq && segs.len() == 7 && segs[5] == "queries" {
            let own = self.own_jobs.is_some_and(|j| j.contains(segs[6]));
            return if own { Ok(()) } else { Err(endpoint("results of a job not issued here")) };
        }
        if url.query().is_some() {
            return Err(endpoint("a query string on this endpoint"));
        }
        Ok(())
    }

    fn check_query(&self, sql: &str) -> Result<(), PolicyError> {
        self.check_sql(sql)
    }
}

/// A transport that records, in an [`OwnJobs`], the job id of every
/// successful `jobs.query` it sends: what lets a [`MetadataGuard`] allow
/// the results of exactly those jobs.
#[derive(Debug)]
pub struct Tracked<'a, T> {
    inner: T,
    jobs: &'a OwnJobs,
}

impl<'a, T> Tracked<'a, T> {
    /// `inner`, recording into `jobs`.
    pub fn new(inner: T, jobs: &'a OwnJobs) -> Self {
        Self { inner, jobs }
    }

    /// The wrapped transport.
    pub fn inner(&self) -> &T {
        &self.inner
    }
}

fn is_jobs_query(req: &Request<'_>) -> bool {
    req.method == Method::Post && req.url.split('?').next().is_some_and(|u| u.ends_with("/queries"))
}

impl<T: Transport> Transport for Tracked<'_, T> {
    type Send<'b>
        = impl Future<Output = objstore_r::Result<Response>> + 'b
    where
        Self: 'b;
    type Body<'b>
        = T::Body<'b>
    where
        Self: 'b;
    type Open<'b>
        = T::Open<'b>
    where
        Self: 'b;

    fn send<'b>(&'b self, req: Request<'b>) -> Self::Send<'b> {
        async move {
            let query = is_jobs_query(&req);
            let resp = self.inner.send(req).await?;
            if query && resp.is_success() {
                if let Some(job) =
                    crate::exec::decode::parse_page(&resp.body).ok().and_then(|p| p.job_id)
                {
                    self.jobs.record(&job);
                }
            }
            Ok(resp)
        }
    }

    fn open<'b>(&'b self, req: Request<'b>) -> Self::Open<'b> {
        self.inner.open(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn g() -> MetadataGuard<'static> {
        MetadataGuard::default()
    }

    #[test]
    fn rules_are_valid() {
        assert!(AllowList::new(RULES).is_ok());
    }

    #[test]
    fn accepts_information_schema_and_graph_datasets() {
        for sql in [
            "SELECT * FROM `p`.`region-us`.INFORMATION_SCHEMA.JOBS",
            "SELECT table_name FROM p.d.INFORMATION_SCHEMA.TABLES WHERE table_schema = 'x'",
            "SELECT * FROM `p.d`.__TABLES__",
            "SELECT * FROM p.ops.runs r JOIN infra.resources USING (id)",
            "WITH j AS (SELECT * FROM `p.region-us.INFORMATION_SCHEMA.JOBS`) SELECT COUNT(*) FROM j",
            "SELECT SAFE.PARSE_JSON(x), NET.HOST(u) FROM ops.events -- comment\n",
            "SELECT 1;",
        ] {
            assert_eq!(g().check_sql(sql), Ok(()), "{sql}");
        }
    }

    #[test]
    fn refuses_data_writes_scripts_and_tricks() {
        for sql in [
            "SELECT * FROM p.mart.orders",
            "SELECT * FROM orders",
            "SELECT * FROM ops.runs, secret.t",
            "SELECT (SELECT MAX(x) FROM p.mart.orders) FROM ops.runs",
            "SELECT * FROM ops.runs WHERE EXISTS (SELECT 1 FROM p.d.t)",
            "SELECT * FROM ops.runs; SELECT * FROM p.d.t",
            "SELECT * FROM ops.runs /* ; */ ; DROP TABLE ops.runs",
            "DELETE FROM ops.runs WHERE TRUE",
            "INSERT INTO ops.runs SELECT * FROM ops.runs",
            "CREATE TABLE ops.x AS SELECT 1",
            "DECLARE x INT64; SELECT 1",
            "BEGIN SELECT 1; END",
            "SELECT * FROM EXTERNAL_QUERY('c', 'SELECT 1')",
            "SELECT EXTERNAL_QUERY('c', 'SELECT 1')",
            "SELECT AI.GENERATE('x')",
            "SELECT p.d.fn(1) FROM ops.runs",
            "SELECT * FROM ops.runs r, r.items",
            "SELECT * FROM `ops.runs`, `p.mart`.orders",
            "SELECT * FROM `OPS`.runs",
            "WITH ops AS (SELECT 1) SELECT * FROM ops.runs, mart.t",
            "SELECT (SELECT 1 FROM (WITH s AS (SELECT 1) SELECT * FROM s)), (SELECT 1 FROM s)",
            "SELECT FROM (",
            "",
        ] {
            assert!(g().check_sql(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn project_scoped_graph_datasets() {
        let ds = [DatasetRef { project: Some("graph-p"), dataset: "infra" }];
        let guard = MetadataGuard::new(&ds, None);
        assert!(guard.check_sql("SELECT * FROM `Graph-P.infra.nodes`").is_ok());
        assert!(guard.check_sql("SELECT * FROM other.infra.nodes").is_err());
        assert!(guard.check_sql("SELECT * FROM infra.nodes").is_err(), "project unknown");
    }

    #[test]
    fn endpoints() {
        let jobs = OwnJobs::new();
        jobs.record("job_mine");
        let guard = MetadataGuard::new(&DEFAULT_GRAPH_DATASETS, Some(&jobs));
        let ok = |m: Method, u: &str| guard.check(m, &Url::parse(u).unwrap()).is_ok();
        let b = "https://bigquery.googleapis.com/bigquery/v2/projects/p";
        assert!(ok(Method::Get, &format!("{b}/datasets/d/tables/t?view=BASIC")));
        assert!(!ok(Method::Get, &format!("{b}/datasets/d/tables/t")));
        assert!(!ok(Method::Get, &format!("{b}/datasets/d/tables/t?view=FULL")));
        assert!(!ok(Method::Get, &format!("{b}/datasets/d/tables/t?view=BASIC&view=FULL")));
        assert!(!ok(Method::Get, &format!("{b}/datasets/d/tables/t/data?view=BASIC")));
        assert!(ok(Method::Get, &format!("{b}/datasets/d")));
        assert!(!ok(Method::Patch, &format!("{b}/datasets/d/tables/t?view=BASIC")));
        assert!(ok(Method::Post, &format!("{b}/queries")));
        assert!(ok(Method::Get, &format!("{b}/queries/job_mine?location=US&timeoutMs=1")));
        assert!(!ok(Method::Get, &format!("{b}/queries/job_theirs?location=US")));
        assert!(!ok(Method::Post, &format!("{b}/jobs")));
        assert!(!ok(Method::Get, &format!("{b}/jobs/job_mine")));
        let l = "https://datalineage.googleapis.com/v1/projects/p/locations/us";
        assert!(ok(Method::Post, &format!("{l}:searchLinks")));
        assert!(ok(Method::Get, &format!("{l}/processes/x")));
        assert!(!ok(Method::Delete, &format!("{l}/processes/x")));
        assert!(!ok(Method::Get, "https://storage.googleapis.com/storage/v1/b/x/o/y?alt=media"));
        assert!(!ok(Method::Get, "http://metadata.google.internal/computeMetadata/v1/"));
        assert!(!MetadataGuard::default()
            .check(Method::Get, &Url::parse(&format!("{b}/queries/job_mine")).unwrap())
            .is_ok());
    }

    #[test]
    fn own_jobs_are_bounded() {
        let j = OwnJobs::new();
        for i in 0..=OWN_JOBS_KEPT {
            j.record(&format!("j{i}"));
        }
        assert!(!j.contains("j0") && j.contains("j1") && j.contains(&format!("j{OWN_JOBS_KEPT}")));
    }
}
