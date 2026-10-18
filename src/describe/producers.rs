// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Where each table's data comes from: the SQL that produced it.
//!
//! - **Views and materialized views**: their definition, read from the
//!   table resource (`tables.get`, free).
//! - **Tables**: `INFORMATION_SCHEMA.JOBS` over a lookback window, one query
//!   per region (a few MB of job metadata). Jobs writing a table are grouped
//!   by the *shape* of their SQL (string and number literals normalised, so
//!   the daily runs of one scheduled query are one writer), and up to
//!   [`MAX_WRITERS`] distinct writers per table are returned, latest first,
//!   each with its statement type, latest SQL and cadence (writes, first and
//!   last write, how many principals). Row-producing statements (CTAS,
//!   INSERT, MERGE, a scheduled SELECT with a destination) rank before
//!   UPDATEs. DELETE and TRUNCATE produce no values and are not returned.
//!   A statement run inside a script (a dbt model, a scheduled script) is
//!   recorded as a child job holding only that statement; its SQL is taken
//!   from the parent script instead, so the temporary tables it reads can be
//!   traced through. Identities never leave this module: only the count is
//!   kept.

use crate::base::batch::{Batch, Cell};
use crate::exec::{RunOpts, Warehouse};
use std::collections::BTreeMap;

/// How a table was last produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Producer {
    /// `project.dataset.table`.
    pub table: String,
    /// `CREATE_TABLE_AS_SELECT`, `INSERT`, `MERGE`, `CREATE_VIEW`,
    /// `VIEW` (definition read from the table), `SELECT` (a destination
    /// write), `UPDATE`, `DELETE`, ...
    pub statement_type: String,
    /// The SQL.
    pub query: String,
    /// Successful writes in the window (0 for view definitions).
    pub writes: u64,
    /// Distinct principals that wrote.
    pub writers: u64,
    /// First and last write, epoch milliseconds.
    pub first_ms: u64,
    /// Last write, epoch milliseconds.
    pub last_ms: u64,
    /// Other distinct writers of the same table (a backfill beside the
    /// daily load, several INSERT sources, UPDATEs), latest first.
    pub others: Vec<Producer>,
}

/// Distinct writers kept per table.
pub const MAX_WRITERS: usize = 3;

impl Producer {
    /// This writer and the others, primary first.
    pub fn all(&self) -> impl Iterator<Item = &Producer> {
        std::iter::once(self).chain(self.others.iter())
    }
}

/// JOBS discovery settings.
#[derive(Clone, Debug)]
pub struct JobsConfig {
    /// Projects whose job history is searched (jobs run where they are
    /// billed, which may differ from where the tables live).
    pub job_projects: Vec<String>,
    /// Region of the jobs (`us`, `eu`, `northamerica-northeast1`, ...).
    pub region: String,
    /// Lookback, days (JOBS keeps 180).
    pub lookback_days: u32,
    /// Byte cap for the JOBS query.
    pub max_bytes_billed: u64,
    /// The dataset holding a project's `observed_lineage` view (generated
    /// from [`jobs_projection_sql`]), when the project has one: job history
    /// is then read from the view, pre-pruned, instead of re-scanning JOBS,
    /// falling back to JOBS when the view cannot be read. `None` reads JOBS.
    pub ops_dataset: Option<String>,
}

impl JobsConfig {
    /// Settings for `job_projects` in `region`: 90 days, 5 GiB, JOBS itself.
    #[must_use]
    pub fn new(job_projects: Vec<String>, region: &str) -> Self {
        Self {
            job_projects,
            region: region.to_ascii_lowercase(),
            lookback_days: 90,
            max_bytes_billed: 5 << 30,
            ops_dataset: None,
        }
    }
}

fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 1024
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}

/// The view a project's `ops` dataset holds, generated from
/// [`jobs_projection_sql`].
pub const OBSERVED_LINEAGE_VIEW: &str = "observed_lineage";

/// A BigQuery job's resource key, as the deploy and the audit logs spell it:
/// `//bigquery.googleapis.com/projects/{project}/locations/{location}/jobs/{job}`.
#[must_use]
pub fn bigquery_job_key(project: &str, location: &str, job: &str) -> String {
    format!("//bigquery.googleapis.com/projects/{project}/locations/{location}/jobs/{job}")
}

/// A JOBS region as the job's location: the multi-regions `US` and `EU`
/// upper-case, a region as written (lower-case).
fn location_of(region: &str) -> String {
    let r = region.to_ascii_lowercase();
    if matches!(r.as_str(), "us" | "eu") {
        r.to_ascii_uppercase()
    } else {
        r
    }
}

/// The one `INFORMATION_SCHEMA.JOBS` projection, as SQL text: every query
/// job of `jobs_project` in `region` over the last `lookback_days` (1–180),
/// one row per job. The `ops.observed_lineage` and `ops.runs` views are
/// generated from this text and [`jobs_query`] reads through it, so a view
/// and the client never disagree on a column.
///
/// It projects only what lineage and run state need: the job's resource key
/// ([`bigquery_job_key`]), ids, state, times, error reason and message,
/// labels, the destination table, the referenced tables, the SQL (a script
/// child's from its parent script) and its shape fingerprint, and a
/// fingerprint of the principal (never the address). `None` when a name is
/// not a plain identifier (never interpolated).
#[must_use]
pub fn jobs_projection_sql(jobs_project: &str, region: &str, lookback_days: u32) -> Option<String> {
    if !valid_name(jobs_project) || !valid_name(region) || region.contains('.') {
        return None;
    }
    let source = format!(
        "`{jobs_project}`.`region-{}`.INFORMATION_SCHEMA.JOBS",
        region.to_ascii_lowercase()
    );
    let days = lookback_days.clamp(1, 180);
    let window =
        format!("creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)");
    let location = location_of(region);
    Some(format!(
        "SELECT CONCAT('//bigquery.googleapis.com/projects/', c.project_id, '/locations/{location}/jobs/', c.job_id) AS job_key, \
c.job_id, c.project_id AS job_project, c.parent_job_id, c.state, c.statement_type AS st, c.creation_time AS ts, \
c.start_time, c.end_time, c.error_result.reason AS error_code, c.error_result.message AS error_message, c.labels, \
c.destination_table.project_id AS project, c.destination_table.dataset_id AS dataset, \
c.destination_table.table_id AS tbl, c.referenced_tables, COALESCE(s.query, c.query) AS q, \
FARM_FINGERPRINT(c.user_email) AS who, \
FARM_FINGERPRINT(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(s.query, c.query), r\"'[^']*'\", '?'), r'[0-9]+', '0')) AS shape \
FROM {source} c LEFT JOIN (SELECT job_id, query FROM {source} WHERE {window} AND statement_type = 'SCRIPT') s \
ON s.job_id = c.parent_job_id WHERE c.{window} AND c.job_type = 'QUERY'"
    ))
}

/// Writers of the tables of `datasets` (each `project.dataset`) from a
/// relation with [`jobs_projection_sql`]'s columns, as SQL text.
fn writers_over(relation: &str, datasets: &[String]) -> Option<String> {
    if !datasets.iter().all(|d| valid_name(d) && d.contains('.')) || datasets.is_empty() {
        return None;
    }
    let filter: Vec<String> = datasets
        .iter()
        .map(|d| {
            let (p, ds) = d.split_once('.').unwrap_or(("", d));
            format!("(project = '{p}' AND dataset = '{ds}')")
        })
        .collect();
    Some(format!(
        "WITH j AS (SELECT project, dataset, tbl, st, q, who, ts, shape FROM {relation} \
WHERE state = 'DONE' AND error_code IS NULL AND st IN ('CREATE_TABLE_AS_SELECT', 'INSERT', \
'MERGE', 'CREATE_VIEW', 'CREATE_MATERIALIZED_VIEW', 'SELECT', 'UPDATE') AND ({filters})), \
w AS (SELECT project, dataset, tbl, shape, COUNT(*) AS writes, COUNT(DISTINCT who) AS writers, \
UNIX_MILLIS(MIN(ts)) AS first_ms, UNIX_MILLIS(MAX(ts)) AS last_ms, \
ARRAY_AGG(STRUCT(st, q) ORDER BY ts DESC LIMIT 1)[OFFSET(0)] AS latest FROM j GROUP BY project, dataset, tbl, shape) \
SELECT project, dataset, tbl, writes, writers, first_ms, last_ms, latest.st AS statement_type, latest.q AS query FROM w \
QUALIFY ROW_NUMBER() OVER (PARTITION BY project, dataset, tbl ORDER BY IF(latest.st = 'UPDATE', 1, 0), last_ms DESC) <= {max}",
        filters = filter.join(" OR "),
        max = MAX_WRITERS,
    ))
}

/// The JOBS query for tables of `datasets` (each `project.dataset`), read
/// through [`jobs_projection_sql`], or `None` when a name is not a plain
/// identifier (never interpolated).
#[must_use]
pub fn jobs_query(jobs_project: &str, cfg: &JobsConfig, datasets: &[String]) -> Option<String> {
    let projection = jobs_projection_sql(jobs_project, &cfg.region, cfg.lookback_days)?;
    writers_over(&format!("({projection})"), datasets)
}

/// The same query over a project's `observed_lineage` view in `ops_dataset`
/// (already the projection; only the lookback window is applied here).
#[must_use]
pub fn jobs_query_from_view(
    jobs_project: &str,
    ops_dataset: &str,
    cfg: &JobsConfig,
    datasets: &[String],
) -> Option<String> {
    if !valid_name(jobs_project) || !valid_name(ops_dataset) || ops_dataset.contains('.') {
        return None;
    }
    let days = cfg.lookback_days.clamp(1, 180);
    writers_over(
        &format!(
            "(SELECT * FROM `{jobs_project}`.`{ops_dataset}`.{OBSERVED_LINEAGE_VIEW} \
WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY))"
        ),
        datasets,
    )
}

/// Rank a table's writers: row-producing statements before UPDATEs, then
/// latest first; the first is the primary, up to [`MAX_WRITERS`] in all.
fn rank(mut ws: Vec<Producer>) -> Option<Producer> {
    ws.sort_by(|a, b| {
        (a.statement_type == "UPDATE", std::cmp::Reverse(a.last_ms))
            .cmp(&(b.statement_type == "UPDATE", std::cmp::Reverse(b.last_ms)))
    });
    // The same SQL from two job projects is one writer.
    let mut seen: Vec<String> = Vec::new();
    ws.retain(|w| {
        let fresh = !seen.contains(&w.query);
        seen.push(w.query.clone());
        fresh
    });
    ws.truncate(MAX_WRITERS);
    let mut it = ws.into_iter();
    let mut first = it.next()?;
    first.others = it.collect();
    Some(first)
}

/// Decode JOBS rows into producers (rows of every job project merged).
pub fn decode(batch: &Batch, into: &mut BTreeMap<String, Producer>) {
    let col = |name: &str| batch.names.iter().position(|n| n.eq_ignore_ascii_case(name));
    let (Some(p), Some(d), Some(t), Some(w), Some(u), Some(f), Some(l), Some(st), Some(q)) = (
        col("project"),
        col("dataset"),
        col("tbl"),
        col("writes"),
        col("writers"),
        col("first_ms"),
        col("last_ms"),
        col("statement_type"),
        col("query"),
    ) else {
        return;
    };
    let s = |c: usize, r: usize| match batch.columns[c].get(r) {
        Cell::Str(v) => v.to_owned(),
        _ => String::new(),
    };
    let n = |c: usize, r: usize| match batch.columns[c].get(r) {
        Cell::I64(v) => u64::try_from(v).unwrap_or(0),
        Cell::Str(v) => v.parse().unwrap_or(0),
        _ => 0,
    };
    for r in 0..batch.rows() {
        let table = format!("{}.{}.{}", s(p, r), s(d, r), s(t, r));
        let prod = Producer {
            table: table.clone(),
            statement_type: s(st, r),
            query: s(q, r),
            writes: n(w, r),
            writers: n(u, r),
            first_ms: n(f, r),
            last_ms: n(l, r),
            others: Vec::new(),
        };
        if prod.query.is_empty() {
            continue;
        }
        let mut ws = match into.remove(&table) {
            Some(mut prev) => {
                let mut v = std::mem::take(&mut prev.others);
                v.push(prev);
                v
            }
            None => Vec::new(),
        };
        ws.push(prod);
        if let Some(p) = rank(ws) {
            into.insert(table, p);
        }
    }
}

/// Discover producers for tables of `datasets` from job history. Errors are
/// returned per job project as warnings (a missing permission on one
/// project must not stop the run).
pub async fn discover<W: Warehouse>(
    w: &W,
    cfg: &JobsConfig,
    datasets: &[String],
) -> (BTreeMap<String, Producer>, Vec<String>) {
    let mut out = BTreeMap::new();
    let mut warnings = Vec::new();
    let opts = RunOpts {
        max_bytes_billed: Some(cfg.max_bytes_billed),
        max_rows: 100_000,
        ..RunOpts::default()
    };
    for jp in &cfg.job_projects {
        // The project's ops view first, when it has one: pre-pruned, and
        // the same columns.
        if let Some(sql) =
            cfg.ops_dataset.as_deref().and_then(|ops| jobs_query_from_view(jp, ops, cfg, datasets))
        {
            match w.run(&sql, opts).await {
                Ok(r) => {
                    decode(&r.batch, &mut out);
                    continue;
                }
                Err(e) => warnings.push(format!(
                    "job history view of {jp} unavailable ({e}); reading INFORMATION_SCHEMA.JOBS"
                )),
            }
        }
        let Some(sql) = jobs_query(jp, cfg, datasets) else {
            warnings.push(format!("job history of {jp}: invalid project, region or dataset name"));
            continue;
        };
        match w.run(&sql, opts).await {
            Ok(r) => decode(&r.batch, &mut out),
            Err(e) => warnings.push(format!(
                "job history of {jp} unavailable ({e}); producers of its tables are unknown"
            )),
        }
    }
    (out, warnings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::batch::ColType;

    #[test]
    fn query_shape_and_injection_guard() {
        let cfg = JobsConfig { lookback_days: 999, ..JobsConfig::new(vec![], "US") };
        let q =
            jobs_query("my-project", &cfg, &["my-project.mart".into(), "p2.raw".into()]).unwrap();
        assert!(q.contains("FROM `my-project`.`region-us`.INFORMATION_SCHEMA.JOBS"));
        assert!(q.contains("INTERVAL 180 DAY"), "clamped to the retention");
        assert!(q.contains(
            "(project = 'my-project' AND dataset = 'mart') OR (project = 'p2' AND dataset = 'raw')"
        ));
        // The projection is the query's source, byte for byte.
        let projection = jobs_projection_sql("my-project", "US", 999).unwrap();
        assert!(q.contains(&format!("FROM ({projection})")));
        assert!(projection.contains("'/locations/US/jobs/'"));
        assert!(
            !projection.contains("user_email AS")
                && projection.contains("FARM_FINGERPRINT(c.user_email) AS who")
        );
        assert!(jobs_projection_sql("p`x", "us", 1).is_none());
        assert!(jobs_projection_sql("p", "us.x", 1).is_none());
        let v =
            jobs_query_from_view("my-project", "ops", &cfg, &["my-project.mart".into()]).unwrap();
        assert!(v.contains("FROM (SELECT * FROM `my-project`.`ops`.observed_lineage WHERE ts >= "));
        assert!(jobs_query_from_view("p", "a.b", &cfg, &["p.d".into()]).is_none());
        assert_eq!(
            bigquery_job_key("p", "northamerica-northeast1", "job_1"),
            "//bigquery.googleapis.com/projects/p/locations/northamerica-northeast1/jobs/job_1"
        );
        let output = q.rsplit_once(") SELECT ").unwrap().1;
        assert!(
            !output.contains("who") && !output.contains("user_email"),
            "identities are only counted: {output}"
        );
        assert!(q.contains("COUNT(DISTINCT who) AS writers"));
        assert!(jobs_query("p", &cfg, &["p.x' OR 1=1 --".into()]).is_none());
        assert!(jobs_query("p", &cfg, &["nodot".into()]).is_none());
        assert!(jobs_query("p", &cfg, &[]).is_none());
    }

    #[test]
    fn decode_keeps_distinct_writers_latest_first_updates_last() {
        let schema = [
            ("project", ColType::String),
            ("dataset", ColType::String),
            ("tbl", ColType::String),
            ("writes", ColType::Int64),
            ("writers", ColType::Int64),
            ("first_ms", ColType::Int64),
            ("last_ms", ColType::Int64),
            ("statement_type", ColType::String),
            ("query", ColType::String),
        ];
        let mut b = Batch::new(&schema, 5);
        for (t, w, last, st, q) in [
            ("m", 30, 2_000, "MERGE", "MERGE ..."),
            ("m", 2, 9_000, "UPDATE", "UPDATE ..."),
            ("m", 5, 3_000, "INSERT", "INSERT ..."),
            ("m", 1, 500, "INSERT", "INSERT old ..."),
            ("m", 1, 400, "INSERT", "INSERT oldest ..."),
        ] {
            let row: [Cell<'_>; 9] = [
                Cell::Str("p"),
                Cell::Str("d"),
                Cell::Str(t),
                Cell::I64(w),
                Cell::I64(2),
                Cell::I64(1_000),
                Cell::I64(last),
                Cell::Str(st),
                Cell::Str(q),
            ];
            for (i, c) in row.into_iter().enumerate() {
                b.columns[i].push(c).unwrap();
            }
        }
        let mut out = BTreeMap::new();
        decode(&b, &mut out);
        let p = &out["p.d.m"];
        let order: Vec<(&str, u64)> =
            p.all().map(|w| (w.statement_type.as_str(), w.writes)).collect();
        // Row producers first, latest first; the UPDATE ranks after them
        // and falls outside the three kept.
        assert_eq!(order, vec![("INSERT", 5), ("MERGE", 30), ("INSERT", 1)]);
        assert_eq!(p.others[1].query, "INSERT old ...");
    }
}
