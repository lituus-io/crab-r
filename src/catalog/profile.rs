// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Column profiling, computed inside the warehouse.
//!
//! One aggregate query per table (wide tables are split) computes, for every
//! profilable column: NULLs, approximate distinct count, MIN / MAX, and — for
//! STRING / BOOL / INT64 columns — the most frequent values with counts
//! (`APPROX_TOP_COUNT`). Only these summaries leave the warehouse; no rows
//! are transferred. Cost is bounded three ways: tables that require a
//! partition filter are profiled over their most recent 30 days, tables
//! larger than [`ProfileConfig::sample_above_bytes`] are sampled with
//! `TABLESAMPLE SYSTEM`, and every query carries a byte cap (a table that
//! would exceed it is skipped, not billed). Tables are profiled concurrently.
//!
//! A value list shorter than the requested top-k is the column's complete
//! value set — which is what lets the engine reject a literal that cannot
//! occur (`status = 'complete'` when the data says `'Complete'`).

use crate::base::batch::{Batch, Cell};
use crate::base::json::{parse_u64, Event, Scanner};
use crate::catalog::image::{
    CatalogBuilder, ColumnSpec, Error, ProfileSpec, Result, TableKind, TableSpec,
};
use crate::catalog::types::TypeCode;
use crate::exec::{RunOpts, Warehouse};

/// Profiling settings.
#[derive(Clone, Debug)]
pub struct ProfileConfig {
    /// Most frequent values kept per column (a shorter list is complete).
    pub top_k: u32,
    /// Sample tables larger than this many logical bytes.
    pub sample_above_bytes: u64,
    /// Byte cap per profiling query.
    pub max_bytes_billed: u64,
    /// Tables profiled at once.
    pub concurrency: usize,
    /// Columns per query (wider tables are split).
    pub columns_per_query: usize,
    /// Also profile views (their cost is not known up front).
    pub include_views: bool,
    /// Profile only these datasets (`None`: all).
    pub datasets: Option<Vec<String>>,
    /// Profile only these tables, as `dataset.table` the way the builder
    /// holds them (`None`: all) — incremental runs skip unchanged tables.
    pub tables: Option<Vec<String>>,
}

impl Default for ProfileConfig {
    fn default() -> Self {
        Self {
            top_k: 200,
            sample_above_bytes: 2 << 30,
            max_bytes_billed: 5 << 30,
            concurrency: 8,
            columns_per_query: 150,
            include_views: false,
            datasets: None,
            tables: None,
        }
    }
}

/// What profiling did and cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProfileStats {
    /// Tables profiled.
    pub tables: u32,
    /// Columns profiled.
    pub columns: u32,
    /// Queries issued.
    pub queries: u32,
    /// Bytes billed.
    pub bytes_billed: u64,
    /// Tables skipped (too large for the cap, views, or failures).
    pub skipped: u32,
}

/// A column worth profiling, and how.
#[derive(Clone, Debug)]
struct Target {
    path: String,
    top: bool,
}

fn targets(t: &TableSpec) -> Vec<Target> {
    // Fields below an ARRAY cannot be aggregated without UNNEST: skip them.
    let arrays: Vec<String> = t
        .columns
        .iter()
        .filter(|c| TypeCode::of(&c.data_type) == TypeCode::Array)
        .map(|c| c.path.to_ascii_lowercase())
        .collect();
    t.columns
        .iter()
        .filter(|c| !c.hidden)
        .filter(|c| {
            let lc = c.path.to_ascii_lowercase();
            !arrays.iter().any(|a| lc == *a || lc.starts_with(&format!("{a}.")))
        })
        .filter_map(|c: &ColumnSpec| {
            let code = TypeCode::of(&c.data_type);
            let profilable = matches!(
                code,
                TypeCode::Int64
                    | TypeCode::Float64
                    | TypeCode::Numeric
                    | TypeCode::BigNumeric
                    | TypeCode::Bool
                    | TypeCode::String
                    | TypeCode::Bytes
                    | TypeCode::Date
                    | TypeCode::DateTime
                    | TypeCode::Time
                    | TypeCode::Timestamp
            );
            profilable.then(|| Target {
                path: c.path.clone(),
                top: matches!(code, TypeCode::String | TypeCode::Bool | TypeCode::Int64),
            })
        })
        .collect()
}

/// The recent-window filter for a table that requires a partition filter.
fn partition_filter(t: &TableSpec) -> Option<String> {
    if !t.require_partition_filter {
        return None;
    }
    let part = t.columns.iter().find(|c| c.partitioning)?;
    let col = crate::sql::emit::quote_path(&part.path);
    Some(match TypeCode::of(&part.data_type) {
        TypeCode::Date => format!("{col} >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"),
        TypeCode::DateTime => format!("{col} >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 30 DAY)"),
        TypeCode::Int64 => format!("{col} IS NOT NULL"),
        _ => format!("{col} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)"),
    })
}

/// The profiling queries for one table (one per column chunk).
#[must_use]
fn table_queries(
    project: &str,
    t: &TableSpec,
    cfg: &ProfileConfig,
) -> (Vec<(String, Vec<Target>)>, bool) {
    let cols = targets(t);
    let sample =
        (t.logical_bytes > cfg.sample_above_bytes && t.kind == TableKind::Table).then(|| {
            let pct =
                (cfg.sample_above_bytes as f64 / t.logical_bytes as f64 * 100.0).clamp(0.1, 100.0);
            format!(" TABLESAMPLE SYSTEM ({pct:.2} PERCENT)")
        });
    let filter = partition_filter(t).map(|f| format!(" WHERE {f}")).unwrap_or_default();
    let sampled = sample.is_some() || !filter.is_empty();
    // A builder holds other projects' datasets as `project.dataset`.
    let from = if t.dataset.contains('.') {
        format!("`{}.{}`{}{filter}", t.dataset, t.name, sample.unwrap_or_default())
    } else {
        format!("`{project}.{}.{}`{}{filter}", t.dataset, t.name, sample.unwrap_or_default())
    };
    let mut out = Vec::new();
    for chunk in cols.chunks(cfg.columns_per_query.max(1)) {
        let mut sel = vec!["COUNT(*) AS rows_".to_owned()];
        for (i, c) in chunk.iter().enumerate() {
            let q = crate::sql::emit::quote_path(&c.path);
            sel.push(format!("COUNTIF({q} IS NULL) AS c{i}_n"));
            sel.push(format!("APPROX_COUNT_DISTINCT({q}) AS c{i}_d"));
            sel.push(format!("CAST(MIN({q}) AS STRING) AS c{i}_lo"));
            sel.push(format!("CAST(MAX({q}) AS STRING) AS c{i}_hi"));
            if c.top {
                sel.push(format!(
                    "APPROX_TOP_COUNT(CAST({q} AS STRING), {}) AS c{i}_top",
                    cfg.top_k
                ));
            }
        }
        out.push((format!("SELECT {} FROM {from}", sel.join(", ")), chunk.to_vec()));
    }
    (out, sampled)
}

fn cell_u64(b: &Batch, name: &str) -> u64 {
    let Some(i) = b.names.iter().position(|n| n == name) else { return 0 };
    match b.columns[i].get(0) {
        Cell::I64(v) => v.max(0) as u64,
        Cell::Str(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn cell_str<'b>(b: &'b Batch, name: &str) -> Option<&'b str> {
    let i = b.names.iter().position(|n| n == name)?;
    match b.columns[i].get(0) {
        Cell::Str(s) => Some(s),
        _ => None,
    }
}

/// Parse `[{"value": "...", "count": 12}, ...]` (a rendered APPROX_TOP_COUNT).
fn parse_top(json: &str) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    let Ok(mut s) = Scanner::from_bytes(json.as_bytes()) else { return out };
    if s.expect().ok() != Some(Event::ArrayStart) {
        return out;
    }
    let mut buf = String::new();
    while let Ok(ev) = s.expect() {
        match ev {
            Event::ArrayEnd => break,
            Event::ObjectStart => {
                let (mut value, mut count) = (None, 0u64);
                while let Ok(ev) = s.expect() {
                    match ev {
                        Event::ObjectEnd => break,
                        Event::Key(k) => match (k.as_plain(), s.expect()) {
                            (Some("value"), Ok(Event::Str(v))) => {
                                value = v.decode(&mut buf).ok().map(str::to_owned);
                            }
                            (Some("count"), Ok(Event::Num(n))) => count = parse_u64(n).unwrap_or(0),
                            (Some("count"), Ok(Event::Str(n))) => {
                                count = n.raw().parse().unwrap_or(0);
                            }
                            (_, Ok(v)) => {
                                if s.skip(v).is_err() {
                                    return out;
                                }
                            }
                            (_, Err(_)) => return out,
                        },
                        _ => return out,
                    }
                }
                if let Some(v) = value {
                    out.push((v, count));
                }
            }
            _ => return out,
        }
    }
    out
}

/// Profile every table recorded in `builder` and attach the profiles.
pub async fn profile<W: Warehouse>(
    w: &W,
    builder: &mut CatalogBuilder,
    project: &str,
    cfg: &ProfileConfig,
) -> Result<ProfileStats> {
    let mut plans: Vec<(usize, String, Vec<Target>, bool)> = Vec::new();
    let mut stats = ProfileStats::default();
    for (ti, t) in builder.tables().iter().enumerate() {
        if cfg.datasets.as_ref().is_some_and(|ds| !ds.contains(&t.dataset)) {
            continue;
        }
        if cfg.tables.as_ref().is_some_and(|ts| {
            !ts.iter().any(|x| x.rsplit_once('.') == Some((t.dataset.as_str(), t.name.as_str())))
        }) {
            continue;
        }
        if matches!(t.kind, TableKind::View | TableKind::MaterializedView) && !cfg.include_views {
            stats.skipped += 1;
            continue;
        }
        let (qs, sampled) = table_queries(project, t, cfg);
        for (sql, targets) in qs {
            plans.push((ti, sql, targets, sampled));
        }
    }
    let opts = RunOpts {
        max_bytes_billed: Some(cfg.max_bytes_billed),
        max_rows: 1,
        timeout_ms: 120_000,
        use_cache: true,
    };
    let mut results: Vec<(
        usize,
        Vec<Target>,
        bool,
        crate::exec::Result<crate::exec::QueryResult>,
    )> = Vec::with_capacity(plans.len());
    for chunk in plans.chunks(cfg.concurrency.max(1)) {
        let futs = chunk.iter().map(|(ti, sql, targets, sampled)| async move {
            (*ti, targets.clone(), *sampled, w.run(sql, opts).await)
        });
        results.extend(objstore_r::join::join_all(futs).await);
    }
    let mut failed_tables = std::collections::HashSet::new();
    let mut profiled_tables = std::collections::HashSet::new();
    for (ti, targets, sampled, r) in results {
        stats.queries += 1;
        let r = match r {
            Ok(r) => r,
            Err(e)
                if e.is_sql_error()
                    || matches!(
                        e,
                        crate::exec::Error::BytesLimit { .. }
                            | crate::exec::Error::Denied { .. }
                            | crate::exec::Error::Unauthenticated { .. }
                    ) =>
            {
                failed_tables.insert(ti);
                continue;
            }
            Err(e) => return Err(Error::Sync(e)),
        };
        stats.bytes_billed += r.bytes_billed;
        let b = &r.batch;
        if b.rows() == 0 {
            continue;
        }
        let rows = cell_u64(b, "rows_");
        let (ds, name) = (builder.tables()[ti].dataset.clone(), builder.tables()[ti].name.clone());
        for (i, t) in targets.iter().enumerate() {
            let values = cell_str(b, &format!("c{i}_top")).map(parse_top).unwrap_or_default();
            let complete = t.top && (values.len() as u32) < cfg.top_k && !sampled;
            let spec = ProfileSpec {
                rows,
                nulls: cell_u64(b, &format!("c{i}_n")),
                distinct: cell_u64(b, &format!("c{i}_d")),
                min: cell_str(b, &format!("c{i}_lo")).map(str::to_owned),
                max: cell_str(b, &format!("c{i}_hi")).map(str::to_owned),
                values,
                values_complete: complete,
                sampled,
            };
            builder.set_profile(&ds, &name, &t.path, spec);
            stats.columns += 1;
        }
        profiled_tables.insert(ti);
    }
    stats.tables = profiled_tables.len() as u32;
    stats.skipped += failed_tables.difference(&profiled_tables).count() as u32;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(p: &str, t: &str, o: u32) -> ColumnSpec {
        ColumnSpec {
            path: p.into(),
            data_type: t.into(),
            ordinal: o,
            nullable: true,
            partitioning: false,
            hidden: false,
            clustering: None,
            description: None,
        }
    }

    fn table(bytes: u64, rpf: bool) -> TableSpec {
        TableSpec {
            dataset: "shop".into(),
            name: "orders".into(),
            kind: TableKind::Table,
            description: None,
            row_count: 0,
            logical_bytes: bytes,
            last_modified_ms: 0,
            require_partition_filter: rpf,
            columns: vec![
                col("status", "STRING", 1),
                col("amount", "FLOAT64", 2),
                ColumnSpec { partitioning: true, ..col("ts", "TIMESTAMP", 3) },
                col("items", "ARRAY<STRUCT<sku STRING>>", 4),
                col("items.sku", "STRING", 4),
                col("addr", "STRUCT<city STRING>", 5),
                col("addr.city", "STRING", 5),
                col("geo", "GEOGRAPHY", 6),
            ],
        }
    }

    #[test]
    fn one_query_with_the_right_aggregates() {
        let (qs, sampled) = table_queries("p", &table(1 << 20, false), &ProfileConfig::default());
        assert_eq!(qs.len(), 1);
        assert!(!sampled);
        let (sql, targets) = &qs[0];
        let paths: Vec<&str> = targets.iter().map(|t| t.path.as_str()).collect();
        assert_eq!(
            paths,
            ["status", "amount", "ts", "addr.city"],
            "arrays, structs and geography are skipped"
        );
        assert!(sql.starts_with("SELECT COUNT(*) AS rows_, COUNTIF(status IS NULL) AS c0_n, APPROX_COUNT_DISTINCT(status) AS c0_d"));
        assert!(sql.contains("APPROX_TOP_COUNT(CAST(status AS STRING), 200) AS c0_top"));
        assert!(!sql.contains("c1_top"), "no top values for FLOAT64");
        assert!(sql.contains("MIN(addr.city)"));
        let mut keyword = table(0, false);
        keyword.columns[0].path = "select".into();
        assert!(
            table_queries("p", &keyword, &ProfileConfig::default()).0[0]
                .0
                .contains("COUNTIF(`select` IS NULL)"),
            "keywords stay quoted"
        );
        assert!(sql.ends_with("FROM `p.shop.orders`"));
    }

    #[test]
    fn large_and_partition_filtered_tables_are_bounded() {
        let (qs, sampled) = table_queries("p", &table(20 << 30, true), &ProfileConfig::default());
        assert!(sampled);
        assert!(qs[0].0.ends_with("FROM `p.shop.orders` TABLESAMPLE SYSTEM (10.00 PERCENT) WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)"), "{}", qs[0].0);
    }

    #[test]
    fn wide_tables_are_split() {
        let mut t = table(0, false);
        t.columns = (0..7).map(|i| col(&format!("c{i}"), "INT64", i + 1)).collect();
        let cfg = ProfileConfig { columns_per_query: 3, ..ProfileConfig::default() };
        let (qs, _) = table_queries("p", &t, &cfg);
        assert_eq!(qs.iter().map(|(_, t)| t.len()).collect::<Vec<_>>(), [3, 3, 1]);
    }

    #[test]
    fn top_values_parse_from_rendered_structs() {
        let v = parse_top(
            r#"[{"value":"Complete","count":250},{"value":"Brasil \"BR\"","count":"12"},{"value":null,"count":3}]"#,
        );
        assert_eq!(v, [("Complete".to_owned(), 250), ("Brasil \"BR\"".to_owned(), 12)]);
        assert!(parse_top("not json").is_empty());
    }
}
