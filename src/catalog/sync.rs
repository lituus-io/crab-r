// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Build a catalog from BigQuery INFORMATION_SCHEMA in one concurrent pass.
//!
//! Six **region-wide** views are queried at once (one query per view covers
//! every dataset in the region, instead of one per dataset): `SCHEMATA`,
//! `TABLES`, `COLUMNS`, `COLUMN_FIELD_PATHS`, `TABLE_OPTIONS`,
//! `TABLE_STORAGE`. They run concurrently on the caller's task, so sync
//! latency is the slowest query, not the sum, and the whole sync bills about
//! six minimum-sized metadata queries. Only the columns the catalog needs are
//! selected (never `ddl`).
//!
//! Every identifier spliced into SQL is validated against BigQuery's naming
//! rules first: no injection surface.

use crate::base::batch::{Batch, Cell};
use crate::catalog::image::{CatalogBuilder, ColumnSpec, Error, Result, TableKind, TableSpec};
use crate::exec::{QueryResult, RunOpts, Warehouse};
use std::collections::HashMap;

/// What to sync.
#[derive(Clone, Debug)]
pub struct SyncConfig {
    /// Project whose datasets are catalogued (and which runs the queries).
    pub project: String,
    /// Region or multi-region (`US`, `EU`, `us-central1`).
    pub region: String,
    /// Restrict to these datasets (`None` = all in the region).
    pub datasets: Option<Vec<String>>,
    /// Byte cap per metadata query.
    pub max_bytes_billed: u64,
    /// Query each dataset's own INFORMATION_SCHEMA (required for projects
    /// you can read but not list, such as public datasets) instead of the
    /// region-wide views. Needs `datasets`.
    pub dataset_scoped: bool,
}

impl SyncConfig {
    /// All datasets of `project` in `region`.
    #[must_use]
    pub fn new(project: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            project: project.into(),
            region: region.into(),
            datasets: None,
            max_bytes_billed: 1 << 30,
            dataset_scoped: false,
        }
    }
}

/// What a sync cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncStats {
    /// Queries issued.
    pub queries: u32,
    /// Bytes billed in total.
    pub bytes_billed: u64,
    /// Tables catalogued.
    pub tables: u32,
    /// Columns and nested fields catalogued.
    pub columns: u32,
    /// Wall time, milliseconds (the slowest query).
    pub wall_ms: u64,
}

fn valid_project(p: &str) -> bool {
    !p.is_empty()
        && p.len() <= 128
        && p.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}

fn valid_region(r: &str) -> bool {
    !r.is_empty() && r.len() <= 64 && r.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

fn valid_dataset(d: &str) -> bool {
    !d.is_empty() && d.len() <= 1024 && d.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Dataset-scoped metadata queries (one `UNION ALL` per view across the
/// datasets); the first slot is empty — SCHEMATA is synthesised locally.
fn dataset_scoped_queries(cfg: &SyncConfig) -> Result<[String; 6]> {
    let ds = cfg
        .datasets
        .as_deref()
        .filter(|d| !d.is_empty())
        .ok_or(Error::Corrupt("dataset-scoped sync needs datasets"))?;
    if !valid_project(&cfg.project) || !ds.iter().all(|d| valid_dataset(d)) {
        return Err(Error::Corrupt("invalid project or dataset name"));
    }
    let union = |select: &str, view: &str, tail: &str| -> String {
        ds.iter()
            .map(|d| format!("SELECT {select} FROM `{}.{d}`.{view}{tail}", cfg.project))
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    };
    Ok([
        String::new(),
        union("table_schema, table_name, table_type", "INFORMATION_SCHEMA.TABLES", ""),
        union(
            "table_schema, table_name, column_name, ordinal_position, is_nullable, data_type, is_hidden, is_partitioning_column, clustering_ordinal_position",
            "INFORMATION_SCHEMA.COLUMNS",
            "",
        ),
        union("table_schema, table_name, column_name, field_path, data_type, description", "INFORMATION_SCHEMA.COLUMN_FIELD_PATHS", ""),
        union("table_schema, table_name, option_name, option_value", "INFORMATION_SCHEMA.TABLE_OPTIONS", " WHERE option_name IN ('description', 'require_partition_filter')"),
        union("dataset_id AS table_schema, table_id AS table_name, row_count AS total_rows, size_bytes AS total_logical_bytes, last_modified_time AS modified_ms", "__TABLES__", ""),
    ])
}

/// The six metadata queries for `cfg`.
pub fn sync_queries(cfg: &SyncConfig) -> Result<[String; 6]> {
    if cfg.dataset_scoped {
        return dataset_scoped_queries(cfg);
    }
    if !valid_project(&cfg.project) || !valid_region(&cfg.region) {
        return Err(Error::Corrupt("invalid project or region name"));
    }
    let scope = format!(
        "`{}`.`region-{}`.INFORMATION_SCHEMA",
        cfg.project,
        cfg.region.to_ascii_lowercase()
    );
    let filter = |col: &str| -> Result<String> {
        match &cfg.datasets {
            None => Ok(String::new()),
            Some(ds) => {
                if ds.is_empty() || !ds.iter().all(|d| valid_dataset(d)) {
                    return Err(Error::Corrupt("invalid dataset name"));
                }
                let list: Vec<String> = ds.iter().map(|d| format!("'{d}'")).collect();
                Ok(format!(" WHERE {col} IN ({})", list.join(", ")))
            }
        }
    };
    let f = filter("table_schema")?;
    let and = if f.is_empty() { " WHERE ".to_owned() } else { format!("{f} AND ") };
    Ok([
        format!("SELECT schema_name, location FROM {scope}.SCHEMATA{}", filter("schema_name")?),
        format!("SELECT table_schema, table_name, table_type FROM {scope}.TABLES{f}"),
        format!(
            "SELECT table_schema, table_name, column_name, ordinal_position, is_nullable, data_type, is_hidden, \
             is_partitioning_column, clustering_ordinal_position FROM {scope}.COLUMNS{f}"
        ),
        format!("SELECT table_schema, table_name, column_name, field_path, data_type, description FROM {scope}.COLUMN_FIELD_PATHS{f}"),
        format!(
            "SELECT table_schema, table_name, option_name, option_value FROM {scope}.TABLE_OPTIONS{and}\
             option_name IN ('description', 'require_partition_filter')"
        ),
        format!(
            "SELECT table_schema, table_name, total_rows, total_logical_bytes, \
             UNIX_MILLIS(storage_last_modified_time) AS modified_ms FROM {scope}.TABLE_STORAGE{f}"
        ),
    ])
}

/// Run the metadata queries on `w` and build the catalog.
pub async fn sync<W: Warehouse>(w: &W, cfg: &SyncConfig) -> Result<(CatalogBuilder, SyncStats)> {
    let queries = sync_queries(cfg)?;
    let opts = RunOpts {
        max_bytes_billed: Some(cfg.max_bytes_billed),
        max_rows: 5_000_000,
        timeout_ms: 120_000,
        use_cache: false,
    };
    let live: Vec<&String> = queries.iter().filter(|q| !q.is_empty()).collect();
    let results = objstore_r::join::join_all(live.iter().map(|q| w.run(q, opts))).await;
    let mut out: Vec<QueryResult> = Vec::with_capacity(6);
    if cfg.dataset_scoped {
        out.push(synth_schemata(cfg));
    }
    for r in results {
        out.push(r.map_err(Error::Sync)?);
    }
    let mut stats = SyncStats { queries: live.len() as u32, ..SyncStats::default() };
    for r in &out {
        stats.bytes_billed += r.bytes_billed;
        stats.wall_ms = stats.wall_ms.max(r.wall_ms);
    }
    let builder = assemble(
        &cfg.project,
        &out[0].batch,
        &out[1].batch,
        &out[2].batch,
        &out[3].batch,
        &out[4].batch,
        &out[5].batch,
    )?;
    stats.tables = builder.table_count() as u32;
    stats.columns = builder.column_count() as u32;
    Ok((builder, stats))
}

/// SCHEMATA for a dataset-scoped sync: the requested datasets, location
/// taken from the configured region.
fn synth_schemata(cfg: &SyncConfig) -> QueryResult {
    use crate::base::batch::ColType;
    let ds = cfg.datasets.as_deref().unwrap_or(&[]);
    let mut b =
        Batch::new(&[("schema_name", ColType::String), ("location", ColType::String)], ds.len());
    for d in ds {
        let _ = b.columns[0].push(Cell::Str(d));
        let _ = b.columns[1].push(Cell::Str(&cfg.region));
    }
    QueryResult {
        batch: b,
        total_rows: ds.len() as u64,
        truncated: false,
        bytes_processed: 0,
        bytes_billed: 0,
        cache_hit: true,
        wall_ms: 0,
    }
}

/// Column accessor by name.
struct Rows<'b> {
    b: &'b Batch,
}

impl<'b> Rows<'b> {
    fn col(&self, name: &str) -> Result<usize> {
        self.b
            .names
            .iter()
            .position(|n| n.eq_ignore_ascii_case(name))
            .ok_or(Error::Corrupt("metadata column missing"))
    }
    fn str(&self, c: usize, r: usize) -> Option<&'b str> {
        match self.b.columns[c].get(r) {
            Cell::Str(s) => Some(s),
            _ => None,
        }
    }
    fn int(&self, c: usize, r: usize) -> Option<i64> {
        match self.b.columns[c].get(r) {
            Cell::I64(v) => Some(v),
            Cell::Str(s) => s.parse().ok(),
            _ => None,
        }
    }
    fn yes(&self, c: usize, r: usize) -> bool {
        self.str(c, r).is_some_and(|s| s.eq_ignore_ascii_case("YES"))
    }
}

/// Undo GoogleSQL string-literal quoting in `TABLE_OPTIONS.option_value`
/// (`"text"` or `'text'` with backslash escapes).
fn unquote(v: &str) -> String {
    let t = v.trim();
    let inner = match t.as_bytes().first() {
        Some(b'"' | b'\'') if t.len() >= 2 && t.as_bytes()[0] == t.as_bytes()[t.len() - 1] => {
            &t[1..t.len() - 1]
        }
        _ => return t.to_owned(),
    };
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some(other) => out.push(other),
            None => {}
        }
    }
    out
}

type Key = (String, String);

/// Facts about a top-level column from `COLUMNS` (nested fields inherit).
#[allow(clippy::struct_excessive_bools)] // four independent INFORMATION_SCHEMA flags
struct Top {
    ordinal: u32,
    nullable: bool,
    hidden: bool,
    partitioning: bool,
    clustering: Option<u8>,
    data_type: String,
    name: String,
    seen_in_paths: bool,
}

/// Assemble a builder from the six result sets.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // one linear join of six result sets
fn assemble(
    project: &str,
    schemata: &Batch,
    tables: &Batch,
    columns: &Batch,
    paths: &Batch,
    options: &Batch,
    storage: &Batch,
) -> Result<CatalogBuilder> {
    let mut b = CatalogBuilder::new(project);
    let s = Rows { b: schemata };
    let (sn, sl) = (s.col("schema_name")?, s.col("location")?);
    for r in 0..schemata.rows() {
        if let Some(name) = s.str(sn, r) {
            b.add_dataset(name, s.str(sl, r).unwrap_or(""));
        }
    }

    let mut specs: HashMap<Key, TableSpec> = HashMap::new();
    let t = Rows { b: tables };
    let (ts, tn, tt) = (t.col("table_schema")?, t.col("table_name")?, t.col("table_type")?);
    for r in 0..tables.rows() {
        let (Some(ds), Some(name)) = (t.str(ts, r), t.str(tn, r)) else { continue };
        specs.insert(
            (ds.to_owned(), name.to_owned()),
            TableSpec {
                dataset: ds.to_owned(),
                name: name.to_owned(),
                kind: TableKind::from_table_type(t.str(tt, r).unwrap_or("")),
                description: None,
                row_count: 0,
                logical_bytes: 0,
                last_modified_ms: 0,
                require_partition_filter: false,
                columns: Vec::new(),
            },
        );
    }

    // Top-level column facts, keyed by (dataset, table, lower(column)).
    let mut tops: HashMap<(String, String, String), Top> = HashMap::new();
    let c = Rows { b: columns };
    let (cs, ct, cn, co, cnull, cty, ch, cp, cc) = (
        c.col("table_schema")?,
        c.col("table_name")?,
        c.col("column_name")?,
        c.col("ordinal_position")?,
        c.col("is_nullable")?,
        c.col("data_type")?,
        c.col("is_hidden")?,
        c.col("is_partitioning_column")?,
        c.col("clustering_ordinal_position")?,
    );
    for r in 0..columns.rows() {
        let (Some(ds), Some(tb), Some(name)) = (c.str(cs, r), c.str(ct, r), c.str(cn, r)) else {
            continue;
        };
        tops.insert(
            (ds.to_owned(), tb.to_owned(), name.to_ascii_lowercase()),
            Top {
                ordinal: c.int(co, r).unwrap_or(0).clamp(0, i64::from(u32::MAX)) as u32,
                nullable: c.yes(cnull, r),
                hidden: c.yes(ch, r),
                partitioning: c.yes(cp, r),
                clustering: c.int(cc, r).map(|v| v.clamp(1, 255) as u8),
                data_type: c.str(cty, r).unwrap_or("").to_owned(),
                name: name.to_owned(),
                seen_in_paths: false,
            },
        );
    }

    let p = Rows { b: paths };
    let (ps, pt, pc, pf, pty, pd) = (
        p.col("table_schema")?,
        p.col("table_name")?,
        p.col("column_name")?,
        p.col("field_path")?,
        p.col("data_type")?,
        p.col("description")?,
    );
    for r in 0..paths.rows() {
        let (Some(ds), Some(tb), Some(col), Some(path)) =
            (p.str(ps, r), p.str(pt, r), p.str(pc, r), p.str(pf, r))
        else {
            continue;
        };
        let Some(spec) = specs.get_mut(&(ds.to_owned(), tb.to_owned())) else { continue };
        let top = tops.get_mut(&(ds.to_owned(), tb.to_owned(), col.to_ascii_lowercase()));
        let is_top = path.eq_ignore_ascii_case(col);
        let (ordinal, nullable, hidden, partitioning, clustering) = match top {
            Some(t) => {
                if is_top {
                    t.seen_in_paths = true;
                }
                (
                    t.ordinal,
                    !is_top || t.nullable,
                    t.hidden,
                    is_top && t.partitioning,
                    if is_top { t.clustering } else { None },
                )
            }
            None => (u32::MAX, true, false, false, None),
        };
        spec.columns.push(ColumnSpec {
            path: path.to_owned(),
            data_type: p.str(pty, r).unwrap_or("").to_owned(),
            ordinal,
            nullable,
            partitioning,
            hidden,
            clustering,
            description: p.str(pd, r).filter(|d| !d.is_empty()).map(str::to_owned),
        });
    }
    // Columns with no field-path row (hidden pseudo-columns such as
    // `_PARTITIONTIME`).
    for ((ds, tb, _), top) in &tops {
        if top.seen_in_paths {
            continue;
        }
        if let Some(spec) = specs.get_mut(&(ds.clone(), tb.clone())) {
            spec.columns.push(ColumnSpec {
                path: top.name.clone(),
                data_type: top.data_type.clone(),
                ordinal: top.ordinal,
                nullable: top.nullable,
                partitioning: top.partitioning,
                hidden: top.hidden,
                clustering: top.clustering,
                description: None,
            });
        }
    }

    let o = Rows { b: options };
    let (os, ot, on, ov) = (
        o.col("table_schema")?,
        o.col("table_name")?,
        o.col("option_name")?,
        o.col("option_value")?,
    );
    for r in 0..options.rows() {
        let (Some(ds), Some(tb), Some(name), Some(val)) =
            (o.str(os, r), o.str(ot, r), o.str(on, r), o.str(ov, r))
        else {
            continue;
        };
        let Some(spec) = specs.get_mut(&(ds.to_owned(), tb.to_owned())) else { continue };
        match name {
            "description" => spec.description = Some(unquote(val)).filter(|d| !d.is_empty()),
            "require_partition_filter" => {
                spec.require_partition_filter = val.trim().eq_ignore_ascii_case("true");
            }
            _ => {}
        }
    }

    let st = Rows { b: storage };
    let (ss, stn, sr, sb, sm) = (
        st.col("table_schema")?,
        st.col("table_name")?,
        st.col("total_rows")?,
        st.col("total_logical_bytes")?,
        st.col("modified_ms")?,
    );
    for r in 0..storage.rows() {
        let (Some(ds), Some(tb)) = (st.str(ss, r), st.str(stn, r)) else { continue };
        let Some(spec) = specs.get_mut(&(ds.to_owned(), tb.to_owned())) else { continue };
        spec.row_count = st.int(sr, r).unwrap_or(0).max(0) as u64;
        spec.logical_bytes = st.int(sb, r).unwrap_or(0).max(0) as u64;
        spec.last_modified_ms = st.int(sm, r).unwrap_or(0).max(0) as u64;
    }

    for (_, spec) in specs {
        b.add_table(spec);
    }
    Ok(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::batch::ColType;
    use crate::catalog::image::Catalog;

    fn batch(cols: &[(&str, ColType)], rows: &[&[Option<&str>]]) -> Batch {
        let mut b = Batch::new(cols, rows.len());
        for row in rows {
            for (i, v) in row.iter().enumerate() {
                match (v, cols[i].1) {
                    (None, _) => b.columns[i].push_null(),
                    (Some(s), ColType::Int64) => {
                        b.columns[i].push(Cell::I64(s.parse().unwrap())).unwrap();
                    }
                    (Some(s), _) => b.columns[i].push(Cell::Str(s)).unwrap(),
                }
            }
        }
        b
    }

    #[test]
    fn queries_are_region_wide_and_validated() {
        let q = sync_queries(&SyncConfig::new("my-project", "US")).unwrap();
        assert!(q[2].contains("FROM `my-project`.`region-us`.INFORMATION_SCHEMA.COLUMNS"));
        assert!(!q.iter().any(|s| s.contains("ddl")), "never select the DDL text");
        let mut cfg = SyncConfig::new("p", "EU");
        cfg.datasets = Some(vec!["shop".into(), "fin".into()]);
        let q = sync_queries(&cfg).unwrap();
        assert!(q[1].ends_with("WHERE table_schema IN ('shop', 'fin')"));
        assert!(q[4].contains("WHERE table_schema IN ('shop', 'fin') AND option_name IN"));
        cfg.datasets = Some(vec!["x'; DROP".into()]);
        assert!(sync_queries(&cfg).is_err(), "injection-shaped names are refused");
        assert!(sync_queries(&SyncConfig::new("p`x", "US")).is_err());
    }

    #[test]
    #[allow(clippy::too_many_lines)] // one fixture, six result sets
    fn assembles_nested_fields_options_and_storage() {
        use ColType::{Int64 as I, String as S};
        let schemata =
            batch(&[("schema_name", S), ("location", S)], &[&[Some("shop"), Some("US")]]);
        let tables = batch(
            &[("table_schema", S), ("table_name", S), ("table_type", S)],
            &[&[Some("shop"), Some("orders"), Some("BASE TABLE")]],
        );
        let cols = [
            ("table_schema", S),
            ("table_name", S),
            ("column_name", S),
            ("ordinal_position", I),
            ("is_nullable", S),
            ("data_type", S),
            ("is_hidden", S),
            ("is_partitioning_column", S),
            ("clustering_ordinal_position", I),
        ];
        let columns = batch(
            &cols,
            &[
                &[
                    Some("shop"),
                    Some("orders"),
                    Some("order_id"),
                    Some("1"),
                    Some("NO"),
                    Some("INT64"),
                    Some("NO"),
                    Some("NO"),
                    None,
                ],
                &[
                    Some("shop"),
                    Some("orders"),
                    Some("order_ts"),
                    Some("2"),
                    Some("YES"),
                    Some("TIMESTAMP"),
                    Some("NO"),
                    Some("YES"),
                    None,
                ],
                &[
                    Some("shop"),
                    Some("orders"),
                    Some("items"),
                    Some("3"),
                    Some("YES"),
                    Some("ARRAY<STRUCT<sku STRING>>"),
                    Some("NO"),
                    Some("NO"),
                    Some("1"),
                ],
                &[
                    Some("shop"),
                    Some("orders"),
                    Some("_PARTITIONTIME"),
                    Some("4"),
                    Some("YES"),
                    Some("TIMESTAMP"),
                    Some("YES"),
                    Some("NO"),
                    None,
                ],
            ],
        );
        let pcols = [
            ("table_schema", S),
            ("table_name", S),
            ("column_name", S),
            ("field_path", S),
            ("data_type", S),
            ("description", S),
        ];
        let paths = batch(
            &pcols,
            &[
                &[
                    Some("shop"),
                    Some("orders"),
                    Some("order_id"),
                    Some("order_id"),
                    Some("INT64"),
                    Some("Primary key"),
                ],
                &[
                    Some("shop"),
                    Some("orders"),
                    Some("order_ts"),
                    Some("order_ts"),
                    Some("TIMESTAMP"),
                    None,
                ],
                &[
                    Some("shop"),
                    Some("orders"),
                    Some("items"),
                    Some("items"),
                    Some("ARRAY<STRUCT<sku STRING>>"),
                    None,
                ],
                &[
                    Some("shop"),
                    Some("orders"),
                    Some("items"),
                    Some("items.sku"),
                    Some("STRING"),
                    Some(""),
                ],
            ],
        );
        let options = batch(
            &[("table_schema", S), ("table_name", S), ("option_name", S), ("option_value", S)],
            &[
                &[
                    Some("shop"),
                    Some("orders"),
                    Some("description"),
                    Some(r#""One row per \"order\"""#),
                ],
                &[Some("shop"), Some("orders"), Some("require_partition_filter"), Some("true")],
            ],
        );
        let storage = batch(
            &[
                ("table_schema", S),
                ("table_name", S),
                ("total_rows", I),
                ("total_logical_bytes", I),
                ("modified_ms", I),
            ],
            &[&[Some("shop"), Some("orders"), Some("42"), Some("4096"), Some("1790000000000")]],
        );
        let img = assemble("p", &schemata, &tables, &columns, &paths, &options, &storage)
            .unwrap()
            .build(1, 0)
            .unwrap();
        let c = Catalog::from_bytes(objstore_r::Bytes::from(img)).unwrap();
        let t = c.table(c.find_table("shop", "orders").unwrap()).unwrap();
        assert_eq!(t.description, "One row per \"order\"");
        assert!(t.require_partition_filter);
        assert_eq!(
            (t.row_count, t.logical_bytes, t.last_modified_ms),
            (42, 4096, 1_790_000_000_000)
        );
        let paths: Vec<&str> = c.columns(t.id).map(|c| c.path).collect();
        assert_eq!(paths, ["order_id", "order_ts", "items", "items.sku", "_PARTITIONTIME"]);
        let pk = c.column(c.find_column(t.id, "order_id").unwrap()).unwrap();
        assert!(!pk.nullable());
        assert_eq!(pk.description, "Primary key");
        assert_eq!(c.column(t.partition_col.unwrap()).unwrap().path, "order_ts");
        let pt = c.column(c.find_column(t.id, "_partitiontime").unwrap()).unwrap();
        assert!(pt.hidden());
        let sku = c.column(c.find_column(t.id, "items.sku").unwrap()).unwrap();
        assert!(sku.in_array());
        assert_eq!(sku.description, "", "empty descriptions are dropped");
        assert_eq!(c.column(c.find_column(t.id, "items").unwrap()).unwrap().clustering, Some(1));
    }

    #[test]
    fn dataset_scoped_queries_union_per_dataset() {
        let mut cfg = SyncConfig::new("bigquery-public-data", "US");
        cfg.datasets = Some(vec!["thelook_ecommerce".into(), "samples".into()]);
        cfg.dataset_scoped = true;
        let q = sync_queries(&cfg).unwrap();
        assert!(q[0].is_empty(), "SCHEMATA is synthesised");
        assert!(q[2].contains("FROM `bigquery-public-data.thelook_ecommerce`.INFORMATION_SCHEMA.COLUMNS UNION ALL SELECT"));
        assert!(q[5].contains("`bigquery-public-data.samples`.__TABLES__"));
        cfg.datasets = None;
        assert!(sync_queries(&cfg).is_err());
    }

    #[test]
    fn unquoting_option_values() {
        assert_eq!(unquote(r#""a \"b\" \\ c\nd""#), "a \"b\" \\ c\nd");
        assert_eq!(unquote("'x'"), "x");
        assert_eq!(unquote("true"), "true");
    }
}
