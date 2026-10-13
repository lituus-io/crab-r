// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! The catalog image: build ([`CatalogBuilder`]) and read ([`Catalog`]).
//!
//! ```text
//! graph-r container (header, directory and section checksums; see
//! `graph_r::container`); the owner area holds generation, created_ms and
//! the dataset, table and column counts.
//! sections: STRINGS  string heap (deduplicated UTF-8)
//!           DATASETS 40 B records, sorted by name
//!           TABLES   72 B records, sorted by (dataset, name); a dataset's tables are contiguous
//!           COLUMNS  48 B records, grouped by table, parents before children
//!           TABLE_NAMES  (xxh3("dataset\0table"), table) sorted — name lookup
//!           TABLE_KEYS   (EntityKey, table) sorted — key lookup
//!           COLUMN_NAMES per table range: (xxh3(lower(path)), column) sorted — case-insensitive lookup
//!           COLUMN_KEYS  (EntityKey, column) sorted — key lookup
//!           PROFILES 56 B per column: rows, nulls, distinct, min, max, values
//! ```
//!
//! Everything is fixed-width little-endian, so reads are offset arithmetic
//! over the mapped bytes. [`Catalog::from_bytes`] / [`Catalog::open`] validate
//! the whole image once (checksum, bounds, UTF-8, every cross-reference);
//! after that no accessor can fail, panic or read out of range. Building is
//! deterministic: the same catalog content always produces the same bytes.

use crate::base::key::{column_key, dataset_key, table_key};
use crate::base::EntityKey;
use crate::catalog::types::TypeCode;
use graph_r::bytesio::{get_u32, get_u64, put_u32, put_u64};
use graph_r::hash::xxh3_64;
use graph_r::mmap::MappedFile;
use objstore_r::Bytes;
use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::path::Path;

/// The image's container identity: `"CRBC"`, version 3 (graph-r container).
const FORMAT: graph_r::container::Format =
    graph_r::container::Format { magic: 0x4342_5243, version: 3 };
const DATASET_LEN: usize = 40;
const TABLE_LEN: usize = 72;
const COLUMN_LEN: usize = 48;
const INDEX_LEN: usize = 16;
const PROFILE_LEN: usize = 56;
/// "No such id" in u32 id fields.
const NONE: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
enum Section {
    Strings = 1,
    Datasets = 2,
    Tables = 3,
    Columns = 4,
    TableNames = 5,
    TableKeys = 6,
    ColumnNames = 7,
    ColumnKeys = 8,
    Profiles = 9,
}

const SECTIONS: [Section; 9] = [
    Section::Strings,
    Section::Datasets,
    Section::Tables,
    Section::Columns,
    Section::TableNames,
    Section::TableKeys,
    Section::ColumnNames,
    Section::ColumnKeys,
    Section::Profiles,
];

/// Table flag: queries must filter on the partition column.
const TF_REQUIRE_PARTITION_FILTER: u8 = 1;
/// Column flags.
const CF_NULLABLE: u8 = 1;
const CF_REPEATED: u8 = 2;
const CF_IN_ARRAY: u8 = 4;
const CF_PARTITIONING: u8 = 8;
const CF_HIDDEN: u8 = 16;

/// Catalog errors.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The image is malformed or corrupt.
    Corrupt(&'static str),
    /// A file could not be read or mapped.
    Io(String),
    /// Too many entities for the format's u32 ids.
    TooLarge,
    /// Syncing from the warehouse failed.
    Sync(crate::exec::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corrupt(m) => write!(f, "corrupt catalog image: {m}"),
            Self::Io(m) => write!(f, "catalog i/o: {m}"),
            Self::TooLarge => f.write_str("catalog too large for the image format"),
            Self::Sync(e) => write!(f, "catalog sync: {e}"),
        }
    }
}

impl std::error::Error for Error {}

/// Result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What kind of relation a table is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
#[allow(missing_docs)]
pub enum TableKind {
    Table = 0,
    View = 1,
    MaterializedView = 2,
    External = 3,
    Snapshot = 4,
    Clone = 5,
    Other = 6,
}

impl TableKind {
    /// From INFORMATION_SCHEMA.TABLES.table_type.
    #[must_use]
    pub fn from_table_type(t: &str) -> Self {
        match t {
            "BASE TABLE" => Self::Table,
            "VIEW" => Self::View,
            "MATERIALIZED VIEW" => Self::MaterializedView,
            "EXTERNAL" => Self::External,
            "SNAPSHOT" => Self::Snapshot,
            "CLONE" => Self::Clone,
            _ => Self::Other,
        }
    }

    fn from_tag(t: u8) -> Self {
        match t {
            0 => Self::Table,
            1 => Self::View,
            2 => Self::MaterializedView,
            3 => Self::External,
            4 => Self::Snapshot,
            5 => Self::Clone,
            _ => Self::Other,
        }
    }
}

// ---------------------------------------------------------------------------
// Build side
// ---------------------------------------------------------------------------

/// A column (or nested field) to record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    /// Dotted field path (`addr.city`); a top-level column has no dot.
    pub path: String,
    /// BigQuery type text.
    pub data_type: String,
    /// Ordinal of the top-level column (nested fields share their root's).
    pub ordinal: u32,
    /// Nullable.
    pub nullable: bool,
    /// A partitioning column.
    pub partitioning: bool,
    /// A hidden pseudo-column (`_PARTITIONTIME`, ...).
    pub hidden: bool,
    /// Clustering position (1-based), if clustered.
    pub clustering: Option<u8>,
    /// Description.
    pub description: Option<String>,
}

/// A table to record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSpec {
    /// Dataset name.
    pub dataset: String,
    /// Table name.
    pub name: String,
    /// Kind.
    pub kind: TableKind,
    /// Description.
    pub description: Option<String>,
    /// Row count (0 when unknown).
    pub row_count: u64,
    /// Logical bytes (0 when unknown).
    pub logical_bytes: u64,
    /// Last storage modification, Unix ms (0 when unknown).
    pub last_modified_ms: u64,
    /// `require_partition_filter`.
    pub require_partition_filter: bool,
    /// Columns and nested fields.
    pub columns: Vec<ColumnSpec>,
}

/// Value statistics of one column, computed in the warehouse.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProfileSpec {
    /// Rows profiled (after sampling).
    pub rows: u64,
    /// NULLs among them.
    pub nulls: u64,
    /// Approximate distinct values.
    pub distinct: u64,
    /// Minimum, as text.
    pub min: Option<String>,
    /// Maximum, as text.
    pub max: Option<String>,
    /// Most frequent values with counts, most frequent first.
    pub values: Vec<(String, u64)>,
    /// `values` is the column's complete value set.
    pub values_complete: bool,
    /// The profile was computed on a sample.
    pub sampled: bool,
}

/// Accumulates a project's datasets and tables, then renders an image.
#[derive(Clone, Debug, Default)]
pub struct CatalogBuilder {
    project: String,
    datasets: Vec<(String, String)>,
    tables: Vec<TableSpec>,
    profiles: HashMap<(String, String, String), ProfileSpec>,
}

struct Heap {
    bytes: Vec<u8>,
    seen: HashMap<String, (u32, u32)>,
}

impl Heap {
    fn put(&mut self, s: &str) -> Result<(u32, u32)> {
        if s.is_empty() {
            return Ok((0, 0));
        }
        if let Some(&r) = self.seen.get(s) {
            return Ok(r);
        }
        let off = u32::try_from(self.bytes.len()).map_err(|_| Error::TooLarge)?;
        let len = u32::try_from(s.len()).map_err(|_| Error::TooLarge)?;
        self.bytes.extend_from_slice(s.as_bytes());
        self.seen.insert(s.to_owned(), (off, len));
        Ok((off, len))
    }
}

fn put_ref(out: &mut Vec<u8>, r: (u32, u32)) {
    put_u32(out, r.0);
    put_u32(out, r.1);
}

/// xxh3 of a column path, ASCII-lower-cased (BigQuery column names are
/// case-insensitive).
fn path_hash(path: &str) -> u64 {
    let mut buf = [0u8; 256];
    if path.len() <= buf.len() {
        for (d, s) in buf.iter_mut().zip(path.bytes()) {
            *d = s.to_ascii_lowercase();
        }
        xxh3_64(&buf[..path.len()])
    } else {
        xxh3_64(path.to_ascii_lowercase().as_bytes())
    }
}

fn table_name_hash(dataset: &str, table: &str) -> u64 {
    let mut h = graph_r::hash::Hasher::new();
    h.update(dataset.as_bytes());
    h.update(&[0]);
    h.update(table.as_bytes());
    h.digest64()
}

impl CatalogBuilder {
    /// A builder for `project`.
    #[must_use]
    pub fn new(project: impl Into<String>) -> Self {
        Self { project: project.into(), ..Self::default() }
    }

    /// Record a dataset and its location.
    pub fn add_dataset(&mut self, name: impl Into<String>, location: impl Into<String>) {
        self.datasets.push((name.into(), location.into()));
    }

    /// Record a table.
    pub fn add_table(&mut self, t: TableSpec) {
        self.tables.push(t);
    }

    /// Replace the description of `project.dataset.table` (`path: None`) or
    /// of its column or nested field `path` (case-insensitive). Returns
    /// whether it exists. Used to overlay verified metadata (for example a
    /// describe report) onto a synced catalog before it is built.
    pub fn set_description(
        &mut self,
        project: &str,
        dataset: &str,
        table: &str,
        path: Option<&str>,
        text: &str,
    ) -> bool {
        let home = project == self.project;
        let Some(t) = self.tables.iter_mut().find(|t| {
            t.name == table
                && (if home {
                    t.dataset == dataset
                } else {
                    t.dataset.len() == project.len() + 1 + dataset.len()
                        && t.dataset.starts_with(project)
                        && t.dataset.ends_with(dataset)
                        && t.dataset.as_bytes()[project.len()] == b'.'
                })
        }) else {
            return false;
        };
        let text = Some(text.to_owned()).filter(|d| !d.trim().is_empty());
        match path {
            None => {
                t.description = text;
                true
            }
            Some(p) => match t.columns.iter_mut().find(|c| c.path.eq_ignore_ascii_case(p)) {
                Some(c) => {
                    c.description = text;
                    true
                }
                None => false,
            },
        }
    }

    /// Attach a profile to `dataset.table`'s column `path` (case-insensitive).
    pub fn set_profile(&mut self, dataset: &str, table: &str, path: &str, p: ProfileSpec) {
        self.profiles.insert((dataset.to_owned(), table.to_owned(), path.to_ascii_lowercase()), p);
    }

    /// This builder's project.
    #[must_use]
    pub fn project(&self) -> &str {
        &self.project
    }

    /// Move everything `other` recorded into this builder. Datasets of
    /// another project are qualified (`project.dataset`), so one catalog can
    /// hold tables of several projects (and cross-project SQL binds).
    pub fn absorb(&mut self, other: CatalogBuilder) {
        let qualify = |ds: &str| {
            if other.project == self.project || ds.contains('.') {
                ds.to_owned()
            } else {
                format!("{}.{ds}", other.project)
            }
        };
        for (d, loc) in &other.datasets {
            let q = qualify(d);
            if !self.datasets.iter().any(|(x, _)| *x == q) {
                self.datasets.push((q, loc.clone()));
            }
        }
        for mut t in other.tables {
            t.dataset = qualify(&t.dataset);
            self.tables.push(t);
        }
        for ((d, t, c), p) in other.profiles {
            self.profiles.insert((qualify(&d), t, c), p);
        }
    }

    /// The tables recorded so far (for profilers deciding what to scan).
    #[must_use]
    pub fn tables(&self) -> &[TableSpec] {
        &self.tables
    }

    /// Number of tables recorded.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Number of columns and nested fields recorded.
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.tables.iter().map(|t| t.columns.len()).sum()
    }

    /// Render the image. `generation` and `created_ms` go in the header.
    #[allow(clippy::too_many_lines, clippy::similar_names)] // one linear layout pass
    pub fn build(mut self, generation: u64, created_ms: u64) -> Result<Vec<u8>> {
        // Datasets: every table's dataset exists; deterministic order.
        for t in &self.tables {
            if !self.datasets.iter().any(|(d, _)| *d == t.dataset) {
                self.datasets.push((t.dataset.clone(), String::new()));
            }
        }
        self.datasets.sort();
        self.datasets.dedup_by(|a, b| a.0 == b.0);
        self.tables.sort_by(|a, b| (&a.dataset, &a.name).cmp(&(&b.dataset, &b.name)));
        self.tables.dedup_by(|a, b| a.dataset == b.dataset && a.name == b.name);
        for t in &mut self.tables {
            t.columns.sort_by(|a, b| (a.ordinal, &a.path).cmp(&(b.ordinal, &b.path)));
            t.columns.dedup_by(|a, b| a.path.eq_ignore_ascii_case(&b.path));
        }
        let n_ds = u32::try_from(self.datasets.len()).map_err(|_| Error::TooLarge)?;
        let n_tables = u32::try_from(self.tables.len()).map_err(|_| Error::TooLarge)?;
        let n_cols_total: usize = self.tables.iter().map(|t| t.columns.len()).sum();
        let n_cols = u32::try_from(n_cols_total).map_err(|_| Error::TooLarge)?;

        let mut heap = Heap { bytes: Vec::new(), seen: HashMap::new() };

        let mut datasets = Vec::with_capacity(self.datasets.len() * DATASET_LEN);
        let mut tables = Vec::with_capacity(self.tables.len() * TABLE_LEN);
        let mut columns = Vec::with_capacity(n_cols_total * COLUMN_LEN);
        let mut table_names = Vec::with_capacity(self.tables.len());
        let mut table_keys = Vec::with_capacity(self.tables.len());
        let mut column_names = Vec::with_capacity(n_cols_total * INDEX_LEN);
        let mut column_keys = Vec::with_capacity(n_cols_total);
        let mut profiles = Vec::with_capacity(n_cols_total * PROFILE_LEN);

        let mut ti = 0usize;
        let mut ci = 0u32;
        for (di, (qualified, loc)) in self.datasets.iter().enumerate() {
            // A dataset of another project is recorded as `project.dataset`
            // (dataset names never contain dots; project ids may).
            let (proj, ds) = match qualified.rsplit_once('.') {
                Some((p, d)) => (p, d),
                None => (self.project.as_str(), qualified.as_str()),
            };
            let project = heap.put(proj)?;
            let table_start = ti as u32;
            while ti < self.tables.len() && self.tables[ti].dataset == *qualified {
                let t = &self.tables[ti];
                let tkey = table_key(proj, ds, &t.name);
                let col_start = ci;
                // Parent resolution within this table (paths are unique, parents first).
                let mut by_path: HashMap<String, u32> = HashMap::with_capacity(t.columns.len());
                let mut partition_col = NONE;
                let mut entries: Vec<(u64, u32)> = Vec::with_capacity(t.columns.len());
                for c in &t.columns {
                    let lc = c.path.to_ascii_lowercase();
                    let (parent, depth) = match lc.rfind('.') {
                        Some(i) => (
                            by_path.get(&lc[..i]).copied().unwrap_or(NONE),
                            lc.matches('.').count().min(255) as u8,
                        ),
                        None => (NONE, 0),
                    };
                    let code = TypeCode::of(&c.data_type);
                    let parent_flags =
                        if parent == NONE { 0 } else { get_col_flags(&columns, parent) };
                    let mut flags = 0u8;
                    if c.nullable {
                        flags |= CF_NULLABLE;
                    }
                    if code == TypeCode::Array {
                        flags |= CF_REPEATED;
                    }
                    if parent_flags & (CF_REPEATED | CF_IN_ARRAY) != 0 {
                        flags |= CF_IN_ARRAY;
                    }
                    if c.partitioning {
                        flags |= CF_PARTITIONING;
                        if partition_col == NONE {
                            partition_col = ci;
                        }
                    }
                    if c.hidden {
                        flags |= CF_HIDDEN;
                    }
                    let ckey = column_key(proj, ds, &t.name, &c.path);
                    put_u64(&mut columns, ckey.raw());
                    put_u32(&mut columns, ti as u32);
                    put_u32(&mut columns, parent);
                    put_ref(&mut columns, heap.put(&c.path)?);
                    put_ref(&mut columns, heap.put(&c.data_type)?);
                    put_ref(&mut columns, heap.put(c.description.as_deref().unwrap_or(""))?);
                    columns.extend_from_slice(&[
                        code as u8,
                        flags,
                        c.clustering.unwrap_or(0),
                        depth,
                    ]);
                    put_u32(&mut columns, c.ordinal);
                    entries.push((path_hash(&c.path), ci));
                    column_keys.push((ckey.raw(), ci));
                    let prof = self.profiles.get(&(
                        qualified.clone(),
                        t.name.clone(),
                        c.path.to_ascii_lowercase(),
                    ));
                    write_profile(&mut profiles, &mut heap, prof)?;
                    by_path.insert(lc, ci);
                    ci += 1;
                }
                entries.sort_unstable();
                for (h, id) in entries {
                    put_u64(&mut column_names, h);
                    put_u32(&mut column_names, id);
                    put_u32(&mut column_names, 0);
                }
                put_u64(&mut tables, tkey.raw());
                put_u32(&mut tables, di as u32);
                tables.extend_from_slice(&[
                    t.kind as u8,
                    if t.require_partition_filter { TF_REQUIRE_PARTITION_FILTER } else { 0 },
                    0,
                    0,
                ]);
                put_ref(&mut tables, heap.put(&t.name)?);
                put_ref(&mut tables, heap.put(t.description.as_deref().unwrap_or(""))?);
                put_u32(&mut tables, col_start);
                put_u32(&mut tables, ci - col_start);
                put_u64(&mut tables, t.row_count);
                put_u64(&mut tables, t.logical_bytes);
                put_u64(&mut tables, t.last_modified_ms);
                put_u32(&mut tables, partition_col);
                put_u32(&mut tables, 0);
                table_names.push((table_name_hash(ds, &t.name), ti as u32));
                table_keys.push((tkey.raw(), ti as u32));
                ti += 1;
            }
            put_u64(&mut datasets, dataset_key(proj, ds).raw());
            put_ref(&mut datasets, project);
            put_ref(&mut datasets, heap.put(ds)?);
            put_ref(&mut datasets, heap.put(loc)?);
            put_u32(&mut datasets, table_start);
            put_u32(&mut datasets, ti as u32 - table_start);
        }

        let index = |mut v: Vec<(u64, u32)>| {
            v.sort_unstable();
            let mut out = Vec::with_capacity(v.len() * INDEX_LEN);
            for (k, id) in v {
                put_u64(&mut out, k);
                put_u32(&mut out, id);
                put_u32(&mut out, 0);
            }
            out
        };
        let bodies: [Vec<u8>; 9] = [
            heap.bytes,
            datasets,
            tables,
            columns,
            index(table_names),
            index(table_keys),
            column_names,
            index(column_keys),
            profiles,
        ];

        let mut owner = Vec::with_capacity(28);
        put_u64(&mut owner, generation);
        put_u64(&mut owner, created_ms);
        put_u32(&mut owner, n_ds);
        put_u32(&mut owner, n_tables);
        put_u32(&mut owner, n_cols);
        let mut w = graph_r::container::Writer::new(FORMAT);
        w.owner(&owner);
        for (kind, body) in SECTIONS.iter().zip(bodies) {
            w.section(*kind as u16, body);
        }
        let out = w.finish();
        Ok(out)
    }
}

/// Separator between values, and between a value and its count, in the
/// encoded value list (control characters never kept in values).
const VALUE_SEP: char = '\u{1f}';
const COUNT_SEP: char = '\u{1e}';
const PF_PRESENT: u8 = 1;
const PF_COMPLETE: u8 = 2;
const PF_SAMPLED: u8 = 4;

fn write_profile(out: &mut Vec<u8>, heap: &mut Heap, p: Option<&ProfileSpec>) -> Result<()> {
    let Some(p) = p else {
        out.extend_from_slice(&[0u8; PROFILE_LEN]);
        return Ok(());
    };
    let mut enc = String::new();
    for (v, n) in &p.values {
        if v.chars().any(char::is_control) {
            continue; // separators must stay unambiguous
        }
        if !enc.is_empty() {
            enc.push(VALUE_SEP);
        }
        enc.push_str(v);
        enc.push(COUNT_SEP);
        enc.push_str(&n.to_string());
    }
    put_u64(out, p.rows);
    put_u64(out, p.nulls);
    put_u64(out, p.distinct);
    put_ref(out, heap.put(p.min.as_deref().unwrap_or(""))?);
    put_ref(out, heap.put(p.max.as_deref().unwrap_or(""))?);
    put_ref(out, heap.put(&enc)?);
    let mut flags = PF_PRESENT;
    if p.values_complete {
        flags |= PF_COMPLETE;
    }
    if p.sampled {
        flags |= PF_SAMPLED;
    }
    out.extend_from_slice(&[flags, 0, 0, 0, 0, 0, 0, 0]);
    Ok(())
}

fn get_col_flags(columns: &[u8], id: u32) -> u8 {
    columns.get(id as usize * COLUMN_LEN + 41).copied().unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Read side
// ---------------------------------------------------------------------------

/// Dense table id within one image.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableId(pub u32);

/// Dense column id within one image.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColId(pub u32);

/// A validated, read-only catalog image. Cheap to share by reference; every
/// accessor borrows from it.
pub struct Catalog {
    backing: MappedFile,
    sec: [Range<usize>; 9],
    generation: u64,
    created_ms: u64,
    n_ds: u32,
    n_tables: u32,
    n_cols: u32,
}

impl fmt::Debug for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Catalog")
            .field("generation", &self.generation)
            .field("datasets", &self.n_ds)
            .field("tables", &self.n_tables)
            .field("columns", &self.n_cols)
            .finish_non_exhaustive()
    }
}

/// A column's value profile (borrowed from the image).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Profile<'c> {
    /// Rows profiled.
    pub rows: u64,
    /// NULLs.
    pub nulls: u64,
    /// Approximate distinct values.
    pub distinct: u64,
    /// Minimum, as text.
    pub min: Option<&'c str>,
    /// Maximum, as text.
    pub max: Option<&'c str>,
    encoded: &'c str,
    /// The value list is the complete value set.
    pub values_complete: bool,
    /// Computed on a sample.
    pub sampled: bool,
}

impl<'c> Profile<'c> {
    /// Values with their counts, most frequent first.
    pub fn values(&self) -> impl Iterator<Item = (&'c str, u64)> + 'c {
        let enc = self.encoded;
        enc.split(VALUE_SEP).filter(|s| !s.is_empty()).filter_map(|item| {
            let (v, n) = item.rsplit_once(COUNT_SEP)?;
            Some((v, n.parse().unwrap_or(0)))
        })
    }

    /// NULL fraction (0 when no rows).
    #[must_use]
    pub fn null_fraction(&self) -> f64 {
        if self.rows == 0 {
            0.0
        } else {
            self.nulls as f64 / self.rows as f64
        }
    }
}

/// A dataset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetRef<'c> {
    /// Durable key.
    pub key: EntityKey,
    /// Project.
    pub project: &'c str,
    /// Dataset name.
    pub name: &'c str,
    /// Location (may be empty).
    pub location: &'c str,
    /// Its tables.
    pub tables: Range<u32>,
}

/// A table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableRef<'c> {
    /// Id.
    pub id: TableId,
    /// Durable key.
    pub key: EntityKey,
    /// Dataset index.
    pub dataset: u32,
    /// Dataset name.
    pub dataset_name: &'c str,
    /// Project.
    pub project: &'c str,
    /// Table name.
    pub name: &'c str,
    /// Kind.
    pub kind: TableKind,
    /// Description (may be empty).
    pub description: &'c str,
    /// Row count (0 when unknown).
    pub row_count: u64,
    /// Logical bytes (0 when unknown).
    pub logical_bytes: u64,
    /// Last storage modification, Unix ms.
    pub last_modified_ms: u64,
    /// Partitioning column.
    pub partition_col: Option<ColId>,
    /// Queries must filter on the partition column.
    pub require_partition_filter: bool,
    /// Column id range.
    pub columns: Range<u32>,
}

/// A column or nested field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnRef<'c> {
    /// Id.
    pub id: ColId,
    /// Durable key.
    pub key: EntityKey,
    /// Owning table.
    pub table: TableId,
    /// Parent field for nested paths.
    pub parent: Option<ColId>,
    /// Dotted path as declared.
    pub path: &'c str,
    /// BigQuery type text.
    pub data_type: &'c str,
    /// Type class.
    pub code: TypeCode,
    /// Description (may be empty).
    pub description: &'c str,
    /// Nesting depth (0 = top-level).
    pub depth: u8,
    /// Top-level ordinal.
    pub ordinal: u32,
    /// Clustering position (1-based).
    pub clustering: Option<u8>,
    flags: u8,
}

impl<'c> ColumnRef<'c> {
    /// The last path segment.
    #[must_use]
    pub fn leaf(&self) -> &'c str {
        self.path.rsplit('.').next().unwrap_or(self.path)
    }
    /// Nullable.
    #[must_use]
    pub fn nullable(&self) -> bool {
        self.flags & CF_NULLABLE != 0
    }
    /// Its own type is an ARRAY.
    #[must_use]
    pub fn repeated(&self) -> bool {
        self.flags & CF_REPEATED != 0
    }
    /// Nested inside an ARRAY (needs UNNEST to reach).
    #[must_use]
    pub fn in_array(&self) -> bool {
        self.flags & CF_IN_ARRAY != 0
    }
    /// A partitioning column.
    #[must_use]
    pub fn partitioning(&self) -> bool {
        self.flags & CF_PARTITIONING != 0
    }
    /// A hidden pseudo-column.
    #[must_use]
    pub fn hidden(&self) -> bool {
        self.flags & CF_HIDDEN != 0
    }
}

fn corrupt<T>(m: &'static str) -> Result<T> {
    Err(Error::Corrupt(m))
}

impl Catalog {
    /// Validate and serve an image held in memory (e.g. fetched from an
    /// object store): zero-copy, no file.
    pub fn from_bytes(bytes: Bytes) -> Result<Self> {
        Self::validate(MappedFile::from_bytes(bytes))
    }

    /// Validate and serve an image file through a read-only memory map
    /// (shared page cache across processes).
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let m = MappedFile::open(path.as_ref()).map_err(|e| Error::Io(e.to_string()))?;
        Self::validate(m)
    }

    #[allow(clippy::too_many_lines)] // one validation pass over every record
    fn validate(backing: MappedFile) -> Result<Self> {
        let b = backing.bytes();
        let dir = graph_r::container::Dir::parse(b, FORMAT).map_err(|e| match e {
            graph_r::Error::Format { message } if message.starts_with("unsupported version") => {
                Error::Corrupt("unsupported version")
            }
            _ => Error::Corrupt("bad container (magic, checksum or bounds)"),
        })?;
        let o = dir.owner();
        let (generation, created_ms) = (get_u64(o, 0), get_u64(o, 8));
        let (n_ds, n_tables, n_cols) = (get_u32(o, 16), get_u32(o, 20), get_u32(o, 24));
        let sec: [Range<usize>; 9] = std::array::from_fn(|i| dir.span(SECTIONS[i] as u16));
        let expect = |i: usize, rec: usize, n: u32| sec[i].len() == rec * n as usize;
        if !(expect(1, DATASET_LEN, n_ds)
            && expect(2, TABLE_LEN, n_tables)
            && expect(3, COLUMN_LEN, n_cols)
            && expect(4, INDEX_LEN, n_tables)
            && expect(5, INDEX_LEN, n_tables)
            && expect(6, INDEX_LEN, n_cols)
            && expect(7, INDEX_LEN, n_cols)
            && expect(8, PROFILE_LEN, n_cols))
        {
            return corrupt("section sizes do not match counts");
        }
        let heap = std::str::from_utf8(&b[sec[0].clone()])
            .map_err(|_| Error::Corrupt("string heap is not UTF-8"))?;
        let check_ref = |rec: &[u8], at: usize| -> Result<()> {
            let (off, len) = (get_u32(rec, at) as usize, get_u32(rec, at + 4) as usize);
            let end = off.checked_add(len).ok_or(Error::Corrupt("string ref overflow"))?;
            if end > heap.len() || !heap.is_char_boundary(off) || !heap.is_char_boundary(end) {
                return corrupt("bad string ref");
            }
            Ok(())
        };
        let ds = &b[sec[1].clone()];
        let mut next_table = 0u32;
        for r in ds.chunks_exact(DATASET_LEN) {
            for at in [8, 16, 24] {
                check_ref(r, at)?;
            }
            let (start, count) = (get_u32(r, 32), get_u32(r, 36));
            if start != next_table || start.checked_add(count).is_none_or(|e| e > n_tables) {
                return corrupt("dataset table range");
            }
            next_table = start + count;
        }
        if next_table != n_tables {
            return corrupt("tables not covered by datasets");
        }
        let tb = &b[sec[2].clone()];
        let mut next_col = 0u32;
        for r in tb.chunks_exact(TABLE_LEN) {
            if get_u32(r, 8) >= n_ds {
                return corrupt("table dataset id");
            }
            check_ref(r, 16)?;
            check_ref(r, 24)?;
            let (start, count) = (get_u32(r, 32), get_u32(r, 36));
            if start != next_col || start.checked_add(count).is_none_or(|e| e > n_cols) {
                return corrupt("table column range");
            }
            let part = get_u32(r, 64);
            if part != NONE && !(start..start + count).contains(&part) {
                return corrupt("partition column outside table");
            }
            next_col = start + count;
        }
        if next_col != n_cols {
            return corrupt("columns not covered by tables");
        }
        let cl = &b[sec[3].clone()];
        for (ci, r) in cl.chunks_exact(COLUMN_LEN).enumerate() {
            let t = get_u32(r, 8);
            if t >= n_tables {
                return corrupt("column table id");
            }
            let tr = &tb[t as usize * TABLE_LEN..(t as usize + 1) * TABLE_LEN];
            let range = get_u32(tr, 32)..get_u32(tr, 32) + get_u32(tr, 36);
            if !range.contains(&(ci as u32)) {
                return corrupt("column outside its table range");
            }
            let parent = get_u32(r, 12);
            if parent != NONE && !(range.start..ci as u32).contains(&parent) {
                return corrupt("column parent");
            }
            for at in [16, 24, 32] {
                check_ref(r, at)?;
            }
        }
        let check_index = |i: usize, n: u32| -> Result<()> {
            for r in b[sec[i].clone()].chunks_exact(INDEX_LEN) {
                if get_u32(r, 8) >= n {
                    return corrupt("index id out of range");
                }
            }
            Ok(())
        };
        check_index(4, n_tables)?;
        check_index(5, n_tables)?;
        check_index(6, n_cols)?;
        check_index(7, n_cols)?;
        for r in b[sec[8].clone()].chunks_exact(PROFILE_LEN) {
            for at in [24, 32, 40] {
                check_ref(r, at)?;
            }
        }
        Ok(Self { backing, sec, generation, created_ms, n_ds, n_tables, n_cols })
    }

    fn b(&self) -> &[u8] {
        self.backing.bytes()
    }

    fn section(&self, i: usize) -> &[u8] {
        &self.b()[self.sec[i].clone()]
    }

    fn str_at(&self, rec: &[u8], at: usize) -> &str {
        let heap = self.section(0);
        let (off, len) = (get_u32(rec, at) as usize, get_u32(rec, at + 4) as usize);
        // Validated at open: in bounds and on char boundaries.
        std::str::from_utf8(&heap[off..off + len]).unwrap_or("")
    }

    /// The raw image (for publishing).
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.b()
    }

    /// Image generation.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Build time, Unix ms.
    #[must_use]
    pub fn created_ms(&self) -> u64 {
        self.created_ms
    }

    /// Counts: (datasets, tables, columns).
    #[must_use]
    pub fn counts(&self) -> (u32, u32, u32) {
        (self.n_ds, self.n_tables, self.n_cols)
    }

    /// Dataset `i`.
    #[must_use]
    pub fn dataset(&self, i: u32) -> Option<DatasetRef<'_>> {
        let r = self.section(1).get(i as usize * DATASET_LEN..(i as usize + 1) * DATASET_LEN)?;
        let start = get_u32(r, 32);
        Some(DatasetRef {
            key: EntityKey(get_u64(r, 0)),
            project: self.str_at(r, 8),
            name: self.str_at(r, 16),
            location: self.str_at(r, 24),
            tables: start..start + get_u32(r, 36),
        })
    }

    /// Every dataset.
    pub fn datasets(&self) -> impl Iterator<Item = DatasetRef<'_>> + '_ {
        (0..self.n_ds).filter_map(|i| self.dataset(i))
    }

    /// Table `id`.
    #[must_use]
    pub fn table(&self, id: TableId) -> Option<TableRef<'_>> {
        let r = self.section(2).get(id.0 as usize * TABLE_LEN..(id.0 as usize + 1) * TABLE_LEN)?;
        let dataset = get_u32(r, 8);
        let ds = self
            .section(1)
            .get(dataset as usize * DATASET_LEN..(dataset as usize + 1) * DATASET_LEN)?;
        let start = get_u32(r, 32);
        let part = get_u32(r, 64);
        Some(TableRef {
            id,
            key: EntityKey(get_u64(r, 0)),
            dataset,
            dataset_name: self.str_at(ds, 16),
            project: self.str_at(ds, 8),
            name: self.str_at(r, 16),
            kind: TableKind::from_tag(r[12]),
            description: self.str_at(r, 24),
            row_count: get_u64(r, 40),
            logical_bytes: get_u64(r, 48),
            last_modified_ms: get_u64(r, 56),
            partition_col: (part != NONE).then_some(ColId(part)),
            require_partition_filter: r[13] & TF_REQUIRE_PARTITION_FILTER != 0,
            columns: start..start + get_u32(r, 36),
        })
    }

    /// Every table, in (dataset, name) order.
    pub fn tables(&self) -> impl Iterator<Item = TableRef<'_>> + '_ {
        (0..self.n_tables).filter_map(|i| self.table(TableId(i)))
    }

    /// Column `id`.
    #[must_use]
    pub fn column(&self, id: ColId) -> Option<ColumnRef<'_>> {
        let r =
            self.section(3).get(id.0 as usize * COLUMN_LEN..(id.0 as usize + 1) * COLUMN_LEN)?;
        let parent = get_u32(r, 12);
        Some(ColumnRef {
            id,
            key: EntityKey(get_u64(r, 0)),
            table: TableId(get_u32(r, 8)),
            parent: (parent != NONE).then_some(ColId(parent)),
            path: self.str_at(r, 16),
            data_type: self.str_at(r, 24),
            description: self.str_at(r, 32),
            code: TypeCode::from_tag(r[40]),
            flags: r[41],
            clustering: (r[42] != 0).then_some(r[42]),
            depth: r[43],
            ordinal: get_u32(r, 44),
        })
    }

    /// The columns (and nested fields) of a table, parents first.
    pub fn columns(&self, t: TableId) -> impl DoubleEndedIterator<Item = ColumnRef<'_>> + '_ {
        let range = self.table(t).map_or(0..0, |t| t.columns);
        range.filter_map(|i| self.column(ColId(i)))
    }

    /// Binary search a sorted 16-byte (u64, u32) index for `key`.
    fn search(idx: &[u8], key: u64) -> Range<usize> {
        let n = idx.len() / INDEX_LEN;
        let at = |i: usize| get_u64(idx, i * INDEX_LEN);
        let (mut lo, mut hi) = (0, n);
        while lo < hi {
            let mid = usize::midpoint(lo, hi);
            if at(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let start = lo;
        while lo < n && at(lo) == key {
            lo += 1;
        }
        start..lo
    }

    /// Find a table by project, dataset and name (case-sensitive, as in
    /// BigQuery).
    #[must_use]
    pub fn find_table_in(&self, project: &str, dataset: &str, table: &str) -> Option<TableId> {
        let idx = self.section(4);
        Self::search(idx, table_name_hash(dataset, table))
            .map(|i| TableId(get_u32(idx, i * INDEX_LEN + 8)))
            .find(|&id| {
                self.table(id).is_some_and(|t| {
                    t.name == table && t.dataset_name == dataset && t.project == project
                })
            })
    }

    /// Find a table by dataset and name, case-sensitive (the first match
    /// across projects;
    /// use [`find_table_in`](Self::find_table_in) when datasets of several
    /// projects share a name).
    #[must_use]
    pub fn find_table(&self, dataset: &str, table: &str) -> Option<TableId> {
        let idx = self.section(4);
        Self::search(idx, table_name_hash(dataset, table))
            .map(|i| TableId(get_u32(idx, i * INDEX_LEN + 8)))
            .find(|&id| {
                self.table(id).is_some_and(|t| t.name == table && t.dataset_name == dataset)
            })
    }

    /// Find a table by durable key.
    #[must_use]
    pub fn table_by_key(&self, key: EntityKey) -> Option<TableId> {
        let idx = self.section(5);
        Self::search(idx, key.raw()).next().map(|i| TableId(get_u32(idx, i * INDEX_LEN + 8)))
    }

    /// Find a column or nested field of `table` by dotted path
    /// (case-insensitive, as in BigQuery).
    #[must_use]
    pub fn find_column(&self, table: TableId, path: &str) -> Option<ColId> {
        let t = self.table(table)?;
        let lo = t.columns.start as usize * INDEX_LEN;
        let hi = t.columns.end as usize * INDEX_LEN;
        let idx = self.section(6).get(lo..hi)?;
        Self::search(idx, path_hash(path))
            .map(|i| ColId(get_u32(idx, i * INDEX_LEN + 8)))
            .find(|&id| self.column(id).is_some_and(|c| c.path.eq_ignore_ascii_case(path)))
    }

    /// The value profile of a column, when one was computed.
    #[must_use]
    pub fn profile(&self, id: ColId) -> Option<Profile<'_>> {
        fn opt(s: &str) -> Option<&str> {
            (!s.is_empty()).then_some(s)
        }
        let r =
            self.section(8).get(id.0 as usize * PROFILE_LEN..(id.0 as usize + 1) * PROFILE_LEN)?;
        let flags = r[48];
        if flags & PF_PRESENT == 0 {
            return None;
        }
        Some(Profile {
            rows: get_u64(r, 0),
            nulls: get_u64(r, 8),
            distinct: get_u64(r, 16),
            min: opt(self.str_at(r, 24)),
            max: opt(self.str_at(r, 32)),
            encoded: self.str_at(r, 40),
            values_complete: flags & PF_COMPLETE != 0,
            sampled: flags & PF_SAMPLED != 0,
        })
    }

    /// Find a column by durable key.
    #[must_use]
    pub fn column_by_key(&self, key: EntityKey) -> Option<ColId> {
        let idx = self.section(7);
        Self::search(idx, key.raw()).next().map(|i| ColId(get_u32(idx, i * INDEX_LEN + 8)))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn col(path: &str, ty: &str, ordinal: u32) -> ColumnSpec {
        ColumnSpec {
            path: path.into(),
            data_type: ty.into(),
            ordinal,
            nullable: true,
            partitioning: false,
            hidden: false,
            clustering: None,
            description: None,
        }
    }

    pub(crate) fn sample() -> Vec<u8> {
        let mut b = CatalogBuilder::new("my-project");
        b.add_dataset("shop", "US");
        b.add_table(TableSpec {
            dataset: "shop".into(),
            name: "orders".into(),
            kind: TableKind::Table,
            description: Some("One row per order.".into()),
            row_count: 1_000_000,
            logical_bytes: 64 << 20,
            last_modified_ms: 1_790_000_000_000,
            require_partition_filter: true,
            columns: vec![
                ColumnSpec { partitioning: true, ..col("order_ts", "TIMESTAMP", 3) },
                ColumnSpec {
                    clustering: Some(1),
                    description: Some("Order lifecycle state".into()),
                    ..col("status", "STRING", 2)
                },
                ColumnSpec { nullable: false, ..col("order_id", "INT64", 1) },
                col("items", "ARRAY<STRUCT<sku STRING, qty INT64>>", 4),
                col("items.sku", "STRING", 4),
                col("items.qty", "INT64", 4),
            ],
        });
        b.add_table(TableSpec {
            dataset: "shop".into(),
            name: "Customers".into(),
            kind: TableKind::View,
            description: None,
            row_count: 0,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns: vec![
                col("id", "INT64", 1),
                col("Addr", "STRUCT<city STRING>", 2),
                col("Addr.City", "STRING", 2),
            ],
        });
        b.set_profile(
            "shop",
            "orders",
            "status",
            ProfileSpec {
                rows: 1_000_000,
                nulls: 12,
                distinct: 3,
                min: Some("Cancelled".into()),
                max: Some("Shipped".into()),
                values: vec![
                    ("Shipped".into(), 610_000),
                    ("Complete".into(), 389_000),
                    ("Cancelled".into(), 988),
                ],
                values_complete: true,
                sampled: false,
            },
        );
        b.build(7, 1_790_000_000_000).unwrap()
    }

    #[test]
    fn round_trip_and_lookups() {
        let c = Catalog::from_bytes(Bytes::from(sample())).unwrap();
        assert_eq!(c.counts(), (1, 2, 9));
        assert_eq!(c.generation(), 7);
        let orders = c.find_table("shop", "orders").unwrap();
        let t = c.table(orders).unwrap();
        assert_eq!((t.dataset_name, t.project, t.kind), ("shop", "my-project", TableKind::Table));
        assert!(t.require_partition_filter);
        assert_eq!(t.row_count, 1_000_000);
        let names: Vec<&str> = c.columns(orders).map(|c| c.path).collect();
        assert_eq!(
            names,
            ["order_id", "status", "order_ts", "items", "items.qty", "items.sku"],
            "ordinal, then path"
        );
        let part = c.column(t.partition_col.unwrap()).unwrap();
        assert_eq!(part.path, "order_ts");
        assert!(part.partitioning());
        let status = c.column(c.find_column(orders, "STATUS").unwrap()).unwrap();
        assert_eq!((status.clustering, status.description), (Some(1), "Order lifecycle state"));
        let qty = c.column(c.find_column(orders, "items.qty").unwrap()).unwrap();
        assert!(qty.in_array() && !qty.repeated());
        assert_eq!(qty.leaf(), "qty");
        assert_eq!(c.column(qty.parent.unwrap()).unwrap().path, "items");
        assert!(c.column(c.find_column(orders, "items").unwrap()).unwrap().repeated());
    }

    #[test]
    fn profiles_round_trip() {
        let mut b = CatalogBuilder::new("p");
        b.add_table(TableSpec {
            dataset: "d".into(),
            name: "t".into(),
            kind: TableKind::Table,
            description: None,
            row_count: 10,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns: vec![col("status", "STRING", 1), col("n", "INT64", 2)],
        });
        b.set_profile(
            "d",
            "t",
            "STATUS",
            ProfileSpec {
                rows: 10,
                nulls: 1,
                distinct: 3,
                min: Some("Cancelled".into()),
                max: Some("Shipped".into()),
                values: vec![
                    ("Complete".into(), 5),
                    ("Shipped".into(), 3),
                    ("bad\u{1f}value".into(), 1),
                    ("Cancelled".into(), 1),
                ],
                values_complete: true,
                sampled: false,
            },
        );
        let c = Catalog::from_bytes(Bytes::from(b.build(1, 0).unwrap())).unwrap();
        let t = c.find_table("d", "t").unwrap();
        let p = c.profile(c.find_column(t, "status").unwrap()).unwrap();
        assert_eq!(
            (p.rows, p.nulls, p.distinct, p.min, p.max),
            (10, 1, 3, Some("Cancelled"), Some("Shipped"))
        );
        assert!(p.values_complete && !p.sampled);
        assert_eq!(
            p.values().collect::<Vec<_>>(),
            [("Complete", 5), ("Shipped", 3), ("Cancelled", 1)],
            "control characters are dropped"
        );
        assert!((p.null_fraction() - 0.1).abs() < 1e-12);
        assert!(c.profile(c.find_column(t, "n").unwrap()).is_none());
    }

    #[test]
    fn case_rules_follow_bigquery() {
        let c = Catalog::from_bytes(Bytes::from(sample())).unwrap();
        assert!(c.find_table("shop", "customers").is_none(), "table names are case-sensitive");
        let cust = c.find_table("shop", "Customers").unwrap();
        assert!(c.find_column(cust, "addr.city").is_some(), "column paths are not");
        assert!(c.find_column(cust, "nope").is_none());
    }

    #[test]
    fn keys_match_crab_core_and_resolve() {
        let c = Catalog::from_bytes(Bytes::from(sample())).unwrap();
        let k = table_key("my-project", "shop", "orders");
        let id = c.table_by_key(k).unwrap();
        assert_eq!(c.table(id).unwrap().name, "orders");
        let ck = column_key("my-project", "shop", "orders", "Items.SKU");
        assert_eq!(c.column(c.column_by_key(ck).unwrap()).unwrap().path, "items.sku");
    }

    #[test]
    fn build_is_deterministic_and_order_independent() {
        let a = sample();
        assert_eq!(a, sample());
        // Same content, different insertion order.
        let mut b = CatalogBuilder::new("my-project");
        let src = Catalog::from_bytes(Bytes::from(a.clone())).unwrap();
        let mut specs: Vec<TableSpec> = src
            .tables()
            .map(|t| TableSpec {
                dataset: t.dataset_name.into(),
                name: t.name.into(),
                kind: t.kind,
                description: (!t.description.is_empty()).then(|| t.description.into()),
                row_count: t.row_count,
                logical_bytes: t.logical_bytes,
                last_modified_ms: t.last_modified_ms,
                require_partition_filter: t.require_partition_filter,
                columns: src
                    .columns(t.id)
                    .map(|c| ColumnSpec {
                        path: c.path.into(),
                        data_type: c.data_type.into(),
                        ordinal: c.ordinal,
                        nullable: c.nullable(),
                        partitioning: c.partitioning(),
                        hidden: c.hidden(),
                        clustering: c.clustering,
                        description: (!c.description.is_empty()).then(|| c.description.into()),
                    })
                    .rev()
                    .collect(),
            })
            .collect();
        specs.reverse();
        b.add_dataset("shop", "US");
        for s in specs {
            b.add_table(s);
        }
        for t in src.tables() {
            for c in src.columns(t.id) {
                if let Some(p) = src.profile(c.id) {
                    let values = p.values().map(|(v, n)| (v.to_owned(), n)).collect();
                    let spec = ProfileSpec {
                        rows: p.rows,
                        nulls: p.nulls,
                        distinct: p.distinct,
                        min: p.min.map(Into::into),
                        max: p.max.map(Into::into),
                        values,
                        values_complete: p.values_complete,
                        sampled: p.sampled,
                    };
                    b.set_profile(t.dataset_name, t.name, c.path, spec);
                }
            }
        }
        assert_eq!(b.build(7, 1_790_000_000_000).unwrap(), a);
    }

    #[test]
    fn corruption_is_detected() {
        let img = sample();
        for i in (0..img.len()).step_by(7) {
            let mut bad = img.clone();
            bad[i] ^= 0x55;
            assert!(Catalog::from_bytes(Bytes::from(bad)).is_err(), "flip at {i} accepted");
        }
        assert!(Catalog::from_bytes(Bytes::from(img[..img.len() - 1].to_vec())).is_err());
        assert!(Catalog::from_bytes(Bytes::new()).is_err());
    }

    #[test]
    fn mmap_backing() {
        let dir = std::env::temp_dir().join(format!("crab-cat-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("catalog.img");
        std::fs::write(&p, sample()).unwrap();
        let c = Catalog::open(&p).unwrap();
        assert!(c.find_table("shop", "orders").is_some());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[cfg(test)]
mod seed {
    /// Writes a valid image as a fuzz seed when `CRAB_WRITE_SEED` names a directory.
    #[test]
    fn write_fuzz_seed() {
        if let Ok(dir) = std::env::var("CRAB_WRITE_SEED") {
            std::fs::write(std::path::Path::new(&dir).join("seed.img"), super::tests::sample())
                .unwrap();
        }
    }
}

#[cfg(test)]
mod multi_project_tests {
    use super::*;

    #[test]
    fn datasets_of_other_projects_keep_their_project() {
        let col = ColumnSpec {
            path: "id".into(),
            data_type: "INT64".into(),
            ordinal: 1,
            nullable: true,
            partitioning: false,
            hidden: false,
            clustering: None,
            description: None,
        };
        let t = |ds: &str| TableSpec {
            dataset: ds.into(),
            name: "users".into(),
            kind: TableKind::Table,
            description: None,
            row_count: 0,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns: vec![col.clone()],
        };
        let mut b = CatalogBuilder::new("lab");
        b.add_table(t("shop"));
        b.add_table(t("bigquery-public-data.shop"));
        b.add_table(t("google.com:proj.shop"));
        let c = Catalog::from_bytes(Bytes::from(b.build(1, 0).unwrap())).unwrap();
        let mine = c.find_table_in("lab", "shop", "users").unwrap();
        let public = c.find_table_in("bigquery-public-data", "shop", "users").unwrap();
        let domain = c.find_table_in("google.com:proj", "shop", "users").unwrap();
        assert!(mine != public && public != domain);
        assert_eq!(
            (c.table(public).unwrap().project, c.table(public).unwrap().dataset_name),
            ("bigquery-public-data", "shop")
        );
        assert_ne!(
            c.table(mine).unwrap().key,
            c.table(public).unwrap().key,
            "keys include the project"
        );
        assert!(c.find_table_in("nope", "shop", "users").is_none());
    }
}

#[cfg(test)]
mod overlay_tests {
    use super::*;

    #[test]
    fn descriptions_are_overlaid_by_project_dataset_table_and_path() {
        let col = |p: &str| ColumnSpec {
            path: p.into(),
            data_type: "INT64".into(),
            ordinal: 1,
            nullable: true,
            partitioning: false,
            hidden: false,
            clustering: None,
            description: Some("old".into()),
        };
        let t = |ds: &str| TableSpec {
            dataset: ds.into(),
            name: "users".into(),
            kind: TableKind::Table,
            description: None,
            row_count: 0,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns: vec![col("Id"), col("addr.city")],
        };
        let mut b = CatalogBuilder::new("lab");
        b.add_table(t("shop"));
        b.add_table(t("other.shop"));
        assert!(b.set_description("lab", "shop", "users", Some("id"), "User key."));
        assert!(b.set_description("other", "shop", "users", Some("ADDR.CITY"), "City."));
        assert!(b.set_description("lab", "shop", "users", None, "Users."));
        assert!(!b.set_description("lab", "shop", "users", Some("nope"), "x"));
        assert!(!b.set_description("otherx", "shop", "users", None, "x"));
        let c = Catalog::from_bytes(Bytes::from(b.build(1, 0).unwrap())).unwrap();
        let home = c.find_table_in("lab", "shop", "users").unwrap();
        let other = c.find_table_in("other", "shop", "users").unwrap();
        let desc =
            |id, p: &str| c.columns(id).find(|x| x.path == p).unwrap().description.to_owned();
        assert_eq!(desc(home, "Id"), "User key.");
        assert_eq!(desc(home, "addr.city"), "old");
        assert_eq!(desc(other, "addr.city"), "City.");
        assert_eq!(c.table(home).unwrap().description, "Users.");
    }
}
