// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # crab-meta
//!
//! A metadata image: what is known about a set of tables — descriptions
//! with their provenance, types, producing SQL, each column's expression —
//! and column lineage in **both** directions, in one immutable byte image.
//!
//! - **Zero-copy.** [`MetaImage::from_bytes`] checks the header and section
//!   bounds (O(1)) and keeps the [`Bytes`]; every accessor reads fixed-size
//!   little-endian records and string slices in place. An image can be
//!   memory-mapped and shared by every process on a host.
//! - **Total.** Every read is bounds-checked: a damaged or hostile image
//!   yields `None` or empty strings, never a panic (fuzzed), and no `unsafe`.
//! - **Hashed lookups.** Tables by name and columns by `(table, column)`
//!   resolve through an open-addressing index of xxh3 hashes, verified
//!   against the stored names: no allocation, one or two cache misses.
//! - **Lineage walks without allocation.** [`MetaImage::upstream`] and
//!   [`MetaImage::downstream`] are breadth-first walks over CSR adjacency in
//!   a reusable [`Walk`], with an exact-copy flag per hop and a visitor
//!   that decides where to stop (e.g. at the nearest described ancestor).
//!
//! Layout (all integers little-endian):
//!
//! ```text
//! header   64 B   magic "CRABMETA", version, counts, section offsets
//! tables   64 B × T   name, kind, description, statement type, SQL, rows, ...
//! columns  56 B × C   table, path, type, description, expression, adjacency
//! up       4 B × U    parents per column (CSR), bit 31 = exact copy
//! down     4 B × D    children per column (CSR), bit 31 = exact copy
//! slots    16 B × S   hash index: hash u64, index u32, kind u32
//! heap     strings (deduplicated)
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::cast_possible_truncation,
    clippy::missing_errors_doc,
    clippy::must_use_candidate
)]

use graph_r::hash::xxh3_64_seeded;
pub use objstore_r::Bytes;
use std::collections::HashMap;
use std::fmt;

/// File magic.
pub const MAGIC: &[u8; 8] = b"CRABMETA";
/// Format version.
pub const VERSION: u32 = 1;

const HEADER: usize = 64;
const TABLE: usize = 64;
const COLUMN: usize = 56;
const SLOT: usize = 16;
const EXACT: u32 = 1 << 31;
const SEED_TABLE: u64 = 0x0074_6162_6c65;
const SEED_COLUMN: u64 = 0x636f_6c75_6d6e;
const KIND_TABLE: u32 = 1;
const KIND_COLUMN: u32 = 2;

/// Where a description came from.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Provenance {
    /// Not known.
    #[default]
    Unknown = 0,
    /// Written by people, kept as it was (not audited).
    Declared = 1,
    /// Written by people and supported by checks against the data.
    Confirmed = 2,
    /// Written by people and contradicted by the data: the text is the
    /// evidence-backed correction, pending review.
    Flagged = 3,
    /// Copied verbatim along an exact-copy lineage chain.
    Propagated = 4,
    /// Generated from the producing SQL and the values, then checked.
    Generated = 5,
    /// A table or column outside the described datasets, known through
    /// lineage only.
    External = 6,
}

impl Provenance {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Declared,
            2 => Self::Confirmed,
            3 => Self::Flagged,
            4 => Self::Propagated,
            5 => Self::Generated,
            6 => Self::External,
            _ => Self::Unknown,
        }
    }

    /// Lower-case name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Declared => "declared",
            Self::Confirmed => "confirmed",
            Self::Flagged => "flagged",
            Self::Propagated => "propagated",
            Self::Generated => "generated",
            Self::External => "external",
        }
    }
}

/// How sure the description is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Confidence {
    /// Not rated.
    #[default]
    None = 0,
    /// Low.
    Low = 1,
    /// Medium.
    Medium = 2,
    /// High.
    High = 3,
}

impl Confidence {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Low,
            2 => Self::Medium,
            3 => Self::High,
            _ => Self::None,
        }
    }

    /// Parse `low`, `medium`, `high`.
    pub fn parse(s: &str) -> Self {
        match s {
            "low" => Self::Low,
            "medium" => Self::Medium,
            "high" => Self::High,
            _ => Self::None,
        }
    }

    /// Lower-case name (empty when not rated).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// How a column's value relates to its sources.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Transform {
    /// Not known (no producing SQL).
    #[default]
    Unknown = 0,
    /// Copied unchanged, same name.
    Passthrough = 1,
    /// Copied unchanged, renamed.
    Rename = 2,
    /// Cast to another type.
    Cast = 3,
    /// An aggregate.
    Aggregate = 4,
    /// A window function.
    Window = 5,
    /// A constant.
    Constant = 6,
    /// Any other expression.
    Derived = 7,
}

impl Transform {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Passthrough,
            2 => Self::Rename,
            3 => Self::Cast,
            4 => Self::Aggregate,
            5 => Self::Window,
            6 => Self::Constant,
            7 => Self::Derived,
            _ => Self::Unknown,
        }
    }

    /// Parse a lower-case name.
    pub fn parse(s: &str) -> Self {
        match s {
            "passthrough" => Self::Passthrough,
            "rename" => Self::Rename,
            "cast" => Self::Cast,
            "aggregate" => Self::Aggregate,
            "window" => Self::Window,
            "constant" => Self::Constant,
            "derived" => Self::Derived,
            _ => Self::Unknown,
        }
    }

    /// Lower-case name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Passthrough => "passthrough",
            Self::Rename => "rename",
            Self::Cast => "cast",
            Self::Aggregate => "aggregate",
            Self::Window => "window",
            Self::Constant => "constant",
            Self::Derived => "derived",
        }
    }
}

/// Why an image could not be opened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// Not a metadata image (magic or version).
    NotAnImage,
    /// A section lies outside the bytes.
    Truncated,
    /// More than 2^31 columns or 4 GiB of image.
    TooLarge,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotAnImage => "not a crab-meta image (magic or version)",
            Self::Truncated => "metadata image is truncated",
            Self::TooLarge => "metadata image too large",
        })
    }
}

impl std::error::Error for Error {}

// ---- building -----------------------------------------------------------------

/// A table to put in an image.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableEntry {
    /// `project.dataset.table`.
    pub name: String,
    /// `TABLE`, `VIEW`, `EXTERNAL`, ...
    pub kind: String,
    /// Description.
    pub description: Option<String>,
    /// Where the description came from.
    pub provenance: Provenance,
    /// How it is produced: `MERGE`, `CREATE_TABLE_AS_SELECT`, `VIEW`, ...
    pub statement_type: Option<String>,
    /// The producing SQL.
    pub producer_sql: Option<String>,
    /// Rows.
    pub rows: u64,
    /// Last modification, epoch milliseconds.
    pub last_modified_ms: u64,
}

/// A column to put in an image.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnEntry {
    /// Column path (`a`, `rec.a`).
    pub path: String,
    /// Type.
    pub data_type: String,
    /// Description.
    pub description: Option<String>,
    /// Where the description came from.
    pub provenance: Provenance,
    /// How sure.
    pub confidence: Confidence,
    /// How it relates to its sources.
    pub transform: Transform,
    /// The producing expression.
    pub expression: Option<String>,
}

/// Collects tables, columns and lineage edges, then writes an image.
#[derive(Debug, Default)]
pub struct MetaBuilder {
    tables: Vec<(TableEntry, Vec<ColumnEntry>)>,
    by_name: HashMap<String, usize>,
    edges: Vec<(ColumnKey, ColumnKey, bool)>,
}

/// (table, lower-cased column).
type ColumnKey = (String, String);

fn column_key(table: &str, column: &str) -> ColumnKey {
    (table.to_owned(), column.to_ascii_lowercase())
}

impl MetaBuilder {
    /// An empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add (or replace) a table and its columns.
    pub fn table(&mut self, t: TableEntry, columns: Vec<ColumnEntry>) {
        if let Some(&i) = self.by_name.get(&t.name) {
            self.tables[i] = (t, columns);
        } else {
            self.by_name.insert(t.name.clone(), self.tables.len());
            self.tables.push((t, columns));
        }
    }

    /// `child` (table, column) derives from `parent`; `exact` when it is an
    /// unchanged copy. Endpoints not added as tables become external nodes.
    pub fn edge(&mut self, child: (&str, &str), parent: (&str, &str), exact: bool) {
        self.edges.push((column_key(child.0, child.1), column_key(parent.0, parent.1), exact));
    }

    /// Write the image.
    #[allow(clippy::too_many_lines)] // one pass per section, in file order
    pub fn build(&self) -> Result<Vec<u8>, Error> {
        // Tables, with external ones for edge endpoints nobody described.
        let mut tables: Vec<(TableEntry, Vec<ColumnEntry>)> = self.tables.clone();
        let mut by_name = self.by_name.clone();
        let mut col_ix: HashMap<(String, String), u32> = HashMap::new();
        for ((ct, cc), (pt, pc), _) in &self.edges {
            for (t, c) in [(ct, cc), (pt, pc)] {
                let i = *by_name.entry(t.clone()).or_insert_with(|| {
                    tables.push((
                        TableEntry {
                            name: t.clone(),
                            kind: "EXTERNAL".into(),
                            provenance: Provenance::External,
                            ..TableEntry::default()
                        },
                        Vec::new(),
                    ));
                    tables.len() - 1
                });
                let cols = &mut tables[i].1;
                if !cols.iter().any(|x| x.path.eq_ignore_ascii_case(c)) {
                    cols.push(ColumnEntry {
                        path: c.clone(),
                        provenance: Provenance::External,
                        ..ColumnEntry::default()
                    });
                }
            }
        }
        let mut first_col = Vec::with_capacity(tables.len());
        let mut n_cols: u32 = 0;
        for (t, cols) in &tables {
            first_col.push(n_cols);
            for c in cols {
                col_ix.entry(column_key(&t.name, &c.path)).or_insert(n_cols);
                n_cols = n_cols.checked_add(1).filter(|n| *n < EXACT).ok_or(Error::TooLarge)?;
            }
        }
        // Adjacency, deduplicated (an edge seen twice keeps exact-copy evidence).
        let mut up: Vec<Vec<u32>> = vec![Vec::new(); n_cols as usize];
        let mut down: Vec<Vec<u32>> = vec![Vec::new(); n_cols as usize];
        let add = |list: &mut Vec<u32>, v: u32, exact: bool| match list
            .iter_mut()
            .find(|x| **x & !EXACT == v)
        {
            Some(x) => {
                if exact {
                    *x |= EXACT;
                }
            }
            None => list.push(v | if exact { EXACT } else { 0 }),
        };
        for (child, parent, exact) in &self.edges {
            let (Some(&c), Some(&p)) = (col_ix.get(child), col_ix.get(parent)) else { continue };
            if c == p {
                continue;
            }
            add(&mut up[c as usize], p, *exact);
            add(&mut down[p as usize], c, *exact);
        }
        let n_up: usize = up.iter().map(Vec::len).sum();
        let n_down: usize = down.iter().map(Vec::len).sum();

        // Strings.
        let mut heap: Vec<u8> = Vec::new();
        let mut seen: HashMap<String, (u32, u32)> = HashMap::new();
        let mut put = |s: &str| -> Result<(u32, u32), Error> {
            if s.is_empty() {
                return Ok((0, 0));
            }
            if let Some(&r) = seen.get(s) {
                return Ok(r);
            }
            let off = u32::try_from(heap.len()).map_err(|_| Error::TooLarge)?;
            let len = u32::try_from(s.len()).map_err(|_| Error::TooLarge)?;
            heap.extend_from_slice(s.as_bytes());
            seen.insert(s.to_owned(), (off, len));
            Ok((off, len))
        };

        // Index: tables and columns, open addressing, load ≤ 1/2.
        let entries = tables.len() + n_cols as usize;
        let n_slots = (entries * 2).max(8).next_power_of_two();
        let mut slots = vec![(0u64, 0u32, 0u32); n_slots];
        let mut insert = |h: u64, idx: u32, kind: u32| {
            let mut i = (h as usize) & (n_slots - 1);
            while slots[i].2 != 0 {
                i = (i + 1) & (n_slots - 1);
            }
            slots[i] = (h, idx, kind);
        };

        let off_tables = HEADER;
        let off_columns = off_tables + TABLE * tables.len();
        let off_up = off_columns + COLUMN * n_cols as usize;
        let off_down = off_up + 4 * n_up;
        let off_slots = (off_down + 4 * n_down).next_multiple_of(8);
        let off_heap = off_slots + SLOT * n_slots;
        let mut out = vec![0u8; off_heap];

        let mut ci = 0usize;
        let (mut up_at, mut down_at) = (0u32, 0u32);
        let mut up_bytes = Vec::with_capacity(4 * n_up);
        let mut down_bytes = Vec::with_capacity(4 * n_down);
        for (ti, (t, cols)) in tables.iter().enumerate() {
            let rec = &mut out[off_tables + ti * TABLE..off_tables + (ti + 1) * TABLE];
            let name = put(&t.name)?;
            w_ref(rec, 0, name);
            w_ref(rec, 8, put(&t.kind)?);
            w_ref(rec, 16, put(t.description.as_deref().unwrap_or(""))?);
            w_ref(rec, 24, put(t.statement_type.as_deref().unwrap_or(""))?);
            w_ref(rec, 32, put(t.producer_sql.as_deref().unwrap_or(""))?);
            rec[40..48].copy_from_slice(&t.rows.to_le_bytes());
            rec[48..56].copy_from_slice(&t.last_modified_ms.to_le_bytes());
            rec[56..60].copy_from_slice(&first_col[ti].to_le_bytes());
            rec[60..62].copy_from_slice(&(cols.len().min(u16::MAX as usize) as u16).to_le_bytes());
            rec[62] = t.provenance as u8;
            insert(table_hash(&t.name), ti as u32, KIND_TABLE);
            for c in cols {
                let rec = &mut out[off_columns + ci * COLUMN..off_columns + (ci + 1) * COLUMN];
                rec[0..4].copy_from_slice(&(ti as u32).to_le_bytes());
                w_ref(rec, 4, put(&c.path)?);
                w_ref(rec, 12, put(&c.data_type)?);
                w_ref(rec, 20, put(c.description.as_deref().unwrap_or(""))?);
                w_ref(rec, 28, put(c.expression.as_deref().unwrap_or(""))?);
                let (u, d) = (&up[ci], &down[ci]);
                rec[36..40].copy_from_slice(&up_at.to_le_bytes());
                rec[40..44].copy_from_slice(&(u.len() as u32).to_le_bytes());
                rec[44..48].copy_from_slice(&down_at.to_le_bytes());
                rec[48..52].copy_from_slice(&(d.len() as u32).to_le_bytes());
                rec[52] = c.provenance as u8;
                rec[53] = c.confidence as u8;
                rec[54] = c.transform as u8;
                for v in u {
                    up_bytes.extend_from_slice(&v.to_le_bytes());
                }
                for v in d {
                    down_bytes.extend_from_slice(&v.to_le_bytes());
                }
                up_at += u.len() as u32;
                down_at += d.len() as u32;
                insert(column_hash(&t.name, &c.path), ci as u32, KIND_COLUMN);
                ci += 1;
            }
        }
        out[off_up..off_up + up_bytes.len()].copy_from_slice(&up_bytes);
        out[off_down..off_down + down_bytes.len()].copy_from_slice(&down_bytes);
        for (i, (h, idx, kind)) in slots.iter().enumerate() {
            let at = off_slots + i * SLOT;
            out[at..at + 8].copy_from_slice(&h.to_le_bytes());
            out[at + 8..at + 12].copy_from_slice(&idx.to_le_bytes());
            out[at + 12..at + 16].copy_from_slice(&kind.to_le_bytes());
        }
        out.extend_from_slice(&heap);
        if out.len() > u32::MAX as usize {
            return Err(Error::TooLarge);
        }
        let h = &mut out[..HEADER];
        h[0..8].copy_from_slice(MAGIC);
        let fields = [
            VERSION,
            tables.len() as u32,
            n_cols,
            n_up as u32,
            n_down as u32,
            n_slots as u32,
            off_up as u32,
            off_down as u32,
            off_slots as u32,
            off_heap as u32,
            heap.len() as u32,
            off_columns as u32,
        ];
        for (i, v) in fields.iter().enumerate() {
            h[8 + 4 * i..12 + 4 * i].copy_from_slice(&v.to_le_bytes());
        }
        Ok(out)
    }
}

fn w_ref(rec: &mut [u8], at: usize, (off, len): (u32, u32)) {
    rec[at..at + 4].copy_from_slice(&off.to_le_bytes());
    rec[at + 4..at + 8].copy_from_slice(&len.to_le_bytes());
}

fn nonzero(h: u64) -> u64 {
    h.max(1)
}

fn table_hash(name: &str) -> u64 {
    nonzero(xxh3_64_seeded(name.as_bytes(), SEED_TABLE))
}

/// `table` bytes, a NUL, then the column lower-cased: on the stack for
/// ordinary names.
fn column_hash(table: &str, column: &str) -> u64 {
    let n = table.len() + 1 + column.len();
    let mut stack = [0u8; 256];
    let mut heap = Vec::new();
    let buf: &mut [u8] = if n <= stack.len() {
        &mut stack[..n]
    } else {
        heap.resize(n, 0);
        &mut heap
    };
    buf[..table.len()].copy_from_slice(table.as_bytes());
    buf[table.len()] = 0;
    for (d, s) in buf[table.len() + 1..].iter_mut().zip(column.bytes()) {
        *d = s.to_ascii_lowercase();
    }
    nonzero(xxh3_64_seeded(buf, SEED_COLUMN))
}

// ---- reading ------------------------------------------------------------------

/// An opened image.
#[derive(Clone)]
pub struct MetaImage {
    b: Bytes,
    n_tables: u32,
    n_columns: u32,
    n_up: u32,
    n_down: u32,
    n_slots: u32,
    off_columns: usize,
    off_up: usize,
    off_down: usize,
    off_slots: usize,
    off_heap: usize,
    heap_len: usize,
}

impl fmt::Debug for MetaImage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaImage")
            .field("tables", &self.n_tables)
            .field("columns", &self.n_columns)
            .field("bytes", &self.b.len())
            .finish_non_exhaustive()
    }
}

fn r32(b: &[u8], at: usize) -> u32 {
    b.get(at..at + 4).and_then(|s| s.try_into().ok()).map_or(0, u32::from_le_bytes)
}

fn r64(b: &[u8], at: usize) -> u64 {
    b.get(at..at + 8).and_then(|s| s.try_into().ok()).map_or(0, u64::from_le_bytes)
}

/// A column's position in the image.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnId(pub u32);

/// A table's position in the image.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableId(pub u32);

impl MetaImage {
    /// Open an image (checks the header and that every section lies inside
    /// the bytes; records are read lazily).
    pub fn from_bytes(b: Bytes) -> Result<Self, Error> {
        if b.len() < HEADER || &b[0..8] != MAGIC || r32(&b, 8) != VERSION {
            return Err(Error::NotAnImage);
        }
        let f = |i: usize| r32(&b, 8 + 4 * i);
        let (n_tables, n_columns, n_up, n_down, n_slots) = (f(1), f(2), f(3), f(4), f(5));
        let (off_up, off_down, off_slots, off_heap, heap_len, off_columns) = (
            f(6) as usize,
            f(7) as usize,
            f(8) as usize,
            f(9) as usize,
            f(10) as usize,
            f(11) as usize,
        );
        let fits = |off: usize, n: u32, size: usize| {
            (n as usize)
                .checked_mul(size)
                .and_then(|l| off.checked_add(l))
                .is_some_and(|e| e <= b.len())
        };
        if !(fits(HEADER, n_tables, TABLE)
            && fits(off_columns, n_columns, COLUMN)
            && fits(off_up, n_up, 4)
            && fits(off_down, n_down, 4)
            && fits(off_slots, n_slots, SLOT)
            && off_heap.checked_add(heap_len).is_some_and(|e| e <= b.len()))
        {
            return Err(Error::Truncated);
        }
        if n_slots == 0 || !n_slots.is_power_of_two() || n_columns >= EXACT {
            return Err(Error::NotAnImage);
        }
        Ok(Self {
            b,
            n_tables,
            n_columns,
            n_up,
            n_down,
            n_slots,
            off_columns,
            off_up,
            off_down,
            off_slots,
            off_heap,
            heap_len,
        })
    }

    /// The image bytes.
    pub fn bytes(&self) -> &Bytes {
        &self.b
    }

    /// Number of tables.
    pub fn table_count(&self) -> usize {
        self.n_tables as usize
    }

    /// Number of columns.
    pub fn column_count(&self) -> usize {
        self.n_columns as usize
    }

    fn string(&self, rec: usize, at: usize) -> &str {
        let (off, len) = (r32(&self.b, rec + at) as usize, r32(&self.b, rec + at + 4) as usize);
        if len == 0 || off.saturating_add(len) > self.heap_len {
            return "";
        }
        let start = self.off_heap + off;
        self.b.get(start..start + len).and_then(|s| std::str::from_utf8(s).ok()).unwrap_or("")
    }

    fn probe(&self, h: u64, kind: u32, mut matches: impl FnMut(u32) -> bool) -> Option<u32> {
        let mask = self.n_slots as usize - 1;
        let mut i = (h as usize) & mask;
        for _ in 0..self.n_slots {
            let at = self.off_slots + i * SLOT;
            let k = r32(&self.b, at + 12);
            if k == 0 {
                return None;
            }
            if k == kind && r64(&self.b, at) == h {
                let idx = r32(&self.b, at + 8);
                if matches(idx) {
                    return Some(idx);
                }
            }
            i = (i + 1) & mask;
        }
        None
    }

    /// The table named `project.dataset.table`.
    pub fn table(&self, name: &str) -> Option<Table<'_>> {
        let i = self.probe(table_hash(name), KIND_TABLE, |i| {
            self.table_at(TableId(i)).is_some_and(|t| t.name() == name)
        })?;
        self.table_at(TableId(i))
    }

    /// The table at `id`.
    pub fn table_at(&self, id: TableId) -> Option<Table<'_>> {
        (id.0 < self.n_tables).then(|| Table { img: self, rec: HEADER + id.0 as usize * TABLE, id })
    }

    /// Every table.
    pub fn tables(&self) -> impl Iterator<Item = Table<'_>> + '_ {
        (0..self.n_tables).filter_map(|i| self.table_at(TableId(i)))
    }

    /// Column `column` (case-insensitive) of `table`.
    pub fn column(&self, table: &str, column: &str) -> Option<Column<'_>> {
        let i = self.probe(column_hash(table, column), KIND_COLUMN, |i| {
            self.column_at(ColumnId(i)).is_some_and(|c| {
                c.path().eq_ignore_ascii_case(column)
                    && c.table().is_some_and(|t| t.name() == table)
            })
        })?;
        self.column_at(ColumnId(i))
    }

    /// A column by its full name `project.dataset.table.column` (the column
    /// is the last segment).
    pub fn column_by_name(&self, full: &str) -> Option<Column<'_>> {
        let (t, c) = full.rsplit_once('.')?;
        self.column(t, c)
    }

    /// The column at `id`.
    pub fn column_at(&self, id: ColumnId) -> Option<Column<'_>> {
        (id.0 < self.n_columns).then(|| Column {
            img: self,
            rec: self.off_columns + id.0 as usize * COLUMN,
            id,
        })
    }

    fn adjacency(
        &self,
        base: usize,
        total: u32,
        start: u32,
        len: u32,
    ) -> impl Iterator<Item = (ColumnId, bool)> + '_ {
        let ok = start.checked_add(len).is_some_and(|e| e <= total);
        let (start, len) = if ok { (start, len) } else { (0, 0) };
        (start..start + len).map(move |k| {
            let v = r32(&self.b, base + 4 * k as usize);
            (ColumnId(v & !EXACT), v & EXACT != 0)
        })
    }

    /// Walk the lineage of `from` upstream (sources, their sources, ...),
    /// breadth-first, at most `max_depth` hops. `visit` sees each reached
    /// column once, nearest first, and returns whether to continue past it.
    pub fn upstream(
        &self,
        from: ColumnId,
        max_depth: u32,
        walk: &mut Walk,
        visit: impl FnMut(Hop) -> bool,
    ) {
        self.walk(from, max_depth, walk, visit, true);
    }

    /// Walk downstream: every column computed from `from`, directly or not
    /// (impact analysis).
    pub fn downstream(
        &self,
        from: ColumnId,
        max_depth: u32,
        walk: &mut Walk,
        visit: impl FnMut(Hop) -> bool,
    ) {
        self.walk(from, max_depth, walk, visit, false);
    }

    fn walk(
        &self,
        from: ColumnId,
        max_depth: u32,
        w: &mut Walk,
        mut visit: impl FnMut(Hop) -> bool,
        up: bool,
    ) {
        w.begin(self.n_columns as usize);
        w.mark(from.0);
        w.frontier.clear();
        w.frontier.push((from.0, true));
        for depth in 1..=max_depth {
            w.next.clear();
            for k in 0..w.frontier.len() {
                let (node, chain) = w.frontier[k];
                let Some(c) = self.column_at(ColumnId(node)) else { continue };
                let edges = c.edges(up);
                for (n, exact) in edges {
                    if n.0 >= self.n_columns || !w.mark(n.0) {
                        continue;
                    }
                    let hop = Hop { column: n, depth, exact_chain: chain && exact };
                    if visit(hop) {
                        w.next.push((n.0, hop.exact_chain));
                    }
                }
            }
            if w.next.is_empty() {
                break;
            }
            std::mem::swap(&mut w.frontier, &mut w.next);
        }
    }

    /// The nearest described ancestors of `from` (all at the shallowest
    /// depth that has any), each with whether the whole path is exact
    /// copies — the description a column would inherit.
    pub fn nearest_described(&self, from: ColumnId, max_depth: u32, walk: &mut Walk) -> Vec<Hop> {
        let mut found: Vec<Hop> = Vec::new();
        self.upstream(from, max_depth, walk, |h| {
            if found.first().is_some_and(|f| f.depth < h.depth) {
                return false;
            }
            let described = self.column_at(h.column).is_some_and(|c| !c.description().is_empty());
            if described {
                found.push(h);
            }
            !described
        });
        found
    }
}

/// One column reached by a walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hop {
    /// The column.
    pub column: ColumnId,
    /// Hops from the start.
    pub depth: u32,
    /// Every edge on the path is an exact copy.
    pub exact_chain: bool,
}

/// Reusable walk state: visited marks by generation, and two frontiers.
/// Keep one per thread; walks then allocate nothing once warm.
#[derive(Debug, Default)]
pub struct Walk {
    seen: Vec<u32>,
    generation: u32,
    frontier: Vec<(u32, bool)>,
    next: Vec<(u32, bool)>,
}

impl Walk {
    /// Empty walk state.
    pub fn new() -> Self {
        Self::default()
    }

    fn begin(&mut self, n: usize) {
        if self.seen.len() < n {
            self.seen.resize(n, 0);
        }
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.seen.iter_mut().for_each(|s| *s = 0);
            self.generation = 1;
        }
    }

    /// Mark `i` seen; false when it already was.
    fn mark(&mut self, i: u32) -> bool {
        match self.seen.get_mut(i as usize) {
            Some(s) if *s != self.generation => {
                *s = self.generation;
                true
            }
            _ => false,
        }
    }
}

/// A table of an image.
#[derive(Clone, Copy)]
pub struct Table<'a> {
    img: &'a MetaImage,
    rec: usize,
    id: TableId,
}

impl fmt::Debug for Table<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table").field("name", &self.name()).finish()
    }
}

impl<'a> Table<'a> {
    /// Position.
    pub fn id(&self) -> TableId {
        self.id
    }
    /// `project.dataset.table`.
    pub fn name(&self) -> &'a str {
        self.img.string(self.rec, 0)
    }
    /// `TABLE`, `VIEW`, `EXTERNAL`, ...
    pub fn kind(&self) -> &'a str {
        self.img.string(self.rec, 8)
    }
    /// Description (empty when none).
    pub fn description(&self) -> &'a str {
        self.img.string(self.rec, 16)
    }
    /// How it is produced (empty when unknown).
    pub fn statement_type(&self) -> &'a str {
        self.img.string(self.rec, 24)
    }
    /// The producing SQL (empty when unknown).
    pub fn producer_sql(&self) -> &'a str {
        self.img.string(self.rec, 32)
    }
    /// Rows.
    pub fn rows(&self) -> u64 {
        r64(&self.img.b, self.rec + 40)
    }
    /// Last modification, epoch milliseconds.
    pub fn last_modified_ms(&self) -> u64 {
        r64(&self.img.b, self.rec + 48)
    }
    /// Where the description came from.
    pub fn provenance(&self) -> Provenance {
        Provenance::from_u8(self.img.b.get(self.rec + 62).copied().unwrap_or(0))
    }
    /// Its columns, in table order.
    pub fn columns(&self) -> impl Iterator<Item = Column<'a>> + 'a {
        let img = self.img;
        let first = r32(&img.b, self.rec + 56);
        let n = u32::from(u16::from_le_bytes([
            img.b.get(self.rec + 60).copied().unwrap_or(0),
            img.b.get(self.rec + 61).copied().unwrap_or(0),
        ]));
        let n = if first.checked_add(n).is_some_and(|e| e <= img.n_columns) { n } else { 0 };
        (first..first + n).filter_map(move |i| img.column_at(ColumnId(i)))
    }
}

/// A column of an image.
#[derive(Clone, Copy)]
pub struct Column<'a> {
    img: &'a MetaImage,
    rec: usize,
    id: ColumnId,
}

impl fmt::Debug for Column<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Column")
            .field("table", &self.table().map(|t| t.name()))
            .field("path", &self.path())
            .finish()
    }
}

impl<'a> Column<'a> {
    /// Position.
    pub fn id(&self) -> ColumnId {
        self.id
    }
    /// Its table.
    pub fn table(&self) -> Option<Table<'a>> {
        self.img.table_at(TableId(r32(&self.img.b, self.rec)))
    }
    /// Path (`a`, `rec.a`).
    pub fn path(&self) -> &'a str {
        self.img.string(self.rec, 4)
    }
    /// Type (empty for external columns).
    pub fn data_type(&self) -> &'a str {
        self.img.string(self.rec, 12)
    }
    /// Description (empty when none).
    pub fn description(&self) -> &'a str {
        self.img.string(self.rec, 20)
    }
    /// The producing expression (empty when unknown).
    pub fn expression(&self) -> &'a str {
        self.img.string(self.rec, 28)
    }
    /// Where the description came from.
    pub fn provenance(&self) -> Provenance {
        Provenance::from_u8(self.img.b.get(self.rec + 52).copied().unwrap_or(0))
    }
    /// How sure.
    pub fn confidence(&self) -> Confidence {
        Confidence::from_u8(self.img.b.get(self.rec + 53).copied().unwrap_or(0))
    }
    /// How it relates to its sources.
    pub fn transform(&self) -> Transform {
        Transform::from_u8(self.img.b.get(self.rec + 54).copied().unwrap_or(0))
    }
    /// Upstream (`up`) or downstream edges.
    fn edges(&self, up: bool) -> impl Iterator<Item = (ColumnId, bool)> + 'a {
        let img = self.img;
        let (at, base, total) =
            if up { (36, img.off_up, img.n_up) } else { (44, img.off_down, img.n_down) };
        let (s, l) = (r32(&img.b, self.rec + at), r32(&img.b, self.rec + at + 4));
        img.adjacency(base, total, s, l)
    }
    /// Direct sources, with whether each is an exact copy.
    pub fn parent_edges(&self) -> impl Iterator<Item = (ColumnId, bool)> + 'a {
        self.edges(true)
    }
    /// Direct dependants, with whether each is an exact copy.
    pub fn child_edges(&self) -> impl Iterator<Item = (ColumnId, bool)> + 'a {
        self.edges(false)
    }
    /// Direct sources.
    pub fn parents(&self) -> impl Iterator<Item = Column<'a>> + 'a {
        let img = self.img;
        self.parent_edges().filter_map(move |(c, _)| img.column_at(c))
    }
    /// Direct dependants.
    pub fn children(&self) -> impl Iterator<Item = Column<'a>> + 'a {
        let img = self.img;
        self.child_edges().filter_map(move |(c, _)| img.column_at(c))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(path: &str, desc: Option<&str>, prov: Provenance) -> ColumnEntry {
        ColumnEntry {
            path: path.into(),
            data_type: "INT64".into(),
            description: desc.map(Into::into),
            provenance: prov,
            ..ColumnEntry::default()
        }
    }

    fn image() -> MetaImage {
        let mut b = MetaBuilder::new();
        b.table(
            TableEntry {
                name: "p.raw.orders".into(),
                kind: "TABLE".into(),
                rows: 10,
                ..TableEntry::default()
            },
            vec![
                col("order_id", Some("Order key."), Provenance::Declared),
                col("status", None, Provenance::Unknown),
            ],
        );
        b.table(
            TableEntry {
                name: "p.stg.orders".into(),
                kind: "TABLE".into(),
                statement_type: Some("MERGE".into()),
                producer_sql: Some("MERGE ...".into()),
                ..TableEntry::default()
            },
            vec![
                col("Order_ID", Some("Order key."), Provenance::Propagated),
                col("state", None, Provenance::Unknown),
            ],
        );
        b.table(
            TableEntry { name: "p.mart.m".into(), kind: "VIEW".into(), ..TableEntry::default() },
            vec![col("oid", None, Provenance::Unknown)],
        );
        b.edge(("p.stg.orders", "order_id"), ("p.raw.orders", "order_id"), true);
        b.edge(("p.stg.orders", "order_id"), ("p.raw.orders", "status"), false);
        b.edge(("p.stg.orders", "state"), ("p.raw.orders", "status"), true);
        b.edge(("p.mart.m", "oid"), ("p.stg.orders", "order_id"), true);
        b.edge(("p.raw.orders", "order_id"), ("ext.src.o", "id"), true); // external
        b.edge(("p.mart.m", "oid"), ("p.stg.orders", "order_id"), false); // duplicate keeps exact
        MetaImage::from_bytes(Bytes::from(b.build().unwrap())).unwrap()
    }

    #[test]
    fn lookups_views_and_external_nodes() {
        let img = image();
        assert_eq!((img.table_count(), img.column_count()), (4, 6));
        let t = img.table("p.stg.orders").unwrap();
        assert_eq!(
            (t.kind(), t.statement_type(), t.producer_sql()),
            ("TABLE", "MERGE", "MERGE ...")
        );
        let cols: Vec<&str> = t.columns().map(|c| c.path()).collect();
        assert_eq!(cols, vec!["Order_ID", "state"]);
        let c = img.column("p.stg.orders", "ORDER_id").unwrap();
        assert_eq!((c.description(), c.provenance()), ("Order key.", Provenance::Propagated));
        assert_eq!(img.column_by_name("p.stg.orders.state").unwrap().path(), "state");
        assert!(img.table("p.stg.order").is_none() && img.column("p.stg.orders", "nope").is_none());
        let ext = img.table("ext.src.o").unwrap();
        assert_eq!((ext.kind(), ext.provenance()), ("EXTERNAL", Provenance::External));
        // Parents keep the exact flag; the duplicate edge did not double up.
        let parents: Vec<(&str, bool)> =
            c.parent_edges().map(|(p, e)| (img.column_at(p).unwrap().path(), e)).collect();
        assert_eq!(parents, vec![("order_id", true), ("status", false)]);
        let m = img.column("p.mart.m", "oid").unwrap();
        assert_eq!(m.parent_edges().collect::<Vec<_>>(), vec![(c.id(), true)]);
    }

    #[test]
    fn walks_both_ways_and_finds_what_a_column_inherits() {
        let img = image();
        let mut w = Walk::new();
        let m = img.column("p.mart.m", "oid").unwrap().id();
        let mut up = Vec::new();
        img.upstream(m, 10, &mut w, |h| {
            up.push((
                img.column_at(h.column).unwrap().table().unwrap().name(),
                h.depth,
                h.exact_chain,
            ));
            true
        });
        assert_eq!(
            up,
            vec![
                ("p.stg.orders", 1, true),
                ("p.raw.orders", 2, true),
                ("p.raw.orders", 2, false),
                ("ext.src.o", 3, true)
            ]
        );
        let near = img.nearest_described(m, 10, &mut w);
        assert_eq!(near.len(), 1);
        assert_eq!(
            (img.column_at(near[0].column).unwrap().table().unwrap().name(), near[0].exact_chain),
            ("p.stg.orders", true)
        );
        // Impact: everything computed from raw status.
        let status = img.column("p.raw.orders", "status").unwrap().id();
        let mut down = Vec::new();
        img.downstream(status, 10, &mut w, |h| {
            let c = img.column_at(h.column).unwrap();
            down.push(format!("{}.{}", c.table().unwrap().name(), c.path()));
            true
        });
        assert_eq!(down, vec!["p.stg.orders.Order_ID", "p.stg.orders.state", "p.mart.m.oid"]);
        // Depth limit and early stop.
        let mut n = 0;
        img.upstream(m, 1, &mut w, |_| {
            n += 1;
            true
        });
        assert_eq!(n, 1);
    }

    #[test]
    fn damaged_images_are_refused_or_read_as_empty() {
        let good = image().bytes().to_vec();
        assert_eq!(
            MetaImage::from_bytes(Bytes::from_static(b"nope")).unwrap_err(),
            Error::NotAnImage
        );
        assert_eq!(
            MetaImage::from_bytes(Bytes::from(good[..good.len() / 2].to_vec())).unwrap_err(),
            Error::Truncated
        );
        // Garbage records: reads stay in bounds.
        let mut bad = good.clone();
        for b in &mut bad[HEADER..HEADER + 2 * TABLE] {
            *b = 0xff;
        }
        let img = MetaImage::from_bytes(Bytes::from(bad)).unwrap();
        for t in img.tables() {
            let _ = (t.name(), t.columns().count(), t.provenance());
        }
        assert!(img.table("p.raw.orders").is_none());
    }
}
