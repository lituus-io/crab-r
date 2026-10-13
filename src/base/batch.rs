// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Columnar result batches.
//!
//! Warehouse results, cached samples and local-interpreter output all use one
//! representation: a [`Batch`] of typed [`Column`]s, each a plain vector plus a
//! validity [`Bitmap`]. Strings are one contiguous byte buffer with `u32`
//! offsets (Arrow-style), so a 10 k-row string column is two allocations, not
//! 10 k. Result hashing and the interpreter run column-at-a-time over these
//! vectors.
//!
//! Types mirror BigQuery's scalar types. `NUMERIC` is exact: an `i128` scaled
//! by 10⁹ (BigQuery's precision 38, scale 9). Types without a native column
//! here (`BIGNUMERIC`, `GEOGRAPHY`, `JSON`, `INTERVAL`, `RANGE`, and nested
//! `STRUCT`/`ARRAY` values) are carried as their canonical text form.

use crate::base::{Error, Result};

/// Validity bitmap: bit `i` set means row `i` is non-NULL.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bitmap {
    words: Vec<u64>,
    len: usize,
}

impl Bitmap {
    /// Empty bitmap with room for `n` bits.
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self { words: Vec::with_capacity(n.div_ceil(64)), len: 0 }
    }

    /// Append one bit.
    pub fn push(&mut self, bit: bool) {
        if self.len.is_multiple_of(64) {
            self.words.push(0);
        }
        if bit {
            let last = self.words.len() - 1;
            self.words[last] |= 1 << (self.len % 64);
        }
        self.len += 1;
    }

    /// Bit `i` (false when out of range).
    #[must_use]
    pub fn get(&self, i: usize) -> bool {
        i < self.len && self.words[i / 64] & (1 << (i % 64)) != 0
    }

    /// Number of bits.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of set bits.
    #[must_use]
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Drop all bits, keep the allocation.
    pub fn clear(&mut self) {
        self.words.clear();
        self.len = 0;
    }
}

/// Scalar type of a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ColType {
    /// `BOOL`
    Bool,
    /// `INT64`
    Int64,
    /// `FLOAT64`
    Float64,
    /// `NUMERIC` (i128 scaled by 10⁹)
    Numeric,
    /// `STRING`
    String,
    /// `BYTES`
    Bytes,
    /// `DATE` (days since epoch)
    Date,
    /// `DATETIME` (microseconds since epoch, civil, no zone)
    DateTime,
    /// `TIME` (microseconds since midnight)
    Time,
    /// `TIMESTAMP` (microseconds since epoch, UTC)
    Timestamp,
    /// Any type carried as canonical text (BIGNUMERIC, GEOGRAPHY, JSON,
    /// INTERVAL, RANGE, STRUCT, ARRAY).
    Text,
}

/// Column storage. NULL slots hold a type default and are masked by the
/// validity bitmap.
#[derive(Clone, Debug, PartialEq)]
pub enum ColData {
    /// Booleans.
    Bool(Bitmap),
    /// 64-bit integers (INT64, DATETIME, TIME, TIMESTAMP micros).
    I64(Vec<i64>),
    /// 32-bit integers (DATE days).
    I32(Vec<i32>),
    /// Floats.
    F64(Vec<f64>),
    /// Exact decimals (NUMERIC).
    I128(Vec<i128>),
    /// Variable-length bytes (STRING, BYTES, text-carried types): row `i` is
    /// `bytes[offsets[i]..offsets[i+1]]`; `offsets` has `rows + 1` entries.
    Var {
        /// Row boundaries.
        offsets: Vec<u32>,
        /// Concatenated payloads.
        bytes: Vec<u8>,
    },
}

/// One typed column.
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    /// Declared type.
    pub ty: ColType,
    /// Validity.
    pub valid: Bitmap,
    /// Values.
    pub data: ColData,
}

/// A borrowed cell value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Cell<'a> {
    /// SQL NULL.
    Null,
    /// BOOL.
    Bool(bool),
    /// INT64 / DATETIME / TIME / TIMESTAMP (micros).
    I64(i64),
    /// DATE (days).
    I32(i32),
    /// FLOAT64.
    F64(f64),
    /// NUMERIC (scaled 10⁹).
    I128(i128),
    /// STRING / text-carried types (valid UTF-8).
    Str(&'a str),
    /// BYTES.
    Bytes(&'a [u8]),
}

/// Scale of `NUMERIC` values.
pub const NUMERIC_SCALE: i128 = 1_000_000_000;

impl Column {
    /// An empty column of type `ty` with room for `rows` values.
    #[must_use]
    pub fn new(ty: ColType, rows: usize) -> Self {
        let data = match ty {
            ColType::Bool => ColData::Bool(Bitmap::with_capacity(rows)),
            ColType::Int64 | ColType::DateTime | ColType::Time | ColType::Timestamp => {
                ColData::I64(Vec::with_capacity(rows))
            }
            ColType::Date => ColData::I32(Vec::with_capacity(rows)),
            ColType::Float64 => ColData::F64(Vec::with_capacity(rows)),
            ColType::Numeric => ColData::I128(Vec::with_capacity(rows)),
            ColType::String | ColType::Bytes | ColType::Text => {
                let mut offsets = Vec::with_capacity(rows + 1);
                offsets.push(0);
                ColData::Var { offsets, bytes: Vec::new() }
            }
        };
        Self { ty, valid: Bitmap::with_capacity(rows), data }
    }

    /// Row count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.valid.len()
    }

    /// True when the column has no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.valid.is_empty()
    }

    /// NULL count.
    #[must_use]
    pub fn null_count(&self) -> usize {
        self.len() - self.valid.count_ones()
    }

    /// Append a NULL.
    pub fn push_null(&mut self) {
        self.valid.push(false);
        match &mut self.data {
            ColData::Bool(b) => b.push(false),
            ColData::I64(v) => v.push(0),
            ColData::I32(v) => v.push(0),
            ColData::F64(v) => v.push(0.0),
            ColData::I128(v) => v.push(0),
            ColData::Var { offsets, bytes } => offsets.push(bytes.len() as u32),
        }
    }

    /// Append a non-NULL cell; its variant must match the column's storage.
    pub fn push(&mut self, cell: Cell<'_>) -> Result<()> {
        match (&mut self.data, cell) {
            (_, Cell::Null) => {
                self.push_null();
                return Ok(());
            }
            (ColData::Bool(b), Cell::Bool(x)) => b.push(x),
            (ColData::I64(v), Cell::I64(x)) => v.push(x),
            (ColData::I32(v), Cell::I32(x)) => v.push(x),
            (ColData::F64(v), Cell::F64(x)) => v.push(x),
            (ColData::I128(v), Cell::I128(x)) => v.push(x),
            (ColData::Var { offsets, bytes }, Cell::Str(s)) => {
                push_var(offsets, bytes, s.as_bytes())?;
            }
            (ColData::Var { offsets, bytes }, Cell::Bytes(s)) => push_var(offsets, bytes, s)?,
            _ => return Err(Error::Type { expected: "cell matching column type" }),
        }
        self.valid.push(true);
        Ok(())
    }

    /// Cell `i` (NULL when masked or out of range).
    #[must_use]
    pub fn get(&self, i: usize) -> Cell<'_> {
        if !self.valid.get(i) {
            return Cell::Null;
        }
        match &self.data {
            ColData::Bool(b) => Cell::Bool(b.get(i)),
            ColData::I64(v) => Cell::I64(v[i]),
            ColData::I32(v) => Cell::I32(v[i]),
            ColData::F64(v) => Cell::F64(v[i]),
            ColData::I128(v) => Cell::I128(v[i]),
            ColData::Var { offsets, bytes } => {
                let s = &bytes[offsets[i] as usize..offsets[i + 1] as usize];
                if self.ty == ColType::Bytes {
                    Cell::Bytes(s)
                } else {
                    // Only &str is ever pushed into non-BYTES Var columns.
                    std::str::from_utf8(s).map_or(Cell::Bytes(s), Cell::Str)
                }
            }
        }
    }
}

fn push_var(offsets: &mut Vec<u32>, bytes: &mut Vec<u8>, s: &[u8]) -> Result<()> {
    let end = bytes.len().checked_add(s.len()).filter(|&e| u32::try_from(e).is_ok());
    let Some(end) = end else { return Err(Error::Limit("column byte buffer exceeds 4 GiB")) };
    bytes.extend_from_slice(s);
    offsets.push(end as u32);
    Ok(())
}

/// Parse a decimal literal (`-12.5`, `3`, `0.000000001`) into a scaled
/// `NUMERIC`. More than 9 fractional digits or overflow is an error.
pub fn parse_numeric(s: &str) -> Result<i128> {
    let bad = Error::Type { expected: "NUMERIC literal" };
    let (neg, body) = s.strip_prefix('-').map_or((false, s), |r| (true, r));
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    if int.is_empty() && frac.is_empty() || frac.len() > 9 {
        return Err(bad);
    }
    if !int.bytes().chain(frac.bytes()).all(|c| c.is_ascii_digit()) {
        return Err(bad);
    }
    let mut v: i128 = 0;
    for c in int.bytes() {
        v = v
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(c - b'0')))
            .ok_or(bad.clone())?;
    }
    let mut f: i128 = 0;
    for c in frac.bytes() {
        f = f * 10 + i128::from(c - b'0');
    }
    f *= 10i128.pow(9 - frac.len() as u32);
    let v = v.checked_mul(NUMERIC_SCALE).and_then(|v| v.checked_add(f)).ok_or(bad)?;
    Ok(if neg { -v } else { v })
}

/// A named, ordered set of equal-length columns.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Batch {
    /// Column names, parallel to `columns`.
    pub names: Vec<String>,
    /// Columns.
    pub columns: Vec<Column>,
}

impl Batch {
    /// An empty batch with the given schema and row capacity.
    #[must_use]
    pub fn new(schema: &[(&str, ColType)], rows: usize) -> Self {
        Self {
            names: schema.iter().map(|(n, _)| (*n).to_owned()).collect(),
            columns: schema.iter().map(|&(_, t)| Column::new(t, rows)).collect(),
        }
    }

    /// Row count (0 for a batch with no columns).
    #[must_use]
    pub fn rows(&self) -> usize {
        self.columns.first().map_or(0, Column::len)
    }

    /// Check every column has the same length.
    pub fn validate(&self) -> Result<()> {
        let n = self.rows();
        if self.names.len() != self.columns.len() || self.columns.iter().any(|c| c.len() != n) {
            return Err(Error::Type { expected: "rectangular batch" });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitmap_push_get_count() {
        let mut b = Bitmap::with_capacity(3);
        for i in 0..130 {
            b.push(i % 3 == 0);
        }
        assert_eq!(b.len(), 130);
        assert!(b.get(0) && !b.get(1) && b.get(129));
        assert!(!b.get(1000));
        assert_eq!(b.count_ones(), 44);
    }

    #[test]
    fn var_column_stores_contiguously() {
        let mut c = Column::new(ColType::String, 4);
        c.push(Cell::Str("ab")).unwrap();
        c.push_null();
        c.push(Cell::Str("")).unwrap();
        c.push(Cell::Str("cde")).unwrap();
        assert_eq!(c.get(0), Cell::Str("ab"));
        assert_eq!(c.get(1), Cell::Null);
        assert_eq!(c.get(2), Cell::Str(""));
        assert_eq!(c.get(3), Cell::Str("cde"));
        assert_eq!(c.null_count(), 1);
        let ColData::Var { bytes, .. } = &c.data else { panic!() };
        assert_eq!(bytes, b"abcde");
    }

    #[test]
    fn type_mismatch_is_an_error() {
        let mut c = Column::new(ColType::Int64, 1);
        assert!(c.push(Cell::F64(1.0)).is_err());
        assert_eq!(c.len(), 0, "a rejected push leaves no row");
    }

    #[test]
    fn nulls_in_every_storage_kind() {
        for ty in [
            ColType::Bool,
            ColType::Int64,
            ColType::Date,
            ColType::Float64,
            ColType::Numeric,
            ColType::Bytes,
        ] {
            let mut c = Column::new(ty, 1);
            c.push_null();
            assert_eq!(c.get(0), Cell::Null);
            assert_eq!(c.null_count(), 1);
        }
    }

    #[test]
    fn numeric_parsing_is_exact() {
        assert_eq!(parse_numeric("1").unwrap(), NUMERIC_SCALE);
        assert_eq!(parse_numeric("-12.5").unwrap(), -12_500_000_000);
        assert_eq!(parse_numeric("0.000000001").unwrap(), 1);
        assert_eq!(parse_numeric(".5").unwrap(), 500_000_000);
        for bad in ["", "-", "1.0000000001", "1e5", "1.2.3", "abc"] {
            assert!(parse_numeric(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn batch_validation() {
        let mut b = Batch::new(&[("a", ColType::Int64), ("b", ColType::String)], 2);
        b.columns[0].push(Cell::I64(1)).unwrap();
        assert!(b.validate().is_err());
        b.columns[1].push(Cell::Str("x")).unwrap();
        assert!(b.validate().is_ok());
        assert_eq!(b.rows(), 1);
    }
}
