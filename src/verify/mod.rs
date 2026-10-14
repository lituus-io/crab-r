// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # `verify`
//!
//! Result hashing for execution-match scoring and candidate voting.
//!
//! [`hash_batch`] reduces a result [`Batch`] to a [`ResultHash`] in one
//! column-at-a-time pass (no row materialisation). Values are normalised so
//! that results which *mean* the same compare equal:
//!
//! - numbers compare by value across INT64 / NUMERIC / FLOAT64 (`5` = `5.0`
//!   = `NUMERIC 5`); non-integral values compare at 12 significant digits, so
//!   `SUM(x)/COUNT(x)` and `AVG(x)` agree;
//! - NULL is its own value; strings and bytes compare exactly; dates,
//!   datetimes, times and timestamps by their instant.
//!
//! Three views are kept:
//! - [`ResultHash::bag`]: the multiset of rows (row order ignored) — the
//!   standard execution-accuracy comparison;
//! - [`ResultHash::seq`]: rows in order — for queries whose ORDER BY matters;
//! - [`ResultHash::loose`]: rows as multisets of cells — equal when only the
//!   column order differs.
//!
//! Hashes are 128-bit sums/xors of well-mixed 64-bit row hashes, so equal
//! results always hash equal and distinct results collide with negligible
//! probability.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

use crate::base::batch::{Batch, ColData, ColType, NUMERIC_SCALE};

/// A result's fingerprint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ResultHash {
    /// Row count.
    pub rows: u64,
    /// Column count.
    pub cols: u32,
    /// Multiset of rows (row order ignored, column order significant).
    pub bag: u128,
    /// Sequence of rows (order significant).
    pub seq: u128,
    /// Multiset of rows-as-cell-multisets (row and column order ignored).
    pub loose: u128,
}

/// How two results relate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Match {
    /// Same rows (and order, when compared ordered).
    Same,
    /// Same rows except for column order.
    ColumnOrder,
    /// Different.
    Different,
}

impl ResultHash {
    /// Compare two results; `ordered` also requires the same row order.
    #[must_use]
    pub fn compare(&self, other: &Self, ordered: bool) -> Match {
        if self.rows != other.rows || self.cols != other.cols {
            return Match::Different;
        }
        let rows_equal = if ordered { self.seq == other.seq } else { self.bag == other.bag };
        if rows_equal {
            Match::Same
        } else if self.loose == other.loose && !ordered {
            Match::ColumnOrder
        } else {
            Match::Different
        }
    }

    /// A single 64-bit key for grouping candidates by result.
    #[must_use]
    pub fn group_key(&self) -> u64 {
        fmix(
            self.bag as u64
                ^ (self.bag >> 64) as u64
                ^ self.rows.rotate_left(17)
                ^ u64::from(self.cols),
        )
    }
}

use graph_r::hash::fmix64 as fmix;

#[inline]
fn mix2(a: u64, b: u64) -> u64 {
    fmix(a ^ b.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(23))
}

const T_NULL: u64 = 0x6E75_6C6C;
const T_BOOL: u64 = 0x626F_6F6C;
const T_INT: u64 = 0x696E_7400;
const T_REAL: u64 = 0x7265_616C;
const T_STR: u64 = 0x7374_7200;
const T_BYTES: u64 = 0x6279_7465;
const T_DATE: u64 = 0x6461_7465;
const T_DATETIME: u64 = 0x6474_696D;
const T_TIME: u64 = 0x7469_6D65;
const T_TS: u64 = 0x7473_7461;

fn int_hash(v: i128) -> u64 {
    mix2(T_INT, (v as u64) ^ ((v >> 64) as u64).rotate_left(1))
}

/// Numbers by value: integral values as integers, others with the mantissa
/// rounded to 40 bits (≈ 12 significant decimal digits) — cheap bit
/// arithmetic, no logarithms.
fn float_hash(x: f64) -> u64 {
    const DROP: u32 = 12;
    if x.is_nan() {
        return mix2(T_REAL, 0x7FF8);
    }
    if x.fract() == 0.0 && x.abs() < 9.0e18 {
        return int_hash(x as i128);
    }
    let bits = x.to_bits();
    let rounded = bits.wrapping_add(1 << (DROP - 1)) & !((1u64 << DROP) - 1);
    mix2(T_REAL, rounded)
}

fn numeric_hash(v: i128) -> u64 {
    if v % NUMERIC_SCALE == 0 {
        int_hash(v / NUMERIC_SCALE)
    } else {
        float_hash(v as f64 / NUMERIC_SCALE as f64)
    }
}

fn bytes_hash(tag: u64, b: &[u8]) -> u64 {
    // FNV-style fold, then a strong finaliser (dependency-free, fast enough
    // for result-sized strings).
    let mut h = tag ^ (b.len() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut chunks = b.chunks_exact(8);
    for c in &mut chunks {
        let w = u64::from_le_bytes(c.try_into().unwrap_or([0; 8]));
        h = (h ^ w).wrapping_mul(0x0000_0100_0000_01B3).rotate_left(31);
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut buf = [0u8; 8];
        buf[..rest.len()].copy_from_slice(rest);
        h = (h ^ u64::from_le_bytes(buf)).wrapping_mul(0x0000_0100_0000_01B3).rotate_left(31);
    }
    fmix(h)
}

/// Hash every cell of `col` into `out` (one u64 per row).
fn column_cells(
    ty: ColType,
    valid: &crate::base::batch::Bitmap,
    data: &ColData,
    out: &mut Vec<u64>,
) {
    let rows = valid.len();
    out.clear();
    out.reserve(rows);
    let null = mix2(T_NULL, 0);
    match data {
        ColData::Bool(b) => out.extend((0..rows).map(|i| {
            if valid.get(i) {
                mix2(T_BOOL, u64::from(b.get(i)))
            } else {
                null
            }
        })),
        ColData::I64(v) => {
            let tag = match ty {
                ColType::DateTime => Some(T_DATETIME),
                ColType::Time => Some(T_TIME),
                ColType::Timestamp => Some(T_TS),
                _ => None,
            };
            out.extend(v.iter().enumerate().map(|(i, &x)| {
                if !valid.get(i) {
                    null
                } else if let Some(t) = tag {
                    mix2(t, x as u64)
                } else {
                    int_hash(i128::from(x))
                }
            }));
        }
        ColData::I32(v) => out.extend(v.iter().enumerate().map(|(i, &x)| {
            if valid.get(i) {
                mix2(T_DATE, x as u64)
            } else {
                null
            }
        })),
        ColData::F64(v) => {
            out.extend(
                v.iter().enumerate().map(|(i, &x)| if valid.get(i) { float_hash(x) } else { null }),
            );
        }
        ColData::I128(v) => {
            out.extend(
                v.iter()
                    .enumerate()
                    .map(|(i, &x)| if valid.get(i) { numeric_hash(x) } else { null }),
            );
        }
        ColData::Var { offsets, bytes } => {
            let tag = if ty == ColType::Bytes { T_BYTES } else { T_STR };
            out.extend((0..rows).map(|i| {
                if valid.get(i) {
                    bytes_hash(tag, &bytes[offsets[i] as usize..offsets[i + 1] as usize])
                } else {
                    null
                }
            }));
        }
    }
}

/// Reusable scratch for [`hash_batch_with`].
#[derive(Debug, Default)]
pub struct Scratch {
    cells: Vec<u64>,
    rows: Vec<u64>,
    loose: Vec<u64>,
}

/// Hash a result batch.
#[must_use]
pub fn hash_batch(b: &Batch) -> ResultHash {
    hash_batch_with(b, &mut Scratch::default())
}

/// Hash a result batch, reusing `scratch` (no allocation once warm).
pub fn hash_batch_with(b: &Batch, scratch: &mut Scratch) -> ResultHash {
    let rows = b.rows();
    let Scratch { cells, rows: acc, loose } = scratch;
    acc.clear();
    acc.resize(rows, 0x5EED_0000_0000_0001);
    loose.clear();
    loose.resize(rows, 0);
    for (ci, c) in b.columns.iter().enumerate() {
        column_cells(c.ty, &c.valid, &c.data, cells);
        let salt = (ci as u64 + 1).wrapping_mul(0xA24B_AED4_963E_E407);
        for ((a, l), &h) in acc.iter_mut().zip(loose.iter_mut()).zip(cells.iter()) {
            *a = mix2(*a ^ salt, h);
            *l = l.wrapping_add(fmix(h));
        }
    }
    let (mut sum, mut xor, mut lsum, mut lxor) = (0u64, 0u64, 0u64, 0u64);
    let mut seq_a = 0x0123_4567_89AB_CDEFu64;
    let mut seq_b = 0xFEDC_BA98_7654_3210u64;
    for (&a, &l) in acc.iter().zip(loose.iter()) {
        let r = fmix(a);
        sum = sum.wrapping_add(r);
        xor ^= r.rotate_left(13);
        let lr = fmix(l ^ 0x10_05E0);
        lsum = lsum.wrapping_add(lr);
        lxor ^= lr.rotate_left(13);
        seq_a = mix2(seq_a, r);
        seq_b = mix2(seq_b ^ r, seq_a);
    }
    let pack = |a: u64, b: u64| (u128::from(a) << 64) | u128::from(b);
    ResultHash {
        rows: rows as u64,
        cols: b.columns.len() as u32,
        bag: pack(sum, xor),
        seq: pack(seq_a, seq_b),
        loose: pack(lsum, lxor),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::batch::{Cell, ColType};

    fn batch(cols: &[(&str, ColType)], rows: &[Vec<Cell<'_>>]) -> Batch {
        let mut b = Batch::new(cols, rows.len());
        for r in rows {
            for (i, c) in r.iter().enumerate() {
                b.columns[i].push(*c).unwrap();
            }
        }
        b
    }

    #[test]
    fn row_order_is_ignored_by_bag_but_not_seq() {
        let a = batch(&[("x", ColType::Int64)], &[vec![Cell::I64(1)], vec![Cell::I64(2)]]);
        let b = batch(&[("x", ColType::Int64)], &[vec![Cell::I64(2)], vec![Cell::I64(1)]]);
        let (ha, hb) = (hash_batch(&a), hash_batch(&b));
        assert_eq!(ha.compare(&hb, false), Match::Same);
        assert_eq!(ha.compare(&hb, true), Match::Different);
    }

    #[test]
    fn numbers_compare_by_value_across_types() {
        let i = batch(&[("x", ColType::Int64)], &[vec![Cell::I64(5)]]);
        let f = batch(&[("x", ColType::Float64)], &[vec![Cell::F64(5.0)]]);
        let n = batch(&[("x", ColType::Numeric)], &[vec![Cell::I128(5 * NUMERIC_SCALE)]]);
        let hi = hash_batch(&i);
        assert_eq!(hi.compare(&hash_batch(&f), false), Match::Same);
        assert_eq!(hi.compare(&hash_batch(&n), false), Match::Same);
        let avg1 = batch(&[("x", ColType::Float64)], &[vec![Cell::F64(10.0 / 3.0)]]);
        let avg2 = batch(&[("x", ColType::Float64)], &[vec![Cell::F64(3.333_333_333_333_333_5)]]);
        assert_eq!(
            hash_batch(&avg1).compare(&hash_batch(&avg2), false),
            Match::Same,
            "12 significant digits"
        );
        let n2 = batch(&[("x", ColType::Numeric)], &[vec![Cell::I128(12_500_000_000)]]);
        let f2 = batch(&[("x", ColType::Float64)], &[vec![Cell::F64(12.5)]]);
        assert_eq!(hash_batch(&n2).compare(&hash_batch(&f2), false), Match::Same);
        let other = batch(&[("x", ColType::Float64)], &[vec![Cell::F64(3.3334)]]);
        assert_eq!(hash_batch(&avg1).compare(&hash_batch(&other), false), Match::Different);
    }

    #[test]
    fn duplicates_and_nulls_matter() {
        let one = batch(&[("x", ColType::String)], &[vec![Cell::Str("a")]]);
        let two = batch(&[("x", ColType::String)], &[vec![Cell::Str("a")], vec![Cell::Str("a")]]);
        assert_eq!(hash_batch(&one).compare(&hash_batch(&two), false), Match::Different);
        let null = batch(&[("x", ColType::String)], &[vec![Cell::Null]]);
        let empty = batch(&[("x", ColType::String)], &[vec![Cell::Str("")]]);
        assert_eq!(hash_batch(&null).compare(&hash_batch(&empty), false), Match::Different);
        // Pairs of duplicate rows must not cancel out (xor alone would).
        let aa = batch(&[("x", ColType::Int64)], &[vec![Cell::I64(1)], vec![Cell::I64(1)]]);
        let bb = batch(&[("x", ColType::Int64)], &[vec![Cell::I64(2)], vec![Cell::I64(2)]]);
        assert_ne!(hash_batch(&aa).bag, hash_batch(&bb).bag);
    }

    #[test]
    fn column_order_is_detected_separately() {
        let ab = batch(
            &[("a", ColType::Int64), ("b", ColType::String)],
            &[vec![Cell::I64(1), Cell::Str("x")]],
        );
        let ba = batch(
            &[("b", ColType::String), ("a", ColType::Int64)],
            &[vec![Cell::Str("x"), Cell::I64(1)]],
        );
        assert_eq!(hash_batch(&ab).compare(&hash_batch(&ba), false), Match::ColumnOrder);
    }

    #[test]
    fn cells_do_not_leak_across_columns() {
        let a = batch(
            &[("a", ColType::Int64), ("b", ColType::Int64)],
            &[vec![Cell::I64(1), Cell::I64(2)], vec![Cell::I64(3), Cell::I64(4)]],
        );
        let b = batch(
            &[("a", ColType::Int64), ("b", ColType::Int64)],
            &[vec![Cell::I64(1), Cell::I64(4)], vec![Cell::I64(3), Cell::I64(2)]],
        );
        assert_eq!(hash_batch(&a).compare(&hash_batch(&b), false), Match::Different);
    }

    #[test]
    fn temporal_types_do_not_collide_with_integers() {
        let d = batch(&[("x", ColType::Date)], &[vec![Cell::I32(5)]]);
        let i = batch(&[("x", ColType::Int64)], &[vec![Cell::I64(5)]]);
        assert_eq!(hash_batch(&d).compare(&hash_batch(&i), false), Match::Different);
    }

    #[test]
    fn empty_results() {
        let e1 = batch(&[("x", ColType::Int64)], &[]);
        let e2 = batch(&[("y", ColType::String)], &[]);
        assert_eq!(
            hash_batch(&e1).compare(&hash_batch(&e2), false),
            Match::Same,
            "no rows: equal regardless of type"
        );
        assert_eq!(hash_batch(&e1).group_key(), hash_batch(&e2).group_key());
    }
}
