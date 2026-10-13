// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Expression types.
//!
//! A [`Ty`] is 12 bytes and `Copy`: a type class, the element class for
//! arrays, and — for values that come from a catalog column — the column id,
//! so nested `STRUCT` fields and arrays of structs are navigated through the
//! catalog's field paths instead of a materialised type tree.

use crate::catalog::{ColId, TypeCode};

/// An expression's type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Ty {
    /// Type class (`Array` for arrays).
    pub code: TypeCode,
    /// Element class when `code == Array`.
    pub elem: TypeCode,
    /// The catalog column (or nested field) this value is, when known:
    /// field access and UNNEST navigate its sub-paths.
    pub col: Option<ColId>,
    /// The literal `NULL` (coerces to anything).
    pub null: bool,
}

impl Ty {
    /// Unknown (never produces a type error).
    pub const UNKNOWN: Self =
        Self { code: TypeCode::Unknown, elem: TypeCode::Unknown, col: None, null: false };
    /// Literal NULL.
    pub const NULL: Self =
        Self { code: TypeCode::Unknown, elem: TypeCode::Unknown, col: None, null: true };

    /// A scalar of class `code`.
    #[must_use]
    pub const fn scalar(code: TypeCode) -> Self {
        Self { code, elem: TypeCode::Unknown, col: None, null: false }
    }

    /// An array of `elem`.
    #[must_use]
    pub const fn array(elem: TypeCode) -> Self {
        Self { code: TypeCode::Array, elem, col: None, null: false }
    }

    /// Unknown or NULL: compatible with everything.
    #[must_use]
    pub fn is_open(self) -> bool {
        self.null || self.code == TypeCode::Unknown
    }

    /// Numeric class (or open).
    #[must_use]
    pub fn numeric_or_open(self) -> bool {
        self.is_open() || self.code.is_numeric()
    }

    /// The element type of an array (open for non-arrays and unknowns).
    #[must_use]
    pub fn element(self) -> Self {
        if self.code == TypeCode::Array {
            Self { code: self.elem, elem: TypeCode::Unknown, col: self.col, null: false }
        } else {
            Self::UNKNOWN
        }
    }
}

/// Supertype for arithmetic on two numeric operands (BigQuery coercion:
/// INT64 → NUMERIC → BIGNUMERIC → FLOAT64).
#[must_use]
pub fn numeric_super(a: TypeCode, b: TypeCode) -> TypeCode {
    use TypeCode::{BigNumeric, Float64, Int64, Numeric};
    let rank = |t: TypeCode| match t {
        Int64 => 1,
        Numeric => 2,
        BigNumeric => 3,
        Float64 => 4,
        _ => 0,
    };
    match rank(a).max(rank(b)) {
        1 => Int64,
        2 => Numeric,
        3 => BigNumeric,
        4 => Float64,
        _ => TypeCode::Unknown,
    }
}

/// Can values of these two types be compared with `=`, `<`, `IN`, ...?
/// `literal_rhs` relaxes STRING literals against date/time and numeric-like
/// columns, which BigQuery coerces.
#[must_use]
pub fn comparable(a: Ty, b: Ty, a_is_string_literal: bool, b_is_string_literal: bool) -> bool {
    if a.is_open() || b.is_open() {
        return true;
    }
    let (x, y) = (a.code, b.code);
    if x == y {
        return true;
    }
    if x.is_numeric() && y.is_numeric() {
        return true;
    }
    // String literals coerce to DATE / DATETIME / TIME / TIMESTAMP (and to
    // RANGE / INTERVAL / JSON literal forms).
    let coerces = |lit_is_str: bool, other: TypeCode| {
        lit_is_str
            && (other.is_temporal()
                || matches!(other, TypeCode::Interval | TypeCode::Range | TypeCode::Json))
    };
    if coerces(a_is_string_literal, y) || coerces(b_is_string_literal, x) {
        return true;
    }
    // DATETIME vs TIMESTAMP and DATE vs DATETIME are not implicitly comparable.
    false
}

/// A type named in SQL text (`CAST(x AS NUMERIC)`), classified.
#[must_use]
pub fn from_type_name(name: &str) -> Ty {
    let code = TypeCode::of(name);
    Ty::scalar(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_supertypes() {
        assert_eq!(numeric_super(TypeCode::Int64, TypeCode::Int64), TypeCode::Int64);
        assert_eq!(numeric_super(TypeCode::Int64, TypeCode::Numeric), TypeCode::Numeric);
        assert_eq!(numeric_super(TypeCode::Numeric, TypeCode::Float64), TypeCode::Float64);
        assert_eq!(numeric_super(TypeCode::String, TypeCode::Int64), TypeCode::Int64);
    }

    #[test]
    fn comparability_follows_bigquery() {
        let s = Ty::scalar(TypeCode::String);
        let i = Ty::scalar(TypeCode::Int64);
        let d = Ty::scalar(TypeCode::Date);
        let ts = Ty::scalar(TypeCode::Timestamp);
        assert!(!comparable(i, s, false, false), "INT64 = STRING is a BigQuery error");
        assert!(comparable(d, s, false, true), "DATE = '2026-01-01' coerces");
        assert!(!comparable(d, ts, false, false), "DATE = TIMESTAMP does not");
        assert!(comparable(i, Ty::scalar(TypeCode::Float64), false, false));
        assert!(comparable(Ty::NULL, s, false, false) && comparable(Ty::UNKNOWN, d, false, false));
    }

    #[test]
    fn array_elements_keep_catalog_origin() {
        let a = Ty { col: Some(ColId(3)), ..Ty::array(TypeCode::Struct) };
        let e = a.element();
        assert_eq!((e.code, e.col), (TypeCode::Struct, Some(ColId(3))));
        assert!(Ty::scalar(TypeCode::Int64).element().is_open());
    }
}
