// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! BigQuery column types as reported by INFORMATION_SCHEMA (`INT64`,
//! `STRING(10)`, `NUMERIC(10, 2)`, `ARRAY<STRUCT<a INT64>>`, ...), reduced to
//! a one-byte [`TypeCode`] for the binder's fast checks. The full text is kept
//! alongside in the image for prompts and exact typing.

/// Top-level type class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
#[allow(missing_docs)]
pub enum TypeCode {
    Unknown = 0,
    Int64 = 1,
    Float64 = 2,
    Numeric = 3,
    BigNumeric = 4,
    Bool = 5,
    String = 6,
    Bytes = 7,
    Date = 8,
    DateTime = 9,
    Time = 10,
    Timestamp = 11,
    Geography = 12,
    Json = 13,
    Interval = 14,
    Range = 15,
    Struct = 16,
    Array = 17,
}

impl TypeCode {
    /// Decode an on-disk tag (unknown tags → `Unknown`, never an error).
    #[must_use]
    pub fn from_tag(t: u8) -> Self {
        match t {
            1 => Self::Int64,
            2 => Self::Float64,
            3 => Self::Numeric,
            4 => Self::BigNumeric,
            5 => Self::Bool,
            6 => Self::String,
            7 => Self::Bytes,
            8 => Self::Date,
            9 => Self::DateTime,
            10 => Self::Time,
            11 => Self::Timestamp,
            12 => Self::Geography,
            13 => Self::Json,
            14 => Self::Interval,
            15 => Self::Range,
            16 => Self::Struct,
            17 => Self::Array,
            _ => Self::Unknown,
        }
    }

    /// Classify a BigQuery type string.
    #[must_use]
    pub fn of(data_type: &str) -> Self {
        let t = data_type.trim();
        let head_end = t.find(['<', '(', ' ']).unwrap_or(t.len());
        let head = &t[..head_end];
        let eq = |k: &str| head.eq_ignore_ascii_case(k);
        if eq("ARRAY") {
            Self::Array
        } else if eq("STRUCT") || eq("RECORD") {
            Self::Struct
        } else if eq("INT64")
            || eq("INTEGER")
            || eq("INT")
            || eq("SMALLINT")
            || eq("BIGINT")
            || eq("TINYINT")
            || eq("BYTEINT")
        {
            Self::Int64
        } else if eq("FLOAT64") || eq("FLOAT") {
            Self::Float64
        } else if eq("NUMERIC") || eq("DECIMAL") {
            Self::Numeric
        } else if eq("BIGNUMERIC") || eq("BIGDECIMAL") {
            Self::BigNumeric
        } else if eq("BOOL") || eq("BOOLEAN") {
            Self::Bool
        } else if eq("STRING") {
            Self::String
        } else if eq("BYTES") {
            Self::Bytes
        } else if eq("DATE") {
            Self::Date
        } else if eq("DATETIME") {
            Self::DateTime
        } else if eq("TIME") {
            Self::Time
        } else if eq("TIMESTAMP") {
            Self::Timestamp
        } else if eq("GEOGRAPHY") {
            Self::Geography
        } else if eq("JSON") {
            Self::Json
        } else if eq("INTERVAL") {
            Self::Interval
        } else if eq("RANGE") {
            Self::Range
        } else {
            Self::Unknown
        }
    }

    /// Numeric (arithmetic, SUM/AVG-able).
    #[must_use]
    pub fn is_numeric(self) -> bool {
        matches!(self, Self::Int64 | Self::Float64 | Self::Numeric | Self::BigNumeric)
    }

    /// Date/time-like (range filters, truncation).
    #[must_use]
    pub fn is_temporal(self) -> bool {
        matches!(self, Self::Date | Self::DateTime | Self::Time | Self::Timestamp)
    }
}

/// For `ARRAY<T>`, the element type text; `None` otherwise.
#[must_use]
pub fn array_element(data_type: &str) -> Option<&str> {
    let t = data_type.trim();
    let inner = t.strip_suffix('>')?;
    let lt = inner.find('<')?;
    inner[..lt].trim().eq_ignore_ascii_case("ARRAY").then(|| inner[lt + 1..].trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_information_schema_types() {
        for (t, c) in [
            ("INT64", TypeCode::Int64),
            ("STRING(10)", TypeCode::String),
            ("NUMERIC(10, 2)", TypeCode::Numeric),
            ("BIGNUMERIC(76, 38)", TypeCode::BigNumeric),
            ("ARRAY<STRUCT<a INT64, b ARRAY<STRING>>>", TypeCode::Array),
            ("STRUCT<a INT64>", TypeCode::Struct),
            ("TIMESTAMP", TypeCode::Timestamp),
            ("RANGE<DATE>", TypeCode::Range),
            ("JSON", TypeCode::Json),
            ("SOMETHING_NEW", TypeCode::Unknown),
        ] {
            assert_eq!(TypeCode::of(t), c, "{t}");
            assert_eq!(TypeCode::from_tag(c as u8), c);
        }
        assert!(
            TypeCode::Numeric.is_numeric()
                && TypeCode::Date.is_temporal()
                && !TypeCode::String.is_numeric()
        );
    }

    #[test]
    fn array_elements() {
        assert_eq!(array_element("ARRAY<STRUCT<a INT64>>"), Some("STRUCT<a INT64>"));
        assert_eq!(array_element("ARRAY<INT64>"), Some("INT64"));
        assert_eq!(array_element("STRUCT<a ARRAY<INT64>>"), None);
        assert_eq!(array_element("INT64"), None);
    }
}
