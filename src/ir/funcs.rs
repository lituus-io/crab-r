// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The function table: BigQuery functions the binder knows by arity, kind
//! (scalar / aggregate / window) and result-type rule.
//!
//! Lookup is a `match` on the upper-cased name (compiled to a length switch
//! plus comparisons: no hashing, no allocation). `SAFE.` prefixes resolve to
//! the base function. Functions not listed are not errors in `Full` mode
//! (their type is unknown) and are errors with suggestions in `Strict` mode.

use crate::catalog::TypeCode;
use crate::ir::ty::{numeric_super, Ty};

/// Function kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Row-wise.
    Scalar,
    /// Aggregate (optionally windowed with `OVER`).
    Aggregate,
    /// Window-only (requires `OVER`).
    Window,
}

/// Result-type rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ret {
    /// Always this class.
    Fixed(TypeCode),
    /// The type of argument `i`.
    Arg(u8),
    /// The common supertype of arguments `i..`.
    Super(u8),
    /// `SUM`: the numeric argument's class (INT64 stays INT64).
    Sum,
    /// `AVG`: INT64 → FLOAT64, others keep their class.
    Avg,
    /// `ROUND`/`CEIL`/...: INT64 → FLOAT64, others keep their class.
    Round,
    /// `SAFE_DIVIDE`: INT64/INT64 → FLOAT64, else the supertype.
    Div,
    /// An array of argument `i`'s type.
    ArrayOfArg(u8),
    /// An array of this class.
    ArrayOf(TypeCode),
    /// The element type of array argument `i`.
    ElemOfArg(u8),
    /// Not modelled.
    Unknown,
}

/// A known function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Func {
    /// Canonical upper-case name.
    pub name: &'static str,
    /// Minimum arguments.
    pub min: u8,
    /// Maximum arguments (`u8::MAX` = variadic).
    pub max: u8,
    /// Kind.
    pub kind: Kind,
    /// Result rule.
    pub ret: Ret,
    /// The first argument must be numeric.
    pub numeric_arg: bool,
}

const V: u8 = u8::MAX;

macro_rules! table {
    ($name:ident; $($n:literal => ($min:expr, $max:expr, $k:ident, $r:expr $(, $num:literal)?)),* $(,)?) => {
        /// Look a function up by its already upper-cased name.
        fn $name(upper: &str) -> Option<Func> {
            use Kind::*;
            use Ret::*;
            use TypeCode::*;
            Some(match upper {
                $($n => Func { name: $n, min: $min, max: $max, kind: $k, ret: $r, numeric_arg: false $(|| $num)? },)*
                _ => return None,
            })
        }

        /// Every known function name (for suggestions).
        pub const NAMES: &[&str] = &[$($n),*];
    };
}

table! { lookup_upper;
    // Aggregates.
    "COUNT" => (0, 1, Aggregate, Fixed(Int64)),
    "COUNTIF" => (1, 1, Aggregate, Fixed(Int64)),
    "SUM" => (1, 1, Aggregate, Sum, true),
    "AVG" => (1, 1, Aggregate, Avg, true),
    "MIN" => (1, 1, Aggregate, Arg(0)),
    "MAX" => (1, 1, Aggregate, Arg(0)),
    "MIN_BY" => (2, 2, Aggregate, Arg(0)),
    "MAX_BY" => (2, 2, Aggregate, Arg(0)),
    "ANY_VALUE" => (1, 1, Aggregate, Arg(0)),
    "ARRAY_AGG" => (1, 1, Aggregate, ArrayOfArg(0)),
    "ARRAY_CONCAT_AGG" => (1, 1, Aggregate, Arg(0)),
    "STRING_AGG" => (1, 2, Aggregate, Arg(0)),
    "APPROX_COUNT_DISTINCT" => (1, 1, Aggregate, Fixed(Int64)),
    "APPROX_QUANTILES" => (2, 2, Aggregate, ArrayOfArg(0)),
    "APPROX_TOP_COUNT" => (2, 2, Aggregate, Ret::Unknown),
    "APPROX_TOP_SUM" => (3, 3, Aggregate, Ret::Unknown),
    "LOGICAL_AND" => (1, 1, Aggregate, Fixed(Bool)),
    "LOGICAL_OR" => (1, 1, Aggregate, Fixed(Bool)),
    "BIT_AND" => (1, 1, Aggregate, Fixed(Int64)),
    "BIT_OR" => (1, 1, Aggregate, Fixed(Int64)),
    "BIT_XOR" => (1, 1, Aggregate, Fixed(Int64)),
    "STDDEV" => (1, 1, Aggregate, Fixed(Float64), true),
    "STDDEV_SAMP" => (1, 1, Aggregate, Fixed(Float64), true),
    "STDDEV_POP" => (1, 1, Aggregate, Fixed(Float64), true),
    "VARIANCE" => (1, 1, Aggregate, Fixed(Float64), true),
    "VAR_SAMP" => (1, 1, Aggregate, Fixed(Float64), true),
    "VAR_POP" => (1, 1, Aggregate, Fixed(Float64), true),
    "CORR" => (2, 2, Aggregate, Fixed(Float64), true),
    "COVAR_POP" => (2, 2, Aggregate, Fixed(Float64), true),
    "COVAR_SAMP" => (2, 2, Aggregate, Fixed(Float64), true),
    // Window-only.
    "ROW_NUMBER" => (0, 0, Window, Fixed(Int64)),
    "RANK" => (0, 0, Window, Fixed(Int64)),
    "DENSE_RANK" => (0, 0, Window, Fixed(Int64)),
    "PERCENT_RANK" => (0, 0, Window, Fixed(Float64)),
    "CUME_DIST" => (0, 0, Window, Fixed(Float64)),
    "NTILE" => (1, 1, Window, Fixed(Int64)),
    "LAG" => (1, 3, Window, Arg(0)),
    "LEAD" => (1, 3, Window, Arg(0)),
    "FIRST_VALUE" => (1, 1, Window, Arg(0)),
    "LAST_VALUE" => (1, 1, Window, Arg(0)),
    "NTH_VALUE" => (2, 2, Window, Arg(0)),
    "PERCENTILE_CONT" => (2, 2, Window, Fixed(Float64)),
    "PERCENTILE_DISC" => (2, 2, Window, Arg(0)),
    // Conditional.
    "IF" => (3, 3, Scalar, Super(1)),
    "IFNULL" => (2, 2, Scalar, Super(0)),
    "NULLIF" => (2, 2, Scalar, Arg(0)),
    "COALESCE" => (1, V, Scalar, Super(0)),
    "GREATEST" => (1, V, Scalar, Super(0)),
    "LEAST" => (1, V, Scalar, Super(0)),
    // Math.
    "ABS" => (1, 1, Scalar, Arg(0), true),
    "SIGN" => (1, 1, Scalar, Arg(0), true),
    "ROUND" => (1, 3, Scalar, Round, true),
    "TRUNC" => (1, 2, Scalar, Round, true),
    "CEIL" => (1, 1, Scalar, Round, true),
    "CEILING" => (1, 1, Scalar, Round, true),
    "FLOOR" => (1, 1, Scalar, Round, true),
    "SQRT" => (1, 1, Scalar, Fixed(Float64), true),
    "POW" => (2, 2, Scalar, Fixed(Float64), true),
    "POWER" => (2, 2, Scalar, Fixed(Float64), true),
    "EXP" => (1, 1, Scalar, Fixed(Float64), true),
    "LN" => (1, 1, Scalar, Fixed(Float64), true),
    "LOG" => (1, 2, Scalar, Fixed(Float64), true),
    "LOG10" => (1, 1, Scalar, Fixed(Float64), true),
    "MOD" => (2, 2, Scalar, Super(0), true),
    "DIV" => (2, 2, Scalar, Super(0), true),
    "SAFE_DIVIDE" => (2, 2, Scalar, Div, true),
    "IEEE_DIVIDE" => (2, 2, Scalar, Fixed(Float64), true),
    "SAFE_ADD" => (2, 2, Scalar, Super(0), true),
    "SAFE_SUBTRACT" => (2, 2, Scalar, Super(0), true),
    "SAFE_MULTIPLY" => (2, 2, Scalar, Super(0), true),
    "SAFE_NEGATE" => (1, 1, Scalar, Arg(0), true),
    "RAND" => (0, 0, Scalar, Fixed(Float64)),
    "RANGE_BUCKET" => (2, 2, Scalar, Fixed(Int64)),
    "IS_INF" => (1, 1, Scalar, Fixed(Bool)),
    "IS_NAN" => (1, 1, Scalar, Fixed(Bool)),
    "GENERATE_ARRAY" => (2, 3, Scalar, ArrayOfArg(0)),
    "GENERATE_DATE_ARRAY" => (2, 3, Scalar, ArrayOf(Date)),
    "GENERATE_TIMESTAMP_ARRAY" => (3, 3, Scalar, ArrayOf(Timestamp)),
    // Strings.
    "CONCAT" => (1, V, Scalar, Arg(0)),
    "LENGTH" => (1, 1, Scalar, Fixed(Int64)),
    "CHAR_LENGTH" => (1, 1, Scalar, Fixed(Int64)),
    "CHARACTER_LENGTH" => (1, 1, Scalar, Fixed(Int64)),
    "BYTE_LENGTH" => (1, 1, Scalar, Fixed(Int64)),
    "LOWER" => (1, 1, Scalar, Arg(0)),
    "UPPER" => (1, 1, Scalar, Arg(0)),
    "INITCAP" => (1, 2, Scalar, Fixed(String)),
    "TRIM" => (1, 2, Scalar, Arg(0)),
    "LTRIM" => (1, 2, Scalar, Arg(0)),
    "RTRIM" => (1, 2, Scalar, Arg(0)),
    "SUBSTR" => (2, 3, Scalar, Arg(0)),
    "SUBSTRING" => (2, 3, Scalar, Arg(0)),
    "LEFT" => (2, 2, Scalar, Arg(0)),
    "RIGHT" => (2, 2, Scalar, Arg(0)),
    "REPLACE" => (3, 3, Scalar, Arg(0)),
    "TRANSLATE" => (3, 3, Scalar, Arg(0)),
    "REPEAT" => (2, 2, Scalar, Arg(0)),
    "REVERSE" => (1, 1, Scalar, Arg(0)),
    "LPAD" => (2, 3, Scalar, Arg(0)),
    "RPAD" => (2, 3, Scalar, Arg(0)),
    "STRPOS" => (2, 2, Scalar, Fixed(Int64)),
    "INSTR" => (2, 4, Scalar, Fixed(Int64)),
    "STARTS_WITH" => (2, 2, Scalar, Fixed(Bool)),
    "ENDS_WITH" => (2, 2, Scalar, Fixed(Bool)),
    "CONTAINS_SUBSTR" => (2, 3, Scalar, Fixed(Bool)),
    "REGEXP_CONTAINS" => (2, 2, Scalar, Fixed(Bool)),
    "REGEXP_EXTRACT" => (2, 4, Scalar, Arg(0)),
    "REGEXP_EXTRACT_ALL" => (2, 2, Scalar, ArrayOfArg(0)),
    "REGEXP_REPLACE" => (3, 3, Scalar, Arg(0)),
    "SPLIT" => (1, 2, Scalar, ArrayOfArg(0)),
    "FORMAT" => (1, V, Scalar, Fixed(String)),
    "NORMALIZE" => (1, 2, Scalar, Fixed(String)),
    "NORMALIZE_AND_CASEFOLD" => (1, 2, Scalar, Fixed(String)),
    "SOUNDEX" => (1, 1, Scalar, Fixed(String)),
    "ASCII" => (1, 1, Scalar, Fixed(Int64)),
    "CHR" => (1, 1, Scalar, Fixed(String)),
    "STRING" => (1, 2, Scalar, Fixed(String)),
    "TO_HEX" => (1, 1, Scalar, Fixed(String)),
    "FROM_HEX" => (1, 1, Scalar, Fixed(Bytes)),
    "TO_BASE64" => (1, 1, Scalar, Fixed(String)),
    "FROM_BASE64" => (1, 1, Scalar, Fixed(Bytes)),
    "TO_JSON_STRING" => (1, 2, Scalar, Fixed(String)),
    "MD5" => (1, 1, Scalar, Fixed(Bytes)),
    "SHA1" => (1, 1, Scalar, Fixed(Bytes)),
    "SHA256" => (1, 1, Scalar, Fixed(Bytes)),
    "SHA512" => (1, 1, Scalar, Fixed(Bytes)),
    "FARM_FINGERPRINT" => (1, 1, Scalar, Fixed(Int64)),
    "GENERATE_UUID" => (0, 0, Scalar, Fixed(String)),
    "ARRAY_TO_STRING" => (2, 3, Scalar, Fixed(String)),
    "SAFE_CONVERT_BYTES_TO_STRING" => (1, 1, Scalar, Fixed(String)),
    "CODE_POINTS_TO_STRING" => (1, 1, Scalar, Fixed(String)),
    // Dates and times.
    "CURRENT_DATE" => (0, 1, Scalar, Fixed(Date)),
    "CURRENT_DATETIME" => (0, 1, Scalar, Fixed(DateTime)),
    "CURRENT_TIME" => (0, 1, Scalar, Fixed(Time)),
    "CURRENT_TIMESTAMP" => (0, 0, Scalar, Fixed(Timestamp)),
    "DATE" => (1, 3, Scalar, Fixed(Date)),
    "DATETIME" => (1, 6, Scalar, Fixed(DateTime)),
    "TIME" => (1, 3, Scalar, Fixed(Time)),
    "TIMESTAMP" => (1, 2, Scalar, Fixed(Timestamp)),
    "DATE_ADD" => (2, 2, Scalar, Fixed(Date)),
    "DATE_SUB" => (2, 2, Scalar, Fixed(Date)),
    "DATETIME_ADD" => (2, 2, Scalar, Fixed(DateTime)),
    "DATETIME_SUB" => (2, 2, Scalar, Fixed(DateTime)),
    "TIME_ADD" => (2, 2, Scalar, Fixed(Time)),
    "TIME_SUB" => (2, 2, Scalar, Fixed(Time)),
    "TIMESTAMP_ADD" => (2, 2, Scalar, Fixed(Timestamp)),
    "TIMESTAMP_SUB" => (2, 2, Scalar, Fixed(Timestamp)),
    "DATE_DIFF" => (3, 3, Scalar, Fixed(Int64)),
    "DATETIME_DIFF" => (3, 3, Scalar, Fixed(Int64)),
    "TIME_DIFF" => (3, 3, Scalar, Fixed(Int64)),
    "TIMESTAMP_DIFF" => (3, 3, Scalar, Fixed(Int64)),
    "DATE_TRUNC" => (2, 3, Scalar, Arg(0)),
    "DATETIME_TRUNC" => (2, 3, Scalar, Fixed(DateTime)),
    "TIME_TRUNC" => (2, 2, Scalar, Fixed(Time)),
    "TIMESTAMP_TRUNC" => (2, 3, Scalar, Fixed(Timestamp)),
    "DATE_BUCKET" => (2, 3, Scalar, Arg(0)),
    "LAST_DAY" => (1, 2, Scalar, Arg(0)),
    "FORMAT_DATE" => (2, 2, Scalar, Fixed(String)),
    "FORMAT_DATETIME" => (2, 2, Scalar, Fixed(String)),
    "FORMAT_TIME" => (2, 2, Scalar, Fixed(String)),
    "FORMAT_TIMESTAMP" => (2, 3, Scalar, Fixed(String)),
    "PARSE_DATE" => (2, 2, Scalar, Fixed(Date)),
    "PARSE_DATETIME" => (2, 2, Scalar, Fixed(DateTime)),
    "PARSE_TIME" => (2, 2, Scalar, Fixed(Time)),
    "PARSE_TIMESTAMP" => (2, 3, Scalar, Fixed(Timestamp)),
    "UNIX_DATE" => (1, 1, Scalar, Fixed(Int64)),
    "UNIX_SECONDS" => (1, 1, Scalar, Fixed(Int64)),
    "UNIX_MILLIS" => (1, 1, Scalar, Fixed(Int64)),
    "UNIX_MICROS" => (1, 1, Scalar, Fixed(Int64)),
    "TIMESTAMP_SECONDS" => (1, 1, Scalar, Fixed(Timestamp)),
    "TIMESTAMP_MILLIS" => (1, 1, Scalar, Fixed(Timestamp)),
    "TIMESTAMP_MICROS" => (1, 1, Scalar, Fixed(Timestamp)),
    "DATE_FROM_UNIX_DATE" => (1, 1, Scalar, Fixed(Date)),
    // Arrays.
    "ARRAY_LENGTH" => (1, 1, Scalar, Fixed(Int64)),
    "ARRAY_CONCAT" => (1, V, Scalar, Arg(0)),
    "ARRAY_REVERSE" => (1, 1, Scalar, Arg(0)),
    "ARRAY_SLICE" => (3, 3, Scalar, Arg(0)),
    "ARRAY_FIRST" => (1, 1, Scalar, ElemOfArg(0)),
    "ARRAY_LAST" => (1, 1, Scalar, ElemOfArg(0)),
    "ARRAY_INCLUDES" => (2, 2, Scalar, Fixed(Bool)),
    // JSON.
    "JSON_VALUE" => (1, 2, Scalar, Fixed(String)),
    "JSON_EXTRACT_SCALAR" => (1, 2, Scalar, Fixed(String)),
    "JSON_QUERY" => (1, 2, Scalar, Arg(0)),
    "JSON_EXTRACT" => (1, 2, Scalar, Arg(0)),
    "JSON_VALUE_ARRAY" => (1, 2, Scalar, ArrayOf(String)),
    "JSON_EXTRACT_STRING_ARRAY" => (1, 2, Scalar, ArrayOf(String)),
    "JSON_QUERY_ARRAY" => (1, 2, Scalar, Ret::Unknown),
    "JSON_EXTRACT_ARRAY" => (1, 2, Scalar, Ret::Unknown),
    "JSON_TYPE" => (1, 1, Scalar, Fixed(String)),
    "PARSE_JSON" => (1, 2, Scalar, Fixed(Json)),
    "TO_JSON" => (1, 2, Scalar, Fixed(Json)),
    "INT64" => (1, 1, Scalar, Fixed(Int64)),
    "FLOAT64" => (1, 2, Scalar, Fixed(Float64)),
    "BOOL" => (1, 1, Scalar, Fixed(Bool)),
    // Network.
    "NET.HOST" => (1, 1, Scalar, Fixed(String)),
    "NET.REG_DOMAIN" => (1, 1, Scalar, Fixed(String)),
    "NET.PUBLIC_SUFFIX" => (1, 1, Scalar, Fixed(String)),
    "NET.IP_FROM_STRING" => (1, 1, Scalar, Fixed(Bytes)),
    "NET.SAFE_IP_FROM_STRING" => (1, 1, Scalar, Fixed(Bytes)),
    "NET.IP_TO_STRING" => (1, 1, Scalar, Fixed(String)),
    // Misc.
    "SESSION_USER" => (0, 0, Scalar, Fixed(String)),
    "ERROR" => (1, 1, Scalar, Ret::Unknown),
}

/// Look up a function by its (dotted, any-case) name. `SAFE.` is stripped;
/// `ST_*` geography functions are known with an unmodelled result.
#[must_use]
pub fn lookup(name: &str) -> Option<Func> {
    let mut buf = [0u8; 48];
    if name.len() > buf.len() {
        return None;
    }
    for (d, s) in buf.iter_mut().zip(name.bytes()) {
        *d = s.to_ascii_uppercase();
    }
    let upper = std::str::from_utf8(&buf[..name.len()]).ok()?;
    let upper = upper.strip_prefix("SAFE.").unwrap_or(upper);
    if let Some(f) = lookup_upper(upper) {
        return Some(f);
    }
    upper.starts_with("ST_").then_some(Func {
        name: "ST_*",
        min: 0,
        max: V,
        kind: Kind::Scalar,
        ret: Ret::Unknown,
        numeric_arg: false,
    })
}

/// Result type of `f` applied to `args`.
#[must_use]
pub fn result_type(f: &Func, args: &[Ty]) -> Ty {
    let arg = |i: u8| args.get(usize::from(i)).copied().unwrap_or(Ty::UNKNOWN);
    let code_of = |t: Ty| if t.null { TypeCode::Unknown } else { t.code };
    match f.ret {
        Ret::Fixed(c) => Ty::scalar(c),
        Ret::Arg(i) => Ty { col: None, ..arg(i) },
        Ret::Super(i) => {
            let mut best = Ty::UNKNOWN;
            for t in args.iter().skip(usize::from(i)) {
                if t.is_open() {
                    continue;
                }
                if best.is_open() {
                    best = Ty { col: None, ..*t };
                } else if best.code.is_numeric() && t.code.is_numeric() {
                    best = Ty::scalar(numeric_super(best.code, t.code));
                }
            }
            best
        }
        Ret::Sum => match code_of(arg(0)) {
            c if c.is_numeric() => Ty::scalar(c),
            TypeCode::Interval => Ty::scalar(TypeCode::Interval),
            _ => Ty::UNKNOWN,
        },
        Ret::Avg | Ret::Round => match code_of(arg(0)) {
            TypeCode::Int64 => Ty::scalar(TypeCode::Float64),
            c if c.is_numeric() => Ty::scalar(c),
            _ => Ty::UNKNOWN,
        },
        Ret::Div => {
            let (a, b) = (code_of(arg(0)), code_of(arg(1)));
            if a == TypeCode::Int64 && b == TypeCode::Int64 {
                Ty::scalar(TypeCode::Float64)
            } else {
                Ty::scalar(numeric_super(a, b))
            }
        }
        Ret::ArrayOfArg(i) => {
            let a = arg(i);
            Ty { col: a.col, ..Ty::array(code_of(a)) }
        }
        Ret::ArrayOf(c) => Ty::array(c),
        Ret::ElemOfArg(i) => arg(i).element(),
        Ret::Unknown => Ty::UNKNOWN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_is_case_insensitive_and_strips_safe() {
        assert_eq!(lookup("count").unwrap().name, "COUNT");
        assert_eq!(lookup("SAFE.divide"), None, "DIVIDE is not a function");
        assert_eq!(lookup("safe.parse_date").unwrap().name, "PARSE_DATE");
        assert_eq!(lookup("net.host").unwrap().name, "NET.HOST");
        assert_eq!(lookup("ST_DISTANCE").unwrap().ret, Ret::Unknown);
        assert!(lookup("NO_SUCH_FN").is_none());
        assert!(lookup(&"X".repeat(100)).is_none());
    }

    #[test]
    fn kinds() {
        assert_eq!(lookup("SUM").unwrap().kind, Kind::Aggregate);
        assert_eq!(lookup("ROW_NUMBER").unwrap().kind, Kind::Window);
        assert_eq!(lookup("UPPER").unwrap().kind, Kind::Scalar);
        assert!(lookup("SUM").unwrap().numeric_arg);
    }

    #[test]
    fn result_types() {
        let i = Ty::scalar(TypeCode::Int64);
        let n = Ty::scalar(TypeCode::Numeric);
        let f = Ty::scalar(TypeCode::Float64);
        let s = Ty::scalar(TypeCode::String);
        let rt = |name: &str, args: &[Ty]| result_type(&lookup(name).unwrap(), args);
        assert_eq!(rt("SUM", &[i]).code, TypeCode::Int64);
        assert_eq!(rt("AVG", &[i]).code, TypeCode::Float64);
        assert_eq!(rt("AVG", &[n]).code, TypeCode::Numeric);
        assert_eq!(rt("SAFE_DIVIDE", &[i, i]).code, TypeCode::Float64);
        assert_eq!(rt("SAFE_DIVIDE", &[n, i]).code, TypeCode::Numeric);
        assert_eq!(rt("COALESCE", &[Ty::NULL, i, f]).code, TypeCode::Float64);
        assert_eq!(rt("IF", &[Ty::scalar(TypeCode::Bool), s, Ty::NULL]).code, TypeCode::String);
        let agg = rt("ARRAY_AGG", &[s]);
        assert_eq!((agg.code, agg.elem), (TypeCode::Array, TypeCode::String));
        assert_eq!(rt("ARRAY_FIRST", &[agg]).code, TypeCode::String);
        assert_eq!(
            rt("DATE_TRUNC", &[Ty::scalar(TypeCode::Timestamp), s]).code,
            TypeCode::Timestamp
        );
        assert_eq!(rt("COUNT", &[]).code, TypeCode::Int64);
    }

    #[test]
    fn names_are_upper_case_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for n in NAMES {
            assert_eq!(*n, n.to_ascii_uppercase());
            assert!(seen.insert(*n), "duplicate {n}");
            assert!(lookup(n).is_some());
        }
        assert!(NAMES.len() > 180);
    }
}
