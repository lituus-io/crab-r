// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Binder diagnostics.
//!
//! Each diagnostic carries a source span and, where possible, a suggestion
//! (the closest in-scope name). [`Diag::message`] phrases it the way BigQuery
//! does ("Unrecognized name: statuz; Did you mean status?") because models
//! have seen those messages and repair well against them — and catching the
//! error here costs nothing, where the warehouse would bill a failed job's
//! round trip.

use crab_sql::Sp;
use std::fmt::Write as _;

/// How bad.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    /// The warehouse would reject the query.
    Error,
    /// Likely wrong, but valid.
    Warning,
}

/// What went wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiagKind {
    /// No such table (or CTE).
    UnknownTable,
    /// No such column / alias in scope.
    UnknownName,
    /// A name matches columns of several relations.
    AmbiguousName,
    /// No such field of a STRUCT column.
    UnknownField,
    /// Field access on an ARRAY without UNNEST.
    NeedsUnnest,
    /// No such function.
    UnknownFunction,
    /// Wrong number of arguments.
    ArgCount {
        /// Minimum.
        min: u8,
        /// Maximum (`u8::MAX` = variadic).
        max: u8,
        /// Given.
        got: u8,
    },
    /// Operands of incompatible types.
    TypeMismatch,
    /// Values of this type cannot be compared (GEOGRAPHY, JSON, ARRAY).
    NotComparable(&'static str),
    /// A condition that is not BOOL.
    NotBoolean,
    /// A numeric argument was required.
    NotNumeric,
    /// Aggregate function where aggregation is not allowed.
    AggregateNotAllowed,
    /// Aggregate inside another aggregate's arguments.
    NestedAggregate,
    /// Window function where it is not allowed.
    WindowNotAllowed,
    /// Window function without OVER.
    WindowNeedsOver,
    /// OVER on a non-aggregate, non-window function.
    OverNotAllowed,
    /// A column neither grouped nor aggregated.
    NotGrouped,
    /// A table requiring a partition filter is scanned without one.
    PartitionFilterRequired,
    /// Set-operation branches with different column counts.
    SetOpArity,
    /// A scalar subquery with more than one column.
    SubqueryColumns,
    /// `*` in a position where it is not allowed.
    StarNotAllowed,
    /// `USING` column missing from a side of the join.
    UsingColumn,
    /// Unsupported construct for binding.
    Unsupported,
}

/// A diagnostic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diag {
    /// Kind.
    pub kind: DiagKind,
    /// Severity.
    pub severity: Severity,
    /// Where.
    pub sp: Sp,
    /// Closest valid alternative, when one exists.
    pub suggestion: Option<String>,
}

impl Diag {
    /// Render a BigQuery-style message against the statement's source.
    #[must_use]
    pub fn message(&self, src: &str) -> String {
        let at = self.sp.text(src);
        let mut m = match self.kind {
            DiagKind::UnknownTable => format!("Table not found: {at}"),
            DiagKind::UnknownName => format!("Unrecognized name: {at}"),
            DiagKind::AmbiguousName => format!("Column name {at} is ambiguous"),
            DiagKind::UnknownField => format!("Field name {at} does not exist in STRUCT"),
            DiagKind::NeedsUnnest => format!("Cannot access field {at} on a value with type ARRAY; use UNNEST"),
            DiagKind::UnknownFunction => format!("Function not found: {at}"),
            DiagKind::ArgCount { min, max, got } => {
                if max == u8::MAX {
                    format!("{at} expects at least {min} argument(s), got {got}")
                } else if min == max {
                    format!("{at} expects {min} argument(s), got {got}")
                } else {
                    format!("{at} expects {min} to {max} arguments, got {got}")
                }
            }
            DiagKind::TypeMismatch => format!("No matching signature for {at}: operand types are incompatible"),
            DiagKind::NotComparable(ty) => format!("Equality is not defined for arguments of type {ty} in {at}"),
            DiagKind::NotBoolean => format!("Condition {at} must be BOOL"),
            DiagKind::NotNumeric => format!("{at} requires a numeric argument"),
            DiagKind::AggregateNotAllowed => format!("Aggregate function {at} not allowed here"),
            DiagKind::NestedAggregate => format!("Aggregations of aggregations are not allowed: {at}"),
            DiagKind::WindowNotAllowed => format!("Analytic function {at} not allowed here"),
            DiagKind::WindowNeedsOver => format!("Analytic function {at} requires an OVER clause"),
            DiagKind::OverNotAllowed => format!("{at} is not an aggregate or analytic function; OVER is not allowed"),
            DiagKind::NotGrouped => format!("SELECT list expression references column {at} which is neither grouped nor aggregated"),
            DiagKind::PartitionFilterRequired => {
                format!("Cannot query over table {at} without a filter over column(s) that can be used for partition elimination")
            }
            DiagKind::SetOpArity => "Queries in a set operation must have the same number of columns".to_owned(),
            DiagKind::SubqueryColumns => "Scalar subquery must return exactly one column".to_owned(),
            DiagKind::StarNotAllowed => format!("'*' is not allowed here: {at}"),
            DiagKind::UsingColumn => format!("Column {at} in USING clause not found on both sides of the join"),
            DiagKind::Unsupported => format!("Unsupported construct: {at}"),
        };
        if let Some(s) = &self.suggestion {
            let _ = write!(m, "; Did you mean {s}?");
        }
        m
    }
}

/// Levenshtein distance, case-insensitive, stopping early past `max`.
#[must_use]
pub fn distance(a: &str, b: &str, max: usize) -> usize {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len().abs_diff(b.len()) > max {
        return max + 1;
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0; b.len() + 1];
    for i in 1..=a.len() {
        cur[0] = i;
        let mut row_min = cur[0];
        for j in 1..=b.len() {
            let cost = usize::from(!a[i - 1].eq_ignore_ascii_case(&b[j - 1]));
            cur[j] = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
            row_min = row_min.min(cur[j]);
        }
        if row_min > max {
            return max + 1;
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// The closest candidate to `name` within a length-scaled edit budget.
pub fn suggest<'a>(name: &str, candidates: impl Iterator<Item = &'a str>) -> Option<String> {
    let budget = (name.len() / 3).clamp(1, 3);
    let mut best: Option<(usize, &str)> = None;
    for c in candidates {
        if c.eq_ignore_ascii_case(name) {
            continue; // the same name, just not visible here: no help
        }
        let d = distance(name, c, budget);
        if d <= budget && best.is_none_or(|(bd, bc)| d < bd || (d == bd && c < bc)) {
            best = Some((d, c));
        }
    }
    best.map(|(_, c)| c.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edit_distance() {
        assert_eq!(distance("statuz", "status", 3), 1);
        assert_eq!(distance("Status", "status", 3), 0);
        assert_eq!(distance("abc", "xyzxyz", 2), 3);
        assert_eq!(distance("", "ab", 5), 2);
    }

    #[test]
    fn suggestions_are_close_and_deterministic() {
        let cands = ["status", "order_id", "order_ts", "state"];
        assert_eq!(suggest("statuz", cands.iter().copied()).as_deref(), Some("status"));
        assert_eq!(suggest("ordr_id", cands.iter().copied()).as_deref(), Some("order_id"));
        assert_eq!(suggest("revenue", cands.iter().copied()), None);
    }

    #[test]
    fn messages_read_like_bigquery() {
        let src = "SELECT statuz FROM t";
        let d = Diag {
            kind: DiagKind::UnknownName,
            severity: Severity::Error,
            sp: Sp { start: 7, end: 13 },
            suggestion: Some("status".into()),
        };
        assert_eq!(d.message(src), "Unrecognized name: statuz; Did you mean status?");
    }
}
