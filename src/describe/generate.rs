// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! The two model interactions of a table's describe loop.
//!
//! 1. **Draft and check-writing**: evidence in; per column a description,
//!    semantic type and unit, plus the **validation checks the model wants
//!    run** (aggregate expressions over the table's rows, or whole SELECTs
//!    for claims that need a join); a table description and its grain with
//!    checks.
//! 2. **Judge and revise**: every check with its result in; a verdict per
//!    check (supports / contradicts / inconclusive) with a one-line reason,
//!    revised descriptions for affected columns, confidence, and optionally
//!    more checks for one more round.
//!
//! Both replies are constrained by a JSON schema (column names and every
//! category are enums) when the model accepts one, and parsed defensively
//! either way: unknown columns, bad categories and malformed items are
//! dropped, never trusted.

use crate::base::json::{escape_into, Event, Scanner, Writer};

/// Semantic types a column may be given.
pub const SEMANTIC_TYPES: &[&str] = &[
    "identifier",
    "foreign_key",
    "name",
    "category",
    "flag",
    "amount",
    "currency_amount",
    "quantity",
    "count",
    "ratio",
    "percentage",
    "score",
    "date",
    "timestamp",
    "duration",
    "email",
    "phone",
    "url",
    "country",
    "region",
    "city",
    "postal_code",
    "address",
    "geography",
    "text",
    "json",
    "other",
];

/// Verdicts of the judge.
pub const VERDICTS: &[&str] = &["supports", "contradicts", "inconclusive"];

/// Confidence levels.
pub const CONFIDENCE: &[&str] = &["high", "medium", "low"];

/// System instruction of the draft call.
pub const DRAFT_SYSTEM: &str = "\
You document BigQuery tables for a data catalog, and you prove what you write.
For each column listed under DESCRIBE, write:
- description: 1-2 sentences for analysts (max 250 characters). Lead with what the value means, then how it is derived. Only what the SQL, the upstream descriptions and the profile support: never invent business intent, and never claim how or by whom values are assigned (sequentially, by a team, on creation) unless the SQL shows it. Name units and currencies only when evident. Write what stays true as data arrives: shares ('about 12% NULL') rather than row counts, and never a count relative to the current date or time (it changes daily); describe such anomalies in words ('some timestamps are in the future').
- semantic_type and unit (empty when none).
- checks: 1-3 validation checks that would expose a wrong description. Each check is a claim in words, an SQL aggregate expression over THIS table's rows only (no FROM, no subqueries) returning one number or boolean, and expect: the result that confirms the claim, written as 0, > 0, >= 1, < 100, between 1 and 4, true or false. Examples: never NULL -> COUNTIF(c IS NULL), expect 0; unique -> COUNT(*) - COUNT(DISTINCT c), expect 0; values as listed -> COUNTIF(c NOT IN ('A', 'B')), expect 0; non-negative -> COUNTIF(c < 0), expect 0; range -> MIN(c), expect >= 0; derived as documented -> COUNTIF(c != <recomputation from other columns>), expect 0.
For each column listed under AUDIT (it already has a description; its data profile is withheld on purpose): in statements, quote VERBATIM each testable statement the existing description makes (NULL or not, allowed values, ranges, uniqueness, derivation), each with an aggregate expr and expect set to what the STATEMENT asserts, written from the description alone (e.g. 'never NULL' -> COUNTIF(c IS NULL), expect 0; 'One of: A, B' -> COUNTIF(c NOT IN ('A', 'B')), expect 0). A statement that only names a meaning or a unit ('Price paid, in USD.') is not testable: leave it out; statements may be empty. Repeat the existing description unchanged as description. checks are for claims of your own.
For the table: a description (2-4 sentences: what one row is, main sources, key filters), its grain (the columns that make a row unique) and table_checks: whole read-only SELECT statements returning one row with one number (with expect), for claims needing more than one expression, such as the grain (SELECT COUNT(*) - COUNT(DISTINCT FORMAT('%T', STRUCT(a, b))) AS dup FROM `table`, expect 0) or a foreign key (SELECT COUNTIF(p.id IS NULL) AS orphans FROM `table` t LEFT JOIN `parent` p ON t.k = p.id, expect 0).
Use only column names that exist. Write GoogleSQL.";

/// System instruction of the judge call.
pub const JUDGE_SYSTEM: &str = "\
You verify data-catalog descriptions against evidence. For every check, decide whether its result supports the claim, contradicts it, or is inconclusive (for example a rejected check, or a result computed on a sample that cannot decide uniqueness), with a one-line reason quoting the result exactly as given. Where a line says HOLDS or VIOLATED, the result was compared with the expected value mechanically: HOLDS supports the claim, VIOLATED contradicts it. Never state a number that is not in the evidence. Remove statements that no check, the SQL or the profile supports (for example how or by whom values are assigned).
Then give the final description of every column listed. A column whose existing description is being audited keeps it unchanged unless a check marked [tests existing] is VIOLATED or contradicted; then correct exactly what is wrong. Otherwise: keep a draft that the checks support; correct it when a check contradicts it (state what the data shows as a share, e.g. 'NULL for about 12% of rows', never as a row count or relative to the current time); never keep a statement a check contradicts. Give confidence: high when checks support it, medium when evidence is indirect or inconclusive, low when contradicted and uncertain.
Give the final table description. Optionally request up to 3 more checks only when a contradiction or a rejected check leaves an important claim undecided: expr is an aggregate over the table's rows such as COUNTIF(<condition>) (never a bare condition), with expect.";

/// A small JSON value, for model replies (small, untrusted, parsed once).
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// `null`
    Null,
    /// `true` / `false`
    Bool(bool),
    /// A number, as written.
    Num(String),
    /// A string.
    Str(String),
    /// An array.
    Arr(Vec<Value>),
    /// An object (members in order).
    Obj(Vec<(String, Value)>),
}

impl Value {
    /// Parse `text` (a markdown fence around it is tolerated).
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let t = text.trim();
        let t = t.strip_prefix("```json").or_else(|| t.strip_prefix("```")).unwrap_or(t);
        let t = t.strip_suffix("```").unwrap_or(t).trim();
        let mut s = Scanner::new(t);
        let first = s.expect().ok()?;
        let mut buf = String::new();
        Self::from_event(&mut s, first, &mut buf, 0)
    }

    fn from_event(
        s: &mut Scanner<'_>,
        ev: Event<'_>,
        buf: &mut String,
        depth: u32,
    ) -> Option<Self> {
        if depth > 32 {
            return None;
        }
        Some(match ev {
            Event::Null => Self::Null,
            Event::Bool(b) => Self::Bool(b),
            Event::Num(n) => Self::Num(n.to_owned()),
            Event::Str(v) => Self::Str(v.decode(buf).ok()?.to_owned()),
            Event::ArrayStart => {
                let mut out = Vec::new();
                loop {
                    match s.expect().ok()? {
                        Event::ArrayEnd => break,
                        e => out.push(Self::from_event(s, e, buf, depth + 1)?),
                    }
                }
                Self::Arr(out)
            }
            Event::ObjectStart => {
                let mut out = Vec::new();
                loop {
                    match s.expect().ok()? {
                        Event::ObjectEnd => break,
                        Event::Key(k) => {
                            let key = k.decode(buf).ok()?.to_owned();
                            let e = s.expect().ok()?;
                            out.push((key, Self::from_event(s, e, buf, depth + 1)?));
                        }
                        _ => return None,
                    }
                }
                Self::Obj(out)
            }
            _ => return None,
        })
    }

    /// Member `k` of an object.
    #[must_use]
    pub fn get(&self, k: &str) -> Option<&Value> {
        match self {
            Self::Obj(m) => m.iter().find(|(n, _)| n == k).map(|(_, v)| v),
            _ => None,
        }
    }

    /// A string member, trimmed, non-empty.
    #[must_use]
    pub fn str(&self, k: &str) -> Option<&str> {
        match self.get(k) {
            Some(Self::Str(s)) => Some(s.trim()).filter(|s| !s.is_empty()),
            _ => None,
        }
    }

    /// Array elements (empty when absent).
    #[must_use]
    pub fn arr(&self, k: &str) -> &[Value] {
        match self.get(k) {
            Some(Self::Arr(a)) => a,
            _ => &[],
        }
    }

    /// An unsigned integer member.
    #[must_use]
    pub fn uint(&self, k: &str) -> Option<u64> {
        match self.get(k) {
            Some(Self::Num(n)) => n.parse().ok(),
            Some(Self::Str(n)) => n.trim().parse().ok(),
            _ => None,
        }
    }
}

/// A check as the model wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DraftCheck {
    /// Column (`None`: table).
    pub column: Option<String>,
    /// The claim in words.
    pub claim: String,
    /// Aggregate expression, or a whole SELECT (table checks).
    pub sql: String,
    /// `sql` is a whole SELECT.
    pub query: bool,
    /// The result that confirms the claim (`0`, `> 0`, `between 1 and 4`).
    pub expect: Option<String>,
    /// The claim comes from the column's existing description (audit).
    pub tests_existing: bool,
}

/// A column's draft.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDraft {
    /// Column (lower-cased).
    pub column: String,
    /// Description.
    pub description: String,
    /// Semantic type (one of [`SEMANTIC_TYPES`]).
    pub semantic_type: String,
    /// Unit, when evident.
    pub unit: Option<String>,
}

/// The draft reply.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Draft {
    /// Table description.
    pub table_description: Option<String>,
    /// Grain columns.
    pub grain: Vec<String>,
    /// Column drafts.
    pub columns: Vec<ColumnDraft>,
    /// Checks (column and table).
    pub checks: Vec<DraftCheck>,
}

/// A verdict on one check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verdict {
    /// Check id.
    pub check: u32,
    /// One of [`VERDICTS`].
    pub verdict: String,
    /// Why.
    pub reason: String,
}

/// A column's final description from the judge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Judged {
    /// Column.
    pub column: String,
    /// Description.
    pub description: String,
    /// One of [`CONFIDENCE`].
    pub confidence: String,
}

/// The judge's reply.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Judgement {
    /// Per check.
    pub verdicts: Vec<Verdict>,
    /// Per column.
    pub columns: Vec<Judged>,
    /// Table description.
    pub table_description: Option<String>,
    /// More checks for one more round.
    pub more_checks: Vec<DraftCheck>,
}

fn enum_array(w: &mut Writer<'_>, values: &[&str]) {
    w.key("enum").begin_array();
    for v in values {
        w.string(v);
    }
    w.end_array();
}

fn string_prop(w: &mut Writer<'_>, name: &str) {
    w.key(name).begin_object().key("type").string("string").end_object();
}

fn check_items(w: &mut Writer<'_>, sql_key: &str) {
    w.key("type")
        .string("array")
        .key("items")
        .begin_object()
        .key("type")
        .string("object")
        .key("properties")
        .begin_object();
    string_prop(w, "claim");
    string_prop(w, sql_key);
    string_prop(w, "expect");
    w.end_object()
        .key("required")
        .begin_array()
        .string("claim")
        .string(sql_key)
        .string("expect")
        .end_array()
        .end_object();
}

/// JSON schema of the draft reply: `columns` to describe or audit, and
/// every column of the table (`all`) for the grain.
#[must_use]
pub fn draft_schema(columns: &[String], all: &[String]) -> String {
    let names: Vec<&str> = columns.iter().map(String::as_str).collect();
    let every: Vec<&str> = all.iter().map(String::as_str).collect();
    let mut out = String::with_capacity(1024 + columns.len() * 24);
    let mut w = Writer::new(&mut out);
    w.begin_object().key("type").string("object").key("properties").begin_object();
    string_prop(&mut w, "table_description");
    w.key("grain")
        .begin_object()
        .key("type")
        .string("array")
        .key("items")
        .begin_object()
        .key("type")
        .string("string");
    enum_array(&mut w, &every);
    w.end_object().end_object();
    w.key("table_checks").begin_object();
    check_items(&mut w, "sql");
    w.end_object();
    w.key("columns")
        .begin_object()
        .key("type")
        .string("array")
        .key("items")
        .begin_object()
        .key("type")
        .string("object")
        .key("properties")
        .begin_object();
    w.key("column").begin_object().key("type").string("string");
    enum_array(&mut w, &names);
    w.end_object();
    string_prop(&mut w, "description");
    w.key("semantic_type").begin_object().key("type").string("string");
    enum_array(&mut w, SEMANTIC_TYPES);
    w.end_object();
    string_prop(&mut w, "unit");
    w.key("checks").begin_object();
    check_items(&mut w, "expr");
    w.end_object();
    w.key("statements").begin_object();
    check_items(&mut w, "expr");
    w.end_object();
    w.end_object()
        .key("required")
        .begin_array()
        .string("column")
        .string("description")
        .string("semantic_type")
        .string("checks")
        .end_array()
        .end_object()
        .end_object();
    w.end_object()
        .key("required")
        .begin_array()
        .string("table_description")
        .string("columns")
        .end_array()
        .end_object();
    out
}

/// JSON schema of the judge reply.
#[must_use]
pub fn judge_schema(columns: &[String], check_ids: &[u32]) -> String {
    let names: Vec<&str> = columns.iter().map(String::as_str).collect();
    let mut out = String::with_capacity(1024);
    let mut w = Writer::new(&mut out);
    w.begin_object().key("type").string("object").key("properties").begin_object();
    w.key("verdicts")
        .begin_object()
        .key("type")
        .string("array")
        .key("items")
        .begin_object()
        .key("type")
        .string("object")
        .key("properties")
        .begin_object();
    // String enum: Vertex AI accepts enums of strings only.
    w.key("check").begin_object().key("type").string("string");
    w.key("enum").begin_array();
    for id in check_ids {
        w.string(&id.to_string());
    }
    w.end_array().end_object();
    w.key("verdict").begin_object().key("type").string("string");
    enum_array(&mut w, VERDICTS);
    w.end_object();
    string_prop(&mut w, "reason");
    w.end_object()
        .key("required")
        .begin_array()
        .string("check")
        .string("verdict")
        .string("reason")
        .end_array()
        .end_object()
        .end_object();
    w.key("columns")
        .begin_object()
        .key("type")
        .string("array")
        .key("items")
        .begin_object()
        .key("type")
        .string("object")
        .key("properties")
        .begin_object();
    w.key("column").begin_object().key("type").string("string");
    enum_array(&mut w, &names);
    w.end_object();
    string_prop(&mut w, "description");
    w.key("confidence").begin_object().key("type").string("string");
    enum_array(&mut w, CONFIDENCE);
    w.end_object();
    w.end_object()
        .key("required")
        .begin_array()
        .string("column")
        .string("description")
        .string("confidence")
        .end_array()
        .end_object()
        .end_object();
    string_prop(&mut w, "table_description");
    w.key("more_checks")
        .begin_object()
        .key("type")
        .string("array")
        .key("items")
        .begin_object()
        .key("type")
        .string("object")
        .key("properties")
        .begin_object();
    w.key("column").begin_object().key("type").string("string");
    enum_array(&mut w, &names);
    w.end_object();
    string_prop(&mut w, "claim");
    string_prop(&mut w, "expr");
    string_prop(&mut w, "sql");
    string_prop(&mut w, "expect");
    w.end_object()
        .key("required")
        .begin_array()
        .string("claim")
        .string("expect")
        .end_array()
        .end_object()
        .end_object();
    w.end_object()
        .key("required")
        .begin_array()
        .string("verdicts")
        .string("columns")
        .end_array()
        .end_object();
    out
}

/// Longest description kept (BigQuery allows 1 024 characters).
pub const MAX_DESCRIPTION: usize = 1_024;

fn clip(s: &str) -> String {
    s.chars().take(MAX_DESCRIPTION).collect::<String>().trim().to_owned()
}

/// Parse a draft reply: descriptions only for `allowed` columns, grain
/// from any of the table's columns (`all`).
#[must_use]
pub fn parse_draft(
    text: &str,
    allowed: &[String],
    all: &[String],
    existing: &[(String, String)],
) -> Option<Draft> {
    let v = Value::parse(text)?;
    let ok = |c: &str| allowed.iter().any(|a| a.eq_ignore_ascii_case(c));
    let in_table = |c: &str| all.iter().any(|a| a.eq_ignore_ascii_case(c));
    let mut d =
        Draft { table_description: v.str("table_description").map(clip), ..Draft::default() };
    for g in v.arr("grain") {
        if let Value::Str(c) = g {
            if in_table(c) {
                d.grain.push(c.to_ascii_lowercase());
            }
        }
    }
    for c in v.arr("table_checks") {
        if let (Some(claim), Some(sql)) = (c.str("claim"), c.str("sql")) {
            d.checks.push(DraftCheck {
                column: None,
                claim: claim.to_owned(),
                sql: sql.to_owned(),
                query: true,
                expect: c.str("expect").map(str::to_owned),
                tests_existing: false,
            });
        }
    }
    for c in v.arr("columns") {
        let Some(col) = c.str("column").filter(|c| ok(c)) else { continue };
        let col = col.to_ascii_lowercase();
        if d.columns.iter().any(|x| x.column == col) {
            continue;
        }
        let st = c.str("semantic_type").filter(|s| SEMANTIC_TYPES.contains(s)).unwrap_or("other");
        if let Some(desc) = c.str("description") {
            d.columns.push(ColumnDraft {
                column: col.clone(),
                description: clip(desc),
                semantic_type: st.to_owned(),
                unit: c.str("unit").map(str::to_owned),
            });
        }
        for k in c.arr("checks").iter().take(4) {
            if let (Some(claim), Some(expr)) = (k.str("claim"), k.str("expr")) {
                d.checks.push(DraftCheck {
                    column: Some(col.clone()),
                    claim: claim.to_owned(),
                    sql: expr.to_owned(),
                    query: false,
                    expect: k.str("expect").map(str::to_owned),
                    tests_existing: false,
                });
            }
        }
        // Audit statements: kept only when quoted from the existing
        // description (an invented claim cannot flag a description).
        let desc = existing.iter().find(|(c, _)| *c == col).map(|(_, d)| squash(d));
        for k in c.arr("statements").iter().take(6) {
            let (Some(stmt), Some(expr), Some(text)) =
                (k.str("claim"), k.str("expr"), desc.as_deref())
            else {
                continue;
            };
            let q = squash(stmt);
            if q.len() < 3 || !text.contains(&q) || !testable(stmt) {
                continue;
            }
            d.checks.push(DraftCheck {
                column: Some(col.clone()),
                claim: stmt.to_owned(),
                sql: expr.to_owned(),
                query: false,
                expect: k.str("expect").map(str::to_owned),
                tests_existing: true,
            });
        }
    }
    Some(d)
}

/// Does a statement assert something a query can check (nullness, allowed
/// values, uniqueness, bounds, format)? A statement that only names a
/// meaning or a unit ("Customer identifier.", "Price paid, in USD.") does
/// not, and must never be "tested" into a contradiction.
#[must_use]
pub fn testable(statement: &str) -> bool {
    const MARKERS: &[&str] = &[
        "null",
        "never",
        "always",
        "empty",
        "blank",
        "missing",
        "one of",
        "only",
        "either",
        "values",
        "unique",
        "distinct",
        "duplicate",
        "between",
        "at least",
        "at most",
        "positive",
        "negative",
        "zero",
        "greater",
        "less",
        "more than",
        "fewer",
        "maximum",
        "minimum",
        "range",
        "lowercase",
        "lower-case",
        "uppercase",
        "upper-case",
        "format",
        "iso",
        "true",
        "false",
    ];
    let lc = statement.to_ascii_lowercase();
    let words: Vec<&str> =
        lc.split(|c: char| !c.is_alphanumeric() && c != '-').filter(|w| !w.is_empty()).collect();
    let has = |m: &str| if m.contains(' ') { lc.contains(m) } else { words.contains(&m) };
    MARKERS.iter().any(|m| has(m))
        || statement.contains('\'')
        || statement.contains('%')
        || statement.chars().any(|c| c.is_ascii_digit())
}

/// Lower-cased alphanumerics only (quotes and spacing do not matter when
/// matching a quoted statement to its description).
fn squash(s: &str) -> String {
    s.chars().filter(|c| c.is_alphanumeric()).flat_map(char::to_lowercase).collect()
}

/// Parse a judge reply.
#[must_use]
pub fn parse_judgement(text: &str, allowed: &[String], check_ids: &[u32]) -> Option<Judgement> {
    let v = Value::parse(text)?;
    let ok = |c: &str| allowed.iter().any(|a| a.eq_ignore_ascii_case(c));
    let mut j = Judgement {
        table_description: v.str("table_description").map(clip),
        ..Judgement::default()
    };
    for x in v.arr("verdicts") {
        let (Some(id), Some(verdict)) = (x.uint("check"), x.str("verdict")) else { continue };
        let id = u32::try_from(id).unwrap_or(u32::MAX);
        if check_ids.contains(&id) && VERDICTS.contains(&verdict) {
            j.verdicts.push(Verdict {
                check: id,
                verdict: verdict.to_owned(),
                reason: x.str("reason").unwrap_or("").chars().take(300).collect(),
            });
        }
    }
    for x in v.arr("columns") {
        let (Some(col), Some(desc)) = (x.str("column").filter(|c| ok(c)), x.str("description"))
        else {
            continue;
        };
        let conf = x.str("confidence").filter(|c| CONFIDENCE.contains(c)).unwrap_or("medium");
        j.columns.push(Judged {
            column: col.to_ascii_lowercase(),
            description: clip(desc),
            confidence: conf.to_owned(),
        });
    }
    for x in v.arr("more_checks").iter().take(3) {
        let Some(claim) = x.str("claim") else { continue };
        let column = x.str("column").filter(|c| ok(c)).map(str::to_ascii_lowercase);
        match (x.str("expr"), x.str("sql")) {
            (Some(e), _) => j.more_checks.push(DraftCheck {
                column,
                claim: claim.to_owned(),
                sql: e.to_owned(),
                query: false,
                expect: x.str("expect").map(str::to_owned),
                tests_existing: x.get("tests_existing") == Some(&Value::Bool(true)),
            }),
            (None, Some(q)) => j.more_checks.push(DraftCheck {
                column,
                claim: claim.to_owned(),
                sql: q.to_owned(),
                query: true,
                expect: x.str("expect").map(str::to_owned),
                tests_existing: false,
            }),
            _ => {}
        }
    }
    Some(j)
}

/// A JSON string literal (for embedding evidence text in prompts).
#[must_use]
pub fn quoted(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    escape_into(s, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols() -> Vec<String> {
        vec!["amount".into(), "status".into()]
    }

    #[test]
    fn drafts_are_parsed_defensively() {
        let reply = r#"```json
        {"table_description": "One row per order.", "grain": ["order_id", "status"],
         "table_checks": [{"claim": "unique per status", "sql": "SELECT COUNT(*) - COUNT(DISTINCT status) FROM `p.d.t`"}],
         "columns": [
           {"column": "amount", "description": "Order amount in USD.", "semantic_type": "currency_amount", "unit": "USD",
            "checks": [{"claim": "non-negative", "expr": "COUNTIF(amount < 0)"}, {"claim": "x", "expr": "a"}, {"claim": "y", "expr": "b"}, {"claim": "z", "expr": "c"}]},
           {"column": "invented", "description": "nope", "semantic_type": "text", "checks": []},
           {"column": "STATUS", "description": "Order state.", "semantic_type": "made_up", "checks": [{"claim": "no expr"}]}
         ]}
        ```"#;
        let d = parse_draft(
            reply,
            &cols(),
            &["status".to_owned(), "order_id".to_owned(), "amount".to_owned()],
            &[],
        )
        .unwrap();
        assert_eq!(d.grain, vec!["order_id", "status"], "grain may use any column of the table");
        assert_eq!(d.columns.len(), 2, "invented column dropped");
        assert_eq!(d.columns[1].column, "status");
        assert_eq!(d.columns[1].semantic_type, "other", "unknown category falls back");
        assert_eq!(
            d.checks.iter().filter(|c| c.column.as_deref() == Some("amount")).count(),
            4,
            "at most 4 checks per column"
        );
        assert!(d.checks.iter().any(|c| c.query && c.column.is_none()));
        assert!(parse_draft("not json", &cols(), &cols(), &[]).is_none());
    }

    #[test]
    fn audit_statements_must_be_quoted_from_the_description() {
        let existing = vec![(
            "status".to_owned(),
            "One of: Shipped, Complete, Processing. Never NULL.".to_owned(),
        )];
        let reply = r#"{"table_description": "t", "columns": [{"column": "status", "description": "One of: Shipped, Complete, Processing. Never NULL.", "semantic_type": "category", "checks": [],
            "statements": [
              {"claim": "One of: Shipped, Complete, Processing", "expr": "COUNTIF(status NOT IN ('Shipped', 'Complete', 'Processing'))", "expect": "0"},
              {"claim": "never null", "expr": "COUNTIF(status IS NULL)", "expect": "0"},
              {"claim": "status is unique", "expr": "COUNT(*) - COUNT(DISTINCT status)", "expect": "0"}]}]}"#;
        let d = parse_draft(reply, &cols(), &cols(), &existing).unwrap();
        let claims: Vec<&str> =
            d.checks.iter().filter(|c| c.tests_existing).map(|c| c.claim.as_str()).collect();
        assert_eq!(
            claims,
            vec!["One of: Shipped, Complete, Processing", "never null"],
            "an invented claim is not an audit"
        );
        for (s, t) in [
            ("Customer identifier.", false),
            ("Price paid for the item, in USD.", false),
            ("Return timestamp; never NULL.", true),
            ("One of: A, B", true),
            ("Always set", true),
            ("Between 1 and 4 items", true),
            ("Unique per order", true),
            ("'Y' or 'N'", true),
        ] {
            assert_eq!(testable(s), t, "{s}");
        }
    }

    #[test]
    fn judgements_keep_only_known_checks_and_columns() {
        let reply = r#"{"verdicts": [{"check": 1, "verdict": "contradicts", "reason": "12% NULL"}, {"check": 99, "verdict": "supports", "reason": "?"}, {"check": "2", "verdict": "maybe", "reason": "?"}],
            "columns": [{"column": "status", "description": "Order state; NULL for about 12% of rows.", "confidence": "high"}, {"column": "ghost", "description": "x", "confidence": "high"}],
            "table_description": "One row per order.",
            "more_checks": [{"column": "status", "claim": "values", "expr": "COUNTIF(status NOT IN ('A'))"}, {"claim": "fk", "sql": "SELECT 1"}, {"claim": "nothing"}]}"#;
        let j = parse_judgement(reply, &cols(), &[1, 2]).unwrap();
        assert_eq!(
            j.verdicts,
            vec![Verdict { check: 1, verdict: "contradicts".into(), reason: "12% NULL".into() }]
        );
        assert_eq!(j.columns.len(), 1);
        assert_eq!(j.more_checks.len(), 2);
        assert!(j.more_checks[1].query);
    }

    #[test]
    fn schemas_constrain_names_and_categories() {
        let s = draft_schema(&cols(), &cols());
        assert!(s.contains(r#""column":{"type":"string","enum":["amount","status"]}"#), "{s}");
        assert!(s.contains("currency_amount"));
        let j = judge_schema(&cols(), &[3, 4]);
        assert!(j.contains(r#""check":{"type":"string","enum":["3","4"]}"#), "{j}");
        assert!(Value::parse(&s).is_some() && Value::parse(&j).is_some(), "schemas are valid JSON");
    }
}
