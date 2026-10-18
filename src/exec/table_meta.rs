// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! `table_meta`: exact table metadata for many tables at once, each with its
//! own typed outcome.
//!
//! One `tables.get?view=BASIC` per table (schema and partitioning; no
//! statistics, no rows), all in flight together, so a batch of twenty costs
//! about one round trip. A table's answer keeps BigQuery's own bytes: the
//! schema is the resource's `schema` object with only these changes, made by
//! splicing the raw JSON (so policy tags, collation, default values and
//! members this code has never heard of survive verbatim):
//!
//! - the columns named in `omit_columns` (dotted paths, any case) removed;
//! - at most [`Limits::max_columns`] columns, nested at most
//!   [`Limits::max_depth`] deep, are kept (the rest counted in `omitted`);
//! - descriptions longer than [`Limits::max_description`] characters cut.
//!
//! A batch never fails: every table gets one [`Outcome`] (`ok`, `no_access`,
//! `not_found`, `no_credentials`, `expired`, `unreachable`,
//! `invalid_name`). A request answered with a 4xx is never repeated.

use crate::base::json::{escape_into, Event, Scanner, Writer};
use crate::base::key::{BqName, Level};
use objstore_r::transport::{Request, Retry, Transport};
use std::borrow::Cow;
use std::ops::Range;

/// What became of one table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Read.
    Ok,
    /// The caller may not read the table's metadata (403).
    NoAccess,
    /// No such table, or not visible to the caller (404).
    NotFound,
    /// No credential was supplied.
    NoCredentials,
    /// The credential was refused as expired or invalid (401).
    Expired,
    /// BigQuery could not be reached, or answered with a server error, a
    /// throttle, or something that is not a table resource.
    Unreachable,
    /// Not a `project.dataset.table` name (no request was made).
    InvalidName,
}

impl Outcome {
    /// The outcome's wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NoAccess => "no_access",
            Self::NotFound => "not_found",
            Self::NoCredentials => "no_credentials",
            Self::Expired => "expired",
            Self::Unreachable => "unreachable",
            Self::InvalidName => "invalid_name",
        }
    }

    /// The outcome of an HTTP status that is not a success.
    #[must_use]
    pub fn of_status(status: u16) -> Self {
        match status {
            401 => Self::Expired,
            403 => Self::NoAccess,
            404 => Self::NotFound,
            400 | 405..=428 | 430..=499 => Self::InvalidName,
            _ => Self::Unreachable,
        }
    }

    /// The outcome of a transport failure.
    #[must_use]
    pub fn of_error(e: &objstore_r::Error) -> Self {
        match e {
            objstore_r::Error::NoCredentials { .. } => Self::NoCredentials,
            objstore_r::Error::Auth { .. } => Self::Expired,
            _ => Self::Unreachable,
        }
    }
}

/// Bounds on what one table's answer holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Columns kept (nested fields count), in schema order.
    pub max_columns: usize,
    /// Deepest nesting kept (a top-level column is depth 1).
    pub max_depth: usize,
    /// Longest description kept, characters.
    pub max_description: usize,
    /// Largest table resource read, bytes.
    pub max_resource_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_columns: 2_000,
            max_depth: 15,
            max_description: 1_000,
            max_resource_bytes: 8 << 20,
        }
    }
}

/// What to read.
#[derive(Clone, Copy, Debug, Default)]
pub struct MetaOptions<'a> {
    /// Include a view's or materialized view's defining SQL.
    pub include_view_sql: bool,
    /// Columns left out, as dotted paths in any case (`address.city`).
    pub omit_columns: &'a [&'a str],
    /// Bounds.
    pub limits: Limits,
}

/// One table's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableAnswer {
    /// The name as asked.
    pub table: String,
    /// What became of it.
    pub outcome: Outcome,
    /// For an `ok`: the answer as a JSON object (see [`table_meta`]);
    /// otherwise a short message (never a token or a response body).
    pub detail: String,
}

impl TableAnswer {
    fn failed(table: &str, outcome: Outcome, message: impl Into<String>) -> Self {
        Self { table: table.to_owned(), outcome, detail: message.into() }
    }

    /// The answer as JSON: `{"table", "available", "reason", ...}`, with the
    /// metadata members for an `ok` and `"message"` otherwise.
    #[must_use]
    pub fn to_json(&self) -> String {
        let mut out = String::with_capacity(self.detail.len() + 96);
        let mut w = Writer::new(&mut out);
        w.begin_object()
            .key("table")
            .string(&self.table)
            .key("available")
            .bool(self.outcome == Outcome::Ok)
            .key("reason")
            .string(self.outcome.as_str());
        if self.outcome == Outcome::Ok {
            // The members of the answer object, without its braces.
            let inner = self.detail.trim();
            let inner = inner.strip_prefix('{').and_then(|s| s.strip_suffix('}')).unwrap_or("");
            if !inner.trim().is_empty() {
                w.raw_members(inner);
            }
        } else {
            w.key("message").string(&self.detail);
        }
        w.end_object();
        out
    }
}

/// Answers as one JSON array.
#[must_use]
pub fn answers_json(answers: &[TableAnswer]) -> String {
    let mut out = String::from("[");
    for (i, a) in answers.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&a.to_json());
    }
    out.push(']');
    out
}

/// Tables fetched at once.
pub const CONCURRENCY: usize = 32;

/// Read `tables` (`project.dataset.table`, any spelling [`BqName`] reads)
/// through `transport`, against the API at `origin`
/// (`https://bigquery.googleapis.com`): one answer per table, in order.
///
/// An `ok` answer's object holds `type`, `location`, `description`,
/// `labels`, `time_partitioning`, `range_partitioning`, `clustering` (each
/// as BigQuery sent it, when present), `schema` (spliced, see the module
/// docs), `columns`, `omitted`, `truncated_descriptions`, `etag`, and with
/// `include_view_sql` a view's `view_sql`.
pub async fn table_meta<T: Transport>(
    transport: &T,
    origin: &str,
    tables: &[&str],
    opts: &MetaOptions<'_>,
) -> Vec<TableAnswer> {
    let origin = origin.trim_end_matches('/');
    let mut out = Vec::with_capacity(tables.len());
    for chunk in tables.chunks(CONCURRENCY) {
        let answers =
            objstore_r::join::join_all(chunk.iter().map(|t| one(transport, origin, t, opts))).await;
        out.extend(answers);
    }
    out
}

const INVALID: &str = "not a project.dataset.table name";

/// `table`'s project, dataset and table, when it names a table.
fn table_name(table: &str) -> Option<(&str, &str, &str)> {
    BqName::parse(table)
        .ok()
        .filter(|n| n.level() == Level::Table)
        .and_then(|n| Some((n.project, n.dataset?, n.table?)))
}

async fn one<T: Transport>(
    transport: &T,
    origin: &str,
    table: &str,
    opts: &MetaOptions<'_>,
) -> TableAnswer {
    let Some((p, d, t)) = BqName::parse(table)
        .ok()
        .filter(|n| n.level() == Level::Table)
        .and_then(|n| Some((n.project, n.dataset?, n.table?)))
    else {
        return TableAnswer::failed(
            table,
            Outcome::InvalidName,
            "not a project.dataset.table name",
        );
    };
    let project = crate::base::key::canonical_project(p);
    let mut url = crate::exec::metadata::table_url(origin, &project, d, t);
    url.push_str("?view=BASIC");
    // Never repeated on a 4xx (nor anything else): a batch answers at once.
    let resp = match transport.send(Request::get(&url).retry(Retry::Never)).await {
        Ok(r) => r,
        Err(e) => {
            let outcome = Outcome::of_error(&e);
            let msg = match &e {
                objstore_r::Error::Policy(_) => "refused by the request policy",
                _ => outcome.as_str(),
            };
            return TableAnswer::failed(table, outcome, msg);
        }
    };
    if !resp.is_success() {
        let outcome = Outcome::of_status(resp.status);
        return TableAnswer::failed(table, outcome, format!("BigQuery answered {}", resp.status));
    }
    if resp.body.len() > opts.limits.max_resource_bytes {
        return TableAnswer::failed(table, Outcome::Unreachable, "table resource too large");
    }
    match answer(&resp.body, opts) {
        Ok(json) => TableAnswer { table: table.to_owned(), outcome: Outcome::Ok, detail: json },
        Err(_) => TableAnswer::failed(table, Outcome::Unreachable, "not a table resource"),
    }
}

/// The value's first byte at or after `from` (past `:`, `,` and spaces).
fn value_start(src: &str, from: usize) -> usize {
    src[from..]
        .find(|c: char| !(c.is_whitespace() || c == ':' || c == ','))
        .map_or(from, |i| from + i)
}

/// The answer object for one table resource.
pub fn answer(raw: &[u8], opts: &MetaOptions<'_>) -> crate::base::Result<String> {
    let src = std::str::from_utf8(raw).map_err(|_| crate::base::Error::json("not UTF-8"))?;
    let mut s = Scanner::new(src);
    if s.expect()? != Event::ObjectStart {
        return Err(crate::base::Error::json("not an object"));
    }
    let mut members: Vec<(&'static str, Range<usize>)> = Vec::new();
    let mut description: Option<String> = None;
    let mut view_sql: Option<String> = None;
    let mut buf = String::new();
    loop {
        match s.expect()? {
            Event::ObjectEnd => break,
            Event::Key(k) => {
                let name = match k.as_plain().unwrap_or("") {
                    "type" => Some("type"),
                    "location" => Some("location"),
                    "labels" => Some("labels"),
                    "timePartitioning" => Some("time_partitioning"),
                    "rangePartitioning" => Some("range_partitioning"),
                    "clustering" => Some("clustering"),
                    "schema" => Some("schema"),
                    "etag" => Some("etag"),
                    _ => None,
                };
                let before = s.offset();
                let v = s.expect()?;
                match (k.as_plain(), v) {
                    (Some("description"), Event::Str(d)) => {
                        description = Some(d.decode(&mut buf)?.to_owned());
                    }
                    (Some("view" | "materializedView"), Event::ObjectStart) => {
                        loop {
                            match s.expect()? {
                                Event::ObjectEnd => break,
                                Event::Key(q) if q.eq_plain("query") => match s.expect()? {
                                    Event::Str(v) => {
                                        view_sql = Some(v.decode(&mut buf)?.to_owned())
                                    }
                                    other => s.skip(other)?,
                                },
                                Event::Key(_) => {
                                    let v = s.expect()?;
                                    s.skip(v)?;
                                }
                                _ => return Err(crate::base::Error::json("expected key")),
                            }
                        }
                        continue;
                    }
                    _ => {}
                }
                s.skip(v)?;
                if let Some(name) = name {
                    members.push((name, value_start(src, before)..s.offset()));
                }
            }
            _ => return Err(crate::base::Error::json("expected key")),
        }
    }
    let omit: Vec<String> = opts.omit_columns.iter().map(|c| c.to_ascii_lowercase()).collect();
    let mut out = String::with_capacity(src.len() + 64);
    let mut w = Writer::new(&mut out);
    w.begin_object();
    let mut trimmed = Trimmed::default();
    for (name, range) in &members {
        let text = &src[range.clone()];
        if *name == "schema" {
            trimmed = trim_schema(text, &omit, &opts.limits)?;
            w.key("schema").raw(&trimmed.json);
        } else {
            w.key(name).raw(text);
        }
    }
    if let Some(d) = description {
        let (d, cut) = cut(&d, opts.limits.max_description);
        trimmed.truncated += usize::from(cut);
        w.key("description").string(d);
    }
    if opts.include_view_sql {
        if let Some(q) = &view_sql {
            w.key("view_sql").string(q);
        }
    }
    w.key("columns").uint(trimmed.columns as u64);
    w.key("omitted").uint(trimmed.omitted as u64);
    w.key("truncated_descriptions").uint(trimmed.truncated as u64);
    w.end_object();
    Ok(out)
}

/// `s` cut to `max` characters, and whether it was.
fn cut(s: &str, max: usize) -> (&str, bool) {
    match s.char_indices().nth(max) {
        Some((i, _)) => (&s[..i], true),
        None => (s, false),
    }
}

/// A schema after [`trim_schema`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trimmed {
    /// The schema JSON.
    pub json: String,
    /// Columns kept (nested fields count).
    pub columns: usize,
    /// Fields removed (a removed `RECORD` counts once).
    pub omitted: usize,
    /// Descriptions cut.
    pub truncated: usize,
}

struct TrimState<'o> {
    omit: &'o [String],
    limits: Limits,
    columns: usize,
    omitted: usize,
    truncated: usize,
}

/// Apply `omit` (lowercase dotted paths) and `limits` to `schema` (a
/// `{"fields": [...]}` object or the bare array) by splicing: every kept
/// byte is copied as it was.
pub fn trim_schema(schema: &str, omit: &[String], limits: &Limits) -> crate::base::Result<Trimmed> {
    let mut st = TrimState { omit, limits: *limits, columns: 0, omitted: 0, truncated: 0 };
    let mut s = Scanner::new(schema);
    let json = match s.expect()? {
        Event::ArrayStart => {
            let start = value_start(schema, 0);
            s.skip(Event::ArrayStart)?;
            fields_array(schema, start..s.offset(), "", 1, &mut st)?.into_owned()
        }
        Event::ObjectStart => {
            let mut fields: Option<Range<usize>> = None;
            loop {
                match s.expect()? {
                    Event::ObjectEnd => break,
                    Event::Key(k) => {
                        let before = s.offset();
                        let v = s.expect()?;
                        let is_array = v == Event::ArrayStart;
                        s.skip(v)?;
                        if k.eq_plain("fields") && is_array {
                            fields = Some(value_start(schema, before)..s.offset());
                        }
                    }
                    _ => return Err(crate::base::Error::json("expected key")),
                }
            }
            match fields {
                Some(r) => {
                    let new = fields_array(schema, r.clone(), "", 1, &mut st)?;
                    let mut out = String::with_capacity(schema.len());
                    out.push_str(&schema[..r.start]);
                    out.push_str(&new);
                    out.push_str(&schema[r.end..]);
                    out
                }
                None => schema.to_owned(),
            }
        }
        _ => return Err(crate::base::Error::json("schema is not an object or array")),
    };
    Ok(Trimmed { json, columns: st.columns, omitted: st.omitted, truncated: st.truncated })
}

/// One field object of a `fields` array, as scanned.
struct Field {
    range: Range<usize>,
    path: Option<String>,
    description: Option<(Range<usize>, String)>,
    nested: Option<Range<usize>>,
}

/// The `fields` array at `range` of `src`, trimmed: borrowed when nothing
/// in it changed.
fn fields_array<'s>(
    src: &'s str,
    range: Range<usize>,
    prefix: &str,
    depth: usize,
    st: &mut TrimState<'_>,
) -> crate::base::Result<Cow<'s, str>> {
    let text = &src[range.clone()];
    let mut s = Scanner::new(text);
    if s.expect()? != Event::ArrayStart {
        return Err(crate::base::Error::json("fields is not an array"));
    }
    let mut buf = String::new();
    // Each element: its source range, and its rendering when it changed
    // (`None` inside: dropped).
    let mut elems: Vec<(Range<usize>, Option<Option<String>>)> = Vec::new();
    loop {
        let before = s.offset();
        let ev = s.expect()?;
        match ev {
            Event::ArrayEnd => break,
            Event::ObjectStart => {
                let mut f = Field {
                    range: value_start(text, before)..0,
                    path: None,
                    description: None,
                    nested: None,
                };
                loop {
                    match s.expect()? {
                        Event::ObjectEnd => break,
                        Event::Key(k) => {
                            let at = s.offset();
                            let v = s.expect()?;
                            if k.eq_plain("name") {
                                if let Event::Str(n) = v {
                                    let n = n.decode(&mut buf)?.to_ascii_lowercase();
                                    f.path = Some(if prefix.is_empty() {
                                        n
                                    } else {
                                        format!("{prefix}.{n}")
                                    });
                                }
                            } else if k.eq_plain("description") {
                                if let Event::Str(d) = v {
                                    let d = d.decode(&mut buf)?.to_owned();
                                    s.skip(v)?;
                                    f.description = Some((value_start(text, at)..s.offset(), d));
                                    continue;
                                }
                            } else if k.eq_plain("fields") && v == Event::ArrayStart {
                                s.skip(v)?;
                                f.nested = Some(value_start(text, at)..s.offset());
                                continue;
                            }
                            s.skip(v)?;
                        }
                        _ => return Err(crate::base::Error::json("expected key")),
                    }
                }
                f.range.end = s.offset();
                let rendered = field(text, &f, depth, st)?;
                elems.push((f.range.clone(), rendered));
            }
            other => {
                // Not a field object: kept as it is.
                s.skip(other)?;
                elems.push((value_start(text, before)..s.offset(), None));
            }
        }
    }
    if elems.iter().all(|(_, r)| r.is_none()) {
        return Ok(Cow::Borrowed(&src[range]));
    }
    let mut out = String::with_capacity(text.len());
    out.push('[');
    let mut first = true;
    for (r, rendered) in &elems {
        let piece = match rendered {
            None => &text[r.clone()],
            Some(None) => continue,
            Some(Some(t)) => t.as_str(),
        };
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(piece);
    }
    out.push(']');
    Ok(Cow::Owned(out))
}

/// One field: `None` kept as it is, `Some(None)` dropped, `Some(Some(t))`
/// kept as `t`.
fn field(
    text: &str,
    f: &Field,
    depth: usize,
    st: &mut TrimState<'_>,
) -> crate::base::Result<Option<Option<String>>> {
    let omitted = f.path.as_ref().is_some_and(|p| st.omit.iter().any(|o| o == p));
    if omitted || depth > st.limits.max_depth || st.columns >= st.limits.max_columns {
        st.omitted += 1;
        return Ok(Some(None));
    }
    st.columns += 1;
    let mut edits: Vec<(Range<usize>, String)> = Vec::new();
    if let Some((r, d)) = &f.description {
        let (kept, was_cut) = cut(d, st.limits.max_description);
        if was_cut {
            st.truncated += 1;
            let mut lit = String::with_capacity(kept.len() + 2);
            escape_into(kept, &mut lit);
            edits.push((r.clone(), lit));
        }
    }
    if let Some(r) = &f.nested {
        let prefix = f.path.as_deref().unwrap_or("");
        if let Cow::Owned(t) = fields_array(text, r.clone(), prefix, depth + 1, st)? {
            edits.push((r.clone(), t));
        }
    }
    if edits.is_empty() {
        return Ok(None);
    }
    edits.sort_by_key(|(r, _)| r.start);
    let mut out = String::with_capacity(f.range.len());
    let mut at = f.range.start;
    for (r, t) in &edits {
        out.push_str(&text[at..r.start]);
        out.push_str(t);
        at = r.end;
    }
    out.push_str(&text[at..f.range.end]);
    Ok(Some(Some(out)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str = r#"{"fields": [
        {"name": "id", "type": "INT64", "mode": "REQUIRED", "defaultValueExpression": "0"},
        {"name": "Email", "type": "STRING", "policyTags": {"names": ["projects/p/locations/us/taxonomies/1/policyTags/2"]}, "description": "Contact address"},
        {"name": "addr", "type": "RECORD", "fields": [
            {"name": "city", "type": "STRING", "collation": "und:ci", "x-unknown": [1, {"y": null}]},
            {"name": "zip", "type": "STRING"}
        ]},
        {"name": "note", "type": "STRING", "description": "abcdef"}
    ], "x-schema-extra": true}"#;

    fn lim(cols: usize, depth: usize, desc: usize) -> Limits {
        Limits { max_columns: cols, max_depth: depth, max_description: desc, ..Limits::default() }
    }

    #[test]
    fn nothing_to_do_is_byte_identical() {
        let t = trim_schema(SCHEMA, &[], &Limits::default()).unwrap();
        assert_eq!(t.json, SCHEMA);
        assert_eq!((t.columns, t.omitted, t.truncated), (6, 0, 0));
    }

    #[test]
    fn omission_keeps_every_other_byte() {
        let t =
            trim_schema(SCHEMA, &["email".into(), "addr.zip".into()], &Limits::default()).unwrap();
        assert_eq!((t.columns, t.omitted), (4, 2));
        assert!(!t.json.contains("Email") && !t.json.contains("zip"));
        for kept in [
            r#"{"name": "id", "type": "INT64", "mode": "REQUIRED", "defaultValueExpression": "0"}"#,
            r#"{"name": "city", "type": "STRING", "collation": "und:ci", "x-unknown": [1, {"y": null}]}"#,
            r#""x-schema-extra": true"#,
        ] {
            assert!(t.json.contains(kept), "{kept} in {}", t.json);
        }
        crate::base::json::Scanner::new(&t.json).skip_value().unwrap();
    }

    #[test]
    fn caps_on_columns_depth_and_descriptions() {
        let t = trim_schema(SCHEMA, &[], &lim(2, 15, 1000)).unwrap();
        assert_eq!((t.columns, t.omitted), (2, 2), "id, Email kept; addr and note dropped");
        let t = trim_schema(SCHEMA, &[], &lim(100, 1, 1000)).unwrap();
        assert_eq!((t.columns, t.omitted), (4, 2), "nested fields beyond depth 1 dropped");
        assert!(
            t.json.contains(r#""fields": []"#) || t.json.contains(r#""fields":[]"#),
            "{}",
            t.json
        );
        let t = trim_schema(SCHEMA, &[], &lim(100, 15, 3)).unwrap();
        assert_eq!(t.truncated, 2);
        assert!(t.json.contains(r#""description": "abc""#) && t.json.contains(r#""Con""#));
    }

    #[test]
    fn answer_keeps_resource_members_and_typed_outcomes() {
        let raw = format!(
            r#"{{"kind":"bigquery#table","etag":"E1","type":"VIEW","location":"US","labels":{{"team":"a"}},
            "description":"{}","timePartitioning":{{"type":"DAY","field":"ts"}},"clustering":{{"fields":["id"]}},
            "schema":{SCHEMA},"view":{{"query":"SELECT 1","useLegacySql":false}},"numRows":"5"}}"#,
            "d".repeat(1200)
        );
        let a = answer(
            raw.as_bytes(),
            &MetaOptions { include_view_sql: true, ..MetaOptions::default() },
        )
        .unwrap();
        for want in [
            r#""type":"VIEW""#,
            r#""location":"US""#,
            r#""labels":{"team":"a"}"#,
            r#""time_partitioning":{"type":"DAY","field":"ts"}"#,
            r#""clustering":{"fields":["id"]}"#,
            r#""view_sql":"SELECT 1""#,
            r#""etag":"E1""#,
            r#""truncated_descriptions":1"#,
        ] {
            assert!(a.contains(want), "{want} in {a}");
        }
        assert!(!a.contains("numRows"), "no statistics");
        assert!(!answer(raw.as_bytes(), &MetaOptions::default()).unwrap().contains("view_sql"));
        assert_eq!(Outcome::of_status(403), Outcome::NoAccess);
        assert_eq!(Outcome::of_status(404), Outcome::NotFound);
        assert_eq!(Outcome::of_status(401), Outcome::Expired);
        assert_eq!(Outcome::of_status(429), Outcome::Unreachable);
        assert_eq!(Outcome::of_status(503), Outcome::Unreachable);
        assert_eq!(Outcome::of_status(400), Outcome::InvalidName);
        let ok = TableAnswer { table: "p.d.t".into(), outcome: Outcome::Ok, detail: a };
        let j = ok.to_json();
        assert!(j.starts_with(r#"{"table":"p.d.t","available":true,"reason":"ok","#), "{j}");
        crate::base::json::Scanner::new(&j).skip_value().unwrap();
        let no = TableAnswer::failed("p.d.t", Outcome::NoAccess, "BigQuery answered 403");
        assert_eq!(
            no.to_json(),
            r#"{"table":"p.d.t","available":false,"reason":"no_access","message":"BigQuery answered 403"}"#
        );
    }
}

/// The BigQuery API origin.
pub const BIGQUERY_ORIGIN: &str = "https://bigquery.googleapis.com";

/// A client that reads table metadata and nothing else: the caller's
/// credential, under a [`crate::exec::guard::MetadataGuard`] (feature
/// `http`).
#[cfg(feature = "http")]
#[derive(Debug)]
pub struct MetaClient<'g, T: objstore_r::token::TokenProvider> {
    client: objstore_r::http::Client<T, crate::exec::guard::MetadataGuard<'g>>,
    origin: String,
}

#[cfg(feature = "http")]
impl<'g, T: objstore_r::token::TokenProvider> MetaClient<'g, T> {
    /// A client authenticating with `tokens` (for a caller's token,
    /// `Adc::from_access_token`) under `guard`, against `endpoint` (default
    /// [`BIGQUERY_ORIGIN`]). Bodies are capped at `limits.max_resource_bytes`
    /// and nothing is retried.
    pub fn new(
        tokens: T,
        guard: crate::exec::guard::MetadataGuard<'g>,
        endpoint: Option<&str>,
        limits: &Limits,
    ) -> objstore_r::Result<Self> {
        let cfg = objstore_r::http::HttpConfig {
            max_retries: 0,
            max_response_bytes: limits.max_resource_bytes as u64,
            timeout: std::time::Duration::from_secs(20),
            ..objstore_r::http::HttpConfig::default()
        };
        let client = objstore_r::http::Client::with_policy(tokens, guard, cfg)?;
        let origin = endpoint.unwrap_or(BIGQUERY_ORIGIN).trim_end_matches('/').to_owned();
        Ok(Self { client, origin })
    }

    /// [`table_meta`] through this client.
    pub async fn table_meta(&self, tables: &[&str], opts: &MetaOptions<'_>) -> Vec<TableAnswer> {
        table_meta(&self.client, &self.origin, tables, opts).await
    }
}

/// What a batch gets when no client could be built (no credential, say):
/// `outcome` for every table, except `invalid_name` for a name that is not
/// one.
#[must_use]
pub fn all_failed(tables: &[&str], outcome: Outcome, message: &str) -> Vec<TableAnswer> {
    tables
        .iter()
        .map(|t| match table_name(t) {
            Some(_) => TableAnswer::failed(t, outcome, message),
            None => TableAnswer::failed(t, Outcome::InvalidName, INVALID),
        })
        .collect()
}
