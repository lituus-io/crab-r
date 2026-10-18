// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Decoding BigQuery REST responses (`jobs.query`, `jobs.getQueryResults`).
//!
//! Two passes over the zero-copy scanner: the first reads metadata and the
//! schema (which may appear anywhere in the object), the second decodes
//! `rows` straight into typed columns. Scalars arrive as strings and are
//! parsed into native column types; `TIMESTAMP` is requested as int64
//! microseconds. Nested `RECORD` / `REPEATED` values are rendered as compact
//! JSON text with field names (readable, and deterministic for hashing).

use crate::base::batch::{parse_numeric, Batch, Cell, ColType, Column};
use crate::base::date::{parse_datetime, parse_time, Date};
use crate::base::json::{escape_into, parse_u64, Event, Scanner};
use crate::exec::{Error, Result, SqlPos};

/// BigQuery field types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BqType {
    Int64,
    Float64,
    Numeric,
    Bool,
    String,
    Bytes,
    Date,
    DateTime,
    Time,
    Timestamp,
    Record,
    /// BIGNUMERIC, GEOGRAPHY, JSON, INTERVAL, RANGE, and anything newer.
    Text,
}

impl BqType {
    fn parse(t: &str) -> Self {
        match t {
            "INTEGER" | "INT64" => Self::Int64,
            "FLOAT" | "FLOAT64" => Self::Float64,
            "NUMERIC" => Self::Numeric,
            "BOOLEAN" | "BOOL" => Self::Bool,
            "STRING" => Self::String,
            "BYTES" => Self::Bytes,
            "DATE" => Self::Date,
            "DATETIME" => Self::DateTime,
            "TIME" => Self::Time,
            "TIMESTAMP" => Self::Timestamp,
            "RECORD" | "STRUCT" => Self::Record,
            _ => Self::Text,
        }
    }

    /// Scalars rendered unquoted inside nested JSON.
    fn bare_json(self) -> bool {
        matches!(self, Self::Int64 | Self::Float64 | Self::Numeric | Self::Bool)
    }
}

/// One schema field (children for records).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) ty: BqType,
    /// The declared type name (`INTEGER`, `GEOGRAPHY`, ...).
    pub(crate) type_name: String,
    pub(crate) repeated: bool,
    pub(crate) children: Vec<Field>,
}

impl Field {
    /// BigQuery type text, in standard SQL names.
    pub(crate) fn sql_type(&self) -> String {
        let base = match self.ty {
            BqType::Int64 => "INT64".to_owned(),
            BqType::Float64 => "FLOAT64".to_owned(),
            BqType::Bool => "BOOL".to_owned(),
            BqType::Record => {
                let inner: Vec<String> =
                    self.children.iter().map(|c| format!("{} {}", c.name, c.sql_type())).collect();
                format!("STRUCT<{}>", inner.join(", "))
            }
            _ => self.type_name.clone(),
        };
        if self.repeated {
            format!("ARRAY<{base}>")
        } else {
            base
        }
    }

    /// The column type this field decodes into.
    pub(crate) fn col_type(&self) -> ColType {
        if self.repeated {
            return ColType::Text;
        }
        match self.ty {
            BqType::Int64 => ColType::Int64,
            BqType::Float64 => ColType::Float64,
            BqType::Numeric => ColType::Numeric,
            BqType::Bool => ColType::Bool,
            BqType::String => ColType::String,
            BqType::Bytes => ColType::Bytes,
            BqType::Date => ColType::Date,
            BqType::DateTime => ColType::DateTime,
            BqType::Time => ColType::Time,
            BqType::Timestamp => ColType::Timestamp,
            BqType::Record | BqType::Text => ColType::Text,
        }
    }
}

/// Everything in a response except the rows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Page {
    pub(crate) complete: bool,
    pub(crate) job_id: Option<String>,
    pub(crate) location: Option<String>,
    pub(crate) schema: Option<Vec<Field>>,
    pub(crate) total_rows: u64,
    pub(crate) next_token: Option<String>,
    pub(crate) bytes_processed: u64,
    pub(crate) bytes_billed: u64,
    pub(crate) cache_hit: bool,
}

fn bad(msg: &'static str) -> Error {
    Error::Decode(crate::base::Error::Json { at: 0, msg })
}

fn num_str(ev: Event<'_>, buf: &mut String) -> Result<u64> {
    Ok(match ev {
        Event::Str(s) => parse_u64(s.decode(buf)?)?,
        Event::Num(n) => parse_u64(n)?,
        _ => 0,
    })
}

fn string(ev: Event<'_>, buf: &mut String) -> Result<Option<String>> {
    Ok(match ev {
        Event::Str(s) => Some(s.decode(buf)?.to_owned()),
        _ => None,
    })
}

/// Pass 1: metadata and schema.
pub(crate) fn parse_page(body: &[u8]) -> Result<Page> {
    let mut s = Scanner::from_bytes(body)?;
    let mut buf = String::new();
    let mut page = Page::default();
    if s.expect()? != Event::ObjectStart {
        return Err(bad("response is not an object"));
    }
    loop {
        match s.expect()? {
            Event::ObjectEnd => break,
            Event::Key(k) => {
                let name = k.as_plain().unwrap_or("");
                let v = s.expect()?;
                match name {
                    "jobComplete" => page.complete = v == Event::Bool(true),
                    "cacheHit" => page.cache_hit = v == Event::Bool(true),
                    "totalRows" => page.total_rows = num_str(v, &mut buf)?,
                    "totalBytesProcessed" => page.bytes_processed = num_str(v, &mut buf)?,
                    "totalBytesBilled" => page.bytes_billed = num_str(v, &mut buf)?,
                    "pageToken" => page.next_token = string(v, &mut buf)?,
                    "jobReference" if v == Event::ObjectStart => loop {
                        match s.expect()? {
                            Event::ObjectEnd => break,
                            Event::Key(k) => {
                                let kn = k.as_plain().unwrap_or("");
                                let v = s.expect()?;
                                match kn {
                                    "jobId" => page.job_id = string(v, &mut buf)?,
                                    "location" => page.location = string(v, &mut buf)?,
                                    _ => s.skip(v)?,
                                }
                            }
                            _ => return Err(bad("expected key")),
                        }
                    },
                    "schema" if v == Event::ObjectStart => loop {
                        match s.expect()? {
                            Event::ObjectEnd => break,
                            Event::Key(k) if k.eq_plain("fields") => {
                                let first = s.expect()?;
                                page.schema = Some(parse_fields(&mut s, first, &mut buf, 0)?);
                            }
                            Event::Key(_) => {
                                let v = s.expect()?;
                                s.skip(v)?;
                            }
                            _ => return Err(bad("expected key")),
                        }
                    },
                    _ => s.skip(v)?,
                }
            }
            _ => return Err(bad("expected key")),
        }
    }
    Ok(page)
}

fn parse_fields(
    s: &mut Scanner<'_>,
    first: Event<'_>,
    buf: &mut String,
    depth: u32,
) -> Result<Vec<Field>> {
    if depth > 15 || first != Event::ArrayStart {
        return Err(bad("bad schema fields"));
    }
    let mut out = Vec::new();
    loop {
        match s.expect()? {
            Event::ArrayEnd => return Ok(out),
            Event::ObjectStart => {
                let mut field = Field {
                    name: String::new(),
                    ty: BqType::Text,
                    type_name: String::new(),
                    repeated: false,
                    children: Vec::new(),
                };
                loop {
                    match s.expect()? {
                        Event::ObjectEnd => break,
                        Event::Key(k) => {
                            let kn = k.as_plain().unwrap_or("");
                            let v = s.expect()?;
                            match (kn, v) {
                                ("name", Event::Str(n)) => {
                                    n.decode(buf)?.clone_into(&mut field.name);
                                }
                                ("type", Event::Str(t)) => {
                                    let t = t.decode(buf)?;
                                    field.ty = BqType::parse(t);
                                    t.clone_into(&mut field.type_name);
                                }
                                ("mode", Event::Str(m)) => {
                                    field.repeated = m.decode(buf)? == "REPEATED";
                                }
                                ("fields", first) => {
                                    field.children = parse_fields(s, first, buf, depth + 1)?;
                                }
                                (_, v) => s.skip(v)?,
                            }
                        }
                        _ => return Err(bad("expected key")),
                    }
                }
                out.push(field);
            }
            _ => return Err(bad("bad schema field")),
        }
    }
}

/// An empty batch shaped like `schema`.
pub(crate) fn batch_for(schema: &[Field], rows: usize) -> Batch {
    Batch {
        names: schema.iter().map(|f| f.name.clone()).collect(),
        columns: schema.iter().map(|f| Column::new(f.col_type(), rows)).collect(),
    }
}

/// Reusable decode buffers.
#[derive(Debug, Default)]
pub(crate) struct Scratch {
    text: String,
    bytes: Vec<u8>,
    json: String,
}

/// Pass 2: append up to `limit` rows from `body` into `out`; returns how
/// many were appended.
pub(crate) fn decode_rows(
    body: &[u8],
    schema: &[Field],
    out: &mut Batch,
    limit: usize,
    sc: &mut Scratch,
) -> Result<usize> {
    let mut s = Scanner::from_bytes(body)?;
    if s.expect()? != Event::ObjectStart {
        return Err(bad("response is not an object"));
    }
    let mut appended = 0;
    loop {
        match s.expect()? {
            Event::ObjectEnd => return Ok(appended),
            Event::Key(k) if k.eq_plain("rows") => {
                if s.expect()? != Event::ArrayStart {
                    return Err(bad("rows is not an array"));
                }
                loop {
                    match s.expect()? {
                        Event::ArrayEnd => break,
                        Event::ObjectStart if appended < limit => {
                            decode_row(&mut s, schema, out, sc)?;
                            appended += 1;
                        }
                        other => s.skip(other)?,
                    }
                }
            }
            Event::Key(_) => {
                let v = s.expect()?;
                s.skip(v)?;
            }
            _ => return Err(bad("expected key")),
        }
    }
}

/// A row `{"f":[{"v":...}, ...]}` after its `{`.
fn decode_row(
    s: &mut Scanner<'_>,
    schema: &[Field],
    out: &mut Batch,
    sc: &mut Scratch,
) -> Result<()> {
    let mut seen = false;
    loop {
        match s.expect()? {
            Event::ObjectEnd => break,
            Event::Key(k) if k.eq_plain("f") => {
                if s.expect()? != Event::ArrayStart {
                    return Err(bad("row f is not an array"));
                }
                for (i, field) in schema.iter().enumerate() {
                    if s.expect()? != Event::ObjectStart {
                        return Err(bad("row has fewer cells than the schema"));
                    }
                    let v = cell_value(s)?;
                    push_value(s, v, field, &mut out.columns[i], sc)?;
                    close_cell(s)?;
                }
                if s.expect()? != Event::ArrayEnd {
                    return Err(bad("row has more cells than the schema"));
                }
                seen = true;
            }
            Event::Key(_) => {
                let v = s.expect()?;
                s.skip(v)?;
            }
            _ => return Err(bad("expected key")),
        }
    }
    if seen {
        Ok(())
    } else {
        Err(bad("row without cells"))
    }
}

/// After a cell's `{`, return the event starting its `v` value.
fn cell_value<'a>(s: &mut Scanner<'a>) -> Result<Event<'a>> {
    match s.expect()? {
        Event::Key(k) if k.eq_plain("v") => Ok(s.expect()?),
        _ => Err(bad("cell without v")),
    }
}

fn close_cell(s: &mut Scanner<'_>) -> Result<()> {
    match s.expect()? {
        Event::ObjectEnd => Ok(()),
        _ => Err(bad("unexpected cell content")),
    }
}

fn push_value(
    s: &mut Scanner<'_>,
    v: Event<'_>,
    field: &Field,
    col: &mut Column,
    sc: &mut Scratch,
) -> Result<()> {
    if v == Event::Null {
        col.push_null();
        return Ok(());
    }
    if field.repeated || field.ty == BqType::Record {
        sc.json.clear();
        render(s, v, field, field.repeated, &mut sc.json, &mut sc.text, 0)?;
        col.push(Cell::Str(&sc.json))?;
        return Ok(());
    }
    let Event::Str(raw) = v else { return Err(bad("scalar cell is not a string")) };
    let text = raw.decode(&mut sc.text)?;
    let cell = match field.ty {
        BqType::Int64 => Cell::I64(text.parse().map_err(|_| bad("bad INT64"))?),
        BqType::Float64 => Cell::F64(parse_float(text)?),
        BqType::Numeric => Cell::I128(parse_numeric(text)?),
        BqType::Bool => Cell::Bool(text == "true"),
        BqType::Date => Cell::I32(Date::parse(text)?.days()),
        BqType::DateTime => Cell::I64(parse_datetime(text)?),
        BqType::Time => Cell::I64(parse_time(text)?),
        BqType::Timestamp => Cell::I64(parse_timestamp(text)?),
        BqType::Bytes => {
            base64_decode(text, &mut sc.bytes)?;
            return col.push(Cell::Bytes(&sc.bytes)).map_err(Error::from);
        }
        BqType::String | BqType::Text | BqType::Record => Cell::Str(text),
    };
    col.push(cell)?;
    Ok(())
}

/// Render a nested value as compact JSON with field names.
fn render(
    s: &mut Scanner<'_>,
    v: Event<'_>,
    field: &Field,
    as_array: bool,
    out: &mut String,
    tmp: &mut String,
    depth: u32,
) -> Result<()> {
    if depth > 32 {
        return Err(bad("value nested too deeply"));
    }
    if v == Event::Null {
        out.push_str("null");
        return Ok(());
    }
    if as_array {
        if v != Event::ArrayStart {
            return Err(bad("repeated value is not an array"));
        }
        out.push('[');
        let mut first = true;
        loop {
            match s.expect()? {
                Event::ArrayEnd => break,
                Event::ObjectStart => {
                    if !first {
                        out.push(',');
                    }
                    first = false;
                    let ev = cell_value(s)?;
                    render(s, ev, field, false, out, tmp, depth + 1)?;
                    close_cell(s)?;
                }
                _ => return Err(bad("bad array element")),
            }
        }
        out.push(']');
        return Ok(());
    }
    if field.ty == BqType::Record {
        if v != Event::ObjectStart {
            return Err(bad("record value is not an object"));
        }
        match s.expect()? {
            Event::Key(k) if k.eq_plain("f") => {}
            _ => return Err(bad("record without f")),
        }
        if s.expect()? != Event::ArrayStart {
            return Err(bad("record f is not an array"));
        }
        out.push('{');
        for (i, child) in field.children.iter().enumerate() {
            if s.expect()? != Event::ObjectStart {
                return Err(bad("record has fewer fields than its schema"));
            }
            if i > 0 {
                out.push(',');
            }
            escape_into(&child.name, out);
            out.push(':');
            let ev = cell_value(s)?;
            render(s, ev, child, child.repeated, out, tmp, depth + 1)?;
            close_cell(s)?;
        }
        if s.expect()? != Event::ArrayEnd || s.expect()? != Event::ObjectEnd {
            return Err(bad("record has more fields than its schema"));
        }
        out.push('}');
        return Ok(());
    }
    let Event::Str(raw) = v else { return Err(bad("scalar is not a string")) };
    let text = raw.decode(tmp)?;
    if field.ty.bare_json()
        && !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_alphanumeric() || b"+-.".contains(&b))
    {
        out.push_str(text);
    } else {
        escape_into(text, out);
    }
    Ok(())
}

fn parse_float(t: &str) -> Result<f64> {
    match t {
        "NaN" => Ok(f64::NAN),
        "Infinity" => Ok(f64::INFINITY),
        "-Infinity" => Ok(f64::NEG_INFINITY),
        _ => t.parse().map_err(|_| bad("bad FLOAT64")),
    }
}

/// `TIMESTAMP` as int64 microseconds (`useInt64Timestamp`), or the legacy
/// float-seconds form.
fn parse_timestamp(t: &str) -> Result<i64> {
    if let Ok(v) = t.parse::<i64>() {
        return Ok(v);
    }
    let secs: f64 = t.parse().map_err(|_| bad("bad TIMESTAMP"))?;
    Ok((secs * 1e6).round() as i64)
}

/// Standard base64 (BigQuery `BYTES`) into `out`.
fn base64_decode(s: &str, out: &mut Vec<u8>) -> Result<()> {
    out.clear();
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for &c in s.as_bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            b'=' => break,
            b'\n' | b'\r' => continue,
            _ => return Err(bad("bad base64")),
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Ok(())
}

/// Map an error response (`{"error": {...}}`) and its HTTP status to a
/// typed error.
pub(crate) fn api_error(status: u16, body: &[u8]) -> Error {
    let objstore_r::transport::ApiError { reason, message, .. } =
        objstore_r::transport::ApiError::parse(status, body);
    match (status, reason.as_str()) {
        (_, "bytesBilledLimitExceeded") => Error::BytesLimit { message },
        (401, _) => Error::Unauthenticated { message },
        (_, "accessDenied") | (403, _) => Error::Denied { message },
        (_, "invalidQuery" | "notFound" | "invalid") | (400, _) => {
            let pos = position(&message);
            Error::Query {
                reason: if reason.is_empty() { "invalid".into() } else { reason },
                message,
                pos,
            }
        }
        _ => Error::Api { status, message },
    }
}

/// `... at [3:17]` → line 3, column 17.
fn position(msg: &str) -> Option<SqlPos> {
    let i = msg.rfind(" at [")?;
    let rest = &msg[i + 5..];
    let end = rest.find(']')?;
    let (l, c) = rest[..end].split_once(':')?;
    Some(SqlPos { line: l.parse().ok()?, col: c.parse().ok()? })
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESP: &str = r#"{
      "kind": "bigquery#queryResponse",
      "rows": [
        {"f": [{"v": "1"}, {"v": "a\"b"}, {"v": "1700000000123456"}, {"v": [{"v": "1"}, {"v": "2"}]}, {"v": {"f": [{"v": "x"}, {"v": null}]}}, {"v": "AQID"}, {"v": "12.5"}, {"v": "2026-09-28"}, {"v": "true"}]},
        {"f": [{"v": null}, {"v": null}, {"v": null}, {"v": []}, {"v": null}, {"v": null}, {"v": null}, {"v": null}, {"v": null}]}
      ],
      "schema": {"fields": [
        {"name": "n", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "s", "type": "STRING"},
        {"name": "ts", "type": "TIMESTAMP"},
        {"name": "arr", "type": "INTEGER", "mode": "REPEATED"},
        {"name": "rec", "type": "RECORD", "fields": [{"name": "a", "type": "STRING"}, {"name": "b", "type": "INTEGER"}]},
        {"name": "by", "type": "BYTES"},
        {"name": "num", "type": "NUMERIC"},
        {"name": "d", "type": "DATE"},
        {"name": "ok", "type": "BOOLEAN"}
      ]},
      "jobReference": {"projectId": "p", "jobId": "job_1", "location": "US"},
      "totalRows": "2", "totalBytesProcessed": "1048576", "jobComplete": true, "cacheHit": false
    }"#;

    #[test]
    fn metadata_and_rows_decode_regardless_of_key_order() {
        let page = parse_page(RESP.as_bytes()).unwrap();
        assert!(page.complete && !page.cache_hit);
        assert_eq!((page.total_rows, page.bytes_processed), (2, 1_048_576));
        assert_eq!(page.job_id.as_deref(), Some("job_1"));
        let schema = page.schema.unwrap();
        let mut b = batch_for(&schema, 2);
        let n = decode_rows(RESP.as_bytes(), &schema, &mut b, 10, &mut Scratch::default()).unwrap();
        assert_eq!(n, 2);
        b.validate().unwrap();
        let c = |i: usize, r: usize| b.columns[i].get(r);
        assert_eq!(c(0, 0), Cell::I64(1));
        assert_eq!(c(1, 0), Cell::Str("a\"b"));
        assert_eq!(c(2, 0), Cell::I64(1_700_000_000_123_456));
        assert_eq!(c(3, 0), Cell::Str("[1,2]"));
        assert_eq!(c(4, 0), Cell::Str(r#"{"a":"x","b":null}"#));
        assert_eq!(c(5, 0), Cell::Bytes(&[1, 2, 3]));
        assert_eq!(c(6, 0), Cell::I128(12_500_000_000));
        assert_eq!(c(7, 0), Cell::I32(Date::parse("2026-09-28").unwrap().days()));
        assert_eq!(c(8, 0), Cell::Bool(true));
        assert_eq!(c(0, 1), Cell::Null);
        assert_eq!(c(3, 1), Cell::Str("[]"));
    }

    #[test]
    fn row_limit_is_respected() {
        let page = parse_page(RESP.as_bytes()).unwrap();
        let schema = page.schema.unwrap();
        let mut b = batch_for(&schema, 1);
        assert_eq!(
            decode_rows(RESP.as_bytes(), &schema, &mut b, 1, &mut Scratch::default()).unwrap(),
            1
        );
        assert_eq!(b.rows(), 1);
    }

    #[test]
    fn errors_map_to_types_with_positions() {
        let body = br#"{"error":{"code":400,"message":"Unrecognized name: statuz at [1:8]","errors":[{"message":"x","domain":"global","reason":"invalidQuery","location":"q"}],"status":"INVALID_ARGUMENT"}}"#;
        match api_error(400, body) {
            Error::Query { reason, pos, message } => {
                assert_eq!(reason, "invalidQuery");
                assert_eq!(pos, Some(SqlPos { line: 1, col: 8 }));
                assert!(message.contains("statuz"));
            }
            e => panic!("{e:?}"),
        }
        let body = br#"{"error":{"code":400,"message":"Query exceeded limit for bytes billed: 1000.","errors":[{"reason":"bytesBilledLimitExceeded"}]}}"#;
        assert!(matches!(api_error(400, body), Error::BytesLimit { .. }));
        assert!(matches!(api_error(403, b"{}"), Error::Denied { .. }));
        assert!(matches!(api_error(401, b"{}"), Error::Unauthenticated { .. }));
        assert!(api_error(503, b"busy").is_retriable());
    }

    #[test]
    fn base64_and_floats() {
        let mut v = Vec::new();
        base64_decode("aGVsbG8=", &mut v).unwrap();
        assert_eq!(v, b"hello");
        base64_decode("", &mut v).unwrap();
        assert!(v.is_empty());
        assert!(base64_decode("a$b", &mut v).is_err());
        assert!(parse_float("NaN").unwrap().is_nan());
        assert!(
            parse_float("-Infinity").unwrap().is_infinite()
                && parse_float("-Infinity").unwrap() < 0.0
        );
        assert_eq!(parse_timestamp("1.5").unwrap(), 1_500_000);
    }

    #[test]
    fn malformed_rows_are_errors() {
        let schema = vec![Field {
            name: "n".into(),
            ty: BqType::Int64,
            type_name: "INTEGER".into(),
            repeated: false,
            children: vec![],
        }];
        for body in [
            r#"{"rows":[{"f":[]}]}"#,
            r#"{"rows":[{"f":[{"v":"1"},{"v":"2"}]}]}"#,
            r#"{"rows":[{"f":[{"v":"x"}]}]}"#,
            r#"{"rows":[{}]}"#,
        ] {
            let mut b = batch_for(&schema, 1);
            assert!(
                decode_rows(body.as_bytes(), &schema, &mut b, 10, &mut Scratch::default()).is_err(),
                "{body}"
            );
        }
    }
}
