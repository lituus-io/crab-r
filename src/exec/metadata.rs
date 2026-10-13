// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Table metadata over the BigQuery REST API: read a table's resource
//! (free — no job, no bytes billed) and patch it under optimistic
//! concurrency.
//!
//! [`TableMeta`] keeps the **raw resource bytes** and the byte range of its
//! `schema` object, so a writer can splice new descriptions into the exact
//! JSON it read: policy tags, collation, default values and fields this code
//! has never heard of survive untouched. A patch sends `If-Match` with the
//! etag it read; a concurrent change fails with
//! [`Error::PreconditionFailed`] instead
//! of being overwritten.

use crate::base::json::{self, Event, Scanner};
use crate::exec::{Error, Result};
use objstore_r::Bytes;
use std::future::Future;
use std::ops::Range;

/// A table's metadata, as read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableMeta {
    /// Entity tag (for `If-Match`).
    pub etag: String,
    /// `TABLE`, `VIEW`, `MATERIALIZED_VIEW`, `EXTERNAL`, `SNAPSHOT`, ...
    pub kind: String,
    /// Table description.
    pub description: Option<String>,
    /// The defining query of a view or materialized view.
    pub view_query: Option<String>,
    /// Row count (0 for views).
    pub num_rows: u64,
    /// Last modification, epoch milliseconds.
    pub last_modified_ms: u64,
    /// The whole resource, as returned.
    pub raw: Bytes,
    /// Byte range of the `schema` object inside `raw` (empty when absent).
    pub schema: Range<usize>,
}

impl TableMeta {
    /// The raw `schema` object (`{"fields": [...]}`).
    #[must_use]
    pub fn schema_json(&self) -> &str {
        std::str::from_utf8(&self.raw[self.schema.clone()]).unwrap_or("")
    }
}

/// Read and patch table metadata.
pub trait TableMetadata {
    /// Future of [`table_get`](TableMetadata::table_get).
    type Get<'a>: Future<Output = Result<Option<TableMeta>>> + 'a
    where
        Self: 'a;
    /// Future of [`table_patch`](TableMetadata::table_patch).
    type Patch<'a>: Future<Output = Result<TableMeta>> + 'a
    where
        Self: 'a;

    /// The table's metadata; `None` when it does not exist (yet).
    fn table_get<'a>(&'a self, project: &'a str, dataset: &'a str, table: &'a str)
        -> Self::Get<'a>;

    /// Apply `body` (a partial table resource, e.g. `{"schema": ...}`) only
    /// if the table is still at `etag`. Returns the updated metadata.
    fn table_patch<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table: &'a str,
        body: Bytes,
        etag: &'a str,
    ) -> Self::Patch<'a>;
}

/// The `tables/{table}` URL under `origin`.
pub(crate) fn table_url(origin: &str, project: &str, dataset: &str, table: &str) -> String {
    let mut u =
        String::with_capacity(origin.len() + project.len() + dataset.len() + table.len() + 48);
    u.push_str(origin);
    u.push_str("/bigquery/v2/projects/");
    objstore_r::transport::encode_component(project, &mut u);
    u.push_str("/datasets/");
    objstore_r::transport::encode_component(dataset, &mut u);
    u.push_str("/tables/");
    objstore_r::transport::encode_component(table, &mut u);
    u
}

/// Parse a table resource, keeping the raw bytes.
pub(crate) fn parse_table(raw: Bytes) -> Result<TableMeta> {
    let text = std::str::from_utf8(&raw)
        .map_err(|_| Error::from(crate::base::Error::json("table resource is not UTF-8")))?;
    let mut s = Scanner::new(text);
    let mut buf = String::new();
    let mut meta = TableMeta {
        etag: String::new(),
        kind: String::new(),
        description: None,
        view_query: None,
        num_rows: 0,
        last_modified_ms: 0,
        raw: Bytes::new(),
        schema: 0..0,
    };
    if s.expect()? != Event::ObjectStart {
        return Err(Error::from(crate::base::Error::json("table resource is not an object")));
    }
    let str_val = |s: &mut Scanner<'_>, buf: &mut String| -> Result<Option<String>> {
        Ok(match s.expect()? {
            Event::Str(v) => Some(v.decode(buf)?.to_owned()),
            other => {
                s.skip(other)?;
                None
            }
        })
    };
    let num_val = |s: &mut Scanner<'_>| -> Result<u64> {
        Ok(match s.expect()? {
            // BigQuery sends int64 fields as JSON strings.
            Event::Str(v) => v.raw().parse().unwrap_or(0),
            Event::Num(n) => json::parse_u64(n).unwrap_or(0),
            other => {
                s.skip(other)?;
                0
            }
        })
    };
    loop {
        match s.expect()? {
            Event::ObjectEnd => break,
            Event::Key(k) => match k.as_plain().unwrap_or("") {
                "etag" => meta.etag = str_val(&mut s, &mut buf)?.unwrap_or_default(),
                "type" => meta.kind = str_val(&mut s, &mut buf)?.unwrap_or_default(),
                "description" => {
                    meta.description = str_val(&mut s, &mut buf)?.filter(|d| !d.trim().is_empty());
                }
                "numRows" => meta.num_rows = num_val(&mut s)?,
                "lastModifiedTime" => meta.last_modified_ms = num_val(&mut s)?,
                "schema" => {
                    let before = s.offset();
                    let v = s.expect()?;
                    // The value starts at its '{' (after ':' and spaces).
                    let start = text[before..].find('{').map_or(before, |i| before + i);
                    s.skip(v)?;
                    meta.schema = start..s.offset();
                }
                "view" | "materializedView" => {
                    let v = s.expect()?;
                    if v != Event::ObjectStart {
                        s.skip(v)?;
                        continue;
                    }
                    loop {
                        match s.expect()? {
                            Event::ObjectEnd => break,
                            Event::Key(k) if k.eq_plain("query") => {
                                meta.view_query = str_val(&mut s, &mut buf)?;
                            }
                            Event::Key(_) => {
                                let v = s.expect()?;
                                s.skip(v)?;
                            }
                            _ => return Err(Error::from(crate::base::Error::json("expected key"))),
                        }
                    }
                }
                _ => {
                    let v = s.expect()?;
                    s.skip(v)?;
                }
            },
            _ => return Err(Error::from(crate::base::Error::json("expected key"))),
        }
    }
    meta.raw = raw;
    Ok(meta)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_raw_bytes_and_the_schema_range() {
        let raw = r#"{"kind":"bigquery#table","etag":"AbC==","type":"VIEW","description":"Orders by month",
            "schema": {"fields":[{"name":"a","type":"STRING","policyTags":{"names":["x"]},"collation":"und:ci"}]},
            "numRows":"1200","lastModifiedTime":"1790000000000","view":{"query":"SELECT 1 AS a","useLegacySql":false}}"#;
        let m = parse_table(Bytes::from(raw)).unwrap();
        assert_eq!(
            (m.etag.as_str(), m.kind.as_str(), m.description.as_deref()),
            ("AbC==", "VIEW", Some("Orders by month"))
        );
        assert_eq!((m.num_rows, m.last_modified_ms), (1200, 1_790_000_000_000));
        assert_eq!(m.view_query.as_deref(), Some("SELECT 1 AS a"));
        assert_eq!(
            m.schema_json(),
            r#"{"fields":[{"name":"a","type":"STRING","policyTags":{"names":["x"]},"collation":"und:ci"}]}"#
        );
        assert!(parse_table(Bytes::from_static(b"[1]")).is_err());
        assert_eq!(
            table_url("https://h", "p", "d", "t"),
            "https://h/bigquery/v2/projects/p/datasets/d/tables/t"
        );
    }
}
