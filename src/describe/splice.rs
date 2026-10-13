// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Byte-exact description splicing.
//!
//! Given a BigQuery schema as JSON (`{"fields": [...]}` or the bare
//! `[...]` array) and new descriptions by column path, write the same JSON
//! with only the `description` members changed: an existing description's
//! value is replaced in place; a field without one gets
//! `,"description":"..."` inserted before its closing brace. Every other
//! byte — policy tags, collation, default values, members this code does not
//! know, spacing — is copied verbatim, so the result can be committed next
//! to Terraform/Pulumi table definitions or sent back in a patch without
//! dropping anything. No document object model is built: one scan records
//! the edit points, one pass copies around them.

use crate::base::json::{escape_into, Event, Scanner};
use std::collections::BTreeMap;

/// One edit: replace `range` (possibly empty, an insertion point) with
/// `text`.
#[derive(Debug)]
struct Edit {
    at: usize,
    end: usize,
    text: String,
}

/// Splice `updates` (column path, lower-cased → description) into `schema`.
/// Returns the new JSON and how many fields changed. Paths not in the schema
/// are ignored.
pub fn splice(
    schema: &str,
    updates: &BTreeMap<String, String>,
) -> crate::base::Result<(String, usize)> {
    let mut edits: Vec<Edit> = Vec::new();
    let mut s = Scanner::new(schema);
    match s.expect()? {
        Event::ObjectStart => object_with_fields(&mut s, schema, "", updates, &mut edits)?,
        Event::ArrayStart => fields(&mut s, schema, "", updates, &mut edits)?,
        _ => {
            return Err(crate::base::Error::Json { at: 0, msg: "schema is not an object or array" })
        }
    }
    edits.sort_by_key(|e| e.at);
    let mut out =
        String::with_capacity(schema.len() + edits.iter().map(|e| e.text.len()).sum::<usize>());
    let (mut pos, mut n) = (0, 0);
    for e in &edits {
        if e.at < pos {
            // Overlapping edits only arise from malformed duplicates; the
            // first one wins.
            continue;
        }
        out.push_str(&schema[pos..e.at]);
        out.push_str(&e.text);
        pos = e.end;
        n += 1;
    }
    out.push_str(&schema[pos..]);
    Ok((out, n))
}

/// Inside `{ ... "fields": [...] ... }` (the schema object): find `fields`.
fn object_with_fields(
    s: &mut Scanner<'_>,
    src: &str,
    prefix: &str,
    updates: &BTreeMap<String, String>,
    edits: &mut Vec<Edit>,
) -> crate::base::Result<()> {
    loop {
        match s.expect()? {
            Event::ObjectEnd => return Ok(()),
            Event::Key(k) if k.eq_plain("fields") => {
                let v = s.expect()?;
                if v == Event::ArrayStart {
                    fields(s, src, prefix, updates, edits)?;
                } else {
                    s.skip(v)?;
                }
            }
            Event::Key(_) => {
                let v = s.expect()?;
                s.skip(v)?;
            }
            _ => return Err(crate::base::Error::Json { at: s.offset(), msg: "expected key" }),
        }
    }
}

/// A `fields` array (after its `[`): one field object per element.
fn fields(
    s: &mut Scanner<'_>,
    src: &str,
    prefix: &str,
    updates: &BTreeMap<String, String>,
    edits: &mut Vec<Edit>,
) -> crate::base::Result<()> {
    let mut buf = String::new();
    loop {
        match s.expect()? {
            Event::ArrayEnd => return Ok(()),
            Event::ObjectStart => {
                let mut name: Option<String> = None;
                let mut desc: Option<(usize, usize)> = None;
                // Nested fields are scanned after the name is known: record
                // where they are, and recurse once the object is closed.
                let mut nested: Option<usize> = None;
                let mut members = 0usize;
                loop {
                    match s.expect()? {
                        Event::ObjectEnd => break,
                        Event::Key(k) => {
                            members += 1;
                            if k.eq_plain("name") {
                                match s.expect()? {
                                    Event::Str(v) => {
                                        name = Some(v.decode(&mut buf)?.to_ascii_lowercase());
                                    }
                                    other => s.skip(other)?,
                                }
                            } else if k.eq_plain("description") {
                                let before = s.offset();
                                let v = s.expect()?;
                                let start = src[before..]
                                    .find(|c: char| !(c.is_whitespace() || c == ':'))
                                    .map_or(before, |i| before + i);
                                s.skip(v)?;
                                desc = Some((start, s.offset()));
                            } else if k.eq_plain("fields") {
                                let before = s.offset();
                                let v = s.expect()?;
                                // Only an array value: its `[` is the first
                                // byte after the colon and spaces.
                                if v == Event::ArrayStart {
                                    nested = src[before..]
                                        .find(|c: char| !(c.is_whitespace() || c == ':'))
                                        .map(|i| before + i)
                                        .filter(|&i| src.as_bytes()[i] == b'[');
                                }
                                s.skip(v)?;
                            } else {
                                let v = s.expect()?;
                                s.skip(v)?;
                            }
                        }
                        _ => {
                            return Err(crate::base::Error::Json {
                                at: s.offset(),
                                msg: "expected key",
                            })
                        }
                    }
                }
                // The field object's closing brace is the last byte read.
                let close = s.offset() - 1;
                let path = match (&name, prefix.is_empty()) {
                    (Some(n), true) => n.clone(),
                    (Some(n), false) => format!("{prefix}.{n}"),
                    (None, _) => String::new(),
                };
                if let Some(text) = updates.get(&path).filter(|_| !path.is_empty()) {
                    let mut lit = String::with_capacity(text.len() + 2);
                    escape_into(text, &mut lit);
                    if let Some((a, b)) = desc {
                        edits.push(Edit { at: a, end: b, text: lit });
                    } else {
                        // After the last member, in the document's own
                        // style: a new line with the member indentation
                        // when the object spans lines, `": "` when the
                        // document puts a space after colons.
                        let last = src[..close].trim_end().len();
                        let multiline = src[last..close].contains('\n');
                        let colon = if src.contains("\": ") { "\": " } else { "\":" };
                        let sep = if members > 0 { "," } else { "" };
                        let text = if multiline && members > 0 {
                            let line_start = src[..last].rfind('\n').map_or(0, |i| i + 1);
                            let indent: String = src[line_start..]
                                .chars()
                                .take_while(|c| *c == ' ' || *c == '\t')
                                .collect();
                            format!("{sep}\n{indent}\"description{colon}{lit}")
                        } else {
                            format!("{sep}\"description{colon}{lit}")
                        };
                        edits.push(Edit { at: last, end: last, text });
                    }
                }
                if let Some(at) = nested {
                    let mut sub = Scanner::new(&src[at..]);
                    if sub.expect()? == Event::ArrayStart {
                        let mut sub_edits = Vec::new();
                        fields(&mut sub, &src[at..], &path, updates, &mut sub_edits)?;
                        edits.extend(sub_edits.into_iter().map(|e| Edit {
                            at: e.at + at,
                            end: e.end + at,
                            text: e.text,
                        }));
                    }
                }
            }
            other => s.skip(other)?,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn up(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
    }

    #[test]
    fn replaces_inserts_and_preserves_everything_else() {
        let schema = r#"{"fields": [
  {"name": "id", "type": "INTEGER", "mode": "REQUIRED", "policyTags": {"names": ["projects/p/taxonomies/1/policyTags/2"]}},
  {"name": "Email", "type": "STRING", "description": "old \"quoted\"", "collation": "und:ci", "defaultValueExpression": "'n/a'"},
  {"name": "addr", "type": "RECORD", "fields": [{"name": "city", "type": "STRING"}, {"name": "zip", "type": "STRING", "description": ""}]},
  {}
]}"#;
        let (out, n) = splice(
            schema,
            &up(&[
                ("id", "Customer key"),
                ("email", "Primary e-mail, lower-cased\nnote"),
                ("addr.city", "City"),
                ("addr.zip", "Postal code"),
                ("nope", "x"),
            ]),
        )
        .unwrap();
        assert_eq!(n, 4);
        assert!(out.contains(r#""policyTags": {"names": ["projects/p/taxonomies/1/policyTags/2"]},"description": "Customer key"}"#), "{out}");
        assert!(out.contains(r#""description": "Primary e-mail, lower-cased\nnote", "collation": "und:ci", "defaultValueExpression": "'n/a'""#), "{out}");
        assert!(
            out.contains(r#"{"name": "city", "type": "STRING","description": "City"}"#),
            "{out}"
        );
        assert!(
            out.contains(r#"{"name": "zip", "type": "STRING", "description": "Postal code"}"#),
            "{out}"
        );
        assert!(out.ends_with("  {}\n]}"), "untouched tail");
        // Still valid JSON, and a second identical splice changes nothing.
        let (again, _) = splice(&out, &up(&[("id", "Customer key")])).unwrap();
        assert_eq!(again, out);
        // Bare arrays (a `schema.json` for IaC) work too.
        let (arr, n) = splice(r#"[{"name":"a","type":"STRING"}]"#, &up(&[("a", "A")])).unwrap();
        assert_eq!((arr.as_str(), n), (r#"[{"name":"a","type":"STRING","description":"A"}]"#, 1));
    }

    #[test]
    fn insertions_follow_the_document_layout() {
        let pretty = "[\n  {\n    \"name\": \"id\",\n    \"type\": \"INTEGER\"\n  }\n]";
        let (out, _) = splice(pretty, &up(&[("id", "Key")])).unwrap();
        assert_eq!(out, "[\n  {\n    \"name\": \"id\",\n    \"type\": \"INTEGER\",\n    \"description\": \"Key\"\n  }\n]");
    }

    #[test]
    fn rejects_non_schemas() {
        assert!(splice("42", &BTreeMap::new()).is_err());
        assert!(splice(r#"{"fields": [ {"name": "a" "#, &BTreeMap::new()).is_err());
    }

    #[test]
    fn malformed_names_and_fields_never_reach_outside_their_field() {
        // A non-array `fields` must not send the scan to a later `[`, and a
        // non-string `name` must be skipped whole.
        let src = r#"{"fields":[{"name":"rec","fields":"old"},{"name":{"x":[1]},"a":2}],"z":[{"name":"rec"}]}"#;
        let (out, n) = splice(src, &up(&[("rec", "R")])).unwrap();
        assert_eq!(n, 1);
        assert_eq!(
            out,
            r#"{"fields":[{"name":"rec","fields":"old","description":"R"},{"name":{"x":[1]},"a":2}],"z":[{"name":"rec"}]}"#
        );
        let crash = include_str!("../../fuzz/regressions/splice_fields_not_array.json");
        let _ = splice(crash, &up(&[("a", "A"), ("id", ""), ("rec.x", "nested"), ("b", "B")]));
    }
}
