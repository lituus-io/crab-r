// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! OpenAI strict structured outputs, by construction.
//!
//! With `"strict": true` an OpenAI-compatible server accepts a
//! `response_format` schema only when every object schema, at every depth,
//! sets `additionalProperties` to `false` and lists every property in
//! `required`; a field the reply may leave out is spelled as nullable
//! instead. [`write_strict`] rewrites any schema into that form as it is
//! serialised, so whatever schema a caller builds goes out compliant:
//!
//! - an object node (`"type": "object"`, or one carrying `properties`) gets
//!   `"additionalProperties": false` and `required` = every property key, in
//!   order (any `additionalProperties` / `required` it had are replaced);
//! - a property that was not required becomes nullable: a string `type`
//!   becomes `[type, "null"]` (and an `enum` gains `null`), a `type` array
//!   gains `"null"`, and a node without a `type` becomes
//!   `{"anyOf": [node, {"type": "null"}]}`;
//! - sub-schemas are rewritten wherever JSON Schema nests them
//!   (`properties`, `items`, `not`, `anyOf` / `oneOf` / `allOf`,
//!   `prefixItems`, `$defs` / `definitions`); every other member is copied.
//!
//! The rewrite borrows the source text: keys and strings are re-emitted from
//! their source slices (decoded only when they hold escapes), and a schema
//! that is already compliant comes out with the same meaning. Text that is
//! not one JSON document, or an object with a duplicate key (which parsers
//! read differently), is refused rather than sent.

use crate::base::json::{Event, RawStr, Scanner, Writer};
use crate::base::{Error, Result};

/// One JSON value, borrowing the source text.
#[derive(Debug)]
enum Node<'a> {
    Str(RawStr<'a>),
    Num(&'a str),
    Bool(bool),
    Null,
    Arr(Vec<Node<'a>>),
    Obj(Vec<(RawStr<'a>, Node<'a>)>),
}

impl<'a> Node<'a> {
    fn parse(src: &'a str) -> Result<Self> {
        let mut s = Scanner::new(src);
        let first = s.expect()?;
        let node = Self::from_event(&mut s, first)?;
        if s.next_event()?.is_some() {
            return Err(Error::json("text after the schema"));
        }
        Ok(node)
    }

    /// Nesting is bounded by the scanner ([`crate::base::json::MAX_DEPTH`]).
    fn from_event(s: &mut Scanner<'a>, ev: Event<'a>) -> Result<Self> {
        Ok(match ev {
            Event::Str(v) => Self::Str(v),
            Event::Num(n) => Self::Num(n),
            Event::Bool(b) => Self::Bool(b),
            Event::Null => Self::Null,
            Event::ArrayStart => {
                let mut out = Vec::new();
                loop {
                    match s.expect()? {
                        Event::ArrayEnd => break,
                        e => out.push(Self::from_event(s, e)?),
                    }
                }
                Self::Arr(out)
            }
            Event::ObjectStart => {
                let mut out = Vec::new();
                loop {
                    match s.expect()? {
                        Event::ObjectEnd => break,
                        Event::Key(k) => {
                            // Parsers disagree on which duplicate wins, so
                            // a schema with one means two things: refused.
                            if out.iter().any(|(seen, _)| same(*seen, k)) {
                                return Err(Error::json("duplicate key in the schema"));
                            }
                            let e = s.expect()?;
                            out.push((k, Self::from_event(s, e)?));
                        }
                        _ => return Err(Error::json("malformed object")),
                    }
                }
                Self::Obj(out)
            }
            Event::ObjectEnd | Event::ArrayEnd | Event::Key(_) => {
                return Err(Error::json("malformed schema"))
            }
        })
    }

    fn member(&self, key: &str) -> Option<&Node<'a>> {
        match self {
            Self::Obj(m) => m.iter().find(|(k, _)| is(*k, key)).map(|(_, v)| v),
            _ => None,
        }
    }

    fn is_str(&self, s: &str) -> bool {
        matches!(self, Self::Str(v) if is(*v, s))
    }
}

/// `r` decodes to `s` (borrowed compare unless `r` holds escapes).
fn is(r: RawStr<'_>, s: &str) -> bool {
    match r.as_plain() {
        Some(p) => p == s,
        None => r.decode(&mut String::new()).is_ok_and(|d| d == s),
    }
}

/// `a` and `b` decode to the same text.
fn same(a: RawStr<'_>, b: RawStr<'_>) -> bool {
    match a.as_plain() {
        Some(p) => is(b, p),
        None => a.decode(&mut String::new()).is_ok_and(|d| is(b, d)),
    }
}

/// Write `schema` (JSON Schema text) into `w` as a strict-compliant schema.
pub fn write_strict(schema: &str, w: &mut Writer<'_>) -> Result<()> {
    let root = Node::parse(schema)?;
    let mut buf = String::new();
    emit_schema(&root, false, w, &mut buf)
}

/// [`write_strict`] into a new string.
pub fn strict(schema: &str) -> Result<String> {
    let mut out = String::with_capacity(schema.len() + schema.len() / 4 + 64);
    write_strict(schema, &mut Writer::new(&mut out))?;
    Ok(out)
}

fn emit_string(v: RawStr<'_>, w: &mut Writer<'_>, buf: &mut String) -> Result<()> {
    w.string(v.decode(buf)?);
    Ok(())
}

fn emit_key(k: RawStr<'_>, w: &mut Writer<'_>, buf: &mut String) -> Result<()> {
    w.key(k.decode(buf)?);
    Ok(())
}

/// Copy a value that is data, not a schema.
fn emit_value(n: &Node<'_>, w: &mut Writer<'_>, buf: &mut String) -> Result<()> {
    match n {
        Node::Str(v) => emit_string(*v, w, buf)?,
        Node::Num(t) => {
            w.raw(t);
        }
        Node::Bool(b) => {
            w.bool(*b);
        }
        Node::Null => {
            w.null();
        }
        Node::Arr(items) => {
            w.begin_array();
            for i in items {
                emit_value(i, w, buf)?;
            }
            w.end_array();
        }
        Node::Obj(members) => {
            w.begin_object();
            for (k, v) in members {
                emit_key(*k, w, buf)?;
                emit_value(v, w, buf)?;
            }
            w.end_object();
        }
    }
    Ok(())
}

/// A schema node; `nullable` when its property was not required.
fn emit_schema(n: &Node<'_>, nullable: bool, w: &mut Writer<'_>, buf: &mut String) -> Result<()> {
    let Node::Obj(members) = n else {
        // A boolean schema (or something that is not a schema): as given.
        return emit_value(n, w, buf);
    };
    let ty = n.member("type");
    // Nullable by type: possible when the node names its type(s).
    let typed = matches!(ty, Some(Node::Str(_) | Node::Arr(_)));
    if nullable && !typed {
        w.begin_object().key("anyOf").begin_array();
        emit_schema(n, false, w, buf)?;
        w.begin_object().key("type").string("null").end_object();
        w.end_array().end_object();
        return Ok(());
    }
    let props = match n.member("properties") {
        Some(Node::Obj(p)) => Some(p.as_slice()),
        Some(_) => return Err(Error::json("properties is not an object")),
        None => None,
    };
    let is_object = props.is_some()
        || ty.is_some_and(|t| match t {
            Node::Arr(ts) => ts.iter().any(|t| t.is_str("object")),
            t => t.is_str("object"),
        });
    let required: &[Node<'_>] = match n.member("required") {
        Some(Node::Arr(r)) => r,
        _ => &[],
    };
    w.begin_object();
    for (k, v) in members {
        let mut decoded = String::new();
        let key = k.decode(&mut decoded)?;
        match key {
            "additionalProperties" | "required" if is_object => {}
            "type" if nullable => {
                w.key("type");
                emit_nullable_type(v, w, buf)?;
            }
            "enum" if nullable => {
                w.key("enum");
                emit_nullable_enum(v, w, buf)?;
            }
            "properties" | "$defs" | "definitions" => {
                emit_key(*k, w, buf)?;
                let Node::Obj(map) = v else {
                    emit_value(v, w, buf)?;
                    continue;
                };
                emit_schemas(map, (key == "properties").then_some(required), w, buf)?;
            }
            "items" | "not" | "additionalProperties" => {
                emit_key(*k, w, buf)?;
                emit_schema(v, false, w, buf)?;
            }
            "anyOf" | "oneOf" | "allOf" | "prefixItems" => {
                emit_key(*k, w, buf)?;
                match v {
                    Node::Arr(subs) => {
                        w.begin_array();
                        for sub in subs {
                            emit_schema(sub, false, w, buf)?;
                        }
                        w.end_array();
                    }
                    other => emit_value(other, w, buf)?,
                }
            }
            _ => {
                emit_key(*k, w, buf)?;
                emit_value(v, w, buf)?;
            }
        }
    }
    if is_object {
        if props.is_none() {
            w.key("properties").begin_object().end_object();
        }
        w.key("additionalProperties").bool(false);
        w.key("required").begin_array();
        for (name, _) in props.unwrap_or(&[]) {
            emit_string(*name, w, buf)?;
        }
        w.end_array();
    }
    w.end_object();
    Ok(())
}

/// A `type` that admits `null` too.
fn emit_nullable_type(v: &Node<'_>, w: &mut Writer<'_>, buf: &mut String) -> Result<()> {
    match v {
        Node::Str(t) if is(*t, "null") => emit_string(*t, w, buf),
        Node::Str(t) => {
            w.begin_array();
            emit_string(*t, w, buf)?;
            w.string("null").end_array();
            Ok(())
        }
        Node::Arr(ts) => {
            w.begin_array();
            for t in ts {
                emit_value(t, w, buf)?;
            }
            if !ts.iter().any(|t| t.is_str("null")) {
                w.string("null");
            }
            w.end_array();
            Ok(())
        }
        other => emit_value(other, w, buf),
    }
}

/// An `enum` that admits `null` too.
fn emit_nullable_enum(v: &Node<'_>, w: &mut Writer<'_>, buf: &mut String) -> Result<()> {
    let Node::Arr(vals) = v else { return emit_value(v, w, buf) };
    w.begin_array();
    for x in vals {
        emit_value(x, w, buf)?;
    }
    if !vals.iter().any(|x| matches!(x, Node::Null)) {
        w.null();
    }
    w.end_array();
    Ok(())
}

/// A map of named sub-schemas. For `properties` (`required` given), a name
/// the node did not require is nullable.
fn emit_schemas(
    map: &[(RawStr<'_>, Node<'_>)],
    required: Option<&[Node<'_>]>,
    w: &mut Writer<'_>,
    buf: &mut String,
) -> Result<()> {
    w.begin_object();
    for (name, sub) in map {
        emit_key(*name, w, buf)?;
        let optional = required
            .is_some_and(|req| !req.iter().any(|r| matches!(r, Node::Str(s) if same(*s, *name))));
        emit_schema(sub, optional, w, buf)?;
    }
    w.end_object();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::describe::generate::{draft_schema, judge_schema};
    use crate::engine::context::link_schema;
    use proptest::prelude::*;

    /// Why `schema` breaks the strict rules, with the path to the node;
    /// `None` when it complies.
    fn violation(schema: &str) -> Option<String> {
        let root = Node::parse(schema).expect("valid JSON");
        let mut path = Vec::new();
        walk(&root, &mut path).map(|why| format!("at /{}: {why}", path.join("/")))
    }

    fn keys<'a>(n: &Node<'a>) -> Vec<&'a str> {
        match n {
            Node::Obj(m) => m.iter().map(|(k, _)| k.raw()).collect(),
            _ => Vec::new(),
        }
    }

    fn walk(n: &Node<'_>, path: &mut Vec<String>) -> Option<String> {
        let Node::Obj(members) = n else { return None };
        let props = n.member("properties");
        let is_object = props.is_some()
            || n.member("type").is_some_and(|t| match t {
                Node::Arr(ts) => ts.iter().any(|t| t.is_str("object")),
                t => t.is_str("object"),
            });
        if is_object {
            if !matches!(n.member("additionalProperties"), Some(Node::Bool(false))) {
                return Some("additionalProperties is not false".into());
            }
            let want = props.map(keys).unwrap_or_default();
            let got: Vec<&str> = match n.member("required") {
                Some(Node::Arr(r)) => r
                    .iter()
                    .map(|x| if let Node::Str(s) = x { s.raw() } else { "<non-string>" })
                    .collect(),
                _ => return Some("no required array".into()),
            };
            if got != want {
                return Some(format!("required {got:?} != properties {want:?}"));
            }
        }
        for (k, v) in members {
            let key = k.raw();
            let subs: Vec<(String, &Node<'_>)> = match (key, v) {
                ("properties" | "$defs" | "definitions", Node::Obj(map)) => {
                    map.iter().map(|(name, sub)| (format!("{key}/{}", name.raw()), sub)).collect()
                }
                ("items" | "not" | "additionalProperties", sub) => vec![(key.to_owned(), sub)],
                ("anyOf" | "oneOf" | "allOf" | "prefixItems", Node::Arr(a)) => {
                    a.iter().enumerate().map(|(i, sub)| (format!("{key}/{i}"), sub)).collect()
                }
                _ => Vec::new(),
            };
            for (p, sub) in subs {
                path.push(p);
                if let Some(why) = walk(sub, path) {
                    return Some(why);
                }
                path.pop();
            }
        }
        None
    }

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_owned()).collect()
    }

    /// Every schema the crate sends as a `response_format`.
    fn every_schema(
        columns: &[String],
        all: &[String],
        ids: &[u32],
    ) -> Vec<(&'static str, String)> {
        vec![
            ("draft", draft_schema(columns, all)),
            ("judge", judge_schema(columns, ids)),
            ("link", link_schema(all)),
        ]
    }

    #[test]
    fn the_schemas_as_built_are_not_strict_and_rewritten_they_are() {
        let c = cols(&["order_id", "amount_usd"]);
        for (name, raw) in every_schema(&c, &c, &[1, 2]) {
            // The root cause: built for Vertex AI's subset, with optional
            // fields and open objects. A strict server refuses them.
            assert!(violation(&raw).is_some(), "{name} was compliant as built: {raw}");
            let out = strict(&raw).unwrap();
            assert_eq!(violation(&out), None, "{name}: {out}");
        }
    }

    #[test]
    fn optional_fields_become_nullable_and_required_ones_keep_their_schema() {
        let c = cols(&["status"]);
        let out = strict(&draft_schema(&c, &c)).unwrap();
        let root = Node::parse(&out).unwrap();
        let props = root.member("properties").unwrap();
        // Required by the builder: unchanged.
        assert!(props
            .member("table_description")
            .unwrap()
            .member("type")
            .unwrap()
            .is_str("string"));
        // Optional: nullable, its own shape kept.
        let grain = props.member("grain").unwrap();
        assert!(
            matches!(grain.member("type"), Some(Node::Arr(t)) if t.len() == 2 && t[0].is_str("array") && t[1].is_str("null"))
        );
        assert!(grain.member("items").unwrap().member("enum").is_some());
        let col = props.member("columns").unwrap().member("items").unwrap();
        let unit = col.member("properties").unwrap().member("unit").unwrap();
        assert!(matches!(unit.member("type"), Some(Node::Arr(t)) if t[1].is_str("null")));
        // An optional enum admits null in the enum too.
        let s = r#"{"type":"object","properties":{"a":{"type":"string","enum":["x","y"]}}}"#;
        assert_eq!(
            strict(s).unwrap(),
            r#"{"type":"object","properties":{"a":{"type":["string","null"],"enum":["x","y",null]}},"additionalProperties":false,"required":["a"]}"#
        );
    }

    #[test]
    fn nested_and_unusual_shapes() {
        // Untyped optional: wrapped in anyOf; $defs, anyOf and type arrays
        // are walked; an object with no properties is closed and empty.
        let src = r##"{"type":"object","properties":{
            "r":{"$ref":"#/$defs/x"},
            "u":{"anyOf":[{"type":"object","properties":{"k":{"type":"integer"}}},{"type":"string"}]},
            "t":{"type":["object","null"],"properties":{"z":{"type":"boolean"}},"required":["z"]},
            "e":{"type":"object"}},
          "required":["u","t","e"],
          "additionalProperties":true,
          "$defs":{"x":{"type":"object","properties":{"q":{"type":"string"}}}}}"##;
        let out = strict(src).unwrap();
        assert_eq!(violation(&out), None, "{out}");
        let root = Node::parse(&out).unwrap();
        let r = root.member("properties").unwrap().member("r").unwrap();
        assert!(
            matches!(r.member("anyOf"), Some(Node::Arr(a)) if a.len() == 2 && a[1].member("type").unwrap().is_str("null"))
        );
        let t = root.member("properties").unwrap().member("t").unwrap();
        assert!(
            matches!(t.member("type"), Some(Node::Arr(a)) if a.len() == 2),
            "a required node's type is kept"
        );
        let empty = root.member("properties").unwrap().member("e").unwrap();
        assert!(matches!(empty.member("properties"), Some(Node::Obj(m)) if m.is_empty()));
        // Escaped keys and strings survive.
        let escaped = r#"{"type":"object","properties":{"a\"b":{"type":"string","description":"tab\there"}},"required":["a\"b"]}"#;
        let out = strict(escaped).unwrap();
        assert_eq!(violation(&out), None);
        assert!(out.contains(r#""a\"b":{"type":"string","description":"tab\there"}"#), "{out}");
        assert!(out.ends_with(r#""required":["a\"b"]}"#), "{out}");
        // A keyword spelled with escapes is the keyword; the rewrite is a
        // fixed point after one pass.
        let spelled = r#"{"\u0074ype":"object","\u0070roperties":{"x":{"type":"string"}},"required":["\u0078"]}"#;
        let out = strict(spelled).unwrap();
        assert_eq!(
            out,
            r#"{"type":"object","properties":{"x":{"type":"string"}},"additionalProperties":false,"required":["x"]}"#
        );
        assert_eq!(strict(&out).unwrap(), out);
        // Not a schema: refused, never sent half-written.
        for bad in [
            "",
            "{",
            r#"{"type":}"#,
            "{} {}",
            "[1,",
            r#"{"properties":{"a":{}},"properties":{"b":{}}}"#,
            r#"{"a":1,"\u0061":2}"#,
            r#"{"type":"object","properties":["a"]}"#,
        ] {
            assert!(strict(bad).is_err(), "{bad:?}");
        }
        assert_eq!(strict("true").unwrap(), "true");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Whatever the table's column names (quotes, backslashes, control
        /// characters, non-ASCII) and check ids, every schema the crate can
        /// send goes out strict, the rewrite is idempotent, and nothing it
        /// constrains is lost: every enum value is still there.
        #[test]
        fn every_schema_the_crate_sends_goes_out_strict(
            columns in prop::collection::vec("[a-z_\"\\\\\u{1}\u{e9}\u{4e2d}]{1,12}", 0..8),
            extra in prop::collection::vec("[A-Za-z0-9_.]{1,10}", 0..4),
            ids in prop::collection::vec(0u32..10_000, 0..6),
        ) {
            let mut all = columns.clone();
            all.extend(extra);
            for (name, raw) in every_schema(&columns, &all, &ids) {
                let out = strict(&raw).unwrap();
                prop_assert_eq!(violation(&out), None, "{}: {}", name, out);
                prop_assert_eq!(strict(&out).unwrap(), out.clone(), "{} idempotent", name);
                let decoded = |s: &str| {
                    let mut v = Vec::new();
                    let mut sc = Scanner::new(s);
                    let mut buf = String::new();
                    while let Some(e) = sc.next_event().unwrap() {
                        if let Event::Str(x) = e {
                            v.push(x.decode(&mut buf).unwrap().to_owned());
                        }
                    }
                    v
                };
                let got = decoded(&out);
                for want in decoded(&raw) {
                    prop_assert!(got.contains(&want), "{}: lost {:?}", name, want);
                }
            }
        }
    }
}
