// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Zero-copy JSON: a pull [`Scanner`] and a streaming [`Writer`].
//!
//! Model APIs and the BigQuery REST API speak JSON. The scanner walks a
//! validated `&str` and yields [`Event`]s whose strings are borrowed slices
//! of the input ([`RawStr`]); escapes are decoded only when a caller asks, into
//! a caller-owned buffer. Nothing is allocated per value.
//!
//! Hostile input is expected (it arrives from the network): every error is a
//! typed [`Error::Json`] with a byte offset, nesting depth is capped, and the
//! scanner never panics — the fuzz target enforces that.

use crate::{Error, Result};

/// Maximum nesting depth accepted.
pub const MAX_DEPTH: usize = 128;

/// A string exactly as it appears between the quotes, escapes undecoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RawStr<'a> {
    raw: &'a str,
    escaped: bool,
}

impl<'a> RawStr<'a> {
    /// The decoded text when no escape occurs (the common case): zero-copy.
    #[must_use]
    pub fn as_plain(self) -> Option<&'a str> {
        (!self.escaped).then_some(self.raw)
    }

    /// The undecoded source text.
    #[must_use]
    pub fn raw(self) -> &'a str {
        self.raw
    }

    /// True when the string contains escapes.
    #[must_use]
    pub fn has_escapes(self) -> bool {
        self.escaped
    }

    /// Decoded text: borrowed when escape-free, else decoded into `buf`.
    pub fn decode<'b>(self, buf: &'b mut String) -> Result<&'b str>
    where
        'a: 'b,
    {
        if !self.escaped {
            return Ok(self.raw);
        }
        buf.clear();
        unescape(self.raw, buf)?;
        Ok(buf.as_str())
    }

    /// Compare against a plain (escape-free) key without decoding.
    #[must_use]
    pub fn eq_plain(self, s: &str) -> bool {
        !self.escaped && self.raw == s
    }
}

/// One scanner event.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Event<'a> {
    /// `{`
    ObjectStart,
    /// `}`
    ObjectEnd,
    /// `[`
    ArrayStart,
    /// `]`
    ArrayEnd,
    /// An object key (the following event is its value).
    Key(RawStr<'a>),
    /// A string value.
    Str(RawStr<'a>),
    /// A number, as its source text (parse with [`parse_i64`] / [`parse_f64`]).
    Num(&'a str),
    /// `true` / `false`.
    Bool(bool),
    /// `null`.
    Null,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ctx {
    /// In an object, expecting a key (or `}`).
    ObjKey,
    /// In an object, expecting a value after `:`.
    ObjVal,
    /// In an array, expecting a value (or `]`).
    Arr,
}

/// A pull scanner over one JSON document.
#[derive(Debug)]
pub struct Scanner<'a> {
    src: &'a str,
    pos: usize,
    stack: Vec<(Ctx, bool)>, // (context, seen-first-element)
    done: bool,
    started: bool,
}

impl<'a> Scanner<'a> {
    /// Scan `src`.
    #[must_use]
    pub fn new(src: &'a str) -> Self {
        Self { src, pos: 0, stack: Vec::with_capacity(16), done: false, started: false }
    }

    /// Scan UTF-8 bytes (validated once, up front).
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self> {
        let src = std::str::from_utf8(bytes)
            .map_err(|e| Error::Json { at: e.valid_up_to(), msg: "invalid utf-8" })?;
        Ok(Self::new(src))
    }

    /// Current byte offset.
    #[must_use]
    pub fn offset(&self) -> usize {
        self.pos
    }

    /// Current nesting depth.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.stack.len()
    }

    fn err<T>(&self, msg: &'static str) -> Result<T> {
        Err(Error::Json { at: self.pos, msg })
    }

    fn skip_ws(&mut self) {
        let b = self.src.as_bytes();
        while let Some(&c) = b.get(self.pos) {
            if matches!(c, b' ' | b'\t' | b'\n' | b'\r') {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.as_bytes().get(self.pos).copied()
    }

    /// Next event, or `None` at the end of a complete document. Trailing
    /// non-whitespace after the document is an error.
    pub fn next_event(&mut self) -> Result<Option<Event<'a>>> {
        if self.done {
            self.skip_ws();
            if self.pos < self.src.len() {
                return self.err("trailing characters");
            }
            return Ok(None);
        }
        self.skip_ws();
        let Some(&(ctx, seen)) = self.stack.last() else {
            if self.started {
                self.done = true;
                return self.next_event();
            }
            self.started = true;
            return self.value();
        };
        match ctx {
            Ctx::ObjKey => {
                if self.peek() == Some(b'}') {
                    self.pos += 1;
                    self.close();
                    return Ok(Some(Event::ObjectEnd));
                }
                if seen {
                    if self.peek() != Some(b',') {
                        return self.err("expected ',' or '}'");
                    }
                    self.pos += 1;
                    self.skip_ws();
                }
                if self.peek() != Some(b'"') {
                    return self.err("expected key");
                }
                let key = self.string()?;
                self.skip_ws();
                if self.peek() != Some(b':') {
                    return self.err("expected ':'");
                }
                self.pos += 1;
                if let Some(top) = self.stack.last_mut() {
                    *top = (Ctx::ObjVal, true);
                }
                Ok(Some(Event::Key(key)))
            }
            Ctx::ObjVal => {
                if let Some(top) = self.stack.last_mut() {
                    *top = (Ctx::ObjKey, true);
                }
                self.value()
            }
            Ctx::Arr => {
                if self.peek() == Some(b']') {
                    self.pos += 1;
                    self.close();
                    return Ok(Some(Event::ArrayEnd));
                }
                if seen {
                    if self.peek() != Some(b',') {
                        return self.err("expected ',' or ']'");
                    }
                    self.pos += 1;
                    self.skip_ws();
                }
                if let Some(top) = self.stack.last_mut() {
                    top.1 = true;
                }
                self.value()
            }
        }
    }

    fn close(&mut self) {
        self.stack.pop();
    }

    fn open(&mut self, ctx: Ctx) -> Result<()> {
        if self.stack.len() >= MAX_DEPTH {
            return self.err("nesting too deep");
        }
        self.stack.push((ctx, false));
        Ok(())
    }

    fn value(&mut self) -> Result<Option<Event<'a>>> {
        self.skip_ws();
        let Some(c) = self.peek() else { return self.err("unexpected end") };
        let ev = match c {
            b'{' => {
                self.pos += 1;
                self.open(Ctx::ObjKey)?;
                Event::ObjectStart
            }
            b'[' => {
                self.pos += 1;
                self.open(Ctx::Arr)?;
                Event::ArrayStart
            }
            b'"' => Event::Str(self.string()?),
            b't' => self.literal("true", Event::Bool(true))?,
            b'f' => self.literal("false", Event::Bool(false))?,
            b'n' => self.literal("null", Event::Null)?,
            b'-' | b'0'..=b'9' => Event::Num(self.number()?),
            _ => return self.err("unexpected character"),
        };
        Ok(Some(ev))
    }

    fn literal(&mut self, word: &'static str, ev: Event<'a>) -> Result<Event<'a>> {
        if self.src.as_bytes()[self.pos..].starts_with(word.as_bytes()) {
            self.pos += word.len();
            Ok(ev)
        } else {
            self.err("invalid literal")
        }
    }

    fn number(&mut self) -> Result<&'a str> {
        let b = self.src.as_bytes();
        let start = self.pos;
        let mut i = self.pos;
        if b.get(i) == Some(&b'-') {
            i += 1;
        }
        let int_start = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == int_start || (b[int_start] == b'0' && i - int_start > 1) {
            self.pos = i;
            return self.err("invalid number");
        }
        if b.get(i) == Some(&b'.') {
            i += 1;
            let f = i;
            while b.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
            if i == f {
                self.pos = i;
                return self.err("invalid fraction");
            }
        }
        if matches!(b.get(i), Some(b'e' | b'E')) {
            i += 1;
            if matches!(b.get(i), Some(b'+' | b'-')) {
                i += 1;
            }
            let e = i;
            while b.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
            if i == e {
                self.pos = i;
                return self.err("invalid exponent");
            }
        }
        self.pos = i;
        Ok(&self.src[start..i])
    }

    fn string(&mut self) -> Result<RawStr<'a>> {
        let b = self.src.as_bytes();
        debug_assert_eq!(b.get(self.pos), Some(&b'"'));
        let start = self.pos + 1;
        let mut i = start;
        let mut escaped = false;
        loop {
            match b.get(i) {
                None => {
                    self.pos = i;
                    return self.err("unterminated string");
                }
                Some(b'"') => break,
                Some(b'\\') => {
                    escaped = true;
                    match b.get(i + 1) {
                        Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => i += 2,
                        Some(b'u') => {
                            if b.get(i + 2..i + 6)
                                .is_none_or(|h| !h.iter().all(u8::is_ascii_hexdigit))
                            {
                                self.pos = i;
                                return self.err("invalid \\u escape");
                            }
                            i += 6;
                        }
                        _ => {
                            self.pos = i;
                            return self.err("invalid escape");
                        }
                    }
                }
                Some(&c) if c < 0x20 => {
                    self.pos = i;
                    return self.err("control character in string");
                }
                Some(_) => i += 1,
            }
        }
        self.pos = i + 1;
        Ok(RawStr { raw: &self.src[start..i], escaped })
    }

    /// Skip the value whose first event was just returned (`first`), including
    /// all nested content. A scalar `first` is already complete.
    pub fn skip(&mut self, first: Event<'a>) -> Result<()> {
        if !matches!(first, Event::ObjectStart | Event::ArrayStart) {
            return Ok(());
        }
        let target = self.stack.len() - 1;
        while self.stack.len() > target {
            if self.next_event()?.is_none() {
                return self.err("unexpected end");
            }
        }
        Ok(())
    }

    /// Pull the next event, erroring on end of input.
    pub fn expect(&mut self) -> Result<Event<'a>> {
        match self.next_event()? {
            Some(e) => Ok(e),
            None => self.err("unexpected end"),
        }
    }
}

/// Decode JSON escapes from `raw` (the text between quotes) into `out`.
pub fn unescape(raw: &str, out: &mut String) -> Result<()> {
    let b = raw.as_bytes();
    let mut i = 0;
    let mut run = 0;
    while i < b.len() {
        if b[i] != b'\\' {
            i += 1;
            continue;
        }
        out.push_str(&raw[run..i]);
        let at = i;
        let bad = move || Error::Json { at, msg: "invalid escape" };
        let c = *b.get(i + 1).ok_or_else(bad)?;
        i += 2;
        match c {
            b'"' => out.push('"'),
            b'\\' => out.push('\\'),
            b'/' => out.push('/'),
            b'b' => out.push('\u{8}'),
            b'f' => out.push('\u{c}'),
            b'n' => out.push('\n'),
            b'r' => out.push('\r'),
            b't' => out.push('\t'),
            b'u' => {
                let hi = hex4(b, i).ok_or_else(bad)?;
                i += 4;
                let cp = if (0xD800..0xDC00).contains(&hi) {
                    // High surrogate: must be followed by \uDC00..DFFF.
                    if b.get(i) != Some(&b'\\') || b.get(i + 1) != Some(&b'u') {
                        return Err(Error::Json { at: i, msg: "lone surrogate" });
                    }
                    let lo = hex4(b, i + 2).ok_or_else(bad)?;
                    if !(0xDC00..0xE000).contains(&lo) {
                        return Err(Error::Json { at: i, msg: "lone surrogate" });
                    }
                    i += 6;
                    0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                } else {
                    hi
                };
                out.push(
                    char::from_u32(cp).ok_or(Error::Json { at: i, msg: "invalid code point" })?,
                );
            }
            _ => return Err(bad()),
        }
        run = i;
    }
    out.push_str(&raw[run..]);
    Ok(())
}

fn hex4(b: &[u8], at: usize) -> Option<u32> {
    let h = b.get(at..at + 4)?;
    h.iter().try_fold(0u32, |acc, &c| Some(acc * 16 + (c as char).to_digit(16)?))
}

/// Parse a JSON number token as `i64` (integers only).
pub fn parse_i64(num: &str) -> Result<i64> {
    num.parse().map_err(|_| Error::Type { expected: "integer" })
}

/// Parse a JSON number token as `u64` (non-negative integers only).
pub fn parse_u64(num: &str) -> Result<u64> {
    num.parse().map_err(|_| Error::Type { expected: "unsigned integer" })
}

/// Parse a JSON number token as `f64`.
pub fn parse_f64(num: &str) -> Result<f64> {
    num.parse().map_err(|_| Error::Type { expected: "number" })
}

/// A streaming JSON writer into a caller-owned `String` (reused across
/// requests). The caller is responsible for well-formed nesting; commas are
/// inserted automatically.
#[derive(Debug)]
pub struct Writer<'b> {
    out: &'b mut String,
    /// Per open container: has an element been written yet?
    need_comma: Vec<bool>,
    /// A key was just written; the next value follows its `:` directly.
    after_key: bool,
}

impl<'b> Writer<'b> {
    /// Write into `out` (appending).
    pub fn new(out: &'b mut String) -> Self {
        Self { out, need_comma: Vec::with_capacity(16), after_key: false }
    }

    fn sep(&mut self) {
        if std::mem::take(&mut self.after_key) {
            return;
        }
        if let Some(nc) = self.need_comma.last_mut() {
            if *nc {
                self.out.push(',');
            }
            *nc = true;
        }
    }

    /// `{`
    pub fn begin_object(&mut self) -> &mut Self {
        self.sep();
        self.out.push('{');
        self.need_comma.push(false);
        self
    }

    /// `}`
    pub fn end_object(&mut self) -> &mut Self {
        self.need_comma.pop();
        self.out.push('}');
        self
    }

    /// `[`
    pub fn begin_array(&mut self) -> &mut Self {
        self.sep();
        self.out.push('[');
        self.need_comma.push(false);
        self
    }

    /// `]`
    pub fn end_array(&mut self) -> &mut Self {
        self.need_comma.pop();
        self.out.push(']');
        self
    }

    /// An object key; the next call writes its value.
    pub fn key(&mut self, k: &str) -> &mut Self {
        self.sep();
        escape_into(k, self.out);
        self.out.push(':');
        self.after_key = true;
        self
    }

    /// A string value.
    pub fn string(&mut self, s: &str) -> &mut Self {
        self.sep();
        escape_into(s, self.out);
        self
    }

    /// An integer value.
    pub fn int(&mut self, v: i64) -> &mut Self {
        use std::fmt::Write as _;
        self.sep();
        let _ = write!(self.out, "{v}");
        self
    }

    /// An unsigned integer value.
    pub fn uint(&mut self, v: u64) -> &mut Self {
        use std::fmt::Write as _;
        self.sep();
        let _ = write!(self.out, "{v}");
        self
    }

    /// A float value (non-finite values are written as `null`).
    pub fn float(&mut self, v: f64) -> &mut Self {
        use std::fmt::Write as _;
        self.sep();
        if v.is_finite() {
            let _ = write!(self.out, "{v}");
        } else {
            self.out.push_str("null");
        }
        self
    }

    /// A boolean value.
    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.sep();
        self.out.push_str(if v { "true" } else { "false" });
        self
    }

    /// `null`.
    pub fn null(&mut self) -> &mut Self {
        self.sep();
        self.out.push_str("null");
        self
    }

    /// Pre-serialised JSON (e.g. a cached fragment), written verbatim.
    pub fn raw(&mut self, json: &str) -> &mut Self {
        self.sep();
        self.out.push_str(json);
        self
    }

    /// Pre-serialised object members (`"a":1,"b":[2]`, no braces) appended
    /// to the open object, verbatim.
    pub fn raw_members(&mut self, members: &str) -> &mut Self {
        let m = members.trim().trim_matches(',');
        if !m.is_empty() {
            self.sep();
            self.out.push_str(m);
        }
        self
    }
}

/// Append `s` as a quoted, escaped JSON string.
pub fn escape_into(s: &str, out: &mut String) {
    out.reserve(s.len() + 2);
    out.push('"');
    let b = s.as_bytes();
    let mut run = 0;
    for (i, &c) in b.iter().enumerate() {
        let esc: Option<&str> = match c {
            b'"' => Some("\\\""),
            b'\\' => Some("\\\\"),
            b'\n' => Some("\\n"),
            b'\r' => Some("\\r"),
            b'\t' => Some("\\t"),
            0..=0x1f => None,
            _ => continue,
        };
        out.push_str(&s[run..i]);
        if let Some(e) = esc {
            out.push_str(e);
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "\\u{c:04x}");
        }
        run = i + 1;
    }
    out.push_str(&s[run..]);
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events(src: &str) -> Result<Vec<String>> {
        let mut s = Scanner::new(src);
        let mut out = Vec::new();
        while let Some(e) = s.next_event()? {
            out.push(format!("{e:?}"));
        }
        Ok(out)
    }

    #[test]
    fn scans_nested_document() {
        let ev = events(r#" {"a": [1, -2.5e3, true, null], "b": {"c": "x"}} "#).unwrap();
        assert_eq!(ev.len(), 14);
        assert!(ev[0].starts_with("ObjectStart"));
        assert!(ev.last().unwrap().starts_with("ObjectEnd"));
    }

    #[test]
    fn strings_are_zero_copy_without_escapes() {
        let src = r#"{"k":"plain"}"#;
        let mut s = Scanner::new(src);
        s.expect().unwrap();
        let Event::Key(k) = s.expect().unwrap() else { panic!() };
        assert!(k.eq_plain("k"));
        let Event::Str(v) = s.expect().unwrap() else { panic!() };
        let plain = v.as_plain().unwrap();
        assert!(std::ptr::eq(plain.as_ptr(), src[6..].as_ptr()), "borrowed from input");
    }

    #[test]
    fn decodes_escapes_and_surrogate_pairs() {
        let mut buf = String::new();
        let mut s = Scanner::new(r#""a\"b\\né😀""#);
        let Event::Str(v) = s.expect().unwrap() else { panic!() };
        assert_eq!(v.decode(&mut buf).unwrap(), "a\"b\\n\u{e9}\u{1F600}");
    }

    #[test]
    fn rejects_malformed_input_with_offsets() {
        for bad in [
            "{",
            "[1,]",
            r#"{"a" 1}"#,
            r#"{"a":1,}"#,
            "01",
            "1.",
            "1e",
            "tru",
            r#""\x""#,
            r#""\ud800""#,
            "[1] 2",
            "\"a\nb\"",
            "{1:2}",
            "",
        ] {
            let r = events(bad).and_then(|_| {
                let mut out = String::new();
                // Force unescape of every string too.
                let mut s = Scanner::new(bad);
                while let Some(e) = s.next_event()? {
                    if let Event::Str(v) = e {
                        v.decode(&mut out)?;
                    }
                }
                Ok(())
            });
            assert!(r.is_err(), "accepted: {bad:?}");
        }
    }

    #[test]
    fn depth_is_capped() {
        let deep = "[".repeat(MAX_DEPTH + 1) + &"]".repeat(MAX_DEPTH + 1);
        assert!(events(&deep).is_err());
        let ok = "[".repeat(MAX_DEPTH) + &"]".repeat(MAX_DEPTH);
        assert!(events(&ok).is_ok());
    }

    #[test]
    fn skip_consumes_nested_value() {
        let mut s = Scanner::new(r#"{"skip":{"x":[1,{"y":2}]},"keep":7}"#);
        s.expect().unwrap();
        s.expect().unwrap(); // key skip
        let first = s.expect().unwrap();
        s.skip(first).unwrap();
        let Event::Key(k) = s.expect().unwrap() else { panic!() };
        assert!(k.eq_plain("keep"));
        assert_eq!(s.expect().unwrap(), Event::Num("7"));
    }

    #[test]
    fn writer_round_trips_through_scanner() {
        let mut out = String::new();
        Writer::new(&mut out)
            .begin_object()
            .key("s")
            .string("q\"uote\n\u{1}")
            .key("n")
            .int(-5)
            .key("a")
            .begin_array()
            .float(1.5)
            .bool(true)
            .null()
            .float(f64::NAN)
            .end_array()
            .key("o")
            .begin_object()
            .end_object()
            .end_object();
        assert_eq!(out, r#"{"s":"q\"uote\n\u0001","n":-5,"a":[1.5,true,null,null],"o":{}}"#);
        assert!(events(&out).is_ok());
    }

    #[test]
    fn from_bytes_rejects_invalid_utf8() {
        assert!(Scanner::from_bytes(b"\"\xff\"").is_err());
    }
}
