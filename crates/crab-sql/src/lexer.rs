// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Zero-copy GoogleSQL tokenizer.
//!
//! Tokens are `(kind, span)` pairs into the source; nothing is copied or
//! decoded here. Output goes into a caller-owned `Vec<Token>` that is reused
//! across statements. Reserved keywords are recognised with a compile-time
//! open-addressing hash table (one hash, usually one comparison); every other
//! word is an identifier, exactly as GoogleSQL treats non-reserved keywords.
//!
//! Lexical coverage: `--`, `#` and `/* */` comments; unquoted and backtick
//! identifiers; single-, double- and triple-quoted strings with raw (`r`) and
//! bytes (`b`) prefixes; integer, decimal, exponent and hex numbers; named
//! (`@x`), positional (`?`) and system (`@@x`) parameters; all operators.
//!
//! An unterminated string, quoted identifier or comment at the very end of the
//! input is reported as [`LexError::incomplete`] — a truncated *prefix* of
//! valid SQL, which the streaming validator must not reject.

use crate::Sp;

macro_rules! keywords {
    ($($v:ident = $s:literal),* $(,)?) => {
        /// A GoogleSQL reserved keyword.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        #[allow(missing_docs)]
        pub enum Kw { $($v),* }

        const KW_LIST: &[(&str, Kw)] = &[$(($s, Kw::$v)),*];

        impl Kw {
            /// Canonical (upper-case) spelling.
            #[must_use]
            pub fn as_str(self) -> &'static str {
                match self { $(Kw::$v => $s),* }
            }
        }
    };
}

keywords! {
    All = "ALL", And = "AND", Any = "ANY", Array = "ARRAY", As = "AS", Asc = "ASC",
    AssertRowsModified = "ASSERT_ROWS_MODIFIED", At = "AT", Between = "BETWEEN", By = "BY",
    Case = "CASE", Cast = "CAST", Collate = "COLLATE", Contains = "CONTAINS", Create = "CREATE",
    Cross = "CROSS", Cube = "CUBE", Current = "CURRENT", Default = "DEFAULT", Define = "DEFINE",
    Desc = "DESC", Distinct = "DISTINCT", Else = "ELSE", End = "END", Enum = "ENUM",
    Escape = "ESCAPE", Except = "EXCEPT", Exclude = "EXCLUDE", Exists = "EXISTS",
    Extract = "EXTRACT", False = "FALSE", Fetch = "FETCH", Following = "FOLLOWING", For = "FOR",
    From = "FROM", Full = "FULL", Group = "GROUP", Grouping = "GROUPING", Groups = "GROUPS",
    Hash = "HASH", Having = "HAVING", If = "IF", Ignore = "IGNORE", In = "IN", Inner = "INNER",
    Intersect = "INTERSECT", Interval = "INTERVAL", Into = "INTO", Is = "IS", Join = "JOIN",
    Lateral = "LATERAL", Left = "LEFT", Like = "LIKE", Limit = "LIMIT", Lookup = "LOOKUP",
    Merge = "MERGE", Natural = "NATURAL", New = "NEW", No = "NO", Not = "NOT", Null = "NULL",
    Nulls = "NULLS", Of = "OF", On = "ON", Or = "OR", Order = "ORDER", Outer = "OUTER",
    Over = "OVER", Partition = "PARTITION", Preceding = "PRECEDING", Proto = "PROTO",
    Qualify = "QUALIFY", Range = "RANGE", Recursive = "RECURSIVE", Respect = "RESPECT",
    Right = "RIGHT", Rollup = "ROLLUP", Rows = "ROWS", Select = "SELECT", Set = "SET",
    Some = "SOME", Struct = "STRUCT", Tablesample = "TABLESAMPLE", Then = "THEN", To = "TO",
    Treat = "TREAT", True = "TRUE", Unbounded = "UNBOUNDED", Union = "UNION", Unnest = "UNNEST",
    Using = "USING", When = "WHEN", Where = "WHERE", Window = "WINDOW", With = "WITH",
    Within = "WITHIN",
}

/// Longest reserved keyword, bytes.
const MAX_KW_LEN: usize = 20;
const TABLE_BITS: u32 = 9;
const TABLE_SIZE: usize = 1 << TABLE_BITS;

const fn kw_hash(bytes: &[u8]) -> usize {
    // FNV-1a over ASCII-upper-cased bytes.
    let mut h: u32 = 0x811C_9DC5;
    let mut i = 0;
    while i < bytes.len() {
        h ^= bytes[i].to_ascii_uppercase() as u32;
        h = h.wrapping_mul(0x0100_0193);
        i += 1;
    }
    (h >> (32 - TABLE_BITS)) as usize
}

/// Slot → keyword index + 1 (0 = empty); linear probing, built at compile time.
static KW_TABLE: [u8; TABLE_SIZE] = {
    let mut t = [0u8; TABLE_SIZE];
    let mut i = 0;
    while i < KW_LIST.len() {
        let mut slot = kw_hash(KW_LIST[i].0.as_bytes());
        while t[slot] != 0 {
            slot = (slot + 1) & (TABLE_SIZE - 1);
        }
        t[slot] = (i + 1) as u8;
        i += 1;
    }
    t
};

/// The reserved keyword spelled by `word` (case-insensitive), if any.
#[must_use]
pub fn keyword(word: &[u8]) -> Option<Kw> {
    if word.len() > MAX_KW_LEN || word.len() < 2 {
        return None;
    }
    let mut slot = kw_hash(word);
    loop {
        let e = KW_TABLE[slot];
        if e == 0 {
            return None;
        }
        let (s, kw) = KW_LIST[usize::from(e - 1)];
        if s.len() == word.len() && s.as_bytes().eq_ignore_ascii_case(word) {
            return Some(kw);
        }
        slot = (slot + 1) & (TABLE_SIZE - 1);
    }
}

/// Punctuation and operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum Punct {
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
    Semi,
    Plus,
    Minus,
    Star,
    Slash,
    Concat,
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Shl,
    Shr,
    Amp,
    Pipe,
    Caret,
    Tilde,
    Arrow,
    PipeGt,
}

impl Punct {
    /// Canonical spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LParen => "(",
            Self::RParen => ")",
            Self::LBracket => "[",
            Self::RBracket => "]",
            Self::Comma => ",",
            Self::Dot => ".",
            Self::Semi => ";",
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Star => "*",
            Self::Slash => "/",
            Self::Concat => "||",
            Self::Eq => "=",
            Self::Ne => "!=",
            Self::Lt => "<",
            Self::Gt => ">",
            Self::Le => "<=",
            Self::Ge => ">=",
            Self::Shl => "<<",
            Self::Shr => ">>",
            Self::Amp => "&",
            Self::Pipe => "|",
            Self::Caret => "^",
            Self::Tilde => "~",
            Self::Arrow => "=>",
            Self::PipeGt => "|>",
        }
    }
}

/// Token kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Tok {
    /// Unquoted identifier (includes non-reserved keywords).
    Ident,
    /// Backtick-quoted identifier; the span excludes the backticks.
    QuotedIdent,
    /// Reserved keyword.
    Kw(Kw),
    /// Integer literal (decimal or hex).
    Int,
    /// Decimal / exponent literal.
    Float,
    /// String literal; the span includes prefix and quotes.
    Str,
    /// Bytes literal (`b'...'`); the span includes prefix and quotes.
    Bytes,
    /// `@name`; the span excludes `@`.
    Param,
    /// `?`.
    Positional,
    /// `@@name`; the span excludes `@@`.
    SysVar,
    /// Operator or punctuation.
    P(Punct),
    /// End of input.
    Eof,
}

/// One token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token {
    /// Kind.
    pub kind: Tok,
    /// Source span.
    pub sp: Sp,
}

/// A lexical error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LexError {
    /// Where.
    pub at: u32,
    /// What.
    pub msg: &'static str,
    /// The input ended inside a token that could still be completed.
    pub incomplete: bool,
}

/// Tokenise `src` into `out` (cleared first). Always ends with [`Tok::Eof`]
/// on success.
#[allow(clippy::many_single_char_names, clippy::too_many_lines)] // a byte scanner reads best terse and whole
pub fn tokenize(src: &str, out: &mut Vec<Token>) -> Result<(), LexError> {
    out.clear();
    if src.len() > u32::MAX as usize - 1 {
        return Err(LexError { at: 0, msg: "input too large", incomplete: false });
    }
    let b = src.as_bytes();
    let n = b.len();
    let mut i = 0usize;
    let sp = |s: usize, e: usize| Sp { start: s as u32, end: e as u32 };
    let err = |at: usize, msg, incomplete| LexError { at: at as u32, msg, incomplete };
    while i < n {
        let c = b[i];
        // Whitespace.
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Comments.
        if c == b'#' || (c == b'-' && b.get(i + 1) == Some(&b'-')) {
            while i < n && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let start = i;
            i += 2;
            loop {
                if i + 1 >= n {
                    return Err(err(start, "unterminated comment", true));
                }
                if b[i] == b'*' && b[i + 1] == b'/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        let start = i;
        // String / bytes literals with optional r/b prefixes (any order, case-insensitive).
        let (mut is_raw, mut is_bytes, mut q) = (false, false, i);
        while q < n && q - i < 2 && matches!(b[q], b'r' | b'R' | b'b' | b'B') {
            if matches!(b[q], b'r' | b'R') {
                is_raw = true;
            } else {
                is_bytes = true;
            }
            q += 1;
        }
        // Letters are a prefix only when a quote follows them immediately.
        if q < n && (b[q] == b'\'' || b[q] == b'"') {
            let quote = b[q];
            let triple = b.get(q + 1) == Some(&quote) && b.get(q + 2) == Some(&quote);
            let mut j = if triple { q + 3 } else { q + 1 };
            loop {
                if j >= n {
                    return Err(err(start, "unterminated string", true));
                }
                let d = b[j];
                if d == b'\\' && !is_raw {
                    j += 2;
                    continue;
                }
                if d == b'\\' && is_raw {
                    // Raw strings still cannot end on an escaped quote.
                    j += if b.get(j + 1) == Some(&quote) { 2 } else { 1 };
                    continue;
                }
                if !triple && d == b'\n' {
                    return Err(err(start, "newline in string", false));
                }
                if d == quote
                    && (!triple || (b.get(j + 1) == Some(&quote) && b.get(j + 2) == Some(&quote)))
                {
                    j += if triple { 3 } else { 1 };
                    break;
                }
                j += 1;
            }
            i = j.min(n);
            out.push(Token {
                kind: if is_bytes { Tok::Bytes } else { Tok::Str },
                sp: sp(start, i),
            });
            continue;
        }
        // Identifiers / keywords.
        if c.is_ascii_alphabetic() || c == b'_' {
            while i < n && is_ident_byte(b[i]) {
                i += 1;
            }
            let kind = keyword(&b[start..i]).map_or(Tok::Ident, Tok::Kw);
            out.push(Token { kind, sp: sp(start, i) });
            continue;
        }
        // Non-ASCII letters start identifiers too (GoogleSQL allows Unicode in
        // quoted identifiers only, but be permissive and let the binder decide).
        if c >= 0x80 {
            let ch = src[i..].chars().next().unwrap_or('\u{FFFD}');
            if ch.is_alphabetic() {
                i += ch.len_utf8();
                while i < n {
                    let ch = src[i..].chars().next().unwrap_or(' ');
                    if ch.is_alphanumeric() || ch == '_' {
                        i += ch.len_utf8();
                    } else {
                        break;
                    }
                }
                out.push(Token { kind: Tok::Ident, sp: sp(start, i) });
                continue;
            }
            return Err(err(i, "unexpected character", false));
        }
        // Backtick identifiers.
        if c == b'`' {
            let mut j = i + 1;
            loop {
                if j >= n {
                    return Err(err(start, "unterminated quoted identifier", true));
                }
                match b[j] {
                    b'\\' => j += 2,
                    b'`' => break,
                    b'\n' => return Err(err(start, "newline in quoted identifier", false)),
                    _ => j += 1,
                }
            }
            if j == i + 1 {
                return Err(err(start, "empty quoted identifier", false));
            }
            out.push(Token { kind: Tok::QuotedIdent, sp: sp(i + 1, j) });
            i = j + 1;
            continue;
        }
        // Numbers (a leading '.' followed by a digit is a decimal).
        // After a name or closing bracket, `.1` is member access, not a decimal.
        let after_operand = out.last().is_some_and(|t: &Token| {
            matches!(
                t.kind,
                Tok::Ident | Tok::QuotedIdent | Tok::P(Punct::RParen | Punct::RBracket)
            ) && t.sp.end as usize == i
        });
        if c.is_ascii_digit()
            || (c == b'.' && !after_operand && b.get(i + 1).is_some_and(u8::is_ascii_digit))
        {
            if c == b'0' && matches!(b.get(i + 1), Some(b'x' | b'X')) {
                i += 2;
                let h = i;
                while i < n && b[i].is_ascii_hexdigit() {
                    i += 1;
                }
                if i == h {
                    return Err(err(start, "hex literal needs digits", i >= n));
                }
                out.push(Token { kind: Tok::Int, sp: sp(start, i) });
                continue;
            }
            let mut float = false;
            while i < n && b[i].is_ascii_digit() {
                i += 1;
            }
            // `1.` is a decimal unless an identifier follows the dot (`1.x`).
            if i < n
                && b[i] == b'.'
                && !src[i + 1..].chars().next().is_some_and(|ch| ch.is_alphabetic() || ch == '_')
            {
                float = true;
                i += 1;
                while i < n && b[i].is_ascii_digit() {
                    i += 1;
                }
            }
            if i < n && matches!(b[i], b'e' | b'E') {
                let mut j = i + 1;
                if j < n && matches!(b[j], b'+' | b'-') {
                    j += 1;
                }
                if j < n && b[j].is_ascii_digit() {
                    float = true;
                    i = j;
                    while i < n && b[i].is_ascii_digit() {
                        i += 1;
                    }
                } else if j >= n {
                    return Err(err(start, "exponent needs digits", true));
                }
            }
            if i < n && is_ident_byte(b[i]) {
                return Err(err(i, "malformed number", false));
            }
            out.push(Token { kind: if float { Tok::Float } else { Tok::Int }, sp: sp(start, i) });
            continue;
        }
        // Parameters.
        if c == b'@' {
            let sys = b.get(i + 1) == Some(&b'@');
            let s = if sys { i + 2 } else { i + 1 };
            let mut j = s;
            while j < n && is_ident_byte(b[j]) {
                j += 1;
            }
            if j == s {
                return Err(err(start, "parameter needs a name", j >= n));
            }
            out.push(Token { kind: if sys { Tok::SysVar } else { Tok::Param }, sp: sp(s, j) });
            i = j;
            continue;
        }
        if c == b'?' {
            out.push(Token { kind: Tok::Positional, sp: sp(i, i + 1) });
            i += 1;
            continue;
        }
        // Operators.
        let two = b.get(i + 1).copied();
        let (p, len) = match (c, two) {
            (b'|', Some(b'|')) => (Punct::Concat, 2),
            (b'|', Some(b'>')) => (Punct::PipeGt, 2),
            (b'!', Some(b'=')) | (b'<', Some(b'>')) => (Punct::Ne, 2),
            (b'<', Some(b'=')) => (Punct::Le, 2),
            (b'>', Some(b'=')) => (Punct::Ge, 2),
            (b'<', Some(b'<')) => (Punct::Shl, 2),
            (b'>', Some(b'>')) => (Punct::Shr, 2),
            (b'=', Some(b'>')) => (Punct::Arrow, 2),
            (b'(', _) => (Punct::LParen, 1),
            (b')', _) => (Punct::RParen, 1),
            (b'[', _) => (Punct::LBracket, 1),
            (b']', _) => (Punct::RBracket, 1),
            (b',', _) => (Punct::Comma, 1),
            (b'.', _) => (Punct::Dot, 1),
            (b';', _) => (Punct::Semi, 1),
            (b'+', _) => (Punct::Plus, 1),
            (b'-', _) => (Punct::Minus, 1),
            (b'*', _) => (Punct::Star, 1),
            (b'/', _) => (Punct::Slash, 1),
            (b'=', _) => (Punct::Eq, 1),
            (b'<', _) => (Punct::Lt, 1),
            (b'>', _) => (Punct::Gt, 1),
            (b'&', _) => (Punct::Amp, 1),
            (b'|', _) => (Punct::Pipe, 1),
            (b'^', _) => (Punct::Caret, 1),
            (b'~', _) => (Punct::Tilde, 1),
            (b'!', None) => return Err(err(i, "incomplete operator", true)),
            _ => return Err(err(i, "unexpected character", false)),
        };
        out.push(Token { kind: Tok::P(p), sp: sp(i, i + len) });
        i += len;
    }
    out.push(Token { kind: Tok::Eof, sp: sp(n, n) });
    Ok(())
}

/// The value of a string literal token's source text (`'a\'b'`, `r"x"`,
/// triple-quoted): quotes removed, escapes decoded (not for raw strings).
/// `None` for bytes literals and malformed text.
#[must_use]
pub fn string_value(raw: &str) -> Option<std::borrow::Cow<'_, str>> {
    let mut s = raw;
    let mut is_raw = false;
    while let Some(c) = s.chars().next() {
        match c {
            'r' | 'R' => {
                is_raw = true;
                s = &s[1..];
            }
            'b' | 'B' => return None,
            _ => break,
        }
    }
    let q = s.chars().next().filter(|c| *c == '\'' || *c == '"')?;
    let triple: String = std::iter::repeat_n(q, 3).collect();
    let body = if s.len() >= 6 && s.starts_with(&triple) && s.ends_with(&triple) {
        &s[3..s.len() - 3]
    } else if s.len() >= 2 && s.ends_with(q) {
        &s[1..s.len() - 1]
    } else {
        return None;
    };
    if is_raw || !body.contains('\\') {
        return Some(std::borrow::Cow::Borrowed(body));
    }
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next()? {
            'n' => out.push('\n'),
            't' => out.push('\t'),
            'r' => out.push('\r'),
            '0' => out.push('\0'),
            other => out.push(other),
        }
    }
    Some(std::borrow::Cow::Owned(out))
}

#[inline]
fn is_ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(src: &str) -> Vec<Tok> {
        let mut v = Vec::new();
        tokenize(src, &mut v).unwrap();
        v.into_iter().map(|t| t.kind).collect()
    }

    fn texts(src: &str) -> Vec<&str> {
        let mut v = Vec::new();
        tokenize(src, &mut v).unwrap();
        v.iter().map(|t| &src[t.sp.start as usize..t.sp.end as usize]).collect()
    }

    #[test]
    fn every_keyword_is_found_case_insensitively() {
        for (s, kw) in KW_LIST {
            assert_eq!(keyword(s.as_bytes()), Some(*kw), "{s}");
            assert_eq!(keyword(s.to_ascii_lowercase().as_bytes()), Some(*kw));
        }
        for w in ["orders", "OFFSET", "DATE", "SAFE_CAST", "x", "selects", "SELEC"] {
            assert_eq!(keyword(w.as_bytes()), None, "{w} is not reserved");
        }
    }

    #[test]
    fn identifiers_keywords_and_quoted_paths() {
        assert_eq!(
            kinds("SELECT a FROM `p.d.t`"),
            vec![Tok::Kw(Kw::Select), Tok::Ident, Tok::Kw(Kw::From), Tok::QuotedIdent, Tok::Eof]
        );
        assert_eq!(texts("`p.d.t`"), vec!["p.d.t", ""]);
    }

    #[test]
    fn literals() {
        let src = r#"1 0x1F 1.5 .5 2e10 1.5E-3 'a''b' "x\"y" '''tri'ple''' r'\d' b'\x00' RB"z""#;
        let k = kinds(src);
        assert_eq!(&k[..6], &[Tok::Int, Tok::Int, Tok::Float, Tok::Float, Tok::Float, Tok::Float]);
        assert_eq!(k[6], Tok::Str);
        assert_eq!(k[7], Tok::Str);
        assert_eq!(k[8], Tok::Str, "adjacent quotes lex as two strings");
        assert_eq!(&k[9..], &[Tok::Str, Tok::Str, Tok::Bytes, Tok::Bytes, Tok::Eof]);
    }

    #[test]
    fn comments_and_operators() {
        let src = "a--c\n# h\n/* b */ || != <> <= >= << >> => |> @p @@sys ?";
        assert_eq!(
            texts(src),
            vec!["a", "||", "!=", "<>", "<=", ">=", "<<", ">>", "=>", "|>", "p", "sys", "?", ""]
        );
    }

    #[test]
    fn member_access_after_number_is_not_a_float() {
        assert_eq!(kinds("t.1"), vec![Tok::Ident, Tok::P(Punct::Dot), Tok::Int, Tok::Eof]);
        assert_eq!(
            kinds("x[OFFSET(1)].f"),
            vec![
                Tok::Ident,
                Tok::P(Punct::LBracket),
                Tok::Ident,
                Tok::P(Punct::LParen),
                Tok::Int,
                Tok::P(Punct::RParen),
                Tok::P(Punct::RBracket),
                Tok::P(Punct::Dot),
                Tok::Ident,
                Tok::Eof
            ]
        );
    }

    #[test]
    fn truncated_input_is_incomplete_not_invalid() {
        for prefix in
            ["SELECT 'abc", "SELECT `p.d", "SELECT /* c", "SELECT 1e", "SELECT @", "SELECT '''x''"]
        {
            let e = tokenize(prefix, &mut Vec::new()).unwrap_err();
            assert!(e.incomplete, "{prefix:?}: {e:?}");
        }
        for bad in ["SELECT 'a\nb'", "SELECT 1abc", "SELECT $", "SELECT ``"] {
            let e = tokenize(bad, &mut Vec::new()).unwrap_err();
            assert!(!e.incomplete, "{bad:?}");
        }
    }

    #[test]
    fn string_values_decode() {
        assert_eq!(string_value("'abc'").as_deref(), Some("abc"));
        assert_eq!(string_value(r#""a\"b""#).as_deref(), Some("a\"b"));
        assert_eq!(string_value(r"'it\'s'").as_deref(), Some("it's"));
        assert_eq!(string_value(r"r'\d+'").as_deref(), Some(r"\d+"));
        assert_eq!(string_value("'''x'y'''").as_deref(), Some("x'y"));
        assert_eq!(string_value("b'x'"), None);
        assert_eq!(string_value("'unterminated"), None);
    }

    #[test]
    fn buffer_is_reused() {
        let mut v = Vec::new();
        tokenize("SELECT a, b, c FROM t", &mut v).unwrap();
        let cap = v.capacity();
        tokenize("SELECT 1", &mut v).unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(v.capacity(), cap);
    }
}
