// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # crab-values
//!
//! Where do the values a question mentions live?
//!
//! A [`ValueIndex`] is built from the catalog's column profiles (the value
//! lists the warehouse computed) at engine start. It answers:
//!
//! - [`ValueIndex::ground`]: for a question, which phrases name stored values
//!   and in which columns — "Brazil" → `country = 'Brasil'`, "women" →
//!   `gender = 'F'` when a column holds exactly that — by normalised exact
//!   match (case, accents and punctuation folded) over all 1–4-word phrases,
//!   then trigram-candidate + edit-distance fuzzy match for the phrases that
//!   look like names;
//! - [`ValueIndex::check`]: is a SQL string literal a value the column can
//!   hold? For columns whose complete value set is known, a literal outside
//!   it (`'complete'` vs `'Complete'`) cannot match anything — the closest
//!   real value is returned for repair.
//!
//! Everything is sorted arrays and a trigram map built once; a question is
//! grounded in microseconds.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::module_name_repetitions
)]

use crab_catalog::{Catalog, ColId};
use std::collections::HashMap;

/// Normalise for matching: lowercase, common accents folded, apostrophes
/// dropped, runs of other non-alphanumerics collapsed to one space,
/// trimmed. Replaces the contents of `out`.
pub fn normalize_into(s: &str, out: &mut String) {
    out.clear();
    let mut space = false;
    for c in s.chars() {
        let c = fold(c);
        // Apostrophes join: "Levi's" and "Levis" are the same word.
        if matches!(c, '\'' | '\u{2019}' | '`') {
            continue;
        }
        if c.is_alphanumeric() {
            if space && !out.is_empty() {
                out.push(' ');
            }
            space = false;
            out.extend(c.to_lowercase());
        } else {
            space = true;
        }
    }
}

/// Normalise into a new string.
#[must_use]
pub fn normalize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    normalize_into(s, &mut out);
    out
}

/// Fold common Latin accents to their base letter.
fn fold(c: char) -> char {
    match c {
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'À' | 'Á' | 'Â' | 'Ã' | 'Ä' | 'Å' => 'a',
        'ç' | 'Ç' => 'c',
        'è' | 'é' | 'ê' | 'ë' | 'È' | 'É' | 'Ê' | 'Ë' => 'e',
        'ì' | 'í' | 'î' | 'ï' | 'Ì' | 'Í' | 'Î' | 'Ï' => 'i',
        'ñ' | 'Ñ' => 'n',
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'Ò' | 'Ó' | 'Ô' | 'Õ' | 'Ö' | 'Ø' => 'o',
        'ù' | 'ú' | 'û' | 'ü' | 'Ù' | 'Ú' | 'Û' | 'Ü' => 'u',
        'ý' | 'ÿ' | 'Ý' => 'y',
        other => other,
    }
}

/// Edit distance (Levenshtein) with one reusable row; ASCII strings (the
/// common case) are compared bytewise, others by char.
fn edit_with(a: &str, b: &str, row: &mut Vec<usize>) -> usize {
    fn run<T: PartialEq + Copy>(
        a: impl Iterator<Item = T>,
        b: &[T],
        row: &mut Vec<usize>,
    ) -> usize {
        row.clear();
        row.extend(0..=b.len());
        for (i, ca) in a.enumerate() {
            let mut diag = row[0];
            row[0] = i + 1;
            for (j, &cb) in b.iter().enumerate() {
                let up = row[j + 1];
                row[j + 1] = (up + 1).min(row[j] + 1).min(diag + usize::from(ca != cb));
                diag = up;
            }
        }
        row[b.len()]
    }
    if a.is_ascii() && b.is_ascii() {
        run(a.bytes(), b.as_bytes(), row)
    } else {
        let bc: Vec<char> = b.chars().collect();
        run(a.chars(), &bc, row)
    }
}

/// Similarity in 0..=1 from edit distance.
fn similarity_with(a: &str, b: &str, row: &mut Vec<usize>) -> f32 {
    let n = a.chars().count().max(b.chars().count());
    if n == 0 {
        return 1.0;
    }
    1.0 - edit_with(a, b, row) as f32 / n as f32
}

/// Similarity in 0..=1 from edit distance.
fn similarity(a: &str, b: &str) -> f32 {
    similarity_with(a, b, &mut Vec::new())
}

fn trigrams(norm: &str, mut f: impl FnMut(u32)) {
    let padded: Vec<char> =
        std::iter::once(' ').chain(norm.chars()).chain(std::iter::once(' ')).collect();
    for w in padded.windows(3) {
        let mut h: u32 = 0x811C_9DC5;
        for c in w {
            h ^= *c as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
        f(h);
    }
}

/// One indexed value.
#[derive(Clone, Debug)]
struct Entry {
    norm: String,
    value: String,
    col: ColId,
    count: u64,
}

/// How a phrase matched a value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MatchKind {
    /// Equal after normalisation.
    Exact,
    /// Similar (score in 0..=1).
    Fuzzy(f32),
}

/// A grounded value.
#[derive(Clone, Debug, PartialEq)]
pub struct Grounding {
    /// The phrase as written in the question.
    pub phrase: String,
    /// The column holding the value.
    pub col: ColId,
    /// The stored value, exactly.
    pub value: String,
    /// How it matched.
    pub kind: MatchKind,
    /// How often the value occurs (profile count).
    pub count: u64,
}

/// Verdict on a SQL string literal compared with a column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiteralCheck {
    /// The column's value set is not fully known: no verdict.
    Unknown,
    /// The value occurs.
    Present,
    /// The value cannot occur; the closest real value, if any.
    Absent {
        /// Suggested replacement.
        closest: Option<String>,
    },
}

/// The value index. See the crate docs.
#[derive(Debug, Default)]
pub struct ValueIndex {
    /// Entries sorted by (normalised value, column).
    entries: Vec<Entry>,
    /// Distinct words of all normalised values, sorted.
    vocab: Vec<String>,
    /// Trigram → vocabulary word ids (for spelling correction).
    grams: HashMap<u32, Vec<u32>>,
    /// Columns whose complete value set is indexed.
    complete: HashMap<ColId, (u32, u32)>,
    /// Per-column entry ids (for complete sets): range into `by_col`.
    by_col: Vec<u32>,
}

/// One question word.
#[derive(Debug)]
struct Word {
    /// Byte span in the question.
    start: usize,
    end: usize,
    /// Normalised form.
    norm: String,
    /// Closest vocabulary word and its similarity, when misspelt.
    fix: Option<(String, f32)>,
}

/// Shortest word spelling correction applies to, characters.
const MIN_FUZZY_WORD: usize = 4;
/// Least similarity for a word correction.
const MIN_WORD_SIM: f32 = 0.75;

/// `word` is `base` plus a plural suffix.
fn is_plural_of(word: &str, base: &str) -> bool {
    word.strip_prefix(base).is_some_and(|rest| rest == "s" || rest == "es")
}

/// Stopwords never grounded on their own.
const STOP: &[&str] = &[
    "a", "an", "the", "of", "in", "on", "at", "to", "for", "by", "per", "with", "and", "or", "not",
    "is", "are", "was", "were", "be", "been", "do", "does", "did", "how", "many", "much", "what",
    "which", "who", "whom", "whose", "when", "where", "why", "there", "have", "has", "had", "that",
    "this", "these", "those", "it", "its", "their", "from", "as", "than", "then", "each", "every",
    "all", "any", "some", "total", "number", "count", "average", "avg", "sum", "most", "least",
    "top", "bottom", "highest", "lowest", "more", "less", "over", "under", "between", "during",
    "since", "until", "year", "month", "day", "week", "quarter", "last", "first", "show", "list",
    "give", "find", "get", "me", "us", "we", "i", "you", "they", "them", "my", "our", "your", "no",
    "yes", "can", "could", "would", "should", "will", "into", "out", "up", "down", "about", "only",
    "also", "other", "same", "such", "so", "if", "but",
];

/// A question word that never names data on its own (normalised form).
#[must_use]
pub fn is_stop(w: &str) -> bool {
    STOP.contains(&w)
}

impl ValueIndex {
    /// Build from every column profile in `cat`.
    #[must_use]
    pub fn build(cat: &Catalog) -> Self {
        let mut entries: Vec<Entry> = Vec::new();
        let mut complete_cols: Vec<ColId> = Vec::new();
        let mut buf = String::new();
        for t in cat.tables() {
            for c in cat.columns(t.id) {
                let Some(p) = cat.profile(c.id) else { continue };
                let mut any = false;
                for (v, n) in p.values() {
                    normalize_into(v, &mut buf);
                    if buf.is_empty() || buf.len() > 200 {
                        continue;
                    }
                    entries.push(Entry {
                        norm: buf.clone(),
                        value: v.to_owned(),
                        col: c.id,
                        count: n,
                    });
                    any = true;
                }
                if p.values_complete && any {
                    complete_cols.push(c.id);
                }
            }
        }
        entries.sort_by(|a, b| {
            a.norm.cmp(&b.norm).then(a.col.cmp(&b.col)).then(b.count.cmp(&a.count))
        });
        // Word vocabulary: question words are corrected against it, then
        // phrases are looked up exactly. Far smaller than the value set, so
        // its trigram postings stay short.
        let mut vocab: Vec<String> = Vec::new();
        {
            let mut words: Vec<&str> = entries
                .iter()
                .flat_map(|e| e.norm.split(' '))
                .filter(|w| w.chars().count() >= MIN_FUZZY_WORD)
                .collect();
            words.sort_unstable();
            words.dedup();
            vocab.extend(words.into_iter().map(str::to_owned));
        }
        let mut grams: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut seen: Vec<u32> = Vec::new();
        for (i, w) in vocab.iter().enumerate() {
            seen.clear();
            trigrams(w, |g| {
                if !seen.contains(&g) {
                    seen.push(g);
                    grams.entry(g).or_default().push(i as u32);
                }
            });
        }
        // Complete per-column sets, entry ids grouped by column.
        let mut by_col_pairs: Vec<(ColId, u32)> = entries
            .iter()
            .enumerate()
            .filter(|(_, e)| complete_cols.contains(&e.col))
            .map(|(i, e)| (e.col, i as u32))
            .collect();
        by_col_pairs.sort_unstable();
        let mut complete = HashMap::new();
        let mut by_col = Vec::with_capacity(by_col_pairs.len());
        let mut i = 0;
        while i < by_col_pairs.len() {
            let col = by_col_pairs[i].0;
            let start = by_col.len() as u32;
            while i < by_col_pairs.len() && by_col_pairs[i].0 == col {
                by_col.push(by_col_pairs[i].1);
                i += 1;
            }
            complete.insert(col, (start, by_col.len() as u32));
        }
        Self { entries, vocab, grams, complete, by_col }
    }

    /// Number of indexed values.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when nothing is indexed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Entries whose normalised value equals `norm`.
    fn exact_range(&self, norm: &str) -> std::ops::Range<usize> {
        let start = self.entries.partition_point(|e| e.norm.as_str() < norm);
        let end = start + self.entries[start..].partition_point(|e| e.norm == norm);
        start..end
    }

    /// The closest vocabulary word to `word` (not itself in the
    /// vocabulary), with its similarity, if at least `min_sim`.
    fn correct(
        &self,
        word: &str,
        min_sim: f32,
        counts: &mut HashMap<u32, u16>,
    ) -> Option<(&str, f32)> {
        counts.clear();
        let mut n_grams = 0u16;
        trigrams(word, |g| {
            n_grams += 1;
            if let Some(ids) = self.grams.get(&g) {
                for &id in ids {
                    *counts.entry(id).or_insert(0) += 1;
                }
            }
        });
        let need = (n_grams / 3).max(1);
        let len = word.chars().count();
        let mut best: Option<(&str, f32)> = None;
        for (&id, &n) in counts.iter() {
            if n < need {
                continue;
            }
            let cand = self.vocab[id as usize].as_str();
            // Lengths too far apart cannot reach `min_sim`.
            let cl = cand.chars().count();
            if (cl.abs_diff(len) as f32) > (1.0 - min_sim) * cl.max(len) as f32 {
                continue;
            }
            let sim = similarity(word, cand);
            if sim >= min_sim
                && best.is_none_or(|(b, bs)| sim.total_cmp(&bs).then(b.cmp(cand)).is_gt())
            {
                best = Some((cand, sim));
            }
        }
        best
    }

    fn has_word(&self, w: &str) -> bool {
        self.vocab.binary_search_by(|v| v.as_str().cmp(w)).is_ok()
    }

    /// The words of a question, normalised, each with its spelling
    /// correction against the value vocabulary (if misspelt).
    fn words(&self, question: &str) -> Vec<Word> {
        let mut counts: HashMap<u32, u16> = HashMap::new();
        let mut words = Vec::new();
        let mut push = |start: usize, end: usize| {
            let norm = normalize(&question[start..end]);
            if norm.is_empty() {
                return;
            }
            // Only single normalised words are corrected; plural forms of a
            // known word are not misspellings.
            let fix = if !norm.contains(' ')
                && norm.chars().count() >= MIN_FUZZY_WORD
                && !is_stop(&norm)
                && !self.has_word(&norm)
            {
                self.correct(&norm, MIN_WORD_SIM, &mut counts)
                    .filter(|(c, _)| !is_plural_of(&norm, c))
                    .map(|(c, sim)| (c.to_owned(), sim))
            } else {
                None
            };
            words.push(Word { start, end, norm, fix });
        };
        let mut start = None;
        for (i, c) in question.char_indices() {
            let word_char = c.is_alphanumeric() || (c == '\'' || c == '-') && start.is_some();
            match (word_char, start) {
                (true, None) => start = Some(i),
                (false, Some(s)) => {
                    push(s, i);
                    start = None;
                }
                _ => {}
            }
        }
        if let Some(s) = start {
            push(s, question.len());
        }
        words
    }

    /// Ground a question: the stored values its phrases name, best first,
    /// at most `limit`.
    ///
    /// Every 1–4-word phrase (and every quoted string) is looked up exactly
    /// after normalisation. A phrase with no exact match is retried with its
    /// misspelt words replaced by their closest vocabulary words ("Brazil"
    /// → "brasil"): spelling is corrected per word against the (small)
    /// vocabulary of value words, never by scanning values.
    #[must_use]
    pub fn ground(&self, question: &str, limit: usize) -> Vec<Grounding> {
        if self.entries.is_empty() {
            return Vec::new();
        }
        let words = self.words(question);
        let mut out: Vec<Grounding> = Vec::new();
        let mut covered: Vec<(usize, usize)> = Vec::new(); // word index ranges already grounded
        let add =
            |out: &mut Vec<Grounding>, phrase: &str, norm: &str, fuzzy: Option<f32>| -> bool {
                let r = self.exact_range(norm);
                for e in &self.entries[r.clone()] {
                    if !out.iter().any(|g| g.col == e.col && g.value == e.value) {
                        let kind = fuzzy.map_or(MatchKind::Exact, MatchKind::Fuzzy);
                        out.push(Grounding {
                            phrase: phrase.to_owned(),
                            col: e.col,
                            value: e.value.clone(),
                            kind,
                            count: e.count,
                        });
                    }
                }
                !r.is_empty()
            };
        let mut buf = String::new();
        for q in quoted(question) {
            normalize_into(q, &mut buf);
            add(&mut out, q, &buf, None);
        }
        let mut fixed = String::new();
        for n in (1..=4usize).rev() {
            for i in 0..words.len().saturating_sub(n - 1) {
                let w = &words[i..i + n];
                if covered.iter().any(|&(a, b)| a <= i && i + n <= b) {
                    continue; // inside a longer grounded phrase
                }
                if is_stop(&w[0].norm)
                    || is_stop(&w[n - 1].norm)
                    || w.iter().all(|x| x.norm.chars().all(|c| c.is_ascii_digit()))
                {
                    continue;
                }
                let phrase = &question[w[0].start..w[n - 1].end];
                buf.clear();
                for (k, x) in w.iter().enumerate() {
                    if k > 0 {
                        buf.push(' ');
                    }
                    buf.push_str(&x.norm);
                }
                if add(&mut out, phrase, &buf, None) {
                    covered.push((i, i + n));
                    continue;
                }
                if w.iter().any(|x| x.fix.is_some()) {
                    fixed.clear();
                    for (k, x) in w.iter().enumerate() {
                        if k > 0 {
                            fixed.push(' ');
                        }
                        fixed.push_str(x.fix.as_ref().map_or(x.norm.as_str(), |(c, _)| c.as_str()));
                    }
                    let sim = similarity(&buf, &fixed);
                    if add(&mut out, phrase, &fixed, Some(sim)) {
                        covered.push((i, i + n));
                    }
                }
            }
        }
        // Exact before fuzzy, longer phrases first, then frequency.
        out.sort_by(|a, b| {
            let rank = |k: &MatchKind| match k {
                MatchKind::Exact => 2.0,
                MatchKind::Fuzzy(s) => *s,
            };
            rank(&b.kind)
                .total_cmp(&rank(&a.kind))
                .then(b.phrase.len().cmp(&a.phrase.len()))
                .then(b.count.cmp(&a.count))
        });
        out.truncate(limit);
        out
    }

    /// Can `literal` occur in `col`?
    #[must_use]
    pub fn check(&self, col: ColId, literal: &str) -> LiteralCheck {
        let Some(&(s, e)) = self.complete.get(&col) else { return LiteralCheck::Unknown };
        let ids = &self.by_col[s as usize..e as usize];
        if ids.iter().any(|&i| self.entries[i as usize].value == literal) {
            return LiteralCheck::Present;
        }
        let norm = normalize(literal);
        let len = norm.chars().count();
        let mut row = Vec::with_capacity(len + 1);
        let mut best: Option<(f32, &Entry)> = None;
        for &i in ids {
            let en = &self.entries[i as usize];
            let sim = if en.norm == norm {
                2.0
            } else {
                // Lengths too far apart cannot reach the 0.5 floor.
                let el = en.norm.chars().count();
                if el.abs_diff(len) * 2 > el.max(len) {
                    continue;
                }
                similarity_with(&norm, &en.norm, &mut row)
            };
            if sim >= 0.5
                && best
                    .is_none_or(|(bs, b)| sim.total_cmp(&bs).then(en.count.cmp(&b.count)).is_gt())
            {
                best = Some((sim, en));
            }
        }
        let closest = best.map(|(_, en)| en.value.clone());
        LiteralCheck::Absent { closest }
    }
}

/// Quoted substrings of a question ('...' or "...").
fn quoted(q: &str) -> Vec<&str> {
    let mut out = Vec::new();
    for quote in ['\'', '"'] {
        let mut rest = q;
        while let Some(a) = rest.find(quote) {
            let after = &rest[a + 1..];
            let Some(b) = after.find(quote) else { break };
            let inner = &after[..b];
            if !inner.trim().is_empty() && inner.len() <= 200 {
                out.push(inner);
            }
            rest = &after[b + 1..];
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crab_catalog::{CatalogBuilder, ColumnSpec, ProfileSpec, TableKind, TableSpec};

    fn col(p: &str) -> ColumnSpec {
        ColumnSpec {
            path: p.into(),
            data_type: "STRING".into(),
            ordinal: 1,
            nullable: true,
            partitioning: false,
            hidden: false,
            clustering: None,
            description: None,
        }
    }

    fn cat() -> Catalog {
        let mut b = CatalogBuilder::new("p");
        b.add_table(TableSpec {
            dataset: "d".into(),
            name: "users".into(),
            kind: TableKind::Table,
            description: None,
            row_count: 0,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns: vec![col("country"), col("gender"), col("traffic_source"), col("city")],
        });
        let prof = |vals: &[(&str, u64)], complete: bool| ProfileSpec {
            rows: 100,
            nulls: 0,
            distinct: vals.len() as u64,
            min: None,
            max: None,
            values: vals.iter().map(|(v, n)| ((*v).to_owned(), *n)).collect(),
            values_complete: complete,
            sampled: false,
        };
        b.set_profile(
            "d",
            "users",
            "country",
            prof(
                &[
                    ("United States", 40),
                    ("Brasil", 15),
                    ("United Kingdom", 10),
                    ("China", 30),
                    ("España", 5),
                ],
                true,
            ),
        );
        b.set_profile("d", "users", "gender", prof(&[("M", 50), ("F", 50)], true));
        b.set_profile(
            "d",
            "users",
            "traffic_source",
            prof(&[("Search", 70), ("Facebook", 10), ("Email", 20)], true),
        );
        b.set_profile(
            "d",
            "users",
            "city",
            prof(&[("New York", 5), ("São Paulo", 3), ("Chicago", 2)], false),
        );
        Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
    }

    fn col_of(c: &Catalog, name: &str) -> ColId {
        c.find_column(c.find_table("d", "users").unwrap(), name).unwrap()
    }

    #[test]
    fn normalisation_folds_case_accents_and_punctuation() {
        assert_eq!(normalize("  São-Paulo!! "), "sao paulo");
        assert_eq!(normalize("ESPAÑA"), "espana");
        assert_eq!(normalize("U.K."), "u k");
        assert_eq!(normalize("Levi's"), normalize("Levis"));
    }

    #[test]
    fn grounds_exact_multiword_and_fuzzy_values() {
        let c = cat();
        let idx = ValueIndex::build(&c);
        let g = idx.ground(
            "How many users from Brazil came through facebook in New York or sao paulo?",
            10,
        );
        let find = |v: &str| {
            g.iter().find(|x| x.value == v).unwrap_or_else(|| panic!("{v} not grounded in {g:#?}"))
        };
        assert_eq!(find("Facebook").kind, MatchKind::Exact);
        assert_eq!(find("Facebook").col, col_of(&c, "traffic_source"));
        assert_eq!(find("New York").kind, MatchKind::Exact, "multi-word phrase");
        assert_eq!(find("São Paulo").kind, MatchKind::Exact, "accents folded");
        assert!(matches!(find("Brasil").kind, MatchKind::Fuzzy(s) if s > 0.8), "Brazil ≈ Brasil");
        assert!(g.iter().all(|x| x.value != "United States"), "no spurious matches: {g:#?}");
    }

    #[test]
    fn corrects_words_inside_multiword_values_but_not_plurals() {
        let c = cat();
        let idx = ValueIndex::build(&c);
        let g = idx.ground("orders shipped to Sao Paolo", 10);
        assert_eq!(g.len(), 1, "{g:#?}");
        assert_eq!(g[0].value, "São Paulo");
        assert!(matches!(g[0].kind, MatchKind::Fuzzy(s) if s > 0.85));
        assert_eq!(g[0].phrase, "Sao Paolo");
        assert!(idx.ground("list the chinas", 10).is_empty(), "a plural is not a misspelling");
    }

    #[test]
    fn quoted_strings_and_stopwords() {
        let c = cat();
        let idx = ValueIndex::build(&c);
        let g = idx.ground("Count users whose country is 'United Kingdom'", 10);
        assert_eq!(g[0].value, "United Kingdom");
        assert!(idx.ground("how many are there in total?", 10).is_empty());
    }

    #[test]
    fn literal_checks_against_complete_sets() {
        let c = cat();
        let idx = ValueIndex::build(&c);
        let country = col_of(&c, "country");
        assert_eq!(idx.check(country, "Brasil"), LiteralCheck::Present);
        assert_eq!(
            idx.check(country, "brasil"),
            LiteralCheck::Absent { closest: Some("Brasil".into()) }
        );
        assert_eq!(
            idx.check(country, "Brazil"),
            LiteralCheck::Absent { closest: Some("Brasil".into()) }
        );
        assert_eq!(idx.check(country, "Atlantis"), LiteralCheck::Absent { closest: None });
        assert_eq!(
            idx.check(col_of(&c, "gender"), "female"),
            LiteralCheck::Absent { closest: None }
        );
        assert_eq!(
            idx.check(col_of(&c, "city"), "Boston"),
            LiteralCheck::Unknown,
            "incomplete sets give no verdict"
        );
    }
}
