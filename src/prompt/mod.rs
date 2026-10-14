// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # `prompt`
//!
//! What the model sees, assembled for accuracy per token.
//!
//! - [`SchemaContext`]: the catalog rendered once per catalog generation into
//!   compact per-table blocks — one line per column, STRUCT/ARRAY shapes
//!   inline, truncated descriptions, partition and "filter required" notes —
//!   each table with a **fixed short alias** (`orders AS o`). Requests take
//!   ready-made bytes (a whole-schema slice, or a set of table blocks in
//!   canonical order), never re-render; the text is byte-stable, so provider
//!   prefix caches hit.
//! - [`SYSTEM`]: the static CrabSQL rules for BigQuery (first in cache order).
//! - [`context_line`]: the small per-day part (today's date) placed *after*
//!   the schema, so a date change does not invalidate the cached prefix.
//! - [`repair`]: a targeted repair message carrying the failing SQL and the
//!   binder's / warehouse's diagnostics.
//! - [`extract_sql`]: zero-copy extraction of the SQL from a model's output
//!   (fences, labels, trailing semicolons stripped).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use crate::base::date::Date;
use crate::catalog::{Catalog, TableId, TableKind};
use std::collections::HashSet;
use std::fmt::Write as _;
use std::ops::Range;

/// The static system instructions (CrabSQL for BigQuery).
pub const SYSTEM: &str = "\
You translate a question into one BigQuery GoogleSQL query.
Rules:
- Output only the SQL: no prose, no markdown fences, no trailing semicolon.
- Use only the tables and columns listed in the schema. Refer to each table with its given alias (e.g. `proj.shop.orders` AS o) and qualify every column with that alias (o.status).
- Read-only: a single SELECT statement (WITH clauses allowed).
- Use GoogleSQL functions (DATE_TRUNC(d, MONTH), TIMESTAMP_TRUNC, SAFE_DIVIDE, COUNTIF, APPROX_COUNT_DISTINCT, ...). String comparisons are case-sensitive.
- For relative dates, compute literal dates from today's date given below (e.g. DATE '2026-06-30'); do not call CURRENT_DATE() or CURRENT_TIMESTAMP().
- Filter TIMESTAMP and DATETIME columns with half-open ranges: o.ts >= TIMESTAMP '2023-01-01' AND o.ts < TIMESTAMP '2024-01-01' (never BETWEEN with a date-only upper bound).
- A table marked \"filter required\" must be filtered on its partition column.
- To read fields inside an ARRAY column, UNNEST it: FROM t AS o, UNNEST(o.items) AS it.
- Select only the columns the question asks for, with readable aliases.
- If the schema cannot answer the question, output only: CANNOT_ANSWER: <one short reason>";

/// Rules appended to [`SYSTEM`] when output is decoded under the schema
/// grammar (they override the examples above).
pub const GRAMMAR_RULES: &str = "
- Output is decoded under a grammar: keywords in upper case; write catalog columns as alias.column; name WITH tables cte_<name>, FROM subqueries sq_<name> and UNNEST elements un_<name> (e.g. FROM t AS o, UNNEST(o.items) AS un_item).";

/// The prefix of a model's "cannot answer" reply.
pub const CANNOT_ANSWER: &str = "CANNOT_ANSWER";

/// Rendering options.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RenderOpts {
    /// Longest description kept, characters.
    pub max_description: usize,
    /// Include approximate row counts.
    pub row_counts: bool,
    /// Include profile hints (value lists, examples, ranges, NULL share).
    pub profiles: bool,
    /// Longest complete value list rendered in full.
    pub max_values: usize,
}

impl Default for RenderOpts {
    fn default() -> Self {
        Self { max_description: 80, row_counts: true, profiles: true, max_values: 25 }
    }
}

/// One table's rendered block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableBlock {
    /// The table.
    pub table: TableId,
    /// Its fixed alias.
    pub alias: String,
    /// Byte range in [`SchemaContext::text`].
    pub range: Range<usize>,
}

/// The catalog rendered for prompts. Build once per catalog generation.
#[derive(Clone, Debug, Default)]
pub struct SchemaContext {
    text: String,
    blocks: Vec<TableBlock>,
}

impl SchemaContext {
    /// Render every table of `cat` (optionally only these datasets).
    #[must_use]
    pub fn build(cat: &Catalog, datasets: Option<&[&str]>, opts: RenderOpts) -> Self {
        let mut text = String::with_capacity(4096);
        let mut blocks = Vec::new();
        let tables: Vec<_> = cat
            .tables()
            .filter(|t| datasets.is_none_or(|ds| ds.contains(&t.dataset_name)))
            .collect();
        // Shortest aliases go to the tables queries are most likely to touch:
        // assign in descending size (deterministic ties by name), render in
        // canonical order.
        let mut by_size: Vec<usize> = (0..tables.len()).collect();
        by_size.sort_by(|&a, &b| {
            tables[b].row_count.cmp(&tables[a].row_count).then_with(|| {
                (tables[a].dataset_name, tables[a].name)
                    .cmp(&(tables[b].dataset_name, tables[b].name))
            })
        });
        let mut used: HashSet<String> = HashSet::new();
        let mut aliases = vec![String::new(); tables.len()];
        for i in by_size {
            aliases[i] = unique_alias(tables[i].name, &mut used);
        }
        for (t, alias) in tables.iter().zip(aliases) {
            let start = text.len();
            render_table(cat, t.id, &alias, opts, &mut text);
            blocks.push(TableBlock { table: t.id, alias, range: start..text.len() });
        }
        Self { text, blocks }
    }

    /// The whole rendered schema.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Every table block, in canonical order.
    #[must_use]
    pub fn blocks(&self) -> &[TableBlock] {
        &self.blocks
    }

    /// The block of one table.
    #[must_use]
    pub fn block(&self, t: TableId) -> Option<&TableBlock> {
        self.blocks.iter().find(|b| b.table == t)
    }

    /// The fixed alias of a table.
    #[must_use]
    pub fn alias(&self, t: TableId) -> Option<&str> {
        self.block(t).map(|b| b.alias.as_str())
    }

    /// Approximate tokens of the whole schema (≈ 4 bytes per token).
    #[must_use]
    pub fn estimated_tokens(&self) -> u32 {
        u32::try_from(graph_r::text::estimate_tokens(self.text.len())).unwrap_or(u32::MAX)
    }

    /// Append the blocks of `tables` (canonical order, deduplicated) to
    /// `out`: the per-request subset for large catalogs. Blocks are copied
    /// verbatim, so a subset shares its prefix with any other subset that
    /// starts with the same tables.
    pub fn subset_into(&self, tables: &[TableId], out: &mut String) {
        for b in &self.blocks {
            if tables.contains(&b.table) {
                out.push_str(&self.text[b.range.clone()]);
            }
        }
    }
}

/// A short, readable, unique alias: initials of the name's words
/// (`order_items` → `oi`), extended with more letters or a digit on
/// collision, never a reserved keyword.
fn unique_alias(table: &str, used: &mut HashSet<String>) -> String {
    let words: Vec<&str> = table.split(['_', '-']).filter(|w| !w.is_empty()).collect();
    let initials: String = words
        .iter()
        .filter_map(|w| w.chars().next())
        .flat_map(char::to_lowercase)
        .filter(char::is_ascii_alphanumeric)
        .collect();
    let mut base =
        if initials.is_empty() || !initials.starts_with(|c: char| c.is_ascii_alphabetic()) {
            "t".to_owned()
        } else {
            initials
        };
    let ok = |a: &str, used: &HashSet<String>| {
        !used.contains(a) && crate::sql::lexer::keyword(a.as_bytes()).is_none()
    };
    if !ok(&base, used) {
        // Extend with the next letters of the last word (`orders` → `or` is a keyword → `ord`).
        let last: Vec<char> = words
            .last()
            .map(|w| w.to_lowercase().chars().filter(char::is_ascii_alphanumeric).collect())
            .unwrap_or_default();
        for extra in 1..last.len() {
            let candidate: String = base
                .chars()
                .take(base.len() - 1)
                .chain(last.iter().take(extra + 1).copied())
                .collect();
            if ok(&candidate, used) {
                base = candidate;
                break;
            }
        }
    }
    let mut alias = base.clone();
    let mut n = 2;
    while !ok(&alias, used) {
        alias = format!("{base}{n}");
        n += 1;
    }
    used.insert(alias.clone());
    alias
}

fn truncate(s: &str, max: usize) -> &str {
    if s.chars().count() <= max {
        return s;
    }
    let end = s.char_indices().nth(max).map_or(s.len(), |(i, _)| i);
    &s[..end]
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn approx(n: u64) -> String {
    match n {
        0 => String::new(),
        1..=9_999 => n.to_string(),
        10_000..=999_999 => format!("{}K", n / 1000),
        1_000_000..=999_999_999 => format!("{:.1}M", n as f64 / 1e6),
        _ => format!("{:.1}B", n as f64 / 1e9),
    }
}

fn render_table(cat: &Catalog, t: TableId, alias: &str, opts: RenderOpts, out: &mut String) {
    let Some(tb) = cat.table(t) else { return };
    let _ = write!(out, "TABLE `{}.{}.{}` AS {alias}", tb.project, tb.dataset_name, tb.name);
    let mut notes: Vec<String> = Vec::new();
    match tb.kind {
        TableKind::View | TableKind::MaterializedView => notes.push("view".into()),
        _ => {}
    }
    if opts.row_counts && tb.row_count > 0 {
        notes.push(format!("~{} rows", approx(tb.row_count)));
    }
    if let Some(p) = tb.partition_col.and_then(|c| cat.column(c)) {
        notes.push(if tb.require_partition_filter {
            format!("partitioned by {} (filter required)", p.path)
        } else {
            format!("partitioned by {}", p.path)
        });
    }
    let desc = one_line(tb.description);
    if !desc.is_empty() {
        notes.insert(0, truncate(&desc, opts.max_description).to_owned());
    }
    if !notes.is_empty() {
        out.push_str("  -- ");
        out.push_str(&notes.join("; "));
    }
    out.push('\n');
    for c in cat.columns(t) {
        // Nested fields are visible in their parent's type; hidden
        // pseudo-columns only when they partition the table.
        if c.depth > 0 || (c.hidden() && !c.partitioning()) {
            continue;
        }
        let _ = write!(out, "  {alias}.{} {}", c.path, c.data_type);
        if !c.nullable() {
            out.push_str(" NOT NULL");
        }
        let mut tags: Vec<&str> = Vec::new();
        if c.partitioning() {
            tags.push("partition");
        }
        if c.clustering.is_some() {
            tags.push("cluster");
        }
        if c.repeated() {
            tags.push("UNNEST to read");
        }
        if !tags.is_empty() {
            let _ = write!(out, " [{}]", tags.join(", "));
        }
        let d = one_line(c.description);
        let hint =
            if opts.profiles { profile_hint(cat, c.id, c.data_type, opts) } else { String::new() };
        if !d.is_empty() || !hint.is_empty() {
            out.push_str("  -- ");
            out.push_str(truncate(&d, opts.max_description));
            if !d.is_empty() && !hint.is_empty() {
                out.push_str("; ");
            }
            out.push_str(&hint);
        }
        out.push('\n');
    }
}

/// Most distinct values for a column to get examples.
const MAX_EXAMPLE_DISTINCT: u64 = 3_000;

/// A range bound, short: dates without the clock, numbers with at most two
/// decimals.
fn bound(s: &str, data_type: &str) -> String {
    match data_type {
        "DATETIME" | "TIMESTAMP" => truncate(s, 10).to_owned(),
        "FLOAT64" | "NUMERIC" | "BIGNUMERIC" => match s.split_once('.') {
            Some((int, frac)) if frac.len() > 2 => {
                let frac = frac[..2].trim_end_matches('0');
                if frac.is_empty() {
                    int.to_owned()
                } else {
                    format!("{int}.{frac}")
                }
            }
            _ => s.to_owned(),
        },
        _ => s.to_owned(),
    }
}

/// Longest single value rendered, characters.
const MAX_VALUE_CHARS: usize = 40;
/// Longest rendered value list, bytes; longer complete lists fall back to
/// examples.
const MAX_VALUES_BYTES: usize = 320;

/// A value as a GoogleSQL literal the model can copy exactly (unquoted
/// when `quoted` is false).
pub fn literal_into(v: &str, quoted: bool, out: &mut String) {
    if !quoted {
        out.push_str(v);
        return;
    }
    out.push('\'');
    for ch in v.chars() {
        match ch {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out.push('\'');
}

/// The compact profile hint of one column: `values: 'A', 'B'` for small
/// complete sets, `e.g. 'A', 'B', 'C'` otherwise (strings), a `range` for
/// ordered types, and the NULL share when most rows are NULL.
fn profile_hint(
    cat: &Catalog,
    col: crate::catalog::ColId,
    data_type: &str,
    opts: RenderOpts,
) -> String {
    let Some(p) = cat.profile(col) else { return String::new() };
    let mut out = String::new();
    let quoted = data_type == "STRING";
    let listable = quoted || data_type == "BOOL" || data_type == "INT64";
    let values: Vec<(&str, u64)> = if listable {
        p.values().filter(|(v, _)| v.chars().count() <= MAX_VALUE_CHARS).collect()
    } else {
        Vec::new()
    };
    let mut listed = false;
    if p.values_complete
        && !values.is_empty()
        && values.len() <= opts.max_values
        && data_type != "BOOL"
    {
        let mut list = String::from("values: ");
        for (i, (v, _)) in values.iter().enumerate() {
            if i > 0 {
                list.push_str(", ");
            }
            literal_into(v, quoted, &mut list);
        }
        if list.len() <= MAX_VALUES_BYTES {
            out.push_str(&list);
            listed = true;
        }
    }
    // Examples help only for repeated, name-like values: not for keys,
    // e-mails, addresses or hashes (near-unique), nor for columns whose
    // values the question grounding finds anyway (very many distinct).
    let exemplary = p.distinct <= MAX_EXAMPLE_DISTINCT && p.distinct.saturating_mul(2) <= p.rows;
    if !listed && quoted && exemplary && !values.is_empty() {
        out.push_str("e.g. ");
        for (i, (v, _)) in values.iter().take(3).enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            literal_into(v, true, &mut out);
        }
    }
    if !listed {
        let ordered = matches!(
            data_type,
            "DATE" | "DATETIME" | "TIMESTAMP" | "INT64" | "FLOAT64" | "NUMERIC" | "BIGNUMERIC"
        );
        if let (true, Some(lo), Some(hi)) = (ordered, p.min, p.max) {
            if !out.is_empty() {
                out.push_str("; ");
            }
            let _ = write!(out, "range {}..{}", bound(lo, data_type), bound(hi, data_type));
        }
    }
    let nulls = p.null_fraction();
    if nulls >= 0.5 {
        if !out.is_empty() {
            out.push_str("; ");
        }
        let _ = write!(out, "{:.0}% NULL", nulls * 100.0);
    }
    out
}

/// System instruction of the table-linking call.
pub const LINK_SYSTEM: &str = "\
You select the BigQuery tables needed to answer a question with SQL.
- Choose every table the query must read, including tables needed only to join others.
- Choose as few tables as possible.
- Use only the table names listed.";

/// A compact one-line table card for linking: `dataset.table (~rows):
/// col, col, ... -- description`. Top-level columns only, no types.
pub fn table_card(cat: &Catalog, t: TableId, out: &mut String) {
    let Some(tb) = cat.table(t) else { return };
    let _ = write!(out, "{}.{}", tb.dataset_name, tb.name);
    if tb.row_count > 0 {
        let _ = write!(out, " (~{} rows)", approx(tb.row_count));
    }
    out.push(':');
    let mut first = true;
    for c in cat.columns(t).filter(|c| c.depth == 0 && (!c.hidden() || c.partitioning())) {
        out.push_str(if first { " " } else { ", " });
        out.push_str(c.path);
        first = false;
    }
    let d = one_line(tb.description);
    if !d.is_empty() {
        out.push_str("  -- ");
        out.push_str(truncate(&d, 60));
    }
    out.push('\n');
}

/// The per-day context line (after the schema in prompt order).
#[must_use]
pub fn context_line(today: Date) -> String {
    let (q_start, _) =
        today.previous_period(crate::base::date::Grain::Quarter).unwrap_or((today, today));
    format!("Today is {today} ({}). The last full quarter began {q_start}.", weekday(today))
}

fn weekday(d: Date) -> &'static str {
    ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        [(d.iso_weekday() as usize).saturating_sub(1).min(6)]
}

/// The question part.
#[must_use]
pub fn question(q: &str) -> String {
    format!("Question: {}\nSQL:", q.trim())
}

/// A targeted repair message: the failing SQL and what is wrong with it.
#[must_use]
pub fn repair(question: &str, sql: &str, problems: &[String]) -> String {
    let mut s = String::with_capacity(sql.len() + 256);
    let _ = write!(
        s,
        "Question: {}\nYour previous SQL:\n{}\nIt is invalid:\n",
        question.trim(),
        sql.trim()
    );
    for p in problems {
        let _ = writeln!(s, "- {p}");
    }
    s.push_str("Return only the corrected SQL.\nSQL:");
    s
}

/// The SQL inside a model's output: strips surrounding whitespace, a
/// leading `SQL:` label, markdown fences and trailing semicolons. Zero-copy.
#[must_use]
pub fn extract_sql(output: &str) -> &str {
    let mut s = output.trim();
    if let Some(start) = s.find("```") {
        let after = &s[start + 3..];
        let body_start = after.find('\n').map_or(0, |i| i + 1);
        let after_lang = &after[body_start..];
        s = after_lang.find("```").map_or(after_lang, |end| &after_lang[..end]);
    }
    s = s.trim();
    for label in ["SQL:", "sql:"] {
        if let Some(rest) = s.strip_prefix(label) {
            s = rest.trim_start();
        }
    }
    s.trim_end().trim_end_matches(';').trim_end()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogBuilder, ColumnSpec, TableSpec};

    fn col(p: &str, t: &str, o: u32) -> ColumnSpec {
        ColumnSpec {
            path: p.into(),
            data_type: t.into(),
            ordinal: o,
            nullable: true,
            partitioning: false,
            hidden: false,
            clustering: None,
            description: None,
        }
    }

    fn cat() -> Catalog {
        let mut b = CatalogBuilder::new("proj");
        let t = |name: &str, desc: Option<&str>, rows: u64, rpf: bool, cols: Vec<ColumnSpec>| {
            TableSpec {
                dataset: "shop".into(),
                name: name.into(),
                kind: TableKind::Table,
                description: desc.map(Into::into),
                row_count: rows,
                logical_bytes: 0,
                last_modified_ms: 0,
                require_partition_filter: rpf,
                columns: cols,
            }
        };
        b.add_table(t(
            "orders",
            Some("One row per order.\n  Includes test orders."),
            1_234_567,
            true,
            vec![
                ColumnSpec { nullable: false, ..col("order_id", "INT64", 1) },
                ColumnSpec {
                    description: Some("Lifecycle state: pending, shipped, cancelled".into()),
                    clustering: Some(1),
                    ..col("status", "STRING", 2)
                },
                ColumnSpec { partitioning: true, ..col("order_ts", "TIMESTAMP", 3) },
                col("items", "ARRAY<STRUCT<sku STRING, qty INT64>>", 4),
                col("items.sku", "STRING", 4),
                col("items.qty", "INT64", 4),
            ],
        ));
        b.add_table(t("order_items", None, 0, false, vec![col("order_id", "INT64", 1)]));
        b.add_table(t("or", None, 0, false, vec![col("x", "INT64", 1)]));
        b.add_table(t("orgs", None, 0, false, vec![col("x", "INT64", 1)]));
        Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
    }

    #[test]
    fn renders_profile_hints_compactly() {
        use crate::catalog::ProfileSpec;
        let mut b = CatalogBuilder::new("p");
        b.add_table(TableSpec {
            dataset: "d".into(),
            name: "users".into(),
            kind: TableKind::Table,
            description: None,
            row_count: 10,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns: vec![
                col("gender", "STRING", 1),
                col("city", "STRING", 2),
                col("created_at", "TIMESTAMP", 3),
                col("age", "INT64", 4),
                col("note", "STRING", 5),
                col("price", "FLOAT64", 6),
                col("sku", "STRING", 7),
            ],
        });
        let p =
            |vals: &[&str], complete: bool, min: Option<&str>, max: Option<&str>, nulls: u64| {
                ProfileSpec {
                    rows: 10,
                    nulls,
                    distinct: vals.len() as u64,
                    min: min.map(Into::into),
                    max: max.map(Into::into),
                    values: vals.iter().map(|v| ((*v).to_owned(), 1)).collect(),
                    values_complete: complete,
                    sampled: false,
                }
            };
        b.set_profile("d", "users", "gender", p(&["M", "F"], true, None, None, 0));
        b.set_profile(
            "d",
            "users",
            "city",
            p(&["New York", "O'Hare", "Paris", "Rome"], false, None, None, 0),
        );
        b.set_profile(
            "d",
            "users",
            "created_at",
            p(&[], false, Some("2019-01-06 01:38:00+00"), Some("2024-06-30 23:59:00+00"), 0),
        );
        b.set_profile("d", "users", "age", p(&[], false, Some("12"), Some("70"), 0));
        b.set_profile("d", "users", "note", p(&[], false, None, None, 8));
        b.set_profile(
            "d",
            "users",
            "price",
            p(&[], false, Some("0.0082999997779726"), Some("557.15100217983127"), 0),
        );
        // Near-unique values (keys, hashes): no examples.
        b.set_profile(
            "d",
            "users",
            "sku",
            p(&["A1", "B2", "C3", "D4", "E5", "F6"], false, None, None, 0),
        );
        let c = Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap();
        let s = SchemaContext::build(&c, None, RenderOpts::default());
        assert_eq!(
            s.text(),
            "TABLE `p.d.users` AS u  -- ~10 rows\n  u.gender STRING  -- values: 'M', 'F'\n  u.city STRING  -- e.g. 'New York', 'O\\'Hare', 'Paris'\n  u.created_at TIMESTAMP  -- range 2019-01-06..2024-06-30\n  u.age INT64  -- range 12..70\n  u.note STRING  -- 80% NULL\n  u.price FLOAT64  -- range 0..557.15\n  u.sku STRING\n"
        );
        let plain =
            SchemaContext::build(&c, None, RenderOpts { profiles: false, ..RenderOpts::default() });
        assert!(!plain.text().contains("--  "), "{}", plain.text());
        assert!(!plain.text().contains("values"));
    }

    #[test]
    fn renders_compact_blocks_with_fixed_aliases() {
        let c = cat();
        let s = SchemaContext::build(&c, None, RenderOpts::default());
        let orders = c.find_table("shop", "orders").unwrap();
        let block = &s.text()[s.block(orders).unwrap().range.clone()];
        assert_eq!(
            block,
            "TABLE `proj.shop.orders` AS o  -- One row per order. Includes test orders.; ~1.2M rows; partitioned by order_ts (filter required)\n\
             \x20 o.order_id INT64 NOT NULL\n\
             \x20 o.status STRING [cluster]  -- Lifecycle state: pending, shipped, cancelled\n\
             \x20 o.order_ts TIMESTAMP [partition]\n\
             \x20 o.items ARRAY<STRUCT<sku STRING, qty INT64>> [UNNEST to read]\n"
        );
        assert!(s.estimated_tokens() > 0);
    }

    #[test]
    fn aliases_are_unique_short_and_never_keywords() {
        let c = cat();
        let s = SchemaContext::build(&c, None, RenderOpts::default());
        let mut aliases: Vec<&str> = s.blocks().iter().map(|b| b.alias.as_str()).collect();
        let a = |name: &str| s.alias(c.find_table("shop", name).unwrap()).unwrap().to_owned();
        assert_eq!(a("orders"), "o");
        assert_eq!(a("order_items"), "oi");
        assert_ne!(a("or"), "or", "OR is a keyword");
        aliases.sort_unstable();
        aliases.dedup();
        assert_eq!(aliases.len(), 4);
        for x in aliases {
            assert!(crate::sql::lexer::keyword(x.as_bytes()).is_none(), "{x}");
        }
    }

    #[test]
    fn rendering_is_deterministic_and_subsets_share_prefixes() {
        let c = cat();
        let a = SchemaContext::build(&c, None, RenderOpts::default());
        let b = SchemaContext::build(&c, None, RenderOpts::default());
        assert_eq!(a.text(), b.text());
        let ids: Vec<TableId> = a.blocks().iter().map(|b| b.table).collect();
        let (mut s1, mut s2) = (String::new(), String::new());
        a.subset_into(&[ids[0], ids[2]], &mut s1);
        a.subset_into(&[ids[2], ids[0], ids[3]], &mut s2);
        assert!(
            s2.starts_with(&s1),
            "canonical order: a superset extends the smaller subset's prefix"
        );
    }

    #[test]
    fn extracts_sql_from_common_output_shapes() {
        for (out, want) in [
            ("SELECT 1", "SELECT 1"),
            ("  SELECT 1;\n", "SELECT 1"),
            ("```sql\nSELECT o.x\nFROM t AS o\n```", "SELECT o.x\nFROM t AS o"),
            ("Here you go:\n```\nSELECT 2;\n```\nThanks", "SELECT 2"),
            ("SQL: SELECT 3", "SELECT 3"),
        ] {
            assert_eq!(extract_sql(out), want, "{out:?}");
        }
    }

    #[test]
    fn context_and_repair_messages() {
        let today = Date::parse("2026-09-29").unwrap();
        assert_eq!(
            context_line(today),
            "Today is 2026-09-29 (Tuesday). The last full quarter began 2026-04-01."
        );
        let r = repair(
            "How many?",
            "SELECT o.statuz FROM t AS o",
            &["Unrecognized name: statuz; Did you mean status?".into()],
        );
        assert!(
            r.contains("SELECT o.statuz")
                && r.contains("- Unrecognized name: statuz; Did you mean status?")
                && r.ends_with("SQL:")
        );
    }
}
