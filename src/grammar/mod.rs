// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # `grammar`
//!
//! The CrabSQL language as a context-free grammar **restricted to the
//! schema in context**, for backends that decode under a grammar: a model
//! constrained by it cannot write a table or a column that does not exist.
//!
//! - Catalog tables appear only as their backticked name with their fixed
//!   alias (`` `p.d.orders` AS o ``) and catalog columns only as
//!   `alias.column` — the fixed aliases make column scoping context-free, so
//!   this is a plain CFG.
//! - Relations the query makes itself are namespaced: WITH tables `cte_*`,
//!   FROM subqueries `sq_*`, UNNEST elements `un_*`; their columns are free
//!   identifiers. Unqualified identifiers appear only as output aliases and
//!   ORDER BY / GROUP BY references to them.
//! - Functions come from the binder's function table; keywords are upper
//!   case. A declared non-answer (`CANNOT_ANSWER: ...`) is always allowed.
//!
//! One internal grammar is emitted in two dialects: [`Grammar::lark`]
//! (vLLM `guided_grammar`, llguidance; whitespace ignored) and
//! [`Grammar::gbnf`] (llama.cpp; whitespace explicit, required only where
//! two tokens would merge — two words, or two dashes, which start a
//! comment). Every derivation is accepted by the CrabSQL
//! strict parser — the property the tests check by random derivation in
//! both spacings ([`Grammar::derive`]).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::cast_possible_truncation,
    clippy::too_many_lines,
    clippy::doc_markdown
)]

use crate::base::prng::Xoshiro256;
use std::fmt::Write as _;

/// A catalog table in context.
#[derive(Clone, Debug)]
pub struct Table<'a> {
    /// `project.dataset.table`.
    pub fqn: &'a str,
    /// Its fixed alias.
    pub alias: &'a str,
    /// Addressable column paths (`status`, `addr.city`; not fields inside
    /// arrays, which are read through UNNEST).
    pub columns: Vec<&'a str>,
}

/// A terminal class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Term {
    /// Lower-case identifier (output aliases, derived columns).
    Ident,
    /// Namespaced derived relation name (`cte_*`, `sq_*`, `un_*`).
    Derived(&'static str),
    /// Unsigned integer.
    Int,
    /// Decimal number.
    Number,
    /// Single-quoted string literal.
    Str,
    /// The rest of the line (non-answer reason).
    Line,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Sym {
    Lit(String),
    Rule(usize),
    Term(Term),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rep {
    One,
    Opt,
    Star,
    Plus,
}

#[derive(Clone, Debug)]
struct Item {
    sym: Sym,
    rep: Rep,
}

#[derive(Clone, Debug)]
struct Rule {
    name: String,
    alts: Vec<Vec<Item>>,
}

/// A grammar. Build with [`Grammar::sql`].
#[derive(Clone, Debug)]
pub struct Grammar {
    rules: Vec<Rule>,
    /// Per rule: can derive empty / classes of its possible first and last
    /// characters ([`WORD`], [`DASH`]).
    nullable: Vec<bool>,
    first: Vec<u8>,
    last: Vec<u8>,
    /// Per rule: least derivation height (bounds random derivation).
    height: Vec<u32>,
}

/// Grammar construction helper.
struct B {
    rules: Vec<Rule>,
}

fn lit(s: &str) -> Item {
    Item { sym: Sym::Lit(s.to_owned()), rep: Rep::One }
}
fn r(i: usize) -> Item {
    Item { sym: Sym::Rule(i), rep: Rep::One }
}
fn t(x: Term) -> Item {
    Item { sym: Sym::Term(x), rep: Rep::One }
}
fn opt(mut i: Item) -> Item {
    i.rep = Rep::Opt;
    i
}
fn star(mut i: Item) -> Item {
    i.rep = Rep::Star;
    i
}
fn plus(mut i: Item) -> Item {
    i.rep = Rep::Plus;
    i
}

impl B {
    /// Declare a rule (alternatives filled later), returning its index.
    fn decl(&mut self, name: &str) -> usize {
        self.rules.push(Rule { name: name.to_owned(), alts: Vec::new() });
        self.rules.len() - 1
    }
    fn set(&mut self, i: usize, alts: Vec<Vec<Item>>) {
        self.rules[i].alts = alts;
    }
    fn rule(&mut self, name: &str, alts: Vec<Vec<Item>>) -> usize {
        let i = self.decl(name);
        self.set(i, alts);
        i
    }
    /// `a | b | c` over literals.
    fn lits(&mut self, name: &str, words: &[&str]) -> usize {
        self.rule(name, words.iter().map(|w| vec![lit(w)]).collect())
    }
    /// `x ("," x)*`
    fn list(&mut self, name: &str, x: usize) -> usize {
        let tail = self.rule(&format!("{name}-tail"), vec![vec![lit(","), r(x)]]);
        self.rule(name, vec![vec![r(x), star(r(tail))]])
    }
}

/// Date parts accepted as function arguments and in EXTRACT / INTERVAL.
const DATE_PARTS: &[&str] = &[
    "MICROSECOND",
    "MILLISECOND",
    "SECOND",
    "MINUTE",
    "HOUR",
    "DAYOFWEEK",
    "DAY",
    "DAYOFYEAR",
    "WEEK",
    "ISOWEEK",
    "MONTH",
    "QUARTER",
    "YEAR",
    "ISOYEAR",
    "DATE",
    "TIME",
];
const WEEKDAYS: &[&str] =
    &["SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY"];
const TYPES: &[&str] = &[
    "INT64",
    "FLOAT64",
    "NUMERIC",
    "BIGNUMERIC",
    "STRING",
    "BOOL",
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "TIME",
    "BYTES",
];
const BINOPS: &[&str] =
    &["+", "-", "*", "/", "||", "=", "!=", "<>", "<", "<=", ">", ">=", "AND", "OR"];
/// Functions with their own syntax (not generic calls).
const SPECIAL: &[&str] = &["COUNT", "CAST", "SAFE_CAST", "EXTRACT"];

fn is_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Character classes that must not touch across two tokens: two word
/// characters merge into one word, two dashes start a comment.
const WORD: u8 = 1;
const DASH: u8 = 2;

fn class(c: Option<char>) -> u8 {
    match c {
        Some(c) if is_word(c) => WORD,
        Some('-') => DASH,
        _ => 0,
    }
}

/// Whether two adjacent tokens need whitespace between them.
fn must_split(last: u8, first: u8) -> bool {
    last & first != 0
}

impl Grammar {
    /// The CrabSQL grammar over `tables`, with calls to `functions`
    /// (upper-case names).
    #[must_use]
    pub fn sql(tables: &[Table<'_>], functions: &[&str]) -> Self {
        let mut b = B { rules: Vec::new() };
        let start = b.decl("root");
        let query = b.decl("query");
        let expr = b.decl("expr");
        let query_p = b.rule("query-p", vec![vec![lit("("), r(query), lit(")")]]);

        // Leaves.
        let datepart_base = b.lits("datepart-base", DATE_PARTS);
        let weekday = b.lits("weekday", WEEKDAYS);
        let datepart = b.rule(
            "datepart",
            vec![vec![r(datepart_base)], vec![lit("WEEK"), lit("("), r(weekday), lit(")")]],
        );
        let ty = b.lits("type", TYPES);
        let typed = b.lits("typed", &["DATE", "DATETIME", "TIMESTAMP", "TIME"]);
        let literal = b.rule(
            "literal",
            vec![
                vec![t(Term::Number)],
                vec![t(Term::Int)],
                vec![t(Term::Str)],
                vec![lit("TRUE")],
                vec![lit("FALSE")],
                vec![lit("NULL")],
                vec![r(typed), t(Term::Str)],
            ],
        );
        let mut col_alts: Vec<Vec<Item>> = Vec::new();
        let mut table_alts: Vec<Vec<Item>> = Vec::new();
        for tb in tables {
            for c in &tb.columns {
                col_alts.push(vec![lit(&format!("{}.{c}", tb.alias))]);
            }
            table_alts.push(vec![lit(&format!("`{}`", tb.fqn)), lit("AS"), lit(tb.alias)]);
        }
        // A catalog without columns still yields a well-formed grammar.
        let has_cols = !col_alts.is_empty();
        let col = b.rule("column", col_alts);
        let field = b.rule("field", vec![vec![lit("."), t(Term::Ident)]]);
        let derived_ref = b.rule(
            "derived-ref",
            vec![
                vec![t(Term::Derived("cte_")), lit("."), t(Term::Ident)],
                vec![t(Term::Derived("sq_")), lit("."), t(Term::Ident)],
                vec![t(Term::Derived("un_")), star(r(field))],
            ],
        );

        // Calls.
        let funcs: Vec<&str> = functions
            .iter()
            .copied()
            .filter(|f| !SPECIAL.contains(f) && f.chars().all(is_word))
            .collect();
        let fname = b.lits("fname", &funcs);
        let arg = b.rule("arg", vec![vec![r(expr)], vec![r(datepart)]]);
        let args = b.list("args", arg);
        let expr_list = b.list("exprs", expr);
        let dir = b.lits("dir", &["ASC", "DESC"]);
        let nulls = b
            .rule("nulls", vec![vec![lit("NULLS"), lit("FIRST")], vec![lit("NULLS"), lit("LAST")]]);
        let order_item = b.rule(
            "order-item",
            vec![
                vec![r(expr), opt(r(dir)), opt(r(nulls))],
                vec![t(Term::Ident), opt(r(dir)), opt(r(nulls))],
            ],
        );
        let order_list = b.list("order-items", order_item);
        let partition = b.rule("partition", vec![vec![lit("PARTITION"), lit("BY"), r(expr_list)]]);
        let window_order =
            b.rule("window-order", vec![vec![lit("ORDER"), lit("BY"), r(order_list)]]);
        let over = b.rule(
            "over",
            vec![vec![lit("OVER"), lit("("), opt(r(partition)), opt(r(window_order)), lit(")")]],
        );
        let distinct = b.rule("distinct", vec![vec![lit("DISTINCT")]]);
        let call = b.rule(
            "call",
            vec![
                vec![r(fname), lit("("), opt(r(distinct)), opt(r(args)), lit(")"), opt(r(over))],
                vec![lit("COUNT"), lit("("), lit("*"), lit(")"), opt(r(over))],
                vec![lit("COUNT"), lit("("), opt(r(distinct)), r(expr), lit(")"), opt(r(over))],
                vec![lit("CAST"), lit("("), r(expr), lit("AS"), r(ty), lit(")")],
                vec![lit("SAFE_CAST"), lit("("), r(expr), lit("AS"), r(ty), lit(")")],
                vec![lit("EXTRACT"), lit("("), r(datepart), lit("FROM"), r(expr), lit(")")],
            ],
        );
        let when = b.rule("when", vec![vec![lit("WHEN"), r(expr), lit("THEN"), r(expr)]]);
        let else_ = b.rule("else", vec![vec![lit("ELSE"), r(expr)]]);
        let case = b.rule(
            "case",
            vec![
                vec![lit("CASE"), plus(r(when)), opt(r(else_)), lit("END")],
                vec![lit("CASE"), r(expr), plus(r(when)), opt(r(else_)), lit("END")],
            ],
        );
        // INTERVAL takes a simple operand: `INTERVAL 30 DAY`, `INTERVAL (n) DAY`.
        let mut iv_alts = vec![
            vec![t(Term::Int)],
            vec![lit("-"), t(Term::Int)],
            vec![lit("("), r(expr), lit(")")],
            vec![r(derived_ref)],
        ];
        if has_cols {
            iv_alts.push(vec![r(col)]);
        }
        let iv_operand = b.rule("interval-operand", iv_alts);
        let mut primary_alts = vec![
            vec![r(literal)],
            vec![r(derived_ref)],
            vec![r(call)],
            vec![r(case)],
            vec![lit("("), r(expr), lit(")")],
            vec![r(query_p)],
            vec![lit("EXISTS"), r(query_p)],
            vec![lit("INTERVAL"), r(iv_operand), r(datepart_base)],
        ];
        if has_cols {
            primary_alts.insert(1, vec![r(col)]);
        }
        let primary = b.rule("primary", primary_alts);
        let not = b.rule("not", vec![vec![lit("NOT")]]);
        // IN lists hold values: a list item opening with `(SELECT` would read
        // as the subquery form `IN (query)`.
        let mut in_item_alts =
            vec![vec![r(literal)], vec![lit("-"), r(literal)], vec![r(derived_ref)]];
        if has_cols {
            in_item_alts.push(vec![r(col)]);
        }
        let in_item = b.rule("in-item", in_item_alts);
        let in_list = b.list("in-items", in_item);
        let in_rhs = b.rule("in-rhs", vec![vec![r(query_p)], vec![lit("("), r(in_list), lit(")")]]);
        let postfix = b.rule(
            "postfix",
            vec![
                vec![lit("IS"), opt(r(not)), lit("NULL")],
                vec![opt(r(not)), lit("IN"), r(in_rhs)],
                vec![opt(r(not)), lit("LIKE"), r(primary)],
                vec![opt(r(not)), lit("BETWEEN"), r(primary), lit("AND"), r(primary)],
            ],
        );
        // NOT binds only at the start of a boolean term (`a AND NOT b`),
        // never after a comparison (`a = NOT b` is ambiguous and rejected).
        let unary = b.decl("unary");
        b.set(unary, vec![vec![lit("-"), r(unary)], vec![r(primary), star(r(postfix))]]);
        let op = b.lits("op", &BINOPS[..BINOPS.len() - 2]);
        let op_tail = b.rule("op-tail", vec![vec![r(op), r(unary)]]);
        let cmp = b.rule("cmp", vec![vec![r(unary), star(r(op_tail))]]);
        let nterm = b.rule("nterm", vec![vec![star(r(not)), r(cmp)]]);
        let logic = b.lits("logic", &["AND", "OR"]);
        let logic_tail = b.rule("logic-tail", vec![vec![r(logic), r(nterm)]]);
        b.set(expr, vec![vec![r(nterm), star(r(logic_tail))]]);

        // Queries.
        let alias = b.rule("alias", vec![vec![lit("AS"), t(Term::Ident)]]);
        let select_item = b.rule("select-item", vec![vec![r(expr), opt(r(alias))], vec![lit("*")]]);
        let select_list = b.list("select-items", select_item);
        let offset = b.rule("with-offset", vec![vec![lit("WITH"), lit("OFFSET"), opt(r(alias))]]);
        let mut from_alts = vec![
            vec![r(query_p), lit("AS"), t(Term::Derived("sq_"))],
            vec![
                lit("UNNEST"),
                lit("("),
                r(expr),
                lit(")"),
                lit("AS"),
                t(Term::Derived("un_")),
                opt(r(offset)),
            ],
            vec![t(Term::Derived("cte_"))],
        ];
        if !table_alts.is_empty() {
            let table = b.rule("table", table_alts);
            from_alts.insert(0, vec![r(table)]);
        }
        let from_item = b.rule("from-item", from_alts);
        let outer = b.rule("outer", vec![vec![lit("OUTER")]]);
        let join_kind = b.rule(
            "join-kind",
            vec![
                vec![lit("INNER")],
                vec![lit("LEFT"), opt(r(outer))],
                vec![lit("RIGHT"), opt(r(outer))],
                vec![lit("FULL"), opt(r(outer))],
            ],
        );
        let ident_item = b.rule("ident", vec![vec![t(Term::Ident)]]);
        let idents = b.list("idents", ident_item);
        let join_cond = b.rule(
            "join-cond",
            vec![vec![lit("ON"), r(expr)], vec![lit("USING"), lit("("), r(idents), lit(")")]],
        );
        let join = b.rule(
            "join",
            vec![
                vec![opt(r(join_kind)), lit("JOIN"), r(from_item), r(join_cond)],
                vec![lit("CROSS"), lit("JOIN"), r(from_item)],
                vec![lit(","), r(from_item)],
            ],
        );
        let from = b.rule("from", vec![vec![lit("FROM"), r(from_item), star(r(join))]]);
        let where_ = b.rule("where", vec![vec![lit("WHERE"), r(expr)]]);
        let group_item = b.rule("group-item", vec![vec![r(expr)], vec![t(Term::Ident)]]);
        let group_list = b.list("group-items", group_item);
        let group = b.rule("group", vec![vec![lit("GROUP"), lit("BY"), r(group_list)]]);
        let having = b.rule("having", vec![vec![lit("HAVING"), r(expr)]]);
        let qualify = b.rule("qualify", vec![vec![lit("QUALIFY"), r(expr)]]);
        let select = b.rule(
            "select",
            vec![vec![
                lit("SELECT"),
                opt(r(distinct)),
                r(select_list),
                opt(r(from)),
                opt(r(where_)),
                opt(r(group)),
                opt(r(having)),
                opt(r(qualify)),
            ]],
        );
        let set_op = b.rule(
            "set-op",
            vec![
                vec![lit("UNION"), lit("ALL"), r(select)],
                vec![lit("UNION"), lit("DISTINCT"), r(select)],
                vec![lit("INTERSECT"), lit("DISTINCT"), r(select)],
                // No EXCEPT: after `SELECT *` it reads as the star modifier
                // `* EXCEPT (...)`; NOT IN / NOT EXISTS say the same.
            ],
        );
        let cte = b.rule("cte", vec![vec![t(Term::Derived("cte_")), lit("AS"), r(query_p)]]);
        let ctes = b.list("ctes", cte);
        let with = b.rule("with", vec![vec![lit("WITH"), r(ctes)]]);
        let order = b.rule("order", vec![vec![lit("ORDER"), lit("BY"), r(order_list)]]);
        let limit_off = b.rule("limit-offset", vec![vec![lit("OFFSET"), t(Term::Int)]]);
        let limit = b.rule("limit", vec![vec![lit("LIMIT"), t(Term::Int), opt(r(limit_off))]]);
        b.set(
            query,
            vec![vec![opt(r(with)), r(select), star(r(set_op)), opt(r(order)), opt(r(limit))]],
        );
        b.set(start, vec![vec![r(query)], vec![lit("CANNOT_ANSWER:"), t(Term::Line)]]);
        Self::analyse(b.rules)
    }

    fn analyse(rules: Vec<Rule>) -> Self {
        let n = rules.len();
        let mut g = Self {
            rules,
            nullable: vec![false; n],
            first: vec![0; n],
            last: vec![0; n],
            height: vec![u32::MAX; n],
        };
        loop {
            let mut changed = false;
            for i in 0..n {
                let (mut nl, mut fw, mut lw) = (false, 0u8, 0u8);
                let mut h = u32::MAX;
                for alt in &g.rules[i].alts {
                    // Nullable: every item nullable.
                    let alt_null = alt.iter().all(|it| g.item_nullable(it));
                    nl |= alt_null;
                    // First word: through the nullable prefix.
                    for it in alt {
                        fw |= g.sym_first(&it.sym);
                        if !g.item_nullable(it) {
                            break;
                        }
                    }
                    for it in alt.iter().rev() {
                        lw |= g.sym_last(&it.sym);
                        if !g.item_nullable(it) {
                            break;
                        }
                    }
                    let mut ah = 0u32;
                    for it in alt {
                        if matches!(it.rep, Rep::One | Rep::Plus) {
                            ah = ah.max(g.sym_height(&it.sym).saturating_add(1));
                        }
                    }
                    h = h.min(ah.max(1));
                }
                if (nl, fw, lw, h) != (g.nullable[i], g.first[i], g.last[i], g.height[i]) {
                    g.nullable[i] = nl;
                    g.first[i] = fw;
                    g.last[i] = lw;
                    g.height[i] = h;
                    changed = true;
                }
            }
            if !changed {
                return g;
            }
        }
    }

    fn item_nullable(&self, it: &Item) -> bool {
        matches!(it.rep, Rep::Opt | Rep::Star) || matches!(it.sym, Sym::Rule(r) if self.nullable[r])
    }

    fn sym_first(&self, s: &Sym) -> u8 {
        match s {
            Sym::Lit(l) => class(l.chars().next()),
            Sym::Rule(r) => self.first[*r],
            Sym::Term(Term::Str) => 0,
            Sym::Term(Term::Line) => WORD | DASH,
            Sym::Term(_) => WORD,
        }
    }

    fn sym_last(&self, s: &Sym) -> u8 {
        match s {
            Sym::Lit(l) => class(l.chars().last()),
            Sym::Rule(r) => self.last[*r],
            Sym::Term(Term::Str) => 0,
            Sym::Term(Term::Line) => WORD | DASH,
            Sym::Term(_) => WORD,
        }
    }

    fn sym_height(&self, s: &Sym) -> u32 {
        match s {
            Sym::Rule(r) => self.height[*r],
            _ => 0,
        }
    }

    /// Rules in the grammar.
    #[must_use]
    pub fn rules(&self) -> usize {
        self.rules.len()
    }

    /// Lark text (whitespace ignored between tokens).
    #[must_use]
    pub fn lark(&self) -> String {
        let mut out = String::with_capacity(4096);
        for (i, rule) in self.rules.iter().enumerate() {
            let _ = write!(out, "{}: ", lark_name(&rule.name, i == 0));
            for (k, alt) in rule.alts.iter().enumerate() {
                if k > 0 {
                    out.push_str(" | ");
                }
                for (j, it) in alt.iter().enumerate() {
                    if j > 0 {
                        out.push(' ');
                    }
                    match &it.sym {
                        Sym::Lit(l) => quote_into(l, &mut out),
                        Sym::Rule(r) => out.push_str(&lark_name(&self.rules[*r].name, *r == 0)),
                        Sym::Term(t) => out.push_str(lark_term(*t)),
                    }
                    out.push_str(rep_suffix(it.rep));
                }
            }
            out.push('\n');
        }
        out.push_str(
            "IDENT: /[a-z_][a-z0-9_]*/\nCTE_NAME: /cte_[a-z0-9_]+/\nSQ_NAME: /sq_[a-z0-9_]+/\nUN_NAME: /un_[a-z0-9_]+/\nINT: /[0-9]+/\nNUMBER: /[0-9]+\\.[0-9]+/\nSTRING: /'([^'\\\\\\n]|\\\\.)*'/\nLINE: /[^\\n]+/\n%ignore /[ \\t\\n]+/\n",
        );
        out
    }

    /// GBNF text (llama.cpp): whitespace explicit, required only between
    /// two tokens that could merge into one word.
    #[must_use]
    pub fn gbnf(&self) -> String {
        let mut out = String::with_capacity(4096);
        for (i, rule) in self.rules.iter().enumerate() {
            let _ = write!(out, "{} ::= ", gbnf_name(&rule.name, i == 0));
            for (k, alt) in rule.alts.iter().enumerate() {
                if k > 0 {
                    out.push_str(" | ");
                }
                let mut prev = 0u8;
                for (j, it) in alt.iter().enumerate() {
                    let sep = if j == 0 {
                        None
                    } else if must_split(prev, self.sym_first(&it.sym)) {
                        Some("sp")
                    } else {
                        Some("ws")
                    };
                    let mut atom = String::new();
                    match &it.sym {
                        Sym::Lit(l) => quote_into(l, &mut atom),
                        Sym::Rule(r) => atom.push_str(&gbnf_name(&self.rules[*r].name, *r == 0)),
                        Sym::Term(t) => atom.push_str(gbnf_term(*t)),
                    }
                    if j > 0 {
                        out.push(' ');
                    }
                    match (sep, it.rep) {
                        (None, Rep::One) => out.push_str(&atom),
                        (None, rep) => {
                            let _ = write!(out, "({atom}){}", rep_suffix(rep));
                        }
                        (Some(s), Rep::One) => {
                            let _ = write!(out, "{s} {atom}");
                        }
                        (Some(s), rep) => {
                            let _ = write!(out, "({s} {atom}){}", rep_suffix(rep));
                        }
                    }
                    let last = self.sym_last(&it.sym);
                    prev = if self.item_nullable(it) { prev | last } else { last };
                }
            }
            out.push('\n');
        }
        out.push_str(
            "ws ::= [ \\t\\n]*\nsp ::= [ \\t\\n]+\nident ::= [a-z_] [a-z0-9_]*\ncte-name ::= \"cte_\" [a-z0-9_]+\nsq-name ::= \"sq_\" [a-z0-9_]+\nun-name ::= \"un_\" [a-z0-9_]+\nint ::= [0-9]+\nnumber ::= [0-9]+ \".\" [0-9]+\nstring ::= \"'\" ([^'\\\\\\n] | \"\\\\\" [^\\n])* \"'\"\nline ::= [^\\n]+\n",
        );
        out
    }

    /// A random sentence of the grammar (for tests and fuzzing), nesting
    /// queries and expressions at most `max_depth` deep: `tight`
    /// joins tokens with a space only where GBNF requires one, otherwise
    /// every token is separated by a space (Lark's view).
    #[must_use]
    pub fn derive(&self, seed: u64, max_depth: u32, tight: bool) -> String {
        let mut rng = Xoshiro256::new(seed);
        let mut out = String::new();
        let mut prev = 0u8;
        self.derive_rule(0, max_depth, tight, &mut rng, &mut out, &mut prev);
        out
    }

    fn derive_rule(
        &self,
        r: usize,
        depth: u32,
        tight: bool,
        rng: &mut Xoshiro256,
        out: &mut String,
        prev: &mut u8,
    ) {
        // Depth counts nesting of the recursive hubs (queries and
        // expressions), not rule levels.
        let depth = if matches!(self.rules[r].name.as_str(), "query" | "expr") {
            depth.saturating_sub(1)
        } else {
            depth
        };
        let alts = &self.rules[r].alts;
        // Past the depth budget, take the shallowest alternative.
        let alt = if depth == 0 {
            alts.iter()
                .min_by_key(|a| {
                    a.iter()
                        .filter(|it| matches!(it.rep, Rep::One | Rep::Plus))
                        .map(|it| self.sym_height(&it.sym))
                        .max()
                        .unwrap_or(0)
                })
                .unwrap_or(&alts[0])
        } else {
            &alts[rng.below(alts.len() as u64) as usize]
        };
        for it in alt {
            let reps = match (it.rep, depth == 0) {
                (Rep::One, _) | (Rep::Plus, true) => 1,
                (Rep::Opt | Rep::Star, true) => 0,
                (Rep::Opt, false) => rng.below(2),
                (Rep::Star, false) => rng.below(3),
                (Rep::Plus, false) => 1 + rng.below(2),
            };
            for _ in 0..reps {
                self.derive_sym(&it.sym, depth, tight, rng, out, prev);
            }
        }
    }

    fn derive_sym(
        &self,
        s: &Sym,
        depth: u32,
        tight: bool,
        rng: &mut Xoshiro256,
        out: &mut String,
        prev: &mut u8,
    ) {
        let emit = |text: &str, out: &mut String, prev: &mut u8| {
            if !out.is_empty() && (!tight || must_split(*prev, class(text.chars().next()))) {
                out.push(' ');
            }
            out.push_str(text);
            *prev = class(text.chars().last());
        };
        match s {
            Sym::Lit(l) => emit(l, out, prev),
            Sym::Rule(r) => self.derive_rule(*r, depth, tight, rng, out, prev),
            Sym::Term(t) => {
                let pick = |xs: &[&'static str], rng: &mut Xoshiro256| {
                    xs[rng.below(xs.len() as u64) as usize]
                };
                let text = match t {
                    Term::Ident => pick(&["x", "total", "n_orders", "k"], rng).to_owned(),
                    Term::Derived(p) => format!("{p}{}", pick(&["a", "b", "top_2"], rng)),
                    Term::Int => pick(&["0", "7", "2024"], rng).to_owned(),
                    Term::Number => pick(&["0.5", "10.25"], rng).to_owned(),
                    Term::Str => pick(&["'a'", "'O\\'Hare'", "''", "'2024-01-01'"], rng).to_owned(),
                    Term::Line => "the schema has no weather data".to_owned(),
                };
                emit(&text, out, prev);
            }
        }
    }
}

fn rep_suffix(r: Rep) -> &'static str {
    match r {
        Rep::One => "",
        Rep::Opt => "?",
        Rep::Star => "*",
        Rep::Plus => "+",
    }
}

fn lark_name(name: &str, root: bool) -> String {
    if root {
        "start".to_owned()
    } else {
        name.replace('-', "_")
    }
}

fn gbnf_name(name: &str, root: bool) -> String {
    if root {
        "root".to_owned()
    } else {
        format!("r-{name}")
    }
}

fn lark_term(t: Term) -> &'static str {
    match t {
        Term::Ident => "IDENT",
        Term::Derived("cte_") => "CTE_NAME",
        Term::Derived("sq_") => "SQ_NAME",
        Term::Derived(_) => "UN_NAME",
        Term::Int => "INT",
        Term::Number => "NUMBER",
        Term::Str => "STRING",
        Term::Line => "LINE",
    }
}

fn gbnf_term(t: Term) -> &'static str {
    match t {
        Term::Ident => "ident",
        Term::Derived("cte_") => "cte-name",
        Term::Derived("sq_") => "sq-name",
        Term::Derived(_) => "un-name",
        Term::Int => "int",
        Term::Number => "number",
        Term::Str => "string",
        Term::Line => "line",
    }
}

/// A double-quoted literal (both dialects escape `"` and `\` alike).
fn quote_into(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
}
