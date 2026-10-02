// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! GoogleSQL query parser: recursive descent for statements, Pratt parsing
//! for expressions (GoogleSQL precedence).
//!
//! A [`Parser`] owns its token buffer, AST pools and scratch stacks and is
//! reused: after the first few statements it allocates nothing. Child lists
//! are built on scratch stacks and moved into their pool in one contiguous
//! run, so `Span`s stay valid and cache-friendly.
//!
//! Errors carry a span and say whether parsing failed only because the input
//! ended ([`ParseError::incomplete`]) — the signal [`viability`] uses to keep
//! streaming a model's partial SQL instead of aborting it.

use crate::ast::*;
use crate::lexer::{tokenize, Kw, Punct, Tok, Token};
use crate::Sp;
use crab_core::pool::{Idx, Pool, Span};

/// Deepest nesting accepted for model output (`Mode::Strict`), bounding
/// stack use on hostile or degenerate input.
pub const MAX_DEPTH_STRICT: u32 = 64;
/// Deepest nesting accepted for foreign SQL (`Mode::Full`): BI-tool SQL in
/// query logs nests deeper. Safe on a default 2 MiB thread stack.
pub const MAX_DEPTH_FULL: u32 = 128;

/// The error message when nesting exceeds the limit.
pub const TOO_DEEP: &str = "nesting too deep";

/// The error message when the speculation budget is spent (bounded work on
/// adversarial input).
pub const TOO_COMPLEX: &str = "statement too complex to parse";

/// Parsing mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Any GoogleSQL query (foreign SQL: logs, verified pairs, semantic layers).
    Full,
    /// CrabSQL, what models write: additionally no parameters or system
    /// variables. (Name- and function-level rules are the binder's.)
    Strict,
}

/// A parse failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParseError {
    /// Where.
    pub sp: Sp,
    /// What was expected / wrong.
    pub msg: &'static str,
    /// Failed only because the input ended (a viable prefix).
    pub incomplete: bool,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at {}..{}", self.msg, self.sp.start, self.sp.end)
    }
}

impl std::error::Error for ParseError {}

type Res<T> = Result<T, ParseError>;

/// Scratch stacks: children are pushed here, then moved into their pool as
/// one contiguous span.
#[derive(Debug, Default)]
struct Stacks {
    exprs: Vec<ExprId>,
    names: Vec<Name>,
    args: Vec<Arg>,
    arms: Vec<WhenArm>,
    fields: Vec<StructField>,
    type_fields: Vec<StructFieldType>,
    order: Vec<OrderItem>,
    named_windows: Vec<NamedWindow>,
    items: Vec<SelectItem>,
    groups: Vec<GroupItem>,
    ctes: Vec<Cte>,
}

impl Stacks {
    fn mark(&self) -> [usize; 11] {
        [
            self.exprs.len(),
            self.names.len(),
            self.args.len(),
            self.arms.len(),
            self.fields.len(),
            self.type_fields.len(),
            self.order.len(),
            self.named_windows.len(),
            self.items.len(),
            self.groups.len(),
            self.ctes.len(),
        ]
    }

    fn restore(&mut self, m: [usize; 11]) {
        self.exprs.truncate(m[0]);
        self.names.truncate(m[1]);
        self.args.truncate(m[2]);
        self.arms.truncate(m[3]);
        self.fields.truncate(m[4]);
        self.type_fields.truncate(m[5]);
        self.order.truncate(m[6]);
        self.named_windows.truncate(m[7]);
        self.items.truncate(m[8]);
        self.groups.truncate(m[9]);
        self.ctes.truncate(m[10]);
    }
}

/// Retries allowed per statement when a `((...SELECT` run turns out not to
/// open a query: each retry re-parses a bounded region, so the fuel keeps
/// the worst case linear in the input.
const BACKTRACK_FUEL: u32 = 16;

/// A reusable parser.
#[derive(Debug, Default)]
pub struct Parser {
    toks: Vec<Token>,
    ast: Ast,
    st: Stacks,
    stmts: Vec<Stmt>,
    clauses: Vec<MergeClause>,
    sets: Vec<SetItem>,
}

/// A parsed script: every statement, borrowing the parser's pools.
#[derive(Debug, Clone, Copy)]
pub struct Script<'p> {
    /// The source text.
    pub src: &'p str,
    /// The pools.
    pub ast: &'p Ast,
    /// Statements, in order.
    pub stmts: &'p [Stmt],
    /// MERGE clauses (ranges in [`Stmt::clauses`]).
    pub clauses: &'p [MergeClause],
    /// `UPDATE SET` items (ranges in [`MergeAction::Update`]).
    pub sets: &'p [SetItem],
}

impl<'p> Script<'p> {
    /// Source text of a span.
    #[must_use]
    pub fn text(&self, sp: Sp) -> &'p str {
        sp.text(self.src)
    }

    /// The last statement that writes a table or view (a deployment script
    /// may set things up first), else the last query.
    #[must_use]
    pub fn last_write(&self) -> Option<&'p Stmt> {
        self.stmts
            .iter()
            .rev()
            .find(|s| !matches!(s.kind, StmtKind::Query | StmtKind::Other))
            .or_else(|| self.stmts.iter().rev().find(|s| s.kind == StmtKind::Query))
    }

    /// A statement's row-producing query, as a [`Parsed`] the binder takes.
    #[must_use]
    pub fn query(&self, s: &Stmt) -> Option<Parsed<'p>> {
        s.query.map(|root| Parsed { src: self.src, ast: self.ast, root })
    }

    /// The clauses of a MERGE statement.
    #[must_use]
    pub fn merge_clauses(&self, s: &Stmt) -> &'p [MergeClause] {
        &self.clauses[s.clauses.0 as usize..s.clauses.1 as usize]
    }

    /// The `SET` items of an `UPDATE` action.
    #[must_use]
    pub fn set_items(&self, range: (u32, u32)) -> &'p [SetItem] {
        &self.sets[range.0 as usize..range.1 as usize]
    }

    /// A path of names as written, dot-joined, without backticks
    /// (`p.d.t`).
    #[must_use]
    pub fn path(&self, names: Span<Name>) -> String {
        let mut out = String::new();
        for (i, n) in self.ast.names.slice(names).iter().enumerate() {
            if i > 0 {
                out.push('.');
            }
            out.push_str(self.text(n.sp).trim_matches('`'));
        }
        out
    }
}

/// A parsed statement, borrowing the parser's pools.
#[derive(Debug, Clone, Copy)]
pub struct Parsed<'p> {
    /// The source text.
    pub src: &'p str,
    /// The pools.
    pub ast: &'p Ast,
    /// The root query.
    pub root: QueryId,
}

impl Parsed<'_> {
    /// Source text of a span.
    #[must_use]
    pub fn text(&self, sp: Sp) -> &str {
        sp.text(self.src)
    }
}

impl Parser {
    /// A parser with empty buffers.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse one query statement (an optional trailing `;` is allowed).
    pub fn parse<'p>(&'p mut self, src: &'p str, mode: Mode) -> Res<Parsed<'p>> {
        tokenize(src, &mut self.toks).map_err(|e| ParseError {
            sp: Sp { start: e.at, end: e.at },
            msg: e.msg,
            incomplete: e.incomplete,
        })?;
        self.ast.clear();
        let mut p = P {
            src,
            toks: &self.toks,
            pos: 0,
            ast: &mut self.ast,
            st: &mut self.st,
            depth: 0,
            positional: 0,
            mode,
            pending_gt: false,
            fuel: BACKTRACK_FUEL,
        };
        let root = p.query()?;
        p.eat_p(Punct::Semi);
        if p.peek() != Tok::Eof {
            return Err(p.err("expected end of statement"));
        }
        Ok(Parsed { src, ast: &self.ast, root })
    }

    /// Parse a script: statements separated by `;`. Queries, `CREATE TABLE
    /// [AS]`, `CREATE [MATERIALIZED] VIEW`, `INSERT` and `MERGE` are parsed
    /// in full; other statements (procedural scripting, DDL without a query)
    /// are skipped to the next top-level `;` and kept as
    /// [`StmtKind::Other`]. Statements other than queries are accepted only
    /// in [`Mode::Full`]: model output stays a single `SELECT`.
    pub fn parse_script<'p>(&'p mut self, src: &'p str, mode: Mode) -> Res<Script<'p>> {
        tokenize(src, &mut self.toks).map_err(|e| ParseError {
            sp: Sp { start: e.at, end: e.at },
            msg: e.msg,
            incomplete: e.incomplete,
        })?;
        self.ast.clear();
        self.stmts.clear();
        self.clauses.clear();
        self.sets.clear();
        let mut p = P {
            src,
            toks: &self.toks,
            pos: 0,
            ast: &mut self.ast,
            st: &mut self.st,
            depth: 0,
            positional: 0,
            mode,
            pending_gt: false,
            fuel: BACKTRACK_FUEL,
        };
        loop {
            while p.eat_p(Punct::Semi) {}
            if p.peek() == Tok::Eof {
                break;
            }
            let stmt = p.statement(&mut self.clauses, &mut self.sets)?;
            self.stmts.push(stmt);
            if !p.eat_p(Punct::Semi) && p.peek() != Tok::Eof {
                return Err(p.err("expected ';' between statements"));
            }
        }
        Ok(Script {
            src,
            ast: &self.ast,
            stmts: &self.stmts,
            clauses: &self.clauses,
            sets: &self.sets,
        })
    }
}

impl Parser {
    /// The error sits on the last real token, and that token is one that a
    /// continuation can reinterpret (`t.` → `t.col`, `COUNT(*` → `COUNT(*)`,
    /// `ARRAY<` / `>` → nested generics, `|` → `||`, `=` → `=>`, `-` → a
    /// dashed project name).
    fn error_on_open_last_token(&self, src: &str, sp: Sp) -> bool {
        let n = self.toks.len();
        if n < 2 || self.toks[n - 1].kind != Tok::Eof {
            return false;
        }
        let last = self.toks[n - 2];
        last.sp == sp
            && matches!(
                last.kind,
                Tok::P(
                    Punct::Dot
                        | Punct::Star
                        | Punct::Lt
                        | Punct::Gt
                        | Punct::Pipe
                        | Punct::Eq
                        | Punct::Minus
                        | Punct::LParen
                        | Punct::Comma
                )
            )
            && last.sp.end as usize == src.len()
    }
}

/// Is a (possibly partial) model output still on track?
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Viability {
    /// A complete, well-formed statement.
    Complete,
    /// Not finished, but every token so far can still become valid SQL.
    Incomplete,
    /// Already wrong: no continuation can fix it. Stop generating.
    Invalid(ParseError),
}

/// Classify a streamed prefix. Used by the stream-abort sink: `Invalid`
/// aborts the sample (saving its remaining output tokens), `Incomplete`
/// keeps it going. A word touching the end of the input may still grow
/// (`SEL` → `SELECT`), so an error on it is treated as incomplete.
pub fn viability(parser: &mut Parser, src: &str, mode: Mode) -> Viability {
    let result = parser.parse(src, mode).map(|_| ());
    match result {
        Ok(()) => Viability::Complete,
        Err(e) if e.incomplete => Viability::Incomplete,
        Err(e) if parser.error_on_open_last_token(src, e.sp) => Viability::Incomplete,
        Err(e) => {
            let at_end = e.sp.end as usize == src.len() && e.sp.start < e.sp.end;
            let wordish = src[e.sp.start as usize..e.sp.end as usize]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_');
            if at_end && wordish && !src.ends_with(|c: char| c.is_whitespace()) {
                Viability::Incomplete
            } else {
                Viability::Invalid(e)
            }
        }
    }
}

struct P<'a> {
    src: &'a str,
    toks: &'a [Token],
    pos: usize,
    ast: &'a mut Ast,
    st: &'a mut Stacks,
    depth: u32,
    positional: u32,
    mode: Mode,
    /// A `>>` was split: one `>` is still owed to the next `expect_gt`.
    pending_gt: bool,
    /// Remaining backtracking retries.
    fuel: u32,
}

/// Move `stack[mark..]` into `pool` as one span.
fn finish<T: Copy>(stack: &mut Vec<T>, mark: usize, pool: &mut Pool<T>, sp: Sp) -> Res<Span<T>> {
    let r = pool.extend(stack.drain(mark..));
    r.map_err(|_| ParseError { sp, msg: "statement too large", incomplete: false })
}

fn push<T>(pool: &mut Pool<T>, v: T, sp: Sp) -> Res<Idx<T>> {
    pool.push(v).map_err(|_| ParseError { sp, msg: "statement too large", incomplete: false })
}

impl P<'_> {
    // ---- statements (Full mode) -------------------------------------------

    fn statement(&mut self, clauses: &mut Vec<MergeClause>, sets: &mut Vec<SetItem>) -> Res<Stmt> {
        let start = self.sp().start;
        let empty = Span::EMPTY;
        let mut s = Stmt {
            kind: StmtKind::Query,
            sp: self.sp(),
            target: None,
            target_alias: None,
            columns: empty,
            query: None,
            query_sp: Sp::default(),
            source_table: None,
            source_alias: None,
            on: None,
            clauses: (0, 0),
        };
        let writes = self.is_kw(Kw::Create)
            || self.is_kw(Kw::Merge)
            || self.is_word("INSERT")
            || self.is_word("UPDATE");
        if writes && self.mode == Mode::Strict {
            return Err(self.err("only a SELECT statement is allowed"));
        }
        if self.starts_query() {
            {
                let qs = self.sp().start;
                s.query = Some(self.query()?);
                s.query_sp =
                    Sp { start: qs, end: self.toks[self.pos.saturating_sub(1)].sp.end.max(qs) };
            }
        } else if self.eat_kw(Kw::Create) {
            self.create(&mut s)?;
        } else if self.eat_word("INSERT") {
            self.insert(&mut s)?;
        } else if self.eat_kw(Kw::Merge) {
            self.merge(&mut s, clauses, sets)?;
        } else if self.eat_word("UPDATE") {
            self.update(&mut s, sets)?;
        } else if self.mode == Mode::Full {
            s.kind = StmtKind::Other;
            self.skip_statement();
        } else {
            return Err(self.err("expected a SELECT statement"));
        }
        let end = self.toks[self.pos.saturating_sub(1)].sp.end.max(start);
        s.sp = Sp { start, end };
        Ok(s)
    }

    /// Skip to the next `;` at bracket depth 0 (or the end).
    fn skip_statement(&mut self) {
        let mut depth = 0u32;
        loop {
            match self.peek() {
                Tok::Eof => return,
                Tok::P(Punct::Semi) if depth == 0 => return,
                Tok::P(Punct::LParen | Punct::LBracket) => depth += 1,
                Tok::P(Punct::RParen | Punct::RBracket) => depth = depth.saturating_sub(1),
                _ => {}
            }
            self.bump();
        }
    }

    /// Skip one balanced `( ... )` group.
    fn skip_parens(&mut self) -> Res<()> {
        self.expect_p(Punct::LParen, "expected '('")?;
        let mut depth = 1u32;
        while depth > 0 {
            match self.peek() {
                Tok::Eof => return Err(self.err("unclosed '('")),
                Tok::P(Punct::LParen) => depth += 1,
                Tok::P(Punct::RParen) => depth -= 1,
                _ => {}
            }
            self.bump();
        }
        Ok(())
    }

    /// `(name, ...)`, each name optionally followed by `OPTIONS(...)`.
    fn name_list(&mut self) -> Res<Span<Name>> {
        self.expect_p(Punct::LParen, "expected '('")?;
        let sp = self.sp();
        let mark = self.st.names.len();
        loop {
            let n = self.name("expected column name")?;
            self.st.names.push(n);
            if self.eat_word("OPTIONS") {
                self.skip_parens()?;
            }
            if !self.eat_p(Punct::Comma) {
                break;
            }
        }
        self.expect_p(Punct::RParen, "expected ')'")?;
        finish(&mut self.st.names, mark, &mut self.ast.names, sp)
    }

    /// `PARTITION BY ...`, `CLUSTER BY ...`, `OPTIONS (...)`,
    /// `DEFAULT COLLATE ...` in any order.
    fn table_options(&mut self) -> Res<()> {
        loop {
            if self.eat_kw(Kw::Partition) {
                self.expect_kw(Kw::By, "expected BY")?;
                self.expr()?;
            } else if self.eat_word("CLUSTER") {
                self.expect_kw(Kw::By, "expected BY")?;
                loop {
                    self.expr()?;
                    if !self.eat_p(Punct::Comma) {
                        break;
                    }
                }
            } else if self.eat_word("OPTIONS") {
                self.skip_parens()?;
            } else if self.eat_kw(Kw::Default) {
                self.expect_kw(Kw::Collate, "expected COLLATE")?;
                self.bump();
            } else {
                return Ok(());
            }
        }
    }

    fn if_not_exists(&mut self) -> Res<()> {
        if self.eat_kw(Kw::If) {
            self.expect_kw(Kw::Not, "expected NOT")?;
            self.expect_kw(Kw::Exists, "expected EXISTS")?;
        }
        Ok(())
    }

    fn create(&mut self, s: &mut Stmt) -> Res<()> {
        if self.eat_kw(Kw::Or) {
            self.expect_word("REPLACE", "expected REPLACE")?;
        }
        let _ = self.eat_word("TEMP") || self.eat_word("TEMPORARY");
        if self.eat_word("MATERIALIZED") {
            self.expect_word("VIEW", "expected VIEW")?;
            s.kind = StmtKind::CreateMaterializedView;
        } else if self.eat_word("VIEW") {
            s.kind = StmtKind::CreateView;
        } else if self.eat_word("TABLE") {
            s.kind = StmtKind::CreateTable;
        } else {
            // CREATE FUNCTION / SCHEMA / ...: not a table write.
            s.kind = StmtKind::Other;
            self.skip_statement();
            return Ok(());
        }
        self.if_not_exists()?;
        s.target = Some(self.table_path()?);
        if self.is_p(Punct::LParen) {
            if s.kind == StmtKind::CreateTable {
                // Column definitions: types and options are not needed.
                self.skip_parens()?;
            } else {
                s.columns = self.name_list()?;
            }
        }
        if s.kind == StmtKind::CreateTable
            && (self.is_word("LIKE") || self.is_word("COPY") || self.is_word("CLONE"))
        {
            self.bump();
            self.table_path()?;
        }
        self.table_options()?;
        if self.eat_kw(Kw::As) {
            {
                let qs = self.sp().start;
                s.query = Some(self.query()?);
                s.query_sp =
                    Sp { start: qs, end: self.toks[self.pos.saturating_sub(1)].sp.end.max(qs) };
            }
        }
        Ok(())
    }

    fn insert(&mut self, s: &mut Stmt) -> Res<()> {
        s.kind = StmtKind::Insert;
        self.eat_kw(Kw::Into);
        s.target = Some(self.table_path()?);
        if self.is_p(Punct::LParen) && !self.paren_then_query() {
            s.columns = self.name_list()?;
        }
        if self.eat_word("VALUES") {
            loop {
                self.paren_expr_list()?;
                if !self.eat_p(Punct::Comma) {
                    break;
                }
            }
        } else {
            {
                let qs = self.sp().start;
                s.query = Some(self.query()?);
                s.query_sp =
                    Sp { start: qs, end: self.toks[self.pos.saturating_sub(1)].sp.end.max(qs) };
            }
        }
        Ok(())
    }

    fn merge(
        &mut self,
        s: &mut Stmt,
        clauses: &mut Vec<MergeClause>,
        sets: &mut Vec<SetItem>,
    ) -> Res<()> {
        s.kind = StmtKind::Merge;
        self.eat_kw(Kw::Into);
        s.target = Some(self.table_path()?);
        s.target_alias = self.merge_alias()?;
        self.expect_kw(Kw::Using, "expected USING")?;
        if self.is_p(Punct::LParen) {
            {
                let qs = self.sp().start;
                s.query = Some(self.paren_query()?);
                s.query_sp =
                    Sp { start: qs, end: self.toks[self.pos.saturating_sub(1)].sp.end.max(qs) };
            }
        } else {
            s.source_table = Some(self.table_path()?);
        }
        s.source_alias = self.merge_alias()?;
        self.expect_kw(Kw::On, "expected ON")?;
        s.on = Some(self.expr()?);
        let first = clauses.len() as u32;
        while self.eat_kw(Kw::When) {
            let when = if self.eat_kw(Kw::Not) {
                self.expect_word("MATCHED", "expected MATCHED")?;
                if self.eat_kw(Kw::By) {
                    if self.eat_word("SOURCE") {
                        MergeWhen::NotMatchedBySource
                    } else {
                        self.expect_word("TARGET", "expected TARGET or SOURCE")?;
                        MergeWhen::NotMatchedByTarget
                    }
                } else {
                    MergeWhen::NotMatchedByTarget
                }
            } else {
                self.expect_word("MATCHED", "expected MATCHED")?;
                MergeWhen::Matched
            };
            let cond = if self.eat_kw(Kw::And) { Some(self.expr()?) } else { None };
            self.expect_kw(Kw::Then, "expected THEN")?;
            let action = if self.eat_word("UPDATE") {
                self.expect_kw(Kw::Set, "expected SET")?;
                let a = sets.len() as u32;
                loop {
                    let col = self.table_path()?;
                    self.expect_p(Punct::Eq, "expected '='")?;
                    let value = self.expr()?;
                    sets.push(SetItem { col, value });
                    if !self.eat_p(Punct::Comma) {
                        break;
                    }
                }
                MergeAction::Update((a, sets.len() as u32))
            } else if self.eat_word("DELETE") {
                MergeAction::Delete
            } else {
                self.expect_word("INSERT", "expected UPDATE, DELETE or INSERT")?;
                if self.eat_word("ROW") {
                    MergeAction::InsertRow
                } else {
                    let cols =
                        if self.is_p(Punct::LParen) { self.name_list()? } else { Span::EMPTY };
                    self.expect_word("VALUES", "expected VALUES")?;
                    let values = self.paren_expr_list()?;
                    MergeAction::Insert { cols, values }
                }
            };
            clauses.push(MergeClause { when, cond, action });
        }
        if clauses.len() as u32 == first {
            return Err(self.err("expected WHEN"));
        }
        s.clauses = (first, clauses.len() as u32);
        Ok(())
    }

    /// `UPDATE target [[AS] alias] SET col = expr, ... [FROM items] [WHERE cond]`.
    fn update(&mut self, s: &mut Stmt, sets: &mut Vec<SetItem>) -> Res<()> {
        s.kind = StmtKind::Update;
        s.target = Some(self.table_path()?);
        s.target_alias = self.merge_alias()?;
        self.expect_kw(Kw::Set, "expected SET")?;
        let a = sets.len() as u32;
        loop {
            let col = self.table_path()?;
            self.expect_p(Punct::Eq, "expected '='")?;
            let value = self.expr()?;
            sets.push(SetItem { col, value });
            if !self.eat_p(Punct::Comma) {
                break;
            }
        }
        s.clauses = (a, sets.len() as u32);
        if self.eat_kw(Kw::From) {
            let qs = self.sp().start;
            self.table_expr()?;
            s.query_sp =
                Sp { start: qs, end: self.toks[self.pos.saturating_sub(1)].sp.end.max(qs) };
        }
        if self.eat_kw(Kw::Where) {
            s.on = Some(self.expr()?);
        }
        Ok(())
    }

    /// `[AS] alias` after a MERGE target or source.
    fn merge_alias(&mut self) -> Res<Option<Name>> {
        if self.eat_kw(Kw::As) {
            return Ok(Some(self.name("expected alias")?));
        }
        if self.is_name() && !self.is_word("USING") {
            return Ok(Some(self.name("expected alias")?));
        }
        Ok(None)
    }

    // ---- token helpers ---------------------------------------------------

    fn tok(&self) -> Token {
        self.toks[self.pos.min(self.toks.len() - 1)]
    }

    fn peek(&self) -> Tok {
        self.tok().kind
    }

    fn peek_at(&self, k: usize) -> Tok {
        self.toks.get(self.pos + k).map_or(Tok::Eof, |t| t.kind)
    }

    fn sp(&self) -> Sp {
        self.tok().sp
    }

    fn bump(&mut self) -> Token {
        let t = self.tok();
        if self.pos < self.toks.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn err(&self, msg: &'static str) -> ParseError {
        let t = self.tok();
        ParseError { sp: t.sp, msg, incomplete: t.kind == Tok::Eof }
    }

    fn is_kw(&self, k: Kw) -> bool {
        self.peek() == Tok::Kw(k)
    }

    fn eat_kw(&mut self, k: Kw) -> bool {
        if self.is_kw(k) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, k: Kw, msg: &'static str) -> Res<()> {
        if self.eat_kw(k) {
            Ok(())
        } else {
            Err(self.err(msg))
        }
    }

    fn is_p(&self, p: Punct) -> bool {
        self.peek() == Tok::P(p)
    }

    fn eat_p(&mut self, p: Punct) -> bool {
        if self.is_p(p) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_p(&mut self, p: Punct, msg: &'static str) -> Res<()> {
        if self.eat_p(p) {
            Ok(())
        } else {
            Err(self.err(msg))
        }
    }

    fn word_at(&self, k: usize, w: &str) -> bool {
        self.toks
            .get(self.pos + k)
            .is_some_and(|t| t.kind == Tok::Ident && t.sp.text(self.src).eq_ignore_ascii_case(w))
    }

    fn is_word(&self, w: &str) -> bool {
        self.word_at(0, w)
    }

    fn eat_word(&mut self, w: &str) -> bool {
        if self.is_word(w) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_word(&mut self, w: &str, msg: &'static str) -> Res<()> {
        if self.eat_word(w) {
            Ok(())
        } else {
            Err(self.err(msg))
        }
    }

    fn is_name(&self) -> bool {
        matches!(self.peek(), Tok::Ident | Tok::QuotedIdent)
    }

    fn name(&mut self, msg: &'static str) -> Res<Name> {
        match self.peek() {
            Tok::Ident => Ok(Name { sp: self.bump().sp, quoted: false }),
            Tok::QuotedIdent => Ok(Name { sp: self.bump().sp, quoted: true }),
            _ => Err(self.err(msg)),
        }
    }

    fn enter(&mut self) -> Res<()> {
        self.depth += 1;
        let max = if self.mode == Mode::Strict { MAX_DEPTH_STRICT } else { MAX_DEPTH_FULL };
        if self.depth > max {
            return Err(ParseError { sp: self.sp(), msg: TOO_DEEP, incomplete: false });
        }
        Ok(())
    }

    fn leave(&mut self) {
        self.depth -= 1;
    }

    /// Run `f` speculatively. On failure, restore every piece of parser state
    /// and return `None` — unless the fuel is spent or the nesting limit was
    /// hit, in which case the error stands (bounded work on adversarial
    /// input, and no misleading error from another reading).
    fn attempt<T>(&mut self, f: impl FnOnce(&mut Self) -> Res<T>) -> Res<Option<T>> {
        let (pos, depth, positional, pending) =
            (self.pos, self.depth, self.positional, self.pending_gt);
        let (am, sm) = (self.ast.mark(), self.st.mark());
        match f(self) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.incomplete || e.msg == TOO_DEEP || e.msg == TOO_COMPLEX => Err(e),
            // Out of speculation budget: say so, rather than surface the
            // error of whichever reading happened to be tried last.
            Err(e) if self.fuel == 0 => Err(ParseError { msg: TOO_COMPLEX, ..e }),
            Err(_) => {
                self.fuel -= 1;
                self.pos = pos;
                self.depth = depth;
                self.positional = positional;
                self.pending_gt = pending;
                self.ast.restore(&am);
                self.st.restore(sm);
                Ok(None)
            }
        }
    }

    /// `(` directly followed by `SELECT`/`WITH`: unambiguously a query.
    fn paren_then_query(&self) -> bool {
        self.peek() == Tok::P(Punct::LParen)
            && matches!(self.peek_at(1), Tok::Kw(Kw::Select | Kw::With))
    }

    /// Parse `( query )` at the current `(`.
    fn paren_query(&mut self) -> Res<QueryId> {
        self.expect_p(Punct::LParen, "expected '('")?;
        let q = self.query()?;
        self.expect_p(Punct::RParen, "expected ')'")?;
        Ok(q)
    }

    fn expr_node(&mut self, e: Expr) -> Res<ExprId> {
        let sp = self.sp();
        push(&mut self.ast.exprs, e, sp)
    }

    /// Does the current token start a query (`SELECT`, `WITH`, or `(`
    /// followed by one)?
    fn starts_query(&self) -> bool {
        let mut k = 0;
        while self.peek_at(k) == Tok::P(Punct::LParen) {
            k += 1;
        }
        matches!(self.peek_at(k), Tok::Kw(Kw::Select | Kw::With))
    }

    /// `<>` written directly after `STRUCT` is an empty field list, not
    /// the not-equal operator.
    fn eat_empty_angles(&mut self) -> bool {
        if self.is_p(Punct::Ne) && self.sp().text(self.src) == "<>" {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_gt(&mut self) -> Res<()> {
        if self.pending_gt {
            self.pending_gt = false;
            return Ok(());
        }
        if self.eat_p(Punct::Gt) {
            return Ok(());
        }
        if self.is_p(Punct::Shr) {
            self.bump();
            self.pending_gt = true;
            return Ok(());
        }
        Err(self.err("expected '>'"))
    }

    // ---- queries ---------------------------------------------------------

    fn query(&mut self) -> Res<QueryId> {
        self.enter()?;
        let start = self.sp();
        let mut recursive = false;
        let mut ctes = Span::EMPTY;
        if self.eat_kw(Kw::With) {
            recursive = self.eat_kw(Kw::Recursive);
            let mark = self.st.ctes.len();
            loop {
                let name = self.name("expected CTE name")?;
                self.expect_kw(Kw::As, "expected AS")?;
                self.expect_p(Punct::LParen, "expected '('")?;
                let q = self.query()?;
                self.expect_p(Punct::RParen, "expected ')'")?;
                self.st.ctes.push(Cte { name, q });
                if !self.eat_p(Punct::Comma) {
                    break;
                }
            }
            ctes = finish(&mut self.st.ctes, mark, &mut self.ast.ctes, start)?;
        }
        let body = self.set_expr()?;
        let order = if self.eat_kw(Kw::Order) {
            self.expect_kw(Kw::By, "expected BY")?;
            self.order_list()?
        } else {
            Span::EMPTY
        };
        let (mut limit, mut offset) = (None, None);
        if self.eat_kw(Kw::Limit) {
            limit = Some(self.expr()?);
            if self.eat_word("OFFSET") {
                offset = Some(self.expr()?);
            }
        }
        let q = push(
            &mut self.ast.queries,
            Query { recursive, ctes, body, order, limit, offset },
            start,
        )?;
        self.leave();
        Ok(q)
    }

    fn set_expr(&mut self) -> Res<Idx<SetExpr>> {
        let mut left = self.set_term()?;
        loop {
            let op = match self.peek() {
                Tok::Kw(Kw::Union) => SetOp::Union,
                Tok::Kw(Kw::Intersect) => SetOp::Intersect,
                Tok::Kw(Kw::Except) => SetOp::Except,
                _ => break,
            };
            self.bump();
            let all = if self.eat_kw(Kw::All) {
                true
            } else if self.eat_kw(Kw::Distinct) {
                false
            } else {
                return Err(self.err("expected ALL or DISTINCT"));
            };
            let right = self.set_term()?;
            let sp = self.sp();
            left =
                push(&mut self.ast.set_exprs, SetExpr::SetOp { op, all, l: left, r: right }, sp)?;
        }
        Ok(left)
    }

    fn set_term(&mut self) -> Res<Idx<SetExpr>> {
        let sp = self.sp();
        if self.is_kw(Kw::Select) {
            let s = self.select()?;
            return push(&mut self.ast.set_exprs, SetExpr::Select(s), sp);
        }
        if self.eat_p(Punct::LParen) {
            let q = self.query()?;
            self.expect_p(Punct::RParen, "expected ')'")?;
            return push(&mut self.ast.set_exprs, SetExpr::Paren(q), sp);
        }
        if self.is_p(Punct::PipeGt) {
            return Err(self.err("pipe syntax is not supported"));
        }
        Err(self.err("expected SELECT"))
    }

    fn select(&mut self) -> Res<Idx<Select>> {
        let start = self.sp();
        self.expect_kw(Kw::Select, "expected SELECT")?;
        let distinct = if self.eat_kw(Kw::Distinct) {
            true
        } else {
            self.eat_kw(Kw::All);
            false
        };
        let as_ = if self.eat_kw(Kw::As) {
            if self.eat_kw(Kw::Struct) {
                SelectAs::Struct
            } else if self.eat_word("VALUE") {
                SelectAs::Value
            } else {
                return Err(self.err("expected STRUCT or VALUE"));
            }
        } else {
            SelectAs::None
        };
        let mark = self.st.items.len();
        loop {
            let item = self.select_item()?;
            self.st.items.push(item);
            if !self.eat_p(Punct::Comma) {
                break;
            }
            // GoogleSQL allows a trailing comma before FROM / end of list.
            if matches!(
                self.peek(),
                Tok::Kw(
                    Kw::From
                        | Kw::Where
                        | Kw::Group
                        | Kw::Order
                        | Kw::Limit
                        | Kw::Union
                        | Kw::Intersect
                        | Kw::Except
                        | Kw::Window
                        | Kw::Qualify
                        | Kw::Having
                ) | Tok::P(Punct::RParen | Punct::Semi)
                    | Tok::Eof
            ) {
                break;
            }
        }
        let items = finish(&mut self.st.items, mark, &mut self.ast.select_items, start)?;
        let from = if self.eat_kw(Kw::From) { Some(self.table_expr()?) } else { None };
        let where_ = if self.eat_kw(Kw::Where) { Some(self.expr()?) } else { None };
        let group = if self.eat_kw(Kw::Group) {
            self.expect_kw(Kw::By, "expected BY")?;
            if self.eat_kw(Kw::All) {
                GroupBy::All
            } else {
                GroupBy::Items(self.group_items()?)
            }
        } else {
            GroupBy::None
        };
        let having = if self.eat_kw(Kw::Having) { Some(self.expr()?) } else { None };
        let qualify = if self.eat_kw(Kw::Qualify) { Some(self.expr()?) } else { None };
        let windows = if self.eat_kw(Kw::Window) {
            let mark = self.st.named_windows.len();
            loop {
                let name = self.name("expected window name")?;
                self.expect_kw(Kw::As, "expected AS")?;
                let spec = self.window_spec_parens()?;
                self.st.named_windows.push(NamedWindow { name, spec });
                if !self.eat_p(Punct::Comma) {
                    break;
                }
            }
            finish(&mut self.st.named_windows, mark, &mut self.ast.named_windows, start)?
        } else {
            Span::EMPTY
        };
        push(
            &mut self.ast.selects,
            Select { distinct, as_, items, from, where_, group, having, qualify, windows },
            start,
        )
    }

    fn select_item(&mut self) -> Res<SelectItem> {
        if self.eat_p(Punct::Star) {
            return self.star_modifiers(None);
        }
        let e = self.expr()?;
        if self.is_p(Punct::Dot) && self.peek_at(1) == Tok::P(Punct::Star) {
            if !matches!(
                self.ast.exprs[e],
                Expr::Ident(_)
                    | Expr::Field { .. }
                    | Expr::Index { .. }
                    | Expr::Call(_)
                    | Expr::Paren(_)
            ) {
                return Err(self.err("'.*' needs a name or parenthesised expression"));
            }
            self.bump();
            self.bump();
            return self.star_modifiers(Some(e));
        }
        let alias = self.opt_alias()?;
        Ok(SelectItem::Expr { e, alias })
    }

    fn star_modifiers(&mut self, qualifier: Option<ExprId>) -> Res<SelectItem> {
        let sp = self.sp();
        let mut except = Span::EMPTY;
        let mut replace = Span::EMPTY;
        if self.eat_kw(Kw::Except) {
            self.expect_p(Punct::LParen, "expected '('")?;
            let mark = self.st.names.len();
            loop {
                let n = self.name("expected column name")?;
                self.st.names.push(n);
                if !self.eat_p(Punct::Comma) {
                    break;
                }
            }
            self.expect_p(Punct::RParen, "expected ')'")?;
            except = finish(&mut self.st.names, mark, &mut self.ast.names, sp)?;
        }
        if self.eat_word("REPLACE") {
            self.expect_p(Punct::LParen, "expected '('")?;
            let mark = self.st.fields.len();
            loop {
                let e = self.expr()?;
                self.expect_kw(Kw::As, "expected AS")?;
                let n = self.name("expected column name")?;
                self.st.fields.push(StructField { e, alias: Some(n) });
                if !self.eat_p(Punct::Comma) {
                    break;
                }
            }
            self.expect_p(Punct::RParen, "expected ')'")?;
            replace = finish(&mut self.st.fields, mark, &mut self.ast.struct_fields, sp)?;
        }
        Ok(SelectItem::Star { qualifier, except, replace })
    }

    fn opt_alias(&mut self) -> Res<Option<Name>> {
        if self.eat_kw(Kw::As) {
            return self.name("expected alias").map(Some);
        }
        Ok(if self.is_name() { Some(self.name("")?) } else { None })
    }

    fn group_items(&mut self) -> Res<Span<GroupItem>> {
        let sp = self.sp();
        let mark = self.st.groups.len();
        loop {
            let item = if self.eat_kw(Kw::Rollup) {
                GroupItem::Rollup(self.paren_expr_list()?)
            } else if self.eat_kw(Kw::Cube) {
                GroupItem::Cube(self.paren_expr_list()?)
            } else if self.is_kw(Kw::Grouping) && self.word_at(1, "SETS") {
                self.bump();
                self.bump();
                GroupItem::GroupingSets(self.grouping_sets()?)
            } else if self.is_p(Punct::LParen) && self.peek_at(1) == Tok::P(Punct::RParen) {
                self.bump();
                self.bump();
                GroupItem::Empty
            } else {
                GroupItem::Expr(self.expr()?)
            };
            self.st.groups.push(item);
            if !self.eat_p(Punct::Comma) {
                break;
            }
        }
        finish(&mut self.st.groups, mark, &mut self.ast.group_items, sp)
    }

    /// `( set, ... )` where a set is an expression, a tuple, or `()`.
    fn grouping_sets(&mut self) -> Res<Span<ExprId>> {
        self.expect_p(Punct::LParen, "expected '('")?;
        let sp = self.sp();
        let mark = self.st.exprs.len();
        loop {
            let e = if self.is_p(Punct::LParen) && self.peek_at(1) == Tok::P(Punct::RParen) {
                self.bump();
                self.bump();
                self.expr_node(Expr::Tuple(Span::EMPTY))?
            } else {
                self.expr()?
            };
            self.st.exprs.push(e);
            if !self.eat_p(Punct::Comma) {
                break;
            }
        }
        self.expect_p(Punct::RParen, "expected ')'")?;
        finish(&mut self.st.exprs, mark, &mut self.ast.expr_lists, sp)
    }

    // ---- FROM ------------------------------------------------------------

    fn table_expr(&mut self) -> Res<FromId> {
        let mut left = self.table_item()?;
        loop {
            let sp = self.sp();
            let kind = if self.eat_p(Punct::Comma) {
                JoinKind::Comma
            } else if self.eat_kw(Kw::Join) {
                JoinKind::Inner
            } else if self.eat_kw(Kw::Inner) {
                self.expect_kw(Kw::Join, "expected JOIN")?;
                JoinKind::Inner
            } else if self.eat_kw(Kw::Cross) {
                self.expect_kw(Kw::Join, "expected JOIN")?;
                JoinKind::Cross
            } else if let Some(k) = self.outer_join_kind() {
                self.eat_kw(Kw::Outer);
                self.expect_kw(Kw::Join, "expected JOIN")?;
                k
            } else {
                break;
            };
            let right = self.table_item()?;
            let cond = if matches!(kind, JoinKind::Comma | JoinKind::Cross) {
                JoinCond::None
            } else if self.eat_kw(Kw::On) {
                JoinCond::On(self.expr()?)
            } else if self.eat_kw(Kw::Using) {
                self.expect_p(Punct::LParen, "expected '('")?;
                let mark = self.st.names.len();
                loop {
                    let n = self.name("expected column name")?;
                    self.st.names.push(n);
                    if !self.eat_p(Punct::Comma) {
                        break;
                    }
                }
                self.expect_p(Punct::RParen, "expected ')'")?;
                JoinCond::Using(finish(&mut self.st.names, mark, &mut self.ast.names, sp)?)
            } else {
                JoinCond::None
            };
            left = push(&mut self.ast.froms, From::Join { kind, l: left, r: right, cond }, sp)?;
        }
        Ok(left)
    }

    fn outer_join_kind(&mut self) -> Option<JoinKind> {
        let k = match self.peek() {
            Tok::Kw(Kw::Left) => JoinKind::Left,
            Tok::Kw(Kw::Right) => JoinKind::Right,
            Tok::Kw(Kw::Full) => JoinKind::Full,
            _ => return None,
        };
        self.bump();
        Some(k)
    }

    fn table_item(&mut self) -> Res<FromId> {
        self.enter()?;
        let sp = self.sp();
        let item = if self.eat_kw(Kw::Unnest) {
            self.expect_p(Punct::LParen, "expected '('")?;
            let e = self.expr()?;
            self.expect_p(Punct::RParen, "expected ')'")?;
            let alias = self.opt_alias()?;
            let offset = self.with_offset()?;
            From::Unnest { e, alias, offset }
        } else if self.is_p(Punct::LParen) {
            // `(SELECT` is a subquery; `((SELECT` may also open a
            // parenthesised join whose first item is a subquery.
            let q = if self.paren_then_query() {
                Some(self.paren_query()?)
            } else if self.starts_query() {
                self.attempt(|p| {
                    let q = p.paren_query()?;
                    // A subquery item must be followed by what can follow a FROM item.
                    if p.is_p(Punct::Dot) {
                        return Err(p.err("not a subquery"));
                    }
                    Ok(q)
                })?
            } else {
                None
            };
            if let Some(q) = q {
                let alias = self.opt_alias()?;
                From::Subquery { q, alias }
            } else {
                self.bump();
                let inner = self.table_expr()?;
                self.expect_p(Punct::RParen, "expected ')'")?;
                From::Paren(inner)
            }
        } else {
            let path = self.table_path()?;
            if self.is_p(Punct::LParen) {
                return Err(self.err("table-valued functions are not supported"));
            }
            let mut time_travel = None;
            if self.is_kw(Kw::For) && self.word_at(1, "SYSTEM_TIME") {
                self.bump();
                self.bump();
                self.expect_kw(Kw::As, "expected AS")?;
                self.expect_kw(Kw::Of, "expected OF")?;
                time_travel = Some(self.expr()?);
            }
            let alias = self.opt_alias()?;
            let offset = self.with_offset()?;
            let mut sample = None;
            if self.eat_kw(Kw::Tablesample) {
                self.expect_word("SYSTEM", "expected SYSTEM")?;
                self.expect_p(Punct::LParen, "expected '('")?;
                sample = Some(self.expr()?);
                self.expect_word("PERCENT", "expected PERCENT")?;
                self.expect_p(Punct::RParen, "expected ')'")?;
            }
            From::Table { path, alias, time_travel, offset, sample }
        };
        let id = push(&mut self.ast.froms, item, sp)?;
        self.leave();
        Ok(id)
    }

    fn with_offset(&mut self) -> Res<Option<WithOffset>> {
        if self.is_kw(Kw::With) && self.word_at(1, "OFFSET") {
            self.bump();
            self.bump();
            return Ok(Some(WithOffset { alias: self.opt_alias()? }));
        }
        Ok(None)
    }

    /// `a.b.c`, with unquoted dashed project names (`my-proj.ds.t`) merged
    /// into their first segment.
    fn table_path(&mut self) -> Res<Span<Name>> {
        let sp = self.sp();
        let mark = self.st.names.len();
        let mut first = self.name("expected table name")?;
        if !first.quoted {
            while self.is_p(Punct::Minus) && self.sp().start == first.sp.end {
                let next = self.toks.get(self.pos + 1).copied();
                match next {
                    Some(t)
                        if matches!(t.kind, Tok::Ident | Tok::Int)
                            && t.sp.start == self.sp().end =>
                    {
                        self.bump();
                        self.bump();
                        first.sp.end = t.sp.end;
                    }
                    _ => break,
                }
            }
        }
        self.st.names.push(first);
        while self.is_p(Punct::Dot) && matches!(self.peek_at(1), Tok::Ident | Tok::QuotedIdent) {
            self.bump();
            let n = self.name("expected name")?;
            self.st.names.push(n);
        }
        finish(&mut self.st.names, mark, &mut self.ast.names, sp)
    }

    // ---- expressions -----------------------------------------------------

    fn expr(&mut self) -> Res<ExprId> {
        self.expr_bp(0)
    }

    fn paren_expr_list(&mut self) -> Res<Span<ExprId>> {
        self.expect_p(Punct::LParen, "expected '('")?;
        let sp = self.sp();
        let mark = self.st.exprs.len();
        if !self.is_p(Punct::RParen) {
            loop {
                let e = self.expr()?;
                self.st.exprs.push(e);
                if !self.eat_p(Punct::Comma) {
                    break;
                }
            }
        }
        self.expect_p(Punct::RParen, "expected ')'")?;
        finish(&mut self.st.exprs, mark, &mut self.ast.expr_lists, sp)
    }

    fn expr_list_until(&mut self, close: Punct) -> Res<Span<ExprId>> {
        let sp = self.sp();
        let mark = self.st.exprs.len();
        if !self.is_p(close) {
            loop {
                let e = self.expr()?;
                self.st.exprs.push(e);
                if !self.eat_p(Punct::Comma) {
                    break;
                }
            }
        }
        self.expect_p(
            close,
            if close == Punct::RBracket { "expected ']'" } else { "expected ')'" },
        )?;
        finish(&mut self.st.exprs, mark, &mut self.ast.expr_lists, sp)
    }

    #[allow(clippy::too_many_lines)] // one Pratt loop reads best as a whole
    fn expr_bp(&mut self, min: u8) -> Res<ExprId> {
        self.enter()?;
        let mut lhs = if self.is_kw(Kw::Not) {
            // NOT sits below comparison in GoogleSQL precedence: it may start
            // an operand of AND / OR / NOT, but not of a tighter operator
            // (`a = NOT b` must be written `a = (NOT b)`).
            if min > 3 {
                return Err(self.err("NOT must be parenthesised here"));
            }
            self.bump();
            let e = self.expr_bp(3)?;
            self.expr_node(Expr::Unary { op: UnOp::Not, e })?
        } else {
            self.unary()?
        };
        loop {
            let t = self.peek();
            // Comparison-level special forms (binding power 4).
            let not_prefixed = t == Tok::Kw(Kw::Not)
                && matches!(self.peek_at(1), Tok::Kw(Kw::Like | Kw::Between | Kw::In));
            if (not_prefixed || matches!(t, Tok::Kw(Kw::Like | Kw::Between | Kw::In | Kw::Is)))
                && min < 4
            {
                let not = if not_prefixed {
                    self.bump();
                    true
                } else {
                    false
                };
                lhs = match self.bump().kind {
                    Tok::Kw(Kw::Like) => {
                        let quant = match self.peek() {
                            Tok::Kw(Kw::Any) => Some(Quant::Any),
                            Tok::Kw(Kw::Some) => Some(Quant::Some),
                            Tok::Kw(Kw::All) => Some(Quant::All),
                            _ => None,
                        };
                        let pats = if quant.is_some() {
                            self.bump();
                            self.paren_expr_list()?
                        } else {
                            let p = self.expr_bp(4)?;
                            let sp = self.sp();
                            self.st.exprs.push(p);
                            let mark = self.st.exprs.len() - 1;
                            finish(&mut self.st.exprs, mark, &mut self.ast.expr_lists, sp)?
                        };
                        self.expr_node(Expr::Like { e: lhs, pats, quant, not })?
                    }
                    Tok::Kw(Kw::Between) => {
                        let lo = self.expr_bp(4)?;
                        self.expect_kw(Kw::And, "expected AND")?;
                        let hi = self.expr_bp(4)?;
                        self.expr_node(Expr::Between { e: lhs, lo, hi, not })?
                    }
                    Tok::Kw(Kw::In) => {
                        if self.eat_kw(Kw::Unnest) {
                            self.expect_p(Punct::LParen, "expected '('")?;
                            let arr = self.expr()?;
                            self.expect_p(Punct::RParen, "expected ')'")?;
                            self.expr_node(Expr::InUnnest { e: lhs, arr, not })?
                        } else {
                            if !self.is_p(Punct::LParen) {
                                return Err(self.err("expected '(' or UNNEST after IN"));
                            }
                            let q = if self.paren_then_query() {
                                Some(self.paren_query()?)
                            } else if self.starts_query_after_paren() {
                                self.attempt(Self::paren_query)?
                            } else {
                                None
                            };
                            if let Some(q) = q {
                                self.expr_node(Expr::InQuery { e: lhs, q, not })?
                            } else {
                                let list = self.paren_expr_list()?;
                                self.expr_node(Expr::InList { e: lhs, list, not })?
                            }
                        }
                    }
                    _ => {
                        // IS [NOT] ...
                        let not = self.eat_kw(Kw::Not);
                        if self.eat_kw(Kw::Distinct) {
                            self.expect_kw(Kw::From, "expected FROM")?;
                            let r = self.expr_bp(4)?;
                            self.expr_node(Expr::IsDistinct { l: lhs, r, not })?
                        } else {
                            let what = if self.eat_kw(Kw::Null) {
                                IsWhat::Null
                            } else if self.eat_kw(Kw::True) {
                                IsWhat::True
                            } else if self.eat_kw(Kw::False) {
                                IsWhat::False
                            } else if self.eat_word("UNKNOWN") {
                                IsWhat::Unknown
                            } else {
                                return Err(
                                    self.err("expected NULL, TRUE, FALSE, UNKNOWN or DISTINCT")
                                );
                            };
                            self.expr_node(Expr::Is { e: lhs, what, not })?
                        }
                    }
                };
                continue;
            }
            let op = match t {
                Tok::Kw(Kw::Or) => BinOp::Or,
                Tok::Kw(Kw::And) => BinOp::And,
                Tok::P(p) => match p {
                    Punct::Eq => BinOp::Eq,
                    Punct::Ne => BinOp::Ne,
                    Punct::Lt => BinOp::Lt,
                    Punct::Gt => BinOp::Gt,
                    Punct::Le => BinOp::Le,
                    Punct::Ge => BinOp::Ge,
                    Punct::Pipe => BinOp::BitOr,
                    Punct::Caret => BinOp::BitXor,
                    Punct::Amp => BinOp::BitAnd,
                    Punct::Shl => BinOp::Shl,
                    Punct::Shr => BinOp::Shr,
                    Punct::Plus => BinOp::Add,
                    Punct::Minus => BinOp::Sub,
                    Punct::Star => BinOp::Mul,
                    Punct::Slash => BinOp::Div,
                    Punct::Concat => BinOp::Concat,
                    _ => break,
                },
                _ => break,
            };
            let bp = op.precedence();
            if bp <= min {
                break;
            }
            self.bump();
            let r = self.expr_bp(bp)?;
            lhs = self.expr_node(Expr::Binary { op, l: lhs, r })?;
        }
        self.leave();
        Ok(lhs)
    }

    fn starts_query_after_paren(&self) -> bool {
        self.peek() == Tok::P(Punct::LParen) && {
            let mut k = 1;
            while self.peek_at(k) == Tok::P(Punct::LParen) {
                k += 1;
            }
            matches!(self.peek_at(k), Tok::Kw(Kw::Select | Kw::With))
        }
    }

    fn unary(&mut self) -> Res<ExprId> {
        let op = match self.peek() {
            Tok::P(Punct::Minus) => UnOp::Neg,
            Tok::P(Punct::Plus) => UnOp::Plus,
            Tok::P(Punct::Tilde) => UnOp::BitNot,
            _ => {
                let p = self.primary()?;
                return self.postfix(p);
            }
        };
        self.enter()?;
        self.bump();
        let e = self.unary()?;
        self.leave();
        self.expr_node(Expr::Unary { op, e })
    }

    fn postfix(&mut self, mut e: ExprId) -> Res<ExprId> {
        loop {
            if self.is_p(Punct::Dot) && matches!(self.peek_at(1), Tok::Ident | Tok::QuotedIdent) {
                if matches!(
                    self.ast.exprs[e],
                    Expr::Lit { kind: LitKind::Int | LitKind::Float, .. }
                ) {
                    return Err(self.err("a number has no fields"));
                }
                self.bump();
                let field = self.name("expected field name")?;
                e = self.expr_node(Expr::Field { base: e, field })?;
            } else if self.eat_p(Punct::LBracket) {
                let mode = [
                    ("OFFSET", IndexMode::Offset),
                    ("SAFE_OFFSET", IndexMode::SafeOffset),
                    ("ORDINAL", IndexMode::Ordinal),
                    ("SAFE_ORDINAL", IndexMode::SafeOrdinal),
                ]
                .into_iter()
                .find(|(w, _)| self.is_word(w) && self.peek_at(1) == Tok::P(Punct::LParen))
                .map(|(_, m)| m);
                let idx = if let Some(_m) = mode {
                    self.bump();
                    self.bump();
                    let i = self.expr()?;
                    self.expect_p(Punct::RParen, "expected ')'")?;
                    i
                } else {
                    self.expr()?
                };
                self.expect_p(Punct::RBracket, "expected ']'")?;
                e =
                    self.expr_node(Expr::Index { e, idx, mode: mode.unwrap_or(IndexMode::Plain) })?;
            } else {
                return Ok(e);
            }
        }
    }

    #[allow(clippy::too_many_lines)] // the primary-expression table
    fn primary(&mut self) -> Res<ExprId> {
        let t = self.tok();
        match t.kind {
            Tok::Int | Tok::Float | Tok::Str | Tok::Bytes => {
                self.bump();
                let kind = match t.kind {
                    Tok::Int => LitKind::Int,
                    Tok::Float => LitKind::Float,
                    Tok::Str => LitKind::Str,
                    _ => LitKind::Bytes,
                };
                self.expr_node(Expr::Lit { kind, sp: t.sp })
            }
            Tok::Kw(Kw::True | Kw::False) => {
                self.bump();
                self.expr_node(Expr::Lit {
                    kind: LitKind::Bool(t.kind == Tok::Kw(Kw::True)),
                    sp: t.sp,
                })
            }
            Tok::Kw(Kw::Null) => {
                self.bump();
                self.expr_node(Expr::Lit { kind: LitKind::Null, sp: t.sp })
            }
            Tok::Param | Tok::SysVar | Tok::Positional => {
                if self.mode == Mode::Strict {
                    return Err(self.err("parameters are not allowed here"));
                }
                self.bump();
                let e = match t.kind {
                    Tok::Param => Expr::Param(t.sp),
                    Tok::SysVar => Expr::SysVar(t.sp),
                    _ => {
                        self.positional += 1;
                        Expr::Positional(self.positional - 1)
                    }
                };
                self.expr_node(e)
            }
            Tok::Kw(Kw::Cast) => {
                self.bump();
                self.cast(false)
            }
            Tok::Ident if self.is_word("SAFE_CAST") && self.peek_at(1) == Tok::P(Punct::LParen) => {
                self.bump();
                self.cast(true)
            }
            Tok::Kw(Kw::Extract) => {
                self.bump();
                self.expect_p(Punct::LParen, "expected '('")?;
                let part = self.unary()?;
                self.expect_kw(Kw::From, "expected FROM")?;
                let e = self.expr()?;
                let tz = if self.is_kw(Kw::At) {
                    self.bump();
                    self.expect_word("TIME", "expected TIME")?;
                    self.expect_word("ZONE", "expected ZONE")?;
                    Some(self.expr()?)
                } else {
                    None
                };
                self.expect_p(Punct::RParen, "expected ')'")?;
                self.expr_node(Expr::Extract { part, e, tz })
            }
            Tok::Kw(Kw::Case) => {
                self.bump();
                self.case()
            }
            Tok::Kw(Kw::Array) => {
                self.bump();
                if self.is_p(Punct::LParen) {
                    self.bump();
                    let q = self.query()?;
                    self.expect_p(Punct::RParen, "expected ')'")?;
                    return self.expr_node(Expr::ArrayQuery(q));
                }
                let ty = if self.eat_p(Punct::Lt) {
                    let inner = self.type_()?;
                    self.expect_gt()?;
                    Some(push(&mut self.ast.types, TypeNode::Array(inner), t.sp)?)
                } else {
                    None
                };
                self.expect_p(Punct::LBracket, "expected '['")?;
                let elems = self.expr_list_until(Punct::RBracket)?;
                self.expr_node(Expr::Array { ty, elems })
            }
            Tok::P(Punct::LBracket) => {
                self.bump();
                let elems = self.expr_list_until(Punct::RBracket)?;
                self.expr_node(Expr::Array { ty: None, elems })
            }
            Tok::Kw(Kw::Struct) => {
                self.bump();
                let ty = if self.eat_empty_angles() {
                    Some(push(&mut self.ast.types, TypeNode::Struct(Span::EMPTY), t.sp)?)
                } else if self.eat_p(Punct::Lt) {
                    let fields = self.struct_type_fields()?;
                    Some(push(&mut self.ast.types, TypeNode::Struct(fields), t.sp)?)
                } else {
                    None
                };
                self.expect_p(Punct::LParen, "expected '('")?;
                let mark = self.st.fields.len();
                if !self.is_p(Punct::RParen) {
                    loop {
                        let e = self.expr()?;
                        let alias = if self.eat_kw(Kw::As) {
                            Some(self.name("expected field name")?)
                        } else {
                            None
                        };
                        self.st.fields.push(StructField { e, alias });
                        if !self.eat_p(Punct::Comma) {
                            break;
                        }
                    }
                }
                self.expect_p(Punct::RParen, "expected ')'")?;
                let fields = finish(&mut self.st.fields, mark, &mut self.ast.struct_fields, t.sp)?;
                self.expr_node(Expr::Struct { ty, fields })
            }
            Tok::Kw(Kw::Exists) => {
                self.bump();
                self.expect_p(Punct::LParen, "expected '('")?;
                let q = self.query()?;
                self.expect_p(Punct::RParen, "expected ')'")?;
                self.expr_node(Expr::Exists(q))
            }
            Tok::Kw(Kw::Interval) => {
                self.bump();
                let e = self.unary()?;
                let unit = self.name("expected interval unit")?;
                let to = if self.eat_kw(Kw::To) {
                    Some(self.name("expected interval unit")?)
                } else {
                    None
                };
                self.expr_node(Expr::Interval { e, unit, to })
            }
            Tok::P(Punct::LParen) => {
                let q = if self.paren_then_query() {
                    Some(self.paren_query()?)
                } else if self.starts_query_after_paren() {
                    // `((SELECT 1) IN (r))` opens an expression, not a query.
                    self.attempt(Self::paren_query)?
                } else {
                    None
                };
                if let Some(q) = q {
                    return self.expr_node(Expr::Subquery(q));
                }
                self.bump();
                let first = self.expr()?;
                if self.eat_p(Punct::Comma) {
                    let mark = self.st.exprs.len();
                    self.st.exprs.push(first);
                    loop {
                        let e = self.expr()?;
                        self.st.exprs.push(e);
                        if !self.eat_p(Punct::Comma) {
                            break;
                        }
                    }
                    self.expect_p(Punct::RParen, "expected ')'")?;
                    let items = finish(&mut self.st.exprs, mark, &mut self.ast.expr_lists, t.sp)?;
                    return self.expr_node(Expr::Tuple(items));
                }
                self.expect_p(Punct::RParen, "expected ')'")?;
                self.expr_node(Expr::Paren(first))
            }
            // Reserved words that are also function names.
            Tok::Kw(
                Kw::If
                | Kw::Left
                | Kw::Right
                | Kw::Grouping
                | Kw::Range
                | Kw::Contains
                | Kw::Collate,
            ) if self.peek_at(1) == Tok::P(Punct::LParen) => {
                self.bump();
                let sp = t.sp;
                self.st.names.push(Name { sp, quoted: false });
                let mark = self.st.names.len() - 1;
                let name = finish(&mut self.st.names, mark, &mut self.ast.names, sp)?;
                self.call(name)
            }
            Tok::Ident | Tok::QuotedIdent => {
                // Typed literal: DATE '...', TIMESTAMP '...', JSON '...', ...
                if t.kind == Tok::Ident && self.peek_at(1) == Tok::Str {
                    const TYPED: [&str; 9] = [
                        "DATE",
                        "DATETIME",
                        "TIME",
                        "TIMESTAMP",
                        "NUMERIC",
                        "BIGNUMERIC",
                        "DECIMAL",
                        "BIGDECIMAL",
                        "JSON",
                    ];
                    let w = t.sp.text(self.src);
                    if TYPED.iter().any(|k| k.eq_ignore_ascii_case(w)) {
                        self.bump();
                        let s = self.bump().sp;
                        return self.expr_node(Expr::Lit { kind: LitKind::Typed(s), sp: t.sp });
                    }
                }
                // Dotted function name: a.b.c(...)
                let mut k = 0;
                while matches!(self.peek_at(k), Tok::Ident | Tok::QuotedIdent)
                    && self.peek_at(k + 1) == Tok::P(Punct::Dot)
                    && matches!(self.peek_at(k + 2), Tok::Ident | Tok::QuotedIdent)
                {
                    k += 2;
                }
                if self.peek_at(k + 1) == Tok::P(Punct::LParen) {
                    let mark = self.st.names.len();
                    loop {
                        let n = self.name("expected name")?;
                        self.st.names.push(n);
                        if !self.eat_p(Punct::Dot) {
                            break;
                        }
                    }
                    let name = finish(&mut self.st.names, mark, &mut self.ast.names, t.sp)?;
                    return self.call(name);
                }
                let n = self.name("expected name")?;
                self.expr_node(Expr::Ident(n))
            }
            _ => Err(self.err("expected expression")),
        }
    }

    fn cast(&mut self, safe: bool) -> Res<ExprId> {
        self.expect_p(Punct::LParen, "expected '('")?;
        let e = self.expr()?;
        self.expect_kw(Kw::As, "expected AS")?;
        let ty = self.type_()?;
        let format = if self.eat_word("FORMAT") { Some(self.expr()?) } else { None };
        self.expect_p(Punct::RParen, "expected ')'")?;
        self.expr_node(Expr::Cast { e, ty, safe, format })
    }

    fn case(&mut self) -> Res<ExprId> {
        let sp = self.sp();
        let operand = if self.is_kw(Kw::When) { None } else { Some(self.expr()?) };
        let mark = self.st.arms.len();
        while self.eat_kw(Kw::When) {
            let when = self.expr()?;
            self.expect_kw(Kw::Then, "expected THEN")?;
            let then = self.expr()?;
            self.st.arms.push(WhenArm { when, then });
        }
        if self.st.arms.len() == mark {
            return Err(self.err("expected WHEN"));
        }
        let else_ = if self.eat_kw(Kw::Else) { Some(self.expr()?) } else { None };
        self.expect_kw(Kw::End, "expected END")?;
        let arms = finish(&mut self.st.arms, mark, &mut self.ast.arms, sp)?;
        self.expr_node(Expr::Case { operand, arms, else_ })
    }

    fn call(&mut self, name: Span<Name>) -> Res<ExprId> {
        let sp = self.sp();
        self.expect_p(Punct::LParen, "expected '('")?;
        let distinct = self.eat_kw(Kw::Distinct);
        let mark = self.st.args.len();
        if !self.is_p(Punct::RParen) {
            loop {
                if self.is_p(Punct::Star) && self.peek_at(1) == Tok::P(Punct::RParen) {
                    self.bump();
                    let e = self.expr_node(Expr::Star)?;
                    self.st.args.push(Arg { name: None, e });
                    break;
                }
                let arg_name = if self.is_name() && self.peek_at(1) == Tok::P(Punct::Arrow) {
                    let n = self.name("")?;
                    self.bump();
                    Some(n)
                } else {
                    None
                };
                let e = self.expr()?;
                self.st.args.push(Arg { name: arg_name, e });
                if !self.eat_p(Punct::Comma) {
                    break;
                }
            }
        }
        let args = finish(&mut self.st.args, mark, &mut self.ast.args, sp)?;
        let nulls = if self.eat_kw(Kw::Ignore) {
            self.expect_kw(Kw::Nulls, "expected NULLS")?;
            Some(NullTreatment::Ignore)
        } else if self.eat_kw(Kw::Respect) {
            self.expect_kw(Kw::Nulls, "expected NULLS")?;
            Some(NullTreatment::Respect)
        } else {
            None
        };
        let order = if self.eat_kw(Kw::Order) {
            self.expect_kw(Kw::By, "expected BY")?;
            self.order_list()?
        } else {
            Span::EMPTY
        };
        let limit = if self.eat_kw(Kw::Limit) { Some(self.expr()?) } else { None };
        self.expect_p(Punct::RParen, "expected ')'")?;
        let over = if self.eat_kw(Kw::Over) {
            if self.is_p(Punct::LParen) {
                Some(self.window_spec_parens()?)
            } else {
                let base = self.name("expected window name or '('")?;
                let w = WindowSpec {
                    base: Some(base),
                    partition: Span::EMPTY,
                    order: Span::EMPTY,
                    frame: None,
                    bare: true,
                };
                Some(push(&mut self.ast.windows, w, sp)?)
            }
        } else {
            None
        };
        let call = push(
            &mut self.ast.calls,
            Call { name, args, distinct, nulls, order, limit, over },
            sp,
        )?;
        self.expr_node(Expr::Call(call))
    }

    fn order_list(&mut self) -> Res<Span<OrderItem>> {
        let sp = self.sp();
        let mark = self.st.order.len();
        loop {
            let e = self.expr()?;
            let desc = if self.eat_kw(Kw::Desc) {
                Some(true)
            } else if self.eat_kw(Kw::Asc) {
                Some(false)
            } else {
                None
            };
            let nulls_first = if self.eat_kw(Kw::Nulls) {
                if self.eat_word("FIRST") {
                    Some(true)
                } else if self.eat_word("LAST") {
                    Some(false)
                } else {
                    return Err(self.err("expected FIRST or LAST"));
                }
            } else {
                None
            };
            self.st.order.push(OrderItem { e, desc, nulls_first });
            if !self.eat_p(Punct::Comma) {
                break;
            }
        }
        finish(&mut self.st.order, mark, &mut self.ast.order_items, sp)
    }

    fn window_spec_parens(&mut self) -> Res<Idx<WindowSpec>> {
        let sp = self.sp();
        self.expect_p(Punct::LParen, "expected '('")?;
        let base = if self.is_name() { Some(self.name("")?) } else { None };
        let partition = if self.eat_kw(Kw::Partition) {
            self.expect_kw(Kw::By, "expected BY")?;
            let mark = self.st.exprs.len();
            loop {
                let e = self.expr()?;
                self.st.exprs.push(e);
                if !self.eat_p(Punct::Comma) {
                    break;
                }
            }
            finish(&mut self.st.exprs, mark, &mut self.ast.expr_lists, sp)?
        } else {
            Span::EMPTY
        };
        let order = if self.eat_kw(Kw::Order) {
            self.expect_kw(Kw::By, "expected BY")?;
            self.order_list()?
        } else {
            Span::EMPTY
        };
        let frame = match self.peek() {
            Tok::Kw(Kw::Rows | Kw::Range) => {
                let unit = if self.bump().kind == Tok::Kw(Kw::Rows) {
                    FrameUnit::Rows
                } else {
                    FrameUnit::Range
                };
                if self.eat_kw(Kw::Between) {
                    let start = self.bound()?;
                    self.expect_kw(Kw::And, "expected AND")?;
                    let end = self.bound()?;
                    Some(Frame { unit, start, end: Some(end) })
                } else {
                    Some(Frame { unit, start: self.bound()?, end: None })
                }
            }
            _ => None,
        };
        self.expect_p(Punct::RParen, "expected ')'")?;
        push(&mut self.ast.windows, WindowSpec { base, partition, order, frame, bare: false }, sp)
    }

    fn bound(&mut self) -> Res<Bound> {
        if self.eat_kw(Kw::Unbounded) {
            if self.eat_kw(Kw::Preceding) {
                return Ok(Bound::UnboundedPreceding);
            }
            self.expect_kw(Kw::Following, "expected PRECEDING or FOLLOWING")?;
            return Ok(Bound::UnboundedFollowing);
        }
        if self.eat_kw(Kw::Current) {
            self.expect_word("ROW", "expected ROW")?;
            return Ok(Bound::CurrentRow);
        }
        let e = self.expr_bp(4)?;
        if self.eat_kw(Kw::Preceding) {
            Ok(Bound::Preceding(e))
        } else {
            self.expect_kw(Kw::Following, "expected PRECEDING or FOLLOWING")?;
            Ok(Bound::Following(e))
        }
    }

    // ---- types -----------------------------------------------------------

    fn type_(&mut self) -> Res<TypeId> {
        self.enter()?;
        let sp = self.sp();
        let node = if self.eat_kw(Kw::Array) {
            self.expect_p(Punct::Lt, "expected '<'")?;
            let inner = self.type_()?;
            self.expect_gt()?;
            TypeNode::Array(inner)
        } else if self.eat_kw(Kw::Struct) {
            if self.eat_empty_angles() {
                TypeNode::Struct(Span::EMPTY)
            } else {
                self.expect_p(Punct::Lt, "expected '<'")?;
                TypeNode::Struct(self.struct_type_fields()?)
            }
        } else if self.eat_kw(Kw::Range) {
            self.expect_p(Punct::Lt, "expected '<'")?;
            let inner = self.type_()?;
            self.expect_gt()?;
            TypeNode::Range(inner)
        } else {
            let name = self.name("expected type")?;
            let params =
                if self.is_p(Punct::LParen) { self.paren_expr_list()? } else { Span::EMPTY };
            TypeNode::Named { name, params }
        };
        let id = push(&mut self.ast.types, node, sp)?;
        self.leave();
        Ok(id)
    }

    /// Fields of `STRUCT<...>` after the `<`, through the closing `>`.
    fn struct_type_fields(&mut self) -> Res<Span<StructFieldType>> {
        let sp = self.sp();
        let mark = self.st.type_fields.len();
        if !(self.pending_gt || self.is_p(Punct::Gt) || self.is_p(Punct::Shr)) {
            loop {
                // `name TYPE` when a name is followed by something that starts a type.
                let named = self.is_name()
                    && matches!(
                        self.peek_at(1),
                        Tok::Ident | Tok::QuotedIdent | Tok::Kw(Kw::Array | Kw::Struct | Kw::Range)
                    );
                let name = if named { Some(self.name("")?) } else { None };
                let ty = self.type_()?;
                self.st.type_fields.push(StructFieldType { name, ty });
                if !self.eat_p(Punct::Comma) {
                    break;
                }
            }
        }
        self.expect_gt()?;
        finish(&mut self.st.type_fields, mark, &mut self.ast.type_fields, sp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(sql: &str) {
        let mut p = Parser::new();
        if let Err(e) = p.parse(sql, Mode::Full) {
            panic!("{sql}\n{e} near {:?}", &sql[e.sp.start as usize..]);
        }
    }

    fn bad(sql: &str) -> ParseError {
        let mut p = Parser::new();
        p.parse(sql, Mode::Full).map(|_| ()).expect_err(sql)
    }

    #[test]
    fn googlesql_coverage() {
        for sql in [
            "SELECT 1",
            "SELECT a, b AS c, t.* EXCEPT (x) REPLACE (y + 1 AS y), * FROM `p.d.t` AS t",
            "SELECT DISTINCT o.status FROM my-project.shop.orders o WHERE o.ts >= TIMESTAMP '2026-01-01' AND o.x IS NOT NULL",
            "WITH a AS (SELECT 1 AS x), b AS (SELECT x FROM a) SELECT * FROM b ORDER BY x DESC NULLS LAST LIMIT 10 OFFSET 5",
            "SELECT c.id, COUNT(DISTINCT o.id) n FROM c LEFT OUTER JOIN o ON o.cid = c.id GROUP BY 1 HAVING n > 2",
            "SELECT * FROM a FULL JOIN b USING (k) CROSS JOIN c, UNNEST(c.arr) AS e WITH OFFSET AS i",
            "SELECT x FROM t, t.items AS it WITH OFFSET",
            "SELECT ARRAY_AGG(x IGNORE NULLS ORDER BY y DESC LIMIT 5)[SAFE_OFFSET(0)].f FROM t",
            "SELECT SUM(v) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
            "SELECT RANK() OVER w FROM t WINDOW w AS (PARTITION BY a ORDER BY b)",
            "SELECT x FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) = 1",
            "SELECT CASE WHEN a THEN 1 WHEN b THEN 2 ELSE 3 END, CASE s WHEN 'x' THEN 1 END FROM t",
            "SELECT CAST(a AS NUMERIC(10, 2)), SAFE_CAST(b AS ARRAY<STRUCT<x INT64, y ARRAY<STRING>>>) FROM t",
            "SELECT EXTRACT(DAYOFWEEK FROM ts AT TIME ZONE 'America/Toronto'), EXTRACT(WEEK(MONDAY) FROM d) FROM t",
            "SELECT DATE_TRUNC(d, MONTH), DATE_ADD(d, INTERVAL -1 DAY), INTERVAL '1-2' YEAR TO MONTH FROM t",
            "SELECT STRUCT(1 AS a, 'x' AS b), STRUCT<a INT64>(1), [1, 2], ARRAY<INT64>[1], ARRAY(SELECT 1), (1, 2)",
            "SELECT a FROM t WHERE a IN (1, 2) AND b NOT IN (SELECT b FROM u) AND c IN UNNEST(@arr) AND d NOT LIKE 'x%'",
            "SELECT a FROM t WHERE s LIKE ANY ('a%', 'b%') AND x NOT BETWEEN 1 AND 2 AND y IS DISTINCT FROM z",
            "SELECT EXISTS(SELECT 1 FROM u WHERE u.k = t.k), (SELECT MAX(v) FROM u) FROM t",
            "SELECT SAFE.DIVIDE(a, b), NET.HOST(url), IF(a, 1, 2), LEFT(s, 3), `my.fn`(x) FROM t",
            "SELECT 1 UNION ALL SELECT 2 EXCEPT DISTINCT (SELECT 3 INTERSECT DISTINCT SELECT 3)",
            "SELECT AS STRUCT 1 AS a",
            "SELECT a, b, FROM t",
            "SELECT * FROM `p.d.t` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) TABLESAMPLE SYSTEM (10 PERCENT)",
            "SELECT * FROM (SELECT 1) AS s JOIN ((SELECT 2)) USING (x)",
            "SELECT * FROM (a JOIN b ON TRUE) LEFT JOIN c ON c.k = a.k",
            "SELECT ((SELECT 1) IN (r)) << b, ((SELECT 1)), ((SELECT 1) + 1) FROM t",
            "SELECT * FROM ((SELECT 1 AS x) JOIN t ON TRUE), ((SELECT 2)) AS s",
            "SELECT a FROM t WHERE a IN ((SELECT 1), 2) AND b IN ((SELECT 1) UNION ALL (SELECT 2))",
            "SELECT x FROM t GROUP BY ROLLUP (a, b), CUBE (c), GROUPING SETS ((a, b), c, ()), ()",
            "SELECT x FROM t GROUP BY ALL",
            "SELECT JSON_VALUE(j, '$.a'), j['k'], APPROX_QUANTILES(x, 100)[OFFSET(50)] FROM t",
            "SELECT FORMAT_DATE('%Y', d), STRING_AGG(DISTINCT s, ',' ORDER BY s), COUNT(*) FROM t;",
            "SELECT -a * +b - ~c || 'x', a << 2 & b | c ^ d, NOT a = b, a AND NOT NOT b, a = (NOT b) FROM t",
            "SELECT r'\\d+', b'\\x00', '''multi\nline''', 0x1F, 1.5e3, .5 FROM t",
            "SELECT fn(a => 1, b => 'x') FROM t",
            "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
        ] {
            ok(sql);
        }
    }

    #[test]
    fn precedence() {
        let mut p = Parser::new();
        let parsed = p.parse("SELECT a OR b AND NOT c = d + e * f", Mode::Full).unwrap();
        let ast = parsed.ast;
        let sel = match ast.set_exprs[ast.queries[parsed.root].body] {
            SetExpr::Select(s) => ast.selects[s],
            _ => unreachable!(),
        };
        let SelectItem::Expr { e, .. } = ast.select_items.slice(sel.items)[0] else {
            unreachable!()
        };
        // OR at the root, AND on its right, NOT binds looser than '='.
        let Expr::Binary { op: BinOp::Or, r, .. } = ast.exprs[e] else {
            panic!("{:?}", ast.exprs[e])
        };
        let Expr::Binary { op: BinOp::And, r, .. } = ast.exprs[r] else { panic!() };
        let Expr::Unary { op: UnOp::Not, e } = ast.exprs[r] else { panic!() };
        let Expr::Binary { op: BinOp::Eq, r, .. } = ast.exprs[e] else { panic!() };
        let Expr::Binary { op: BinOp::Add, r, .. } = ast.exprs[r] else { panic!() };
        assert!(matches!(ast.exprs[r], Expr::Binary { op: BinOp::Mul, .. }));
    }

    #[test]
    fn nested_generic_close_splits_shift() {
        ok("SELECT CAST(x AS ARRAY<ARRAY<INT64>>) FROM t");
        ok("SELECT CAST(x AS STRUCT<a ARRAY<STRUCT<b INT64>>>) FROM t");
        ok("SELECT STRUCT<>(), STRUCT< >(), CAST(NULL AS STRUCT<>)");
    }

    #[test]
    fn errors_are_located() {
        let e = bad("SELECT a FROM t WHERE");
        assert!(e.incomplete, "{e:?}");
        let e = bad("SELECT a FROM t WHERE )");
        assert!(!e.incomplete);
        assert_eq!(e.msg, "expected expression");
        assert!(!bad("DELETE FROM t").incomplete, "only queries");
        assert!(!bad("SELECT 1 UNION SELECT 2").incomplete, "ALL/DISTINCT required");
        assert!(bad("SELECT (").incomplete);
        assert_eq!(bad("SELECT a = NOT b").msg, "NOT must be parenthesised here");
        assert!(!bad("SELECT a + b.* FROM t").incomplete);
        assert!(!bad("SELECT 1 .* FROM t").incomplete);
    }

    #[test]
    fn strict_mode_rejects_parameters() {
        let mut p = Parser::new();
        assert!(p.parse("SELECT @x", Mode::Strict).is_err());
        assert!(p.parse("SELECT @x", Mode::Full).is_ok());
    }

    #[test]
    fn full_depth_is_stack_safe() {
        // Just under the cap, on a default-size thread, in whatever build
        // profile runs the tests.
        let n = (MAX_DEPTH_FULL / 2 - 2) as usize;
        let sql = format!("SELECT {}1{}", "(".repeat(n), ")".repeat(n));
        std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(move || Parser::new().parse(&sql, Mode::Full).map(|_| ()))
            .unwrap()
            .join()
            .expect("no stack overflow")
            .expect("parses");
    }

    #[test]
    fn depth_is_bounded() {
        let deep = format!("SELECT {}1{}", "(".repeat(5000), ")".repeat(5000));
        let e = bad(&deep);
        assert_eq!(e.msg, "nesting too deep");
        let deep_neg = format!("SELECT {}1", "- ".repeat(5000));
        assert_eq!(bad(&deep_neg).msg, "nesting too deep");
    }

    #[test]
    fn viability_of_prefixes() {
        let mut p = Parser::new();
        let full = "SELECT o.status, COUNT(*) AS n FROM `p.d.orders` AS o WHERE o.ts >= DATE '2026-04-01' GROUP BY o.status";
        for cut in 1..full.len() {
            if !full.is_char_boundary(cut) {
                continue;
            }
            let v = viability(&mut p, &full[..cut], Mode::Strict);
            assert!(
                !matches!(v, Viability::Invalid(_)),
                "prefix {:?} judged invalid: {v:?}",
                &full[..cut]
            );
        }
        assert_eq!(viability(&mut p, full, Mode::Strict), Viability::Complete);
        assert!(matches!(viability(&mut p, "DELETE FROM t", Mode::Strict), Viability::Invalid(_)));
        assert!(matches!(
            viability(&mut p, "SELECT a FROM t WHERE )", Mode::Strict),
            Viability::Invalid(_)
        ));
        assert!(matches!(
            viability(&mut p, "SELECT a FROM t t2 t3 ", Mode::Strict),
            Viability::Invalid(_)
        ));
    }

    #[test]
    fn parser_reuse_keeps_capacity() {
        let mut p = Parser::new();
        p.parse("SELECT a, b, c, d FROM t WHERE a = 1 AND b = 2", Mode::Full).unwrap();
        let cap = p.ast.exprs.len();
        p.parse("SELECT 1", Mode::Full).unwrap();
        assert!(p.ast.exprs.len() < cap);
    }
}

#[cfg(test)]
#[allow(clippy::many_single_char_names)]
mod statement_tests {
    use super::*;

    fn script(sql: &str) -> (Vec<StmtKind>, Vec<String>) {
        let mut p = Parser::new();
        let s = p.parse_script(sql, Mode::Full).unwrap_or_else(|e| panic!("{sql}\n{e:?}"));
        (
            s.stmts.iter().map(|x| x.kind).collect(),
            s.stmts.iter().map(|x| x.target.map(|t| s.path(t)).unwrap_or_default()).collect(),
        )
    }

    #[test]
    fn ctas_views_and_options() {
        let (k, t) = script(
            "CREATE OR REPLACE TABLE `p.mart.customer_month` PARTITION BY month CLUSTER BY cust_id OPTIONS(description='x') AS
             WITH o AS (SELECT customer_id, DATE_TRUNC(order_ts, MONTH) AS month FROM `p.raw.orders`)
             SELECT customer_id AS cust_id, month FROM o",
        );
        assert_eq!((k, t), (vec![StmtKind::CreateTable], vec!["p.mart.customer_month".to_owned()]));
        let mut p = Parser::new();
        let s = p
            .parse_script(
                "CREATE VIEW IF NOT EXISTS ds.v (a OPTIONS(description='A'), b) AS SELECT 1, 2",
                Mode::Full,
            )
            .unwrap();
        let v = s.stmts[0];
        assert_eq!(v.kind, StmtKind::CreateView);
        assert_eq!(s.ast.names.slice(v.columns).len(), 2);
        assert!(s.query(&v).is_some());
        let (k, _) = script("CREATE MATERIALIZED VIEW p.d.mv AS SELECT 1 AS x; CREATE TEMP TABLE t (a INT64 NOT NULL, b STRUCT<c STRING>)");
        assert_eq!(k, vec![StmtKind::CreateMaterializedView, StmtKind::CreateTable]);
    }

    #[test]
    fn insert_forms() {
        let mut p = Parser::new();
        let s = p.parse_script("INSERT INTO p.d.t (a, b) SELECT x, y FROM p.d.s; INSERT p.d.t (SELECT 1, 2); INSERT p.d.t (a) VALUES (1), (2)", Mode::Full).unwrap();
        assert_eq!(s.stmts.iter().map(|x| x.kind).collect::<Vec<_>>(), vec![StmtKind::Insert; 3]);
        assert_eq!(s.ast.names.slice(s.stmts[0].columns).len(), 2);
        assert!(
            s.stmts[0].query.is_some()
                && s.stmts[1].query.is_some()
                && s.stmts[1].columns.is_empty()
        );
        assert!(s.stmts[2].query.is_none(), "VALUES carries no query");
    }

    #[test]
    fn merge_with_every_clause_form() {
        let mut p = Parser::new();
        let sql = "MERGE `p.mart.dim_customer` t
            USING (SELECT id AS customer_id, UPPER(email) AS email FROM `p.raw.crm_a`
                   UNION ALL SELECT cust_no, email FROM `p.raw.crm_b`) s
            ON t.customer_id = s.customer_id
            WHEN MATCHED AND s.email IS NOT NULL THEN UPDATE SET email = s.email, t.updated = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (customer_id, email) VALUES (s.customer_id, s.email)
            WHEN NOT MATCHED BY SOURCE THEN DELETE";
        let s = p.parse_script(sql, Mode::Full).unwrap();
        let m = s.stmts[0];
        assert_eq!(
            (m.kind, s.path(m.target.unwrap())),
            (StmtKind::Merge, "p.mart.dim_customer".to_owned())
        );
        assert_eq!(s.text(m.target_alias.unwrap().sp), "t");
        assert_eq!(s.text(m.source_alias.unwrap().sp), "s");
        let c = s.merge_clauses(&m);
        assert_eq!(
            c.iter().map(|c| c.when).collect::<Vec<_>>(),
            vec![MergeWhen::Matched, MergeWhen::NotMatchedByTarget, MergeWhen::NotMatchedBySource]
        );
        assert!(c[0].cond.is_some());
        let MergeAction::Update(r) = c[0].action else { panic!() };
        assert_eq!(s.set_items(r).len(), 2);
        assert!(
            matches!(c[1].action, MergeAction::Insert { cols, values } if cols.len() == 2 && values.len() == 2)
        );
        assert_eq!(c[2].action, MergeAction::Delete);
        let t = p.parse_script("MERGE INTO d.t USING d.src AS s ON t.k = s.k WHEN NOT MATCHED BY TARGET THEN INSERT ROW", Mode::Full).unwrap();
        assert!(t.stmts[0].source_table.is_some() && t.stmts[0].query.is_none());
        assert_eq!(t.merge_clauses(&t.stmts[0])[0].action, MergeAction::InsertRow);
    }

    #[test]
    fn scripts_keep_the_last_write_and_skip_procedural_statements() {
        let mut p = Parser::new();
        let s = p
            .parse_script(
                "DECLARE d DATE DEFAULT CURRENT_DATE(); SET d = DATE_SUB(d, INTERVAL 1 DAY);
                 CREATE TEMP FUNCTION f(x INT64) AS (x + 1);
                 CREATE OR REPLACE TABLE p.d.a AS SELECT 1 AS x;
                 SELECT * FROM p.d.a;",
                Mode::Full,
            )
            .unwrap();
        assert_eq!(
            s.stmts.iter().map(|x| x.kind).collect::<Vec<_>>(),
            vec![
                StmtKind::Other,
                StmtKind::Other,
                StmtKind::Other,
                StmtKind::CreateTable,
                StmtKind::Query
            ]
        );
        assert_eq!(s.last_write().unwrap().kind, StmtKind::CreateTable);
    }

    #[test]
    fn strict_mode_refuses_writes_and_errors_are_located() {
        let mut p = Parser::new();
        assert!(p.parse_script("CREATE TABLE t AS SELECT 1", Mode::Strict).is_err());
        assert!(p.parse_script("INSERT t SELECT 1", Mode::Strict).is_err());
        assert!(
            p.parse_script("MERGE t USING s ON TRUE", Mode::Full).is_err(),
            "MERGE needs a WHEN clause"
        );
        assert!(p.parse_script("SELECT 1 SELECT 2", Mode::Full).is_err());
        let e = p.parse_script("CREATE VIEW v AS SELECT FROM", Mode::Full).unwrap_err();
        assert!(e.sp.start > 0);
        assert_eq!(p.parse_script("SELECT 1;;", Mode::Strict).unwrap().stmts.len(), 1);
    }
}
