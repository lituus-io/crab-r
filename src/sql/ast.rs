// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The GoogleSQL syntax tree.
//!
//! Every node is plain data: spans into the source ([`Sp`]) and typed handles
//! ([`Idx`], [`Span`]) into the pools of one [`Ast`]. No node borrows the
//! source, so an `Ast` is reused across parses: `clear()` keeps every pool's
//! capacity, and a warm parser allocates nothing per statement. Child lists
//! (arguments, select items, ...) are contiguous in their pool.

use crate::base::pool::{Idx, Pool, Span};
use crate::sql::Sp;

/// An identifier: its span (inside the backticks when quoted).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Name {
    /// Source span of the identifier text.
    pub sp: Sp,
    /// Written with backticks.
    pub quoted: bool,
}

/// Expression handle.
pub type ExprId = Idx<Expr>;
/// Query handle.
pub type QueryId = Idx<Query>;
/// Type handle.
pub type TypeId = Idx<TypeNode>;
/// FROM-item handle.
pub type FromId = Idx<From>;

/// Literal kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LitKind {
    /// Integer (decimal or hex).
    Int,
    /// Floating / decimal.
    Float,
    /// String (span includes prefix and quotes).
    Str,
    /// Bytes (span includes prefix and quotes).
    Bytes,
    /// `TRUE` / `FALSE`.
    Bool(bool),
    /// `NULL`.
    Null,
    /// A typed string literal, e.g. `DATE '2026-01-01'`; the span covers the
    /// string token and `ty` the type keyword.
    Typed(Sp),
}

/// Unary operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnOp {
    /// `-x`
    Neg,
    /// `+x`
    Plus,
    /// `~x`
    BitNot,
    /// `NOT x`
    Not,
}

/// Binary operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum BinOp {
    Mul,
    Div,
    Concat,
    Add,
    Sub,
    Shl,
    Shr,
    BitAnd,
    BitXor,
    BitOr,
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    And,
    Or,
}

impl BinOp {
    /// Canonical spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mul => "*",
            Self::Div => "/",
            Self::Concat => "||",
            Self::Add => "+",
            Self::Sub => "-",
            Self::Shl => "<<",
            Self::Shr => ">>",
            Self::BitAnd => "&",
            Self::BitXor => "^",
            Self::BitOr => "|",
            Self::Eq => "=",
            Self::Ne => "!=",
            Self::Lt => "<",
            Self::Gt => ">",
            Self::Le => "<=",
            Self::Ge => ">=",
            Self::And => "AND",
            Self::Or => "OR",
        }
    }

    /// Binding power (higher binds tighter), GoogleSQL precedence.
    #[must_use]
    pub fn precedence(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And => 2,
            Self::Eq | Self::Ne | Self::Lt | Self::Gt | Self::Le | Self::Ge => 4,
            Self::BitOr => 5,
            Self::BitXor => 6,
            Self::BitAnd => 7,
            Self::Shl | Self::Shr => 8,
            Self::Add | Self::Sub => 9,
            Self::Mul | Self::Div | Self::Concat => 10,
        }
    }
}

/// `IS [NOT] ...` targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum IsWhat {
    Null,
    True,
    False,
    Unknown,
}

/// Quantifier of `LIKE ANY|SOME|ALL (...)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Quant {
    Any,
    Some,
    All,
}

/// Array subscript kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum IndexMode {
    /// `a[i]` (also JSON / map subscripts)
    Plain,
    Offset,
    SafeOffset,
    Ordinal,
    SafeOrdinal,
}

/// A function-call argument (`name => value` when named).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Arg {
    /// Argument name, for named arguments.
    pub name: Option<Name>,
    /// Value.
    pub e: ExprId,
}

/// `IGNORE NULLS` / `RESPECT NULLS`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum NullTreatment {
    Ignore,
    Respect,
}

/// A function call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Call {
    /// Dotted name parts (`SAFE.DIVIDE` → 2 parts).
    pub name: Span<Name>,
    /// Arguments.
    pub args: Span<Arg>,
    /// `DISTINCT` inside the parentheses.
    pub distinct: bool,
    /// `IGNORE|RESPECT NULLS`.
    pub nulls: Option<NullTreatment>,
    /// `ORDER BY` inside the parentheses (aggregates).
    pub order: Span<OrderItem>,
    /// `LIMIT` inside the parentheses (aggregates).
    pub limit: Option<ExprId>,
    /// `OVER (...)` / `OVER name`.
    pub over: Option<Idx<WindowSpec>>,
}

/// One `WHEN ... THEN ...` arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WhenArm {
    /// Condition (or compared value for simple CASE).
    pub when: ExprId,
    /// Result.
    pub then: ExprId,
}

/// A `STRUCT(...)` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StructField {
    /// Value.
    pub e: ExprId,
    /// `AS name`.
    pub alias: Option<Name>,
}

/// Expressions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expr {
    /// A literal.
    Lit {
        /// Kind.
        kind: LitKind,
        /// Span of the literal (for `Typed`, the type keyword).
        sp: Sp,
    },
    /// A single identifier.
    Ident(Name),
    /// `base.field`
    Field {
        /// Base expression.
        base: ExprId,
        /// Field name.
        field: Name,
    },
    /// `@name`
    Param(Sp),
    /// `@@name`
    SysVar(Sp),
    /// `?` (ordinal among positional parameters)
    Positional(u32),
    /// `*` as a function argument (`COUNT(*)`).
    Star,
    /// Unary operator.
    Unary {
        /// Operator.
        op: UnOp,
        /// Operand.
        e: ExprId,
    },
    /// Binary operator.
    Binary {
        /// Operator.
        op: BinOp,
        /// Left.
        l: ExprId,
        /// Right.
        r: ExprId,
    },
    /// `e [NOT] LIKE pattern` or `e [NOT] LIKE ANY|SOME|ALL (patterns)`.
    Like {
        /// Subject.
        e: ExprId,
        /// Pattern(s).
        pats: Span<ExprId>,
        /// Quantifier, if any.
        quant: Option<Quant>,
        /// `NOT LIKE`.
        not: bool,
    },
    /// `e [NOT] BETWEEN lo AND hi`
    Between {
        /// Subject.
        e: ExprId,
        /// Lower bound.
        lo: ExprId,
        /// Upper bound.
        hi: ExprId,
        /// `NOT BETWEEN`.
        not: bool,
    },
    /// `e [NOT] IN (list)`
    InList {
        /// Subject.
        e: ExprId,
        /// Candidates.
        list: Span<ExprId>,
        /// `NOT IN`.
        not: bool,
    },
    /// `e [NOT] IN (subquery)`
    InQuery {
        /// Subject.
        e: ExprId,
        /// Subquery.
        q: QueryId,
        /// `NOT IN`.
        not: bool,
    },
    /// `e [NOT] IN UNNEST(array)`
    InUnnest {
        /// Subject.
        e: ExprId,
        /// Array.
        arr: ExprId,
        /// `NOT IN`.
        not: bool,
    },
    /// `e IS [NOT] NULL|TRUE|FALSE|UNKNOWN`
    Is {
        /// Subject.
        e: ExprId,
        /// Target.
        what: IsWhat,
        /// `IS NOT`.
        not: bool,
    },
    /// `l IS [NOT] DISTINCT FROM r`
    IsDistinct {
        /// Left.
        l: ExprId,
        /// Right.
        r: ExprId,
        /// `IS NOT DISTINCT FROM`.
        not: bool,
    },
    /// Function call.
    Call(Idx<Call>),
    /// `CAST` / `SAFE_CAST`.
    Cast {
        /// Operand.
        e: ExprId,
        /// Target type.
        ty: TypeId,
        /// `SAFE_CAST`.
        safe: bool,
        /// `FORMAT fmt`.
        format: Option<ExprId>,
    },
    /// `EXTRACT(part FROM e [AT TIME ZONE tz])`; `part` may be a call like `WEEK(MONDAY)`.
    Extract {
        /// Date part.
        part: ExprId,
        /// Operand.
        e: ExprId,
        /// Time zone.
        tz: Option<ExprId>,
    },
    /// `CASE [operand] WHEN ... THEN ... [ELSE ...] END`
    Case {
        /// Operand of a simple CASE.
        operand: Option<ExprId>,
        /// Arms.
        arms: Span<WhenArm>,
        /// `ELSE` result.
        else_: Option<ExprId>,
    },
    /// `[ARRAY<T>] [e, ...]`
    Array {
        /// Declared element type.
        ty: Option<TypeId>,
        /// Elements.
        elems: Span<ExprId>,
    },
    /// `ARRAY(subquery)`
    ArrayQuery(QueryId),
    /// `STRUCT[<...>](...)`
    Struct {
        /// Declared type.
        ty: Option<TypeId>,
        /// Fields.
        fields: Span<StructField>,
    },
    /// `(a, b, ...)`
    Tuple(Span<ExprId>),
    /// `e[i]`, `e[OFFSET(i)]`, ...
    Index {
        /// Subject.
        e: ExprId,
        /// Index.
        idx: ExprId,
        /// Kind.
        mode: IndexMode,
    },
    /// `EXISTS (subquery)`
    Exists(QueryId),
    /// `(subquery)` as a scalar.
    Subquery(QueryId),
    /// `INTERVAL e unit [TO unit]`
    Interval {
        /// Amount.
        e: ExprId,
        /// Unit.
        unit: Name,
        /// Upper unit of a range interval.
        to: Option<Name>,
    },
    /// `(e)` kept for faithful emission.
    Paren(ExprId),
}

/// A type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TypeNode {
    /// A named type with optional parameters (`STRING(10)`, `NUMERIC(10, 2)`).
    Named {
        /// Name.
        name: Name,
        /// Parameters.
        params: Span<ExprId>,
    },
    /// `ARRAY<T>`
    Array(TypeId),
    /// `STRUCT<[name] T, ...>`
    Struct(Span<StructFieldType>),
    /// `RANGE<T>`
    Range(TypeId),
}

/// A field of a `STRUCT<...>` type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StructFieldType {
    /// Field name (optional in GoogleSQL).
    pub name: Option<Name>,
    /// Field type.
    pub ty: TypeId,
}

/// `ORDER BY` item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderItem {
    /// Key.
    pub e: ExprId,
    /// `Some(true)` = DESC, `Some(false)` = ASC, `None` = unspecified.
    pub desc: Option<bool>,
    /// `Some(true)` = NULLS FIRST, `Some(false)` = NULLS LAST.
    pub nulls_first: Option<bool>,
}

/// Window frame units.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum FrameUnit {
    Rows,
    Range,
}

/// Window frame bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Bound {
    UnboundedPreceding,
    Preceding(ExprId),
    CurrentRow,
    Following(ExprId),
    UnboundedFollowing,
}

/// Window frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame {
    /// ROWS / RANGE.
    pub unit: FrameUnit,
    /// Start bound.
    pub start: Bound,
    /// End bound (`BETWEEN ... AND end`).
    pub end: Option<Bound>,
}

/// `OVER (...)` specification (or a bare named-window reference).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowSpec {
    /// Referenced named window.
    pub base: Option<Name>,
    /// `PARTITION BY`.
    pub partition: Span<ExprId>,
    /// `ORDER BY`.
    pub order: Span<OrderItem>,
    /// Frame.
    pub frame: Option<Frame>,
    /// `OVER name` without parentheses.
    pub bare: bool,
}

/// `WINDOW name AS (...)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NamedWindow {
    /// Name.
    pub name: Name,
    /// Spec.
    pub spec: Idx<WindowSpec>,
}

/// A select-list item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectItem {
    /// `expr [AS alias]`
    Expr {
        /// Value.
        e: ExprId,
        /// Alias.
        alias: Option<Name>,
    },
    /// `[qualifier.]* [EXCEPT (...)] [REPLACE (...)]`
    Star {
        /// `t` in `t.*`.
        qualifier: Option<ExprId>,
        /// `EXCEPT (cols)`.
        except: Span<Name>,
        /// `REPLACE (expr AS col, ...)`.
        replace: Span<StructField>,
    },
}

/// Join kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    /// `a, b`
    Comma,
}

/// Join condition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinCond {
    /// None (CROSS / comma).
    None,
    /// `ON expr`.
    On(ExprId),
    /// `USING (cols)`.
    Using(Span<Name>),
}

/// `WITH OFFSET [AS alias]` on an array scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WithOffset {
    /// The offset column's alias.
    pub alias: Option<Name>,
}

/// FROM items.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum From {
    /// A table path (`p.d.t`, `` `p.d.t` ``, `alias.array_field`).
    Table {
        /// Path parts.
        path: Span<Name>,
        /// Alias.
        alias: Option<Name>,
        /// `FOR SYSTEM_TIME AS OF expr`.
        time_travel: Option<ExprId>,
        /// `WITH OFFSET [AS name]` on an implicit array path.
        offset: Option<WithOffset>,
        /// `TABLESAMPLE SYSTEM (n PERCENT)`.
        sample: Option<ExprId>,
    },
    /// `(subquery) [AS alias]`
    Subquery {
        /// Query.
        q: QueryId,
        /// Alias.
        alias: Option<Name>,
    },
    /// `UNNEST(expr) [AS alias] [WITH OFFSET [AS name]]`
    Unnest {
        /// Array.
        e: ExprId,
        /// Alias.
        alias: Option<Name>,
        /// `WITH OFFSET [AS name]`.
        offset: Option<WithOffset>,
    },
    /// A join.
    Join {
        /// Kind.
        kind: JoinKind,
        /// Left side.
        l: FromId,
        /// Right side.
        r: FromId,
        /// Condition.
        cond: JoinCond,
    },
    /// `( join )`
    Paren(FromId),
}

/// A GROUP BY element.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupItem {
    /// `expr`
    Expr(ExprId),
    /// `ROLLUP (...)`
    Rollup(Span<ExprId>),
    /// `CUBE (...)`
    Cube(Span<ExprId>),
    /// `GROUPING SETS (...)`; each set is a tuple or single expression.
    GroupingSets(Span<ExprId>),
    /// `()`
    Empty,
}

/// GROUP BY clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupBy {
    /// None.
    None,
    /// `GROUP BY items`.
    Items(Span<GroupItem>),
    /// `GROUP BY ALL`.
    All,
}

/// `SELECT AS STRUCT|VALUE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum SelectAs {
    None,
    Struct,
    Value,
}

/// A SELECT block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Select {
    /// `DISTINCT`.
    pub distinct: bool,
    /// `AS STRUCT|VALUE`.
    pub as_: SelectAs,
    /// Select list.
    pub items: Span<SelectItem>,
    /// FROM.
    pub from: Option<FromId>,
    /// WHERE.
    pub where_: Option<ExprId>,
    /// GROUP BY.
    pub group: GroupBy,
    /// HAVING.
    pub having: Option<ExprId>,
    /// QUALIFY.
    pub qualify: Option<ExprId>,
    /// WINDOW.
    pub windows: Span<NamedWindow>,
}

/// Set operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum SetOp {
    Union,
    Intersect,
    Except,
}

/// A query body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetExpr {
    /// A SELECT.
    Select(Idx<Select>),
    /// `( query )`
    Paren(QueryId),
    /// `l op ALL|DISTINCT r`
    SetOp {
        /// Operator.
        op: SetOp,
        /// `ALL` (vs `DISTINCT`).
        all: bool,
        /// Left.
        l: Idx<SetExpr>,
        /// Right.
        r: Idx<SetExpr>,
    },
}

/// A common table expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cte {
    /// Name.
    pub name: Name,
    /// Body.
    pub q: QueryId,
}

/// A query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Query {
    /// `WITH RECURSIVE`.
    pub recursive: bool,
    /// CTEs.
    pub ctes: Span<Cte>,
    /// Body.
    pub body: Idx<SetExpr>,
    /// ORDER BY.
    pub order: Span<OrderItem>,
    /// LIMIT.
    pub limit: Option<ExprId>,
    /// OFFSET.
    pub offset: Option<ExprId>,
}

/// Apply `body` to every pool of an [`Ast`] (as `pool`, with its position
/// `i`), in one fixed order. `ref` borrows shared, `mut` mutably.
macro_rules! for_each_pool {
    ($kind:tt $ast:expr, |$i:ident, $p:ident| $body:expr) => {
        for_each_pool!(@each $kind $ast, $i, $p, $body; 0 exprs 1 expr_lists 2 names 3 calls 4 args 5 arms
            6 struct_fields 7 types 8 type_fields 9 order_items 10 windows 11 named_windows 12 select_items
            13 froms 14 group_items 15 selects 16 set_exprs 17 ctes 18 queries)
    };
    (@each ref $ast:expr, $i:ident, $p:ident, $body:expr; $($n:literal $field:ident)*) => {
        $({ let $i: usize = $n; let $p = &$ast.$field; $body; })*
    };
    (@each mut $ast:expr, $i:ident, $p:ident, $body:expr; $($n:literal $field:ident)*) => {
        $({ let $i: usize = $n; let $p = &mut $ast.$field; $body; })*
    };
}

/// A restore point for backtracking (see [`Ast::mark`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AstMark([usize; 19]);

/// All pools of one parsed statement. Reused across parses.
#[derive(Debug, Default)]
#[allow(missing_docs)]
pub struct Ast {
    pub exprs: Pool<Expr>,
    pub expr_lists: Pool<ExprId>,
    pub names: Pool<Name>,
    pub calls: Pool<Call>,
    pub args: Pool<Arg>,
    pub arms: Pool<WhenArm>,
    pub struct_fields: Pool<StructField>,
    pub types: Pool<TypeNode>,
    pub type_fields: Pool<StructFieldType>,
    pub order_items: Pool<OrderItem>,
    pub windows: Pool<WindowSpec>,
    pub named_windows: Pool<NamedWindow>,
    pub select_items: Pool<SelectItem>,
    pub froms: Pool<From>,
    pub group_items: Pool<GroupItem>,
    pub selects: Pool<Select>,
    pub set_exprs: Pool<SetExpr>,
    pub ctes: Pool<Cte>,
    pub queries: Pool<Query>,
}

impl Ast {
    /// Empty pools.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Drop all nodes, keeping capacity.
    pub fn clear(&mut self) {
        self.exprs.clear();
        self.expr_lists.clear();
        self.names.clear();
        self.calls.clear();
        self.args.clear();
        self.arms.clear();
        self.struct_fields.clear();
        self.types.clear();
        self.type_fields.clear();
        self.order_items.clear();
        self.windows.clear();
        self.named_windows.clear();
        self.select_items.clear();
        self.froms.clear();
        self.group_items.clear();
        self.selects.clear();
        self.set_exprs.clear();
        self.ctes.clear();
        self.queries.clear();
    }

    /// A restore point: every pool's length.
    #[must_use]
    pub fn mark(&self) -> AstMark {
        let mut m = [0usize; 19];
        for_each_pool!(ref self, |i, pool| m[i] = pool.len());
        AstMark(m)
    }

    /// Roll every pool back to `m` (nodes created after it are discarded).
    pub fn restore(&mut self, m: &AstMark) {
        for_each_pool!(mut self, |i, pool| pool.truncate(m.0[i]));
    }

    /// Total nodes across pools (a size proxy for limits and metrics).
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.exprs.len()
            + self.froms.len()
            + self.selects.len()
            + self.queries.len()
            + self.calls.len()
    }
}

// ---- statements (Full mode) -------------------------------------------------

/// What a statement does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StmtKind {
    /// A plain query.
    Query,
    /// `CREATE [OR REPLACE] [TEMP] TABLE ... [AS query]`.
    CreateTable,
    /// `CREATE [OR REPLACE] VIEW ... AS query`.
    CreateView,
    /// `CREATE [OR REPLACE] MATERIALIZED VIEW ... AS query`.
    CreateMaterializedView,
    /// `INSERT [INTO] table [(cols)] query | VALUES ...`.
    Insert,
    /// `MERGE [INTO] target USING source ON ... WHEN ...`.
    Merge,
    /// `UPDATE target [[AS] alias] SET col = expr, ... [FROM items] [WHERE cond]`.
    /// Its `SET` items are the range [`Stmt::clauses`] into
    /// [`crate::sql::parser::Script::sets`], `WHERE` is [`Stmt::on`], and
    /// [`Stmt::query_sp`] spans the `FROM` items (empty when there are none).
    Update,
    /// A statement that is recognised but carries no query (procedural
    /// script statements, `CREATE TABLE` without `AS`, DDL options), kept
    /// by span only.
    Other,
}

/// One statement of a script. Expressions and names live in the [`Ast`]
/// pools; merge clauses in the script's own lists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stmt {
    /// Kind.
    pub kind: StmtKind,
    /// The whole statement's source span.
    pub sp: Sp,
    /// The table or view written, as a path.
    pub target: Option<Span<Name>>,
    /// Target alias (`MERGE t AS x`).
    pub target_alias: Option<Name>,
    /// Column list: `INSERT t (a, b)`, `CREATE VIEW v (a, b)`.
    pub columns: Span<Name>,
    /// The query producing the rows: `AS query`, `INSERT ... query`, or the
    /// `USING (query)` of a MERGE.
    pub query: Option<QueryId>,
    /// Source span of `query` (with its parentheses for `USING (query)`).
    pub query_sp: Sp,
    /// `MERGE ... USING table`: the source table path, and the source alias.
    pub source_table: Option<Span<Name>>,
    /// Source alias (`USING ... AS s`).
    pub source_alias: Option<Name>,
    /// `MERGE ... ON cond`.
    pub on: Option<ExprId>,
    /// MERGE clauses: range into [`crate::sql::parser::Script::clauses`].
    pub clauses: (u32, u32),
}

/// Which rows a MERGE clause applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeWhen {
    /// `WHEN MATCHED`
    Matched,
    /// `WHEN NOT MATCHED [BY TARGET]`
    NotMatchedByTarget,
    /// `WHEN NOT MATCHED BY SOURCE`
    NotMatchedBySource,
}

/// What a MERGE clause does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeAction {
    /// `UPDATE SET col = expr, ...`: range into [`crate::sql::parser::Script::sets`].
    Update((u32, u32)),
    /// `DELETE`
    Delete,
    /// `INSERT [(cols)] VALUES (exprs)`.
    Insert {
        /// Target columns (empty: all, in order).
        cols: Span<Name>,
        /// Values.
        values: Span<ExprId>,
    },
    /// `INSERT ROW` (all source columns, in order).
    InsertRow,
}

/// One `WHEN ... THEN ...` clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MergeClause {
    /// Which rows.
    pub when: MergeWhen,
    /// `AND condition`.
    pub cond: Option<ExprId>,
    /// Action.
    pub action: MergeAction,
}

/// `col = expr` in `UPDATE SET`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SetItem {
    /// Column (a path: `a` or `rec.a`).
    pub col: Span<Name>,
    /// Value.
    pub value: ExprId,
}
