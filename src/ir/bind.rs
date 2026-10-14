// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The binder: names, types and aggregation rules, against the catalog.
//!
//! Binding annotates the parsed tree in side tables indexed by node id
//! ([`Bound::ty`], [`Bound::refs`], [`Bound::froms`]) instead of building a
//! second tree: one pass, and every buffer is reused across statements.
//!
//! Resolution follows GoogleSQL: table names are case-sensitive and resolve
//! `project.dataset.table`, `dataset.table` (default project) or a bare name
//! (CTE, then default dataset); column names and aliases are
//! case-insensitive; nested fields resolve through catalog field paths; a
//! path whose head is a range variable (`FROM o, o.items AS it`) is an
//! implicit UNNEST. Unresolvable names are errors with suggestions, except in
//! `Full` mode where unknown tables and functions are tolerated (their
//! columns and results are simply untyped) so foreign SQL still binds.

use crate::base::pool::{Idx, Span};
use crate::catalog::{Catalog, ColId, TableId, TypeCode};
use crate::ir::diag::{suggest, Diag, DiagKind, Severity};
use crate::ir::funcs::{self, Kind};
use crate::ir::ty::{comparable, from_type_name, numeric_super, Ty};
use crate::sql::ast::Bound as FrameBound;
use crate::sql::ast::*;
use crate::sql::{Mode, Parsed, Sp};

/// Binding options.
#[derive(Clone, Copy, Debug)]
pub struct BindOpts<'o> {
    /// Project for two-part and one-part table names.
    pub default_project: &'o str,
    /// Dataset for one-part table names.
    pub default_dataset: Option<&'o str>,
    /// Strictness (model output vs foreign SQL).
    pub mode: Mode,
}

/// What a name resolved to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Ref {
    /// Not a name (or unresolved).
    None,
    /// A catalog column / nested field of relation `rel`.
    Column {
        /// Relation (index into [`Bound::rels`]).
        rel: u32,
        /// Catalog column.
        col: ColId,
    },
    /// Output column `index` of a derived relation (CTE / subquery).
    Derived {
        /// Relation.
        rel: u32,
        /// Output index within that relation.
        index: u32,
    },
    /// The element value of an UNNEST relation.
    Element {
        /// Relation.
        rel: u32,
    },
    /// The offset column of `UNNEST ... WITH OFFSET`.
    Offset {
        /// Relation.
        rel: u32,
    },
    /// A column of an unresolved (tolerated) table.
    Opaque {
        /// Relation.
        rel: u32,
    },
    /// A select-list alias (GROUP BY / HAVING / ORDER BY / QUALIFY).
    Alias {
        /// The aliased expression.
        expr: ExprId,
    },
    /// A date part keyword (`MONTH`, `WEEK(MONDAY)`, ...).
    DatePart,
    /// A relation referenced by name as a value (`SELECT t FROM t`, `t.*`).
    Rel {
        /// Relation.
        rel: u32,
    },
}

/// A FROM item's resolution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FromBind {
    /// Not a table item (join, parenthesis).
    None,
    /// A catalog table.
    Table(TableId),
    /// A CTE (index into the statement's CTE list, in definition order).
    Cte(u32),
    /// An implicit UNNEST of an array path.
    ArrayPath,
    /// Unresolved (tolerated in Full mode).
    Unknown,
}

/// A range variable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rel {
    /// Name it is referenced by (alias, or the table's own name).
    pub name: Option<Name>,
    /// Catalog table, for table relations.
    pub table: Option<TableId>,
    /// Output columns, for derived relations (range into [`Bound::outs`]).
    pub outs: (u32, u32),
    /// Element type, for UNNEST relations.
    pub elem: Option<Ty>,
    /// `WITH OFFSET` alias.
    pub offset: Option<Name>,
    /// Unresolved table (tolerated).
    pub opaque: bool,
    /// The FROM item.
    pub from: FromId,
}

/// An output column of a query or derived relation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutCol {
    /// Name as written (alias or implicit), if any.
    pub name: Option<Name>,
    /// Name from the catalog (star expansion).
    pub col_name: Option<ColId>,
    /// Type.
    pub ty: Ty,
    /// The expression producing it (None for star expansion).
    pub expr: Option<ExprId>,
    /// For a set operation's output: the corresponding output columns of
    /// its left and right branches (indices into [`Bound::outs`]), so
    /// lineage sees every branch, not only the first.
    pub union: Option<(u32, u32)>,
}

/// Binding results for one statement. Reused across statements.
#[derive(Debug, Default)]
pub struct Bound {
    /// Type of every expression, by `ExprId`.
    pub ty: Vec<Ty>,
    /// Resolution of every expression, by `ExprId`.
    pub refs: Vec<Ref>,
    /// Resolution of every FROM item, by `FromId`.
    pub froms: Vec<FromBind>,
    /// Relation (range variable) of every FROM item that introduces one.
    pub from_rel: Vec<u32>,
    /// Range variables.
    pub rels: Vec<Rel>,
    /// Output-column pool.
    pub outs: Vec<OutCol>,
    /// The statement's output columns (range into `outs`).
    pub output: (u32, u32),
    /// Diagnostics.
    pub diags: Vec<Diag>,
    /// Catalog tables referenced (sorted, unique).
    pub tables: Vec<TableId>,
    /// Catalog columns referenced (sorted, unique).
    pub columns: Vec<ColId>,
    /// Any aggregate anywhere.
    pub aggregates: bool,
}

impl Bound {
    /// True when no error-severity diagnostic was produced.
    #[must_use]
    pub fn ok(&self) -> bool {
        !self.diags.iter().any(|d| d.severity == Severity::Error)
    }

    /// The statement's output columns.
    #[must_use]
    pub fn output_columns(&self) -> &[OutCol] {
        &self.outs[self.output.0 as usize..self.output.1 as usize]
    }

    fn reset(&mut self, n_exprs: usize, n_froms: usize) {
        self.ty.clear();
        self.ty.resize(n_exprs, Ty::UNKNOWN);
        self.refs.clear();
        self.refs.resize(n_exprs, Ref::None);
        self.froms.clear();
        self.froms.resize(n_froms, FromBind::None);
        self.from_rel.clear();
        self.from_rel.resize(n_froms, u32::MAX);
        self.rels.clear();
        self.outs.clear();
        self.output = (0, 0);
        self.diags.clear();
        self.tables.clear();
        self.columns.clear();
        self.aggregates = false;
    }
}

/// Where an expression is being bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // four independent clause permissions
struct Ctx {
    allow_agg: bool,
    allow_window: bool,
    in_agg: bool,
    /// Select-list aliases may be referenced.
    aliases: bool,
}

const WHERE: Ctx = Ctx { allow_agg: false, allow_window: false, in_agg: false, aliases: false };
const SELECT: Ctx = Ctx { allow_agg: true, allow_window: true, in_agg: false, aliases: false };
const HAVING: Ctx = Ctx { allow_agg: true, allow_window: false, in_agg: false, aliases: true };
const QUALIFY: Ctx = Ctx { allow_agg: true, allow_window: true, in_agg: false, aliases: true };
const GROUP: Ctx = Ctx { allow_agg: false, allow_window: false, in_agg: false, aliases: true };

/// One scope level: the range variables of a SELECT (plus its aliases).
#[derive(Debug, Default)]
struct Frame {
    rels: Vec<u32>,
    /// (alias name, expression, output index) of the SELECT list.
    aliases: Vec<(Name, ExprId)>,
}

/// A reusable binder.
#[derive(Debug, Default)]
pub struct Binder {
    bound: Bound,
    frames: Vec<Frame>,
    depth: usize,
    /// Visible CTEs: (name, output range, index).
    ctes: Vec<(Name, (u32, u32), u32)>,
    cte_count: u32,
    tmp_tys: Vec<Ty>,
    /// Select-list outputs being built, one buffer per open SELECT: a
    /// subquery bound inside the list writes its own outputs to `outs`
    /// directly, so each SELECT's outputs stay one contiguous range.
    item_outs: Vec<Vec<OutCol>>,
    /// Emptied buffers, reused (no allocation per SELECT once warm).
    spare_outs: Vec<Vec<OutCol>>,
}

/// Date-part keywords accepted as bare identifiers in date functions.
const DATE_PARTS: [&str; 20] = [
    "MICROSECOND",
    "MILLISECOND",
    "SECOND",
    "MINUTE",
    "HOUR",
    "DAY",
    "DAYOFWEEK",
    "DAYOFYEAR",
    "WEEK",
    "ISOWEEK",
    "MONTH",
    "QUARTER",
    "YEAR",
    "ISOYEAR",
    "DATE",
    "TIME",
    "DATETIME",
    "MONDAY",
    "SUNDAY",
    "SATURDAY",
];

fn is_date_part(s: &str) -> bool {
    DATE_PARTS.iter().any(|p| p.eq_ignore_ascii_case(s))
        || ["TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY"].iter().any(|p| p.eq_ignore_ascii_case(s))
}

impl Binder {
    /// A binder with empty buffers.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind `parsed` against `cat`.
    pub fn bind<'b>(
        &'b mut self,
        parsed: &Parsed<'_>,
        cat: &Catalog,
        opts: BindOpts<'_>,
    ) -> &'b Bound {
        self.bound.reset(parsed.ast.exprs.len(), parsed.ast.froms.len());
        self.depth = 0;
        self.ctes.clear();
        while let Some(mut b) = self.item_outs.pop() {
            b.clear();
            self.spare_outs.push(b);
        }
        self.cte_count = 0;
        let mut b = B { p: parsed, cat, opts, s: self };
        let out = b.query(parsed.root);
        b.s.bound.output = out;
        let bound = &mut self.bound;
        bound.tables.sort_unstable();
        bound.tables.dedup();
        bound.columns.sort_unstable();
        bound.columns.dedup();
        &self.bound
    }
}

struct B<'x, 'p, 'c> {
    p: &'x Parsed<'p>,
    cat: &'c Catalog,
    opts: BindOpts<'x>,
    s: &'x mut Binder,
}

impl<'p> B<'_, 'p, '_> {
    /// The tree, borrowed for the statement's lifetime (not `&self`).
    fn ast(&self) -> &'p Ast {
        self.p.ast
    }

    fn text(&self, sp: Sp) -> &'p str {
        sp.text(self.p.src)
    }

    fn name_text(&self, n: Name) -> &'p str {
        self.text(n.sp)
    }

    fn diag(&mut self, kind: DiagKind, sp: Sp, suggestion: Option<String>) {
        self.s.bound.diags.push(Diag { kind, severity: Severity::Error, sp, suggestion });
    }

    fn strict(&self) -> bool {
        self.opts.mode == Mode::Strict
    }

    fn set(&mut self, e: ExprId, ty: Ty, r: Ref) -> Ty {
        let i = e.raw() as usize;
        self.s.bound.ty[i] = ty;
        self.s.bound.refs[i] = r;
        ty
    }

    fn push_frame(&mut self) {
        if self.s.frames.len() <= self.s.depth {
            self.s.frames.push(Frame::default());
        }
        let f = &mut self.s.frames[self.s.depth];
        f.rels.clear();
        f.aliases.clear();
        self.s.depth += 1;
    }

    fn pop_frame(&mut self) {
        self.s.depth -= 1;
    }

    fn frame(&mut self) -> &mut Frame {
        let d = self.s.depth - 1;
        &mut self.s.frames[d]
    }

    fn add_rel(&mut self, rel: Rel) -> u32 {
        let id = self.s.bound.rels.len() as u32;
        self.s.bound.from_rel[rel.from.raw() as usize] = id;
        self.s.bound.rels.push(rel);
        self.frame().rels.push(id);
        id
    }

    /// Add a select-list output to the innermost open SELECT.
    fn push_item(&mut self, o: OutCol) {
        match self.s.item_outs.last_mut() {
            Some(buf) => buf.push(o),
            None => self.s.bound.outs.push(o),
        }
    }

    fn push_out(&mut self, o: OutCol) {
        self.s.bound.outs.push(o);
    }

    fn out_name<'t>(&'t self, o: &OutCol) -> Option<&'t str> {
        if let Some(n) = o.name {
            return Some(self.name_text(n));
        }
        o.col_name.and_then(|c| self.cat.column(c)).map(|c| c.leaf())
    }

    // ---- queries ---------------------------------------------------------

    /// Bind a query; returns its output-column range.
    fn query(&mut self, q: QueryId) -> (u32, u32) {
        let q = self.ast().queries[q];
        let cte_mark = self.s.ctes.len();
        for &cte in self.ast().ctes.slice(q.ctes) {
            if q.recursive {
                // Visible to itself; its outputs are known only after binding.
                let idx = self.s.cte_count;
                self.s.cte_count += 1;
                self.s.ctes.push((cte.name, (0, 0), idx));
                let outs = self.query(cte.q);
                let last = self.s.ctes.len() - 1;
                if let Some(slot) = self.s.ctes.get_mut(last) {
                    slot.1 = outs;
                }
            } else {
                let outs = self.query(cte.q);
                let idx = self.s.cte_count;
                self.s.cte_count += 1;
                self.s.ctes.push((cte.name, outs, idx));
            }
        }
        let (outs, select_frame) = self.set_expr(q.body);
        // ORDER BY: in the SELECT's scope (aliases and FROM columns) for a
        // plain SELECT body, else against output columns by name/ordinal.
        if !q.order.is_empty() {
            let items: &[OrderItem] = self.ast().order_items.slice(q.order);
            if select_frame {
                for it in items {
                    self.order_key(it.e, outs);
                }
                self.pop_frame();
            } else {
                for it in items {
                    self.output_ref(it.e, outs);
                }
            }
        } else if select_frame {
            self.pop_frame();
        }
        for e in [q.limit, q.offset].into_iter().flatten() {
            self.expr(e, WHERE);
        }
        self.s.ctes.truncate(cte_mark);
        outs
    }

    /// ORDER BY key for a plain SELECT: ordinal, alias, or expression.
    fn order_key(&mut self, e: ExprId, outs: (u32, u32)) {
        if let Expr::Lit { kind: LitKind::Int, sp } = self.ast().exprs[e] {
            let n: u32 = self.text(sp).parse().unwrap_or(0);
            if n == 0 || n > outs.1 - outs.0 {
                self.diag(DiagKind::UnknownName, sp, None);
            }
            self.set(e, Ty::UNKNOWN, Ref::None);
            return;
        }
        self.expr(e, QUALIFY);
    }

    /// ORDER BY over a set operation: output names or ordinals only.
    fn output_ref(&mut self, e: ExprId, outs: (u32, u32)) {
        match self.ast().exprs[e] {
            Expr::Lit { kind: LitKind::Int, sp } => {
                let n: u32 = self.text(sp).parse().unwrap_or(0);
                if n == 0 || n > outs.1 - outs.0 {
                    self.diag(DiagKind::UnknownName, sp, None);
                }
            }
            Expr::Ident(n) => {
                let name = self.name_text(n).to_owned();
                let found = (outs.0..outs.1).find(|&i| {
                    let o = self.s.bound.outs[i as usize];
                    self.out_name(&o).is_some_and(|x| x.eq_ignore_ascii_case(&name))
                });
                match found {
                    Some(i) => {
                        let ty = self.s.bound.outs[i as usize].ty;
                        self.set(e, ty, Ref::None);
                    }
                    None => {
                        let cands: Vec<String> = (outs.0..outs.1)
                            .filter_map(|i| {
                                let o = self.s.bound.outs[i as usize];
                                self.out_name(&o).map(str::to_owned)
                            })
                            .collect();
                        let sug = suggest(&name, cands.iter().map(String::as_str));
                        self.diag(DiagKind::UnknownName, n.sp, sug);
                    }
                }
            }
            _ => {
                self.diag(DiagKind::Unsupported, self.expr_sp(e), None);
            }
        }
    }

    /// Bind a set expression. Returns its outputs and whether a SELECT frame
    /// is still open (so the caller can bind ORDER BY in it).
    fn set_expr(&mut self, s: Idx<SetExpr>) -> ((u32, u32), bool) {
        match self.ast().set_exprs[s] {
            SetExpr::Select(sel) => (self.select(sel), true),
            SetExpr::Paren(q) => (self.query(q), false),
            SetExpr::SetOp { l, r, .. } => {
                let (lo, lf) = self.set_expr(l);
                if lf {
                    self.pop_frame();
                }
                let (ro, rf) = self.set_expr(r);
                if rf {
                    self.pop_frame();
                }
                if lo.1 - lo.0 != ro.1 - ro.0 {
                    let sp = self.set_sp(r);
                    self.diag(DiagKind::SetOpArity, sp, None);
                }
                // Output names from the left branch; types widened.
                let start = self.s.bound.outs.len() as u32;
                for i in 0..(lo.1 - lo.0) {
                    let mut o = self.s.bound.outs[(lo.0 + i) as usize];
                    o.union = Some((lo.0 + i, ro.0 + i));
                    if let Some(rt) = self.s.bound.outs.get((ro.0 + i) as usize).map(|o| o.ty) {
                        if o.ty.is_open() {
                            o.ty = rt;
                        } else if o.ty.code.is_numeric() && rt.code.is_numeric() {
                            o.ty = Ty::scalar(numeric_super(o.ty.code, rt.code));
                        }
                    }
                    self.push_out(o);
                }
                ((start, self.s.bound.outs.len() as u32), false)
            }
        }
    }

    fn set_sp(&self, s: Idx<SetExpr>) -> Sp {
        match self.ast().set_exprs[s] {
            SetExpr::Select(sel) => {
                let items = self.ast().selects[sel].items;
                self.ast().select_items.slice(items).first().map_or(Sp::default(), |it| match *it {
                    SelectItem::Expr { e, .. } => self.expr_sp(e),
                    SelectItem::Star { .. } => Sp::default(),
                })
            }
            _ => Sp::default(),
        }
    }

    #[allow(clippy::too_many_lines)] // the SELECT clause order, in one place
    fn select(&mut self, sel: Idx<Select>) -> (u32, u32) {
        let s = self.ast().selects[sel];
        self.push_frame();
        if let Some(f) = s.from {
            self.from(f);
        }
        if let Some(w) = s.where_ {
            let t = self.expr(w, WHERE);
            self.expect_bool(w, t);
        }
        // Select list (aliases recorded for later clauses).
        let buf = self.s.spare_outs.pop().unwrap_or_default();
        self.s.item_outs.push(buf);
        let items: &[SelectItem] = self.ast().select_items.slice(s.items);
        for &it in items {
            match it {
                SelectItem::Expr { e, alias } => {
                    let ty = self.expr(e, SELECT);
                    let name = alias.or_else(|| self.implicit_name(e));
                    if let Some(a) = alias {
                        self.frame().aliases.push((a, e));
                    }
                    self.push_item(OutCol { name, col_name: None, ty, expr: Some(e), union: None });
                }
                SelectItem::Star { qualifier, except, replace } => {
                    self.star(qualifier, except, replace);
                }
            }
        }
        let mut mine = self.s.item_outs.pop().unwrap_or_default();
        let start = self.s.bound.outs.len() as u32;
        self.s.bound.outs.append(&mut mine);
        self.s.spare_outs.push(mine);
        let end = self.s.bound.outs.len() as u32;
        let mut group_keys: Vec<ExprId> = Vec::new();
        let grouped = match s.group {
            GroupBy::None => false,
            GroupBy::All => true,
            GroupBy::Items(items) => {
                for &g in self.ast().group_items.slice(items) {
                    match g {
                        GroupItem::Expr(e) => {
                            if let Expr::Lit { kind: LitKind::Int, sp } = self.ast().exprs[e] {
                                let n: u32 = self.text(sp).parse().unwrap_or(0);
                                match (n > 0 && n <= end - start)
                                    .then(|| self.s.bound.outs[(start + n - 1) as usize].expr)
                                {
                                    Some(Some(x)) => group_keys.push(x),
                                    Some(None) => {}
                                    None => self.diag(DiagKind::UnknownName, sp, None),
                                }
                            } else {
                                self.expr(e, GROUP);
                                match self.s.bound.refs[e.raw() as usize] {
                                    Ref::Alias { expr } => group_keys.push(expr),
                                    _ => group_keys.push(e),
                                }
                            }
                        }
                        GroupItem::Rollup(l) | GroupItem::Cube(l) | GroupItem::GroupingSets(l) => {
                            for &e in self.ast().expr_lists.slice(l) {
                                self.group_leaf(e, &mut group_keys);
                            }
                        }
                        GroupItem::Empty => {}
                    }
                }
                true
            }
        };
        if let Some(h) = s.having {
            let t = self.expr(h, HAVING);
            self.expect_bool(h, t);
        }
        if let Some(q) = s.qualify {
            let t = self.expr(q, QUALIFY);
            self.expect_bool(q, t);
        }
        // Aggregation rule: in an aggregated SELECT, every column outside an
        // aggregate must be grouped.
        let aggregated = grouped || self.select_has_aggregate(start, end, s.having);
        if aggregated && !matches!(s.group, GroupBy::All) {
            let keys: Vec<u64> = group_keys
                .iter()
                .map(|&k| crate::ir::fingerprint::expr_hash(self.p, &self.s.bound, k))
                .collect();
            for i in start..end {
                if let Some(e) = self.s.bound.outs[i as usize].expr {
                    self.check_grouped(e, &keys);
                } else {
                    // `*` in an aggregated query.
                    self.diag(DiagKind::NotGrouped, Sp::default(), None);
                }
            }
            if let Some(h) = s.having {
                self.check_grouped(h, &keys);
            }
        }
        self.check_partition_filters(s);
        // Frame stays open for the caller's ORDER BY.
        (start, end)
    }

    fn group_leaf(&mut self, e: ExprId, keys: &mut Vec<ExprId>) {
        if let Expr::Tuple(items) = self.ast().exprs[e] {
            for &x in self.ast().expr_lists.slice(items) {
                self.expr(x, GROUP);
                keys.push(x);
            }
        } else {
            self.expr(e, GROUP);
            keys.push(e);
        }
    }

    fn select_has_aggregate(&self, start: u32, end: u32, having: Option<ExprId>) -> bool {
        (start..end)
            .filter_map(|i| self.s.bound.outs[i as usize].expr)
            .any(|e| self.contains_aggregate(e))
            || having.is_some_and(|h| self.contains_aggregate(h))
    }

    /// Does `e` contain a non-windowed aggregate call (outside subqueries)?
    fn contains_aggregate(&self, e: ExprId) -> bool {
        let mut found = false;
        crate::ir::walk::each_child(self.ast(), e, &mut |x| {
            if !found {
                if let Expr::Call(c) = self.ast().exprs[x] {
                    let call = self.ast().calls[c];
                    if call.over.is_none() && self.call_kind(call) == Some(Kind::Aggregate) {
                        found = true;
                        return false;
                    }
                }
            }
            !found
        });
        found
    }

    fn call_kind(&self, call: Call) -> Option<Kind> {
        let name = self.call_name(call);
        funcs::lookup(&name).map(|f| f.kind)
    }

    fn call_name(&self, call: Call) -> String {
        let parts = self.ast().names.slice(call.name);
        let mut s = String::new();
        for (i, n) in parts.iter().enumerate() {
            if i > 0 {
                s.push('.');
            }
            s.push_str(self.name_text(*n));
        }
        s
    }

    /// Columns outside aggregates must match a group key.
    fn check_grouped(&mut self, e: ExprId, keys: &[u64]) {
        let mut offenders: Vec<Sp> = Vec::new();
        let mut stack = vec![e];
        while let Some(x) = stack.pop() {
            if keys.contains(&crate::ir::fingerprint::expr_hash(self.p, &self.s.bound, x)) {
                continue;
            }
            match self.ast().exprs[x] {
                Expr::Call(c) => {
                    let call = self.ast().calls[c];
                    if call.over.is_none() && self.call_kind(call) == Some(Kind::Aggregate) {
                        continue;
                    }
                }
                Expr::Subquery(_)
                | Expr::Exists(_)
                | Expr::ArrayQuery(_)
                | Expr::InQuery { .. } => continue,
                _ => {}
            }
            match self.s.bound.refs[x.raw() as usize] {
                Ref::Column { .. }
                | Ref::Derived { .. }
                | Ref::Element { .. }
                | Ref::Offset { .. }
                | Ref::Opaque { .. } => {
                    offenders.push(self.expr_sp(x));
                    continue;
                }
                Ref::Alias { expr } => {
                    stack.push(expr);
                    continue;
                }
                _ => {}
            }
            crate::ir::walk::each_child(self.ast(), x, &mut |c| {
                if c != x {
                    stack.push(c);
                }
                c == x
            });
        }
        for sp in offenders {
            self.diag(DiagKind::NotGrouped, sp, None);
        }
    }

    /// BigQuery refuses to scan a `require_partition_filter` table unless a
    /// filter on its partition column is present.
    fn check_partition_filters(&mut self, s: Select) {
        let frame_rels: Vec<u32> = self.frame().rels.clone();
        for rel_id in frame_rels {
            let rel = self.s.bound.rels[rel_id as usize];
            let Some(tid) = rel.table else { continue };
            let Some(t) = self.cat.table(tid) else { continue };
            let (true, Some(part)) = (t.require_partition_filter, t.partition_col) else {
                continue;
            };
            let filtered = s.where_.is_some_and(|w| self.references(w, rel_id, part))
                || self.join_filters(s.from, rel_id, part);
            if !filtered {
                let sp = self.item_sp(rel.from);
                self.diag(
                    DiagKind::PartitionFilterRequired,
                    sp,
                    self.cat.column(part).map(|c| c.path.to_owned()),
                );
            }
        }
    }

    fn references(&self, e: ExprId, rel: u32, col: ColId) -> bool {
        let mut hit = false;
        crate::ir::walk::each_child(self.ast(), e, &mut |x| {
            if self.s.bound.refs[x.raw() as usize] == (Ref::Column { rel, col }) {
                hit = true;
            }
            !hit
        });
        hit
    }

    fn join_filters(&self, f: Option<FromId>, rel: u32, col: ColId) -> bool {
        let Some(f) = f else { return false };
        match self.ast().froms[f] {
            From::Join { l, r, cond, .. } => {
                matches!(cond, JoinCond::On(e) if self.references(e, rel, col))
                    || self.join_filters(Some(l), rel, col)
                    || self.join_filters(Some(r), rel, col)
            }
            From::Paren(i) => self.join_filters(Some(i), rel, col),
            _ => false,
        }
    }

    fn implicit_name(&self, e: ExprId) -> Option<Name> {
        match self.ast().exprs[e] {
            Expr::Ident(n) | Expr::Field { field: n, .. } => Some(n),
            _ => None,
        }
    }

    fn star(&mut self, qualifier: Option<ExprId>, except: Span<Name>, replace: Span<StructField>) {
        let rels: Vec<u32> = match qualifier {
            Some(q) => {
                // `t.*`: the qualifier must name a relation in scope.
                let Expr::Ident(n) = self.ast().exprs[q] else {
                    let sp = self.expr_sp(q);
                    self.diag(DiagKind::StarNotAllowed, sp, None);
                    return;
                };
                match self.find_rel(self.name_text(n)) {
                    Some(r) => {
                        self.set(q, Ty::scalar(TypeCode::Struct), Ref::Rel { rel: r });
                        vec![r]
                    }
                    None => {
                        let sug = self.rel_suggestion(self.name_text(n));
                        self.diag(DiagKind::UnknownName, n.sp, sug);
                        return;
                    }
                }
            }
            None => self.frame().rels.clone(),
        };
        let excluded: Vec<String> = self
            .ast()
            .names
            .slice(except)
            .iter()
            .map(|n| self.name_text(*n).to_ascii_lowercase())
            .collect();
        let replacements: &[StructField] = self.ast().struct_fields.slice(replace);
        for r in rels {
            let rel = self.s.bound.rels[r as usize];
            let mut cols: Vec<OutCol> = Vec::new();
            if let Some(t) = rel.table {
                for c in self.cat.columns(t) {
                    if c.depth == 0 && !c.hidden() {
                        cols.push(OutCol {
                            name: None,
                            col_name: Some(c.id),
                            ty: col_ty(&c),
                            expr: None,
                            union: None,
                        });
                    }
                }
            } else if rel.outs.1 > rel.outs.0 {
                cols.extend_from_slice(
                    &self.s.bound.outs[rel.outs.0 as usize..rel.outs.1 as usize],
                );
            } else if let Some(elem) = rel.elem {
                cols.push(OutCol {
                    name: rel.name,
                    col_name: None,
                    ty: elem,
                    expr: None,
                    union: None,
                });
            }
            for o in cols {
                let name = self.out_name(&o).map(str::to_ascii_lowercase).unwrap_or_default();
                if excluded.contains(&name) {
                    continue;
                }
                let replaced = replacements.iter().find(|f| {
                    f.alias.is_some_and(|a| self.name_text(a).eq_ignore_ascii_case(&name))
                });
                if let Some(f) = replaced.copied() {
                    let ty = self.expr(f.e, SELECT);
                    self.push_item(OutCol {
                        name: f.alias,
                        col_name: None,
                        ty,
                        expr: Some(f.e),
                        union: None,
                    });
                } else {
                    self.push_item(o);
                }
            }
        }
    }

    // ---- FROM ------------------------------------------------------------

    fn from(&mut self, f: FromId) {
        match self.ast().froms[f] {
            From::Table { path, alias, time_travel, offset, sample } => {
                self.table_item(f, path, alias, offset.and_then(|o| o.alias));
                for e in [time_travel, sample].into_iter().flatten() {
                    self.expr(e, WHERE);
                }
            }
            From::Subquery { q, alias } => {
                // A FROM subquery sees outer scopes (correlation) but opens its own.
                let outs = self.query(q);
                self.add_rel(Rel {
                    name: alias,
                    table: None,
                    outs,
                    elem: None,
                    offset: None,
                    opaque: false,
                    from: f,
                });
            }
            From::Unnest { e, alias, offset } => {
                let t = self.expr(e, WHERE);
                if !t.is_open() && t.code != TypeCode::Array {
                    let sp = self.expr_sp(e);
                    self.diag(DiagKind::TypeMismatch, sp, None);
                }
                let elem = t.element();
                self.add_rel(Rel {
                    name: alias,
                    table: None,
                    outs: (0, 0),
                    elem: Some(elem),
                    offset: offset.and_then(|o| o.alias),
                    opaque: false,
                    from: f,
                });
            }
            From::Join { l, r, cond, .. } => {
                self.from(l);
                self.from(r);
                match cond {
                    JoinCond::On(e) => {
                        let t = self.expr(e, WHERE);
                        self.expect_bool(e, t);
                    }
                    JoinCond::Using(cols) => {
                        for &n in self.ast().names.slice(cols) {
                            let name = self.name_text(n).to_owned();
                            let hits = self.columns_named(&name);
                            if hits < 2 {
                                self.diag(DiagKind::UsingColumn, n.sp, None);
                            }
                        }
                    }
                    JoinCond::None => {}
                }
            }
            From::Paren(inner) => self.from(inner),
        }
    }

    /// How many relations in the current frame expose a column `name`.
    fn columns_named(&self, name: &str) -> usize {
        let d = self.s.depth - 1;
        self.s.frames[d]
            .rels
            .iter()
            .filter(|&&r| !matches!(self.rel_column(r, name), Found::No))
            .count()
    }

    #[allow(clippy::too_many_lines)] // path forms (table, CTE, array path) in one place
    fn table_item(
        &mut self,
        f: FromId,
        path: Span<Name>,
        alias: Option<Name>,
        offset: Option<Name>,
    ) {
        let names: &[Name] = self.ast().names.slice(path);
        // Implicit UNNEST: `FROM o, o.items AS it`.
        if names.len() >= 2 && !names[0].quoted {
            if let Some(base) = self.find_rel(self.name_text(names[0])) {
                let rest: Vec<&str> = names[1..].iter().map(|n| self.name_text(*n)).collect();
                let ty = self.rel_path_ty(base, &rest.join("."));
                let elem = match ty {
                    Some(t) if t.code == TypeCode::Array => t.element(),
                    Some(t) if t.is_open() => Ty::UNKNOWN,
                    Some(_) => {
                        let sp = names[names.len() - 1].sp;
                        self.diag(DiagKind::TypeMismatch, sp, None);
                        Ty::UNKNOWN
                    }
                    None => {
                        let sp = names[1].sp;
                        self.diag(DiagKind::UnknownName, sp, None);
                        Ty::UNKNOWN
                    }
                };
                self.s.bound.froms[f.raw() as usize] = FromBind::ArrayPath;
                let name = alias.or(names.last().copied());
                self.add_rel(Rel {
                    name,
                    table: None,
                    outs: (0, 0),
                    elem: Some(elem),
                    offset,
                    opaque: false,
                    from: f,
                });
                return;
            }
        }
        // Split backtick paths (`p.d.t`) into parts.
        let mut parts: Vec<&str> = Vec::with_capacity(3);
        for n in names {
            let t = self.name_text(*n);
            if n.quoted {
                parts.extend(t.split('.'));
            } else {
                parts.push(t);
            }
        }
        let last = *names.last().expect("parser guarantees a name");
        let rel_name = Some(alias.unwrap_or(last));
        // CTE?
        if parts.len() == 1 {
            let found = self
                .s
                .ctes
                .iter()
                .rev()
                .find(|(n, _, _)| self.name_text(*n).eq_ignore_ascii_case(parts[0]))
                .map(|&(_, o, i)| (o, i));
            if let Some((outs, idx)) = found {
                self.s.bound.froms[f.raw() as usize] = FromBind::Cte(idx);
                self.add_rel(Rel {
                    name: rel_name,
                    table: None,
                    outs,
                    elem: None,
                    offset,
                    opaque: false,
                    from: f,
                });
                return;
            }
        }
        let (project, dataset, table) = match parts.as_slice() {
            [p, d, t] => (Some(*p), Some(*d), *t),
            [d, t] => (Some(self.opts.default_project), Some(*d), *t),
            [t] => (Some(self.opts.default_project), self.opts.default_dataset, *t),
            _ => (None, None, ""),
        };
        let tid = match (project, dataset) {
            (Some(p), Some(d)) => self.cat.find_table_in(p, d, table),
            _ => None,
        };
        match tid {
            Some(id) => {
                self.s.bound.froms[f.raw() as usize] = FromBind::Table(id);
                self.s.bound.tables.push(id);
                self.add_rel(Rel {
                    name: rel_name,
                    table: Some(id),
                    outs: (0, 0),
                    elem: None,
                    offset,
                    opaque: false,
                    from: f,
                });
            }
            None => {
                self.s.bound.froms[f.raw() as usize] = FromBind::Unknown;
                if self.strict() {
                    let sug = self.table_suggestion(dataset, table);
                    let sp = Sp { start: names[0].sp.start, end: last.sp.end };
                    self.diag(DiagKind::UnknownTable, sp, sug);
                }
                self.add_rel(Rel {
                    name: rel_name,
                    table: None,
                    outs: (0, 0),
                    elem: None,
                    offset,
                    opaque: true,
                    from: f,
                });
            }
        }
    }

    fn table_suggestion(&self, dataset: Option<&str>, table: &str) -> Option<String> {
        let names: Vec<String> = self
            .cat
            .tables()
            .filter(|t| dataset.is_none_or(|d| t.dataset_name == d))
            .map(|t| {
                if dataset.is_some() {
                    t.name.to_owned()
                } else {
                    format!("{}.{}", t.dataset_name, t.name)
                }
            })
            .collect();
        suggest(table, names.iter().map(String::as_str))
    }

    // ---- name resolution -------------------------------------------------

    /// Find a relation by name, innermost scope first.
    fn find_rel(&self, name: &str) -> Option<u32> {
        for d in (0..self.s.depth).rev() {
            for &r in self.s.frames[d].rels.iter().rev() {
                let rel = &self.s.bound.rels[r as usize];
                if rel.name.is_some_and(|n| self.name_text(n).eq_ignore_ascii_case(name)) {
                    return Some(r);
                }
            }
        }
        None
    }

    fn rel_suggestion(&self, name: &str) -> Option<String> {
        let mut cands: Vec<&str> = Vec::new();
        for d in 0..self.s.depth {
            for &r in &self.s.frames[d].rels {
                if let Some(n) = self.s.bound.rels[r as usize].name {
                    cands.push(self.name_text(n));
                }
            }
        }
        suggest(name, cands.into_iter())
    }

    /// Resolve `name` as a column of relation `r`.
    fn rel_column(&self, r: u32, name: &str) -> Found {
        let rel = &self.s.bound.rels[r as usize];
        if let Some(t) = rel.table {
            return match self.cat.find_column(t, name) {
                Some(c) => Found::Column(c),
                None => Found::No,
            };
        }
        if rel.opaque {
            return Found::Opaque;
        }
        if rel.outs.1 > rel.outs.0 {
            for i in rel.outs.0..rel.outs.1 {
                let o = self.s.bound.outs[i as usize];
                if self.out_name(&o).is_some_and(|n| n.eq_ignore_ascii_case(name)) {
                    return Found::Derived(i - rel.outs.0, o.ty);
                }
            }
            return Found::No;
        }
        if let Some(elem) = rel.elem {
            if rel.name.is_some_and(|n| self.name_text(n).eq_ignore_ascii_case(name)) {
                return Found::Element(elem);
            }
            if rel.offset.is_some_and(|n| self.name_text(n).eq_ignore_ascii_case(name)) {
                return Found::Offset;
            }
            // Fields of an UNNEST-ed struct are reachable unqualified.
            if let Some(c) = elem.col.and_then(|c| self.cat.column(c)) {
                let path = format!("{}.{}", c.path, name);
                if let Some(id) = self.cat.find_column(c.table, &path) {
                    return Found::Column(id);
                }
            }
        }
        Found::No
    }

    /// Type of a dotted path within relation `r` (for implicit UNNEST).
    fn rel_path_ty(&self, r: u32, path: &str) -> Option<Ty> {
        let (head, rest) = path.split_once('.').map_or((path, None), |(h, t)| (h, Some(t)));
        match self.rel_column(r, head) {
            Found::Column(c) => {
                let col = self.cat.column(c)?;
                match rest {
                    None => Some(col_ty(&col)),
                    Some(rest) => self
                        .cat
                        .find_column(col.table, &format!("{}.{rest}", col.path))
                        .and_then(|id| self.cat.column(id))
                        .map(|c| col_ty(&c)),
                }
            }
            Found::Opaque => Some(Ty::UNKNOWN),
            Found::Derived(_, t) | Found::Element(t) => {
                Some(if rest.is_some() { Ty::UNKNOWN } else { t })
            }
            Found::Offset => Some(Ty::scalar(TypeCode::Int64)),
            Found::No => None,
        }
    }

    /// Resolve a bare identifier.
    fn resolve_ident(&mut self, e: ExprId, n: Name, ctx: Ctx) -> Ty {
        let name = self.name_text(n).to_owned();
        // Innermost scope first; within a scope, ambiguity is an error.
        for d in (0..self.s.depth).rev() {
            let rels = self.s.frames[d].rels.clone();
            let mut hit: Option<(u32, Found)> = None;
            let mut ambiguous = false;
            for r in rels {
                let f = self.rel_column(r, &name);
                if !matches!(f, Found::No) {
                    if hit.is_some() && !matches!(f, Found::Opaque) {
                        ambiguous = true;
                    }
                    if hit.is_none() || matches!(hit, Some((_, Found::Opaque))) {
                        hit = Some((r, f));
                    }
                }
            }
            // A relation referenced by name as a value (`SELECT t FROM t`).
            if hit.is_none() {
                if let Some(r) = self.s.frames[d].rels.iter().copied().find(|&r| {
                    let rel = &self.s.bound.rels[r as usize];
                    rel.elem.is_none()
                        && rel.name.is_some_and(|x| self.name_text(x).eq_ignore_ascii_case(&name))
                }) {
                    return self.set(e, Ty::scalar(TypeCode::Struct), Ref::Rel { rel: r });
                }
            }
            if let Some((r, f)) = hit {
                if ambiguous {
                    self.diag(DiagKind::AmbiguousName, n.sp, None);
                }
                return self.bind_found(e, r, f);
            }
            // Select-list aliases of this scope.
            if ctx.aliases && d == self.s.depth - 1 {
                let alias = self.s.frames[d]
                    .aliases
                    .iter()
                    .find(|(a, _)| self.name_text(*a).eq_ignore_ascii_case(&name))
                    .map(|&(_, x)| x);
                if let Some(x) = alias {
                    let ty = self.s.bound.ty[x.raw() as usize];
                    return self.set(e, ty, Ref::Alias { expr: x });
                }
            }
        }
        if is_date_part(&name) {
            return self.set(e, Ty::UNKNOWN, Ref::DatePart);
        }
        let sug = self.name_suggestion(&name);
        self.diag(DiagKind::UnknownName, n.sp, sug);
        self.set(e, Ty::UNKNOWN, Ref::None)
    }

    fn bind_found(&mut self, e: ExprId, r: u32, f: Found) -> Ty {
        match f {
            Found::Column(c) => {
                self.s.bound.columns.push(c);
                let ty = self.cat.column(c).map_or(Ty::UNKNOWN, |c| col_ty(&c));
                self.set(e, ty, Ref::Column { rel: r, col: c })
            }
            Found::Derived(i, t) => self.set(e, t, Ref::Derived { rel: r, index: i }),
            Found::Element(t) => self.set(e, t, Ref::Element { rel: r }),
            Found::Offset => self.set(e, Ty::scalar(TypeCode::Int64), Ref::Offset { rel: r }),
            Found::Opaque => self.set(e, Ty::UNKNOWN, Ref::Opaque { rel: r }),
            Found::No => self.set(e, Ty::UNKNOWN, Ref::None),
        }
    }

    fn name_suggestion(&self, name: &str) -> Option<String> {
        let mut cands: Vec<String> = Vec::new();
        for d in 0..self.s.depth {
            for &r in &self.s.frames[d].rels {
                let rel = self.s.bound.rels[r as usize];
                if let Some(t) = rel.table {
                    cands.extend(
                        self.cat.columns(t).filter(|c| c.depth == 0).map(|c| c.path.to_owned()),
                    );
                }
                for i in rel.outs.0..rel.outs.1 {
                    let o = self.s.bound.outs[i as usize];
                    if let Some(n) = self.out_name(&o) {
                        cands.push(n.to_owned());
                    }
                }
                if let Some(n) = rel.name {
                    cands.push(self.name_text(n).to_owned());
                }
            }
            for (a, _) in &self.s.frames[d].aliases {
                cands.push(self.name_text(*a).to_owned());
            }
        }
        suggest(name, cands.iter().map(String::as_str))
    }

    /// Resolve `base.field...` chains: relation-qualified columns, nested
    /// STRUCT fields, and fields of UNNEST-ed elements.
    fn resolve_field(&mut self, e: ExprId, ctx: Ctx) -> Ty {
        let mut parts: Vec<Name> = Vec::with_capacity(4);
        let mut cur = e;
        let head = loop {
            match self.ast().exprs[cur] {
                Expr::Field { base, field } => {
                    parts.push(field);
                    cur = base;
                }
                Expr::Ident(n) => break Some(n),
                _ => break None,
            }
        };
        parts.reverse();
        let Some(head) = head else {
            // Field of a computed value: bind the base, then navigate its type.
            let Expr::Field { base, field } = self.ast().exprs[e] else { unreachable!() };
            let bt = self.expr(base, ctx);
            let t = self.field_of(bt, field);
            return self.set(e, t, Ref::None);
        };
        let head_text = self.name_text(head).to_owned();
        // 1. Relation-qualified: `o.status`, `o.addr.city`, `it.sku`.
        if let Some(r) = self.find_rel(&head_text) {
            let rel = self.s.bound.rels[r as usize];
            if rel.elem.is_none_or(|t| t.col.is_some() || t.code != TypeCode::Struct) {
                let path: Vec<&str> = parts.iter().map(|n| self.name_text(*n)).collect();
                let joined = path.join(".");
                if rel.elem.is_none() {
                    let found = self.rel_column(r, &joined);
                    let found = match found {
                        Found::No if path.len() > 1 => self.rel_column(r, path[0]),
                        f => f,
                    };
                    if !matches!(found, Found::No) {
                        // A catalogued path that runs through an ARRAY needs UNNEST.
                        if let Found::Column(c) = found {
                            if self.cat.column(c).is_some_and(|col| col.in_array()) {
                                let at = parts.get(1).map_or(parts[0].sp, |n| n.sp);
                                self.diag(DiagKind::NeedsUnnest, at, None);
                                return self.set(e, Ty::UNKNOWN, Ref::None);
                            }
                        }
                        let t = self.bind_found(e, r, found);
                        return self.navigate_rest(e, t, &parts, &joined, found);
                    }
                    let sug = self.rel_columns_suggestion(r, path[0]);
                    self.diag(DiagKind::UnknownName, parts[0].sp, sug);
                    return self.set(e, Ty::UNKNOWN, Ref::None);
                }
                // UNNEST element with a catalog origin: `it.sku`.
                let elem = rel.elem.unwrap_or(Ty::UNKNOWN);
                let t = self.navigate(elem, &parts);
                return self.set(e, t, Ref::Element { rel: r });
            }
        }
        // 2. A column (or element) with nested fields: `addr.city`.
        let head_e = self.first_ident(e);
        let bt = self.resolve_ident(head_e, head, ctx);
        let t = self.navigate(bt, &parts);
        let r = self.s.bound.refs[head_e.raw() as usize];
        let r = match (r, t.col) {
            (Ref::Column { rel, .. }, Some(col)) => {
                self.s.bound.columns.push(col);
                Ref::Column { rel, col }
            }
            (other, _) => other,
        };
        self.set(e, t, r)
    }

    /// When `rel.path` resolved only by its first segment (a STRUCT column
    /// whose sub-path is not catalogued), navigate the rest by type.
    fn navigate_rest(
        &mut self,
        e: ExprId,
        t: Ty,
        parts: &[Name],
        joined: &str,
        found: Found,
    ) -> Ty {
        if let Found::Column(c) = found {
            if self.cat.column(c).is_some_and(|col| !col.path.eq_ignore_ascii_case(joined)) {
                let t2 = self.navigate(t, &parts[1..]);
                let r = self.s.bound.refs[e.raw() as usize];
                return self.set(e, t2, r);
            }
        }
        t
    }

    fn first_ident(&self, mut e: ExprId) -> ExprId {
        while let Expr::Field { base, .. } = self.ast().exprs[e] {
            e = base;
        }
        e
    }

    fn rel_columns_suggestion(&self, r: u32, name: &str) -> Option<String> {
        let rel = self.s.bound.rels[r as usize];
        let mut cands: Vec<String> = Vec::new();
        if let Some(t) = rel.table {
            cands.extend(self.cat.columns(t).map(|c| c.path.to_owned()));
        }
        for i in rel.outs.0..rel.outs.1 {
            let o = self.s.bound.outs[i as usize];
            if let Some(n) = self.out_name(&o) {
                cands.push(n.to_owned());
            }
        }
        suggest(name, cands.iter().map(String::as_str))
    }

    /// Navigate `fields` from a value of type `t`.
    fn navigate(&mut self, mut t: Ty, fields: &[Name]) -> Ty {
        for &f in fields {
            t = self.field_of(t, f);
        }
        t
    }

    fn field_of(&mut self, t: Ty, field: Name) -> Ty {
        if t.is_open() || t.code == TypeCode::Json {
            return if t.code == TypeCode::Json { t } else { Ty::UNKNOWN };
        }
        if t.code == TypeCode::Array {
            self.diag(DiagKind::NeedsUnnest, field.sp, None);
            return Ty::UNKNOWN;
        }
        if t.code != TypeCode::Struct {
            self.diag(DiagKind::UnknownField, field.sp, None);
            return Ty::UNKNOWN;
        }
        let Some(c) = t.col.and_then(|c| self.cat.column(c)) else { return Ty::UNKNOWN };
        let name = self.name_text(field);
        let path = format!("{}.{name}", c.path);
        match self.cat.find_column(c.table, &path).and_then(|id| self.cat.column(id)) {
            Some(child) => {
                self.s.bound.columns.push(child.id);
                col_ty(&child)
            }
            None => {
                let prefix = format!("{}.", c.path.to_ascii_lowercase());
                let cands: Vec<&str> = self
                    .cat
                    .columns(c.table)
                    .filter(|x| {
                        x.depth == c.depth + 1 && x.path.to_ascii_lowercase().starts_with(&prefix)
                    })
                    .map(|x| x.leaf())
                    .collect();
                let sug = suggest(name, cands.into_iter());
                self.diag(DiagKind::UnknownField, field.sp, sug);
                Ty::UNKNOWN
            }
        }
    }

    // ---- expressions -----------------------------------------------------

    fn expr_sp(&self, e: ExprId) -> Sp {
        crate::ir::walk::span(self.p, e)
    }

    fn item_sp(&self, f: FromId) -> Sp {
        match self.ast().froms[f] {
            From::Table { path, .. } => {
                let names = self.ast().names.slice(path);
                match (names.first(), names.last()) {
                    (Some(a), Some(b)) => Sp { start: a.sp.start, end: b.sp.end },
                    _ => Sp::default(),
                }
            }
            _ => Sp::default(),
        }
    }

    fn expect_bool(&mut self, e: ExprId, t: Ty) {
        if !t.is_open() && t.code != TypeCode::Bool {
            let sp = self.expr_sp(e);
            self.diag(DiagKind::NotBoolean, sp, None);
        }
    }

    fn is_string_literal(&self, e: ExprId) -> bool {
        matches!(self.ast().exprs[e], Expr::Lit { kind: LitKind::Str, .. })
    }

    fn compare(&mut self, e: ExprId, a: ExprId, b: ExprId, ta: Ty, tb: Ty) {
        // BigQuery defines no equality or ordering on these types.
        let never = |t: Ty| match t.code {
            TypeCode::Geography if !t.is_open() => Some("GEOGRAPHY"),
            TypeCode::Json if !t.is_open() => Some("JSON"),
            TypeCode::Array if !t.is_open() => Some("ARRAY"),
            _ => None,
        };
        if let Some(ty) = never(ta).or_else(|| never(tb)) {
            let sp = self.expr_sp(e);
            self.diag(DiagKind::NotComparable(ty), sp, None);
            return;
        }
        if !comparable(ta, tb, self.is_string_literal(a), self.is_string_literal(b)) {
            let sp = self.expr_sp(e);
            self.diag(DiagKind::TypeMismatch, sp, None);
        }
    }

    #[allow(clippy::too_many_lines)] // the expression table
    fn expr(&mut self, e: ExprId, ctx: Ctx) -> Ty {
        let node = self.ast().exprs[e];
        match node {
            Expr::Lit { kind, sp } => {
                let t = match kind {
                    LitKind::Int => Ty::scalar(TypeCode::Int64),
                    LitKind::Float => Ty::scalar(TypeCode::Float64),
                    LitKind::Str => Ty::scalar(TypeCode::String),
                    LitKind::Bytes => Ty::scalar(TypeCode::Bytes),
                    LitKind::Bool(_) => Ty::scalar(TypeCode::Bool),
                    LitKind::Null => Ty::NULL,
                    LitKind::Typed(_) => from_type_name(self.text(sp)),
                };
                self.set(e, t, Ref::None)
            }
            Expr::Ident(n) => self.resolve_ident(e, n, ctx),
            Expr::Field { .. } => self.resolve_field(e, ctx),
            Expr::Param(_) | Expr::SysVar(_) | Expr::Positional(_) | Expr::Star => {
                self.set(e, Ty::UNKNOWN, Ref::None)
            }
            Expr::Unary { op, e: x } => {
                let t = self.expr(x, ctx);
                let out = match op {
                    UnOp::Not => {
                        self.expect_bool(x, t);
                        Ty::scalar(TypeCode::Bool)
                    }
                    UnOp::Neg | UnOp::Plus => {
                        if !(t.numeric_or_open() || t.code == TypeCode::Interval) {
                            let sp = self.expr_sp(e);
                            self.diag(DiagKind::NotNumeric, sp, None);
                        }
                        Ty { col: None, ..t }
                    }
                    UnOp::BitNot => Ty { col: None, ..t },
                };
                self.set(e, out, Ref::None)
            }
            Expr::Binary { op, l, r } => {
                let (tl, tr) = (self.expr(l, ctx), self.expr(r, ctx));
                let out = match op {
                    BinOp::And | BinOp::Or => {
                        self.expect_bool(l, tl);
                        self.expect_bool(r, tr);
                        Ty::scalar(TypeCode::Bool)
                    }
                    BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
                        self.compare(e, l, r, tl, tr);
                        Ty::scalar(TypeCode::Bool)
                    }
                    BinOp::Add | BinOp::Sub => self.additive(e, op, tl, tr),
                    BinOp::Mul => self.arith(e, tl, tr, false),
                    BinOp::Div => self.arith(e, tl, tr, true),
                    BinOp::Concat => {
                        if tl.is_open() {
                            tr
                        } else {
                            Ty { col: None, ..tl }
                        }
                    }
                    BinOp::BitAnd | BinOp::BitOr | BinOp::BitXor | BinOp::Shl | BinOp::Shr => {
                        Ty { col: None, ..tl }
                    }
                };
                self.set(e, out, Ref::None)
            }
            Expr::Like { e: x, pats, .. } => {
                let t = self.expr(x, ctx);
                if !(t.is_open() || matches!(t.code, TypeCode::String | TypeCode::Bytes)) {
                    let sp = self.expr_sp(e);
                    self.diag(DiagKind::TypeMismatch, sp, None);
                }
                for &p in self.ast().expr_lists.slice(pats) {
                    self.expr(p, ctx);
                }
                self.set(e, Ty::scalar(TypeCode::Bool), Ref::None)
            }
            Expr::Between { e: x, lo, hi, .. } => {
                let (t, a, b) = (self.expr(x, ctx), self.expr(lo, ctx), self.expr(hi, ctx));
                self.compare(e, x, lo, t, a);
                self.compare(e, x, hi, t, b);
                self.set(e, Ty::scalar(TypeCode::Bool), Ref::None)
            }
            Expr::InList { e: x, list, .. } => {
                let t = self.expr(x, ctx);
                for &i in self.ast().expr_lists.slice(list) {
                    let ti = self.expr(i, ctx);
                    self.compare(e, x, i, t, ti);
                }
                self.set(e, Ty::scalar(TypeCode::Bool), Ref::None)
            }
            Expr::InQuery { e: x, q, .. } => {
                self.expr(x, ctx);
                let outs = self.subquery(q);
                if outs.1 - outs.0 != 1 {
                    let sp = self.expr_sp(e);
                    self.diag(DiagKind::SubqueryColumns, sp, None);
                }
                self.set(e, Ty::scalar(TypeCode::Bool), Ref::None)
            }
            Expr::InUnnest { e: x, arr, .. } => {
                let t = self.expr(x, ctx);
                let ta = self.expr(arr, ctx);
                if !ta.is_open() && ta.code != TypeCode::Array {
                    let sp = self.expr_sp(arr);
                    self.diag(DiagKind::TypeMismatch, sp, None);
                } else if !comparable(t, ta.element(), false, false) {
                    let sp = self.expr_sp(e);
                    self.diag(DiagKind::TypeMismatch, sp, None);
                }
                self.set(e, Ty::scalar(TypeCode::Bool), Ref::None)
            }
            Expr::Is { e: x, .. } => {
                self.expr(x, ctx);
                self.set(e, Ty::scalar(TypeCode::Bool), Ref::None)
            }
            Expr::IsDistinct { l, r, .. } => {
                let (a, b) = (self.expr(l, ctx), self.expr(r, ctx));
                self.compare(e, l, r, a, b);
                self.set(e, Ty::scalar(TypeCode::Bool), Ref::None)
            }
            Expr::Call(c) => self.call(e, c, ctx),
            Expr::Cast { e: x, ty, format, .. } => {
                self.expr(x, ctx);
                if let Some(f) = format {
                    self.expr(f, ctx);
                }
                let t = self.type_node(ty);
                self.set(e, t, Ref::None)
            }
            Expr::Extract { part, e: x, tz } => {
                self.date_part(part);
                self.expr(x, ctx);
                if let Some(z) = tz {
                    self.expr(z, ctx);
                }
                let p = self.part_text(part).to_ascii_uppercase();
                let t = match p.as_str() {
                    "DATE" => Ty::scalar(TypeCode::Date),
                    "TIME" => Ty::scalar(TypeCode::Time),
                    "DATETIME" => Ty::scalar(TypeCode::DateTime),
                    _ => Ty::scalar(TypeCode::Int64),
                };
                self.set(e, t, Ref::None)
            }
            Expr::Case { operand, arms, else_ } => {
                let to = operand.map(|o| self.expr(o, ctx));
                let mut result = Ty::UNKNOWN;
                for &arm in self.ast().arms.slice(arms) {
                    let tw = self.expr(arm.when, ctx);
                    match (operand, to) {
                        (Some(o), Some(t)) => self.compare(arm.when, o, arm.when, t, tw),
                        _ => self.expect_bool(arm.when, tw),
                    }
                    let tt = self.expr(arm.then, ctx);
                    result = widen(result, tt);
                }
                if let Some(x) = else_ {
                    let tx = self.expr(x, ctx);
                    result = widen(result, tx);
                }
                self.set(e, result, Ref::None)
            }
            Expr::Array { ty, elems } => {
                let mut elem = Ty::UNKNOWN;
                for &x in self.ast().expr_lists.slice(elems) {
                    let t = self.expr(x, ctx);
                    elem = widen(elem, t);
                }
                let t = match ty {
                    Some(t) => self.type_node(t),
                    None => Ty::array(elem.code),
                };
                self.set(e, t, Ref::None)
            }
            Expr::ArrayQuery(q) => {
                let outs = self.subquery(q);
                let t = if outs.1 - outs.0 == 1 {
                    Ty::array(self.s.bound.outs[outs.0 as usize].ty.code)
                } else {
                    Ty::array(TypeCode::Struct)
                };
                self.set(e, t, Ref::None)
            }
            Expr::Struct { fields, .. } => {
                for &f in self.ast().struct_fields.slice(fields) {
                    self.expr(f.e, ctx);
                }
                self.set(e, Ty::scalar(TypeCode::Struct), Ref::None)
            }
            Expr::Tuple(items) => {
                for &x in self.ast().expr_lists.slice(items) {
                    self.expr(x, ctx);
                }
                self.set(e, Ty::scalar(TypeCode::Struct), Ref::None)
            }
            Expr::Index { e: x, idx, mode } => {
                let t = self.expr(x, ctx);
                let ti = self.expr(idx, ctx);
                let out = match (t.code, mode) {
                    (TypeCode::Array, _) => {
                        if !ti.numeric_or_open() {
                            let sp = self.expr_sp(idx);
                            self.diag(DiagKind::TypeMismatch, sp, None);
                        }
                        t.element()
                    }
                    (TypeCode::Json, IndexMode::Plain) => t,
                    _ if t.is_open() => Ty::UNKNOWN,
                    _ => {
                        let sp = self.expr_sp(e);
                        self.diag(DiagKind::TypeMismatch, sp, None);
                        Ty::UNKNOWN
                    }
                };
                self.set(e, out, Ref::None)
            }
            Expr::Exists(q) => {
                self.subquery(q);
                self.set(e, Ty::scalar(TypeCode::Bool), Ref::None)
            }
            Expr::Subquery(q) => {
                let outs = self.subquery(q);
                let t = if outs.1 - outs.0 == 1 {
                    Ty { col: None, ..self.s.bound.outs[outs.0 as usize].ty }
                } else {
                    let sp = self.expr_sp(e);
                    self.diag(DiagKind::SubqueryColumns, sp, None);
                    Ty::UNKNOWN
                };
                self.set(e, t, Ref::None)
            }
            Expr::Interval { e: x, .. } => {
                self.expr(x, ctx);
                self.set(e, Ty::scalar(TypeCode::Interval), Ref::None)
            }
            Expr::Paren(x) => {
                let t = self.expr(x, ctx);
                let r = self.s.bound.refs[x.raw() as usize];
                self.set(e, t, r)
            }
        }
    }

    /// Subqueries in expressions see outer scopes (correlation).
    fn subquery(&mut self, q: QueryId) -> (u32, u32) {
        self.query(q)
    }

    fn additive(&mut self, e: ExprId, op: BinOp, tl: Ty, tr: Ty) -> Ty {
        if tl.is_open() || tr.is_open() {
            return if tl.is_open() { Ty { col: None, ..tr } } else { Ty { col: None, ..tl } };
        }
        let (a, b) = (tl.code, tr.code);
        if a.is_numeric() && b.is_numeric() {
            return Ty::scalar(numeric_super(a, b));
        }
        // DATE ± INT64 / INTERVAL, TIMESTAMP ± INTERVAL, TIMESTAMP - TIMESTAMP → INTERVAL.
        if a.is_temporal()
            && (b == TypeCode::Interval || (a == TypeCode::Date && b == TypeCode::Int64))
        {
            return Ty::scalar(a);
        }
        if op == BinOp::Add && b.is_temporal() && a == TypeCode::Interval {
            return Ty::scalar(b);
        }
        if op == BinOp::Sub && a == b && a.is_temporal() {
            return Ty::scalar(TypeCode::Interval);
        }
        if a == TypeCode::Interval && b == TypeCode::Interval {
            return Ty::scalar(TypeCode::Interval);
        }
        let sp = self.expr_sp(e);
        self.diag(DiagKind::TypeMismatch, sp, None);
        Ty::UNKNOWN
    }

    fn arith(&mut self, e: ExprId, tl: Ty, tr: Ty, div: bool) -> Ty {
        if tl.is_open() || tr.is_open() {
            return Ty::UNKNOWN;
        }
        let (a, b) = (tl.code, tr.code);
        if a.is_numeric() && b.is_numeric() {
            if div && a == TypeCode::Int64 && b == TypeCode::Int64 {
                return Ty::scalar(TypeCode::Float64);
            }
            return Ty::scalar(numeric_super(a, b));
        }
        if (a == TypeCode::Interval && b == TypeCode::Int64)
            || (!div && a == TypeCode::Int64 && b == TypeCode::Interval)
        {
            return Ty::scalar(TypeCode::Interval);
        }
        let sp = self.expr_sp(e);
        self.diag(DiagKind::TypeMismatch, sp, None);
        Ty::UNKNOWN
    }

    fn part_text(&self, part: ExprId) -> &str {
        match self.ast().exprs[part] {
            Expr::Ident(n) => self.name_text(n),
            Expr::Call(c) => self
                .ast()
                .names
                .slice(self.ast().calls[c].name)
                .first()
                .map_or("", |n| self.name_text(*n)),
            _ => "",
        }
    }

    /// A date-part argument (`MONTH`, `WEEK(MONDAY)`): never a column.
    fn date_part(&mut self, part: ExprId) -> bool {
        let is_part = match self.ast().exprs[part] {
            Expr::Ident(n) => is_date_part(self.name_text(n)),
            Expr::Call(c) => {
                let call = self.ast().calls[c];
                let names = self.ast().names.slice(call.name);
                names.len() == 1
                    && ["WEEK"].iter().any(|w| w.eq_ignore_ascii_case(self.name_text(names[0])))
            }
            _ => false,
        };
        if is_part {
            self.set(part, Ty::UNKNOWN, Ref::DatePart);
            if let Expr::Call(c) = self.ast().exprs[part] {
                for &a in self.ast().args.slice(self.ast().calls[c].args) {
                    self.set(a.e, Ty::UNKNOWN, Ref::DatePart);
                }
            }
        }
        is_part
    }

    fn call(&mut self, e: ExprId, c: Idx<Call>, ctx: Ctx) -> Ty {
        let call = self.ast().calls[c];
        let name = self.call_name(call);
        let args: &[Arg] = self.ast().args.slice(call.args);
        let name_sp = {
            let ns = self.ast().names.slice(call.name);
            Sp {
                start: ns.first().map_or(0, |n| n.sp.start),
                end: ns.last().map_or(0, |n| n.sp.end),
            }
        };
        let Some(f) = funcs::lookup(&name) else {
            if self.strict() {
                let sug = suggest(&name.to_ascii_uppercase(), funcs::NAMES.iter().copied());
                self.diag(DiagKind::UnknownFunction, name_sp, sug);
            }
            for a in args {
                if !self.date_part(a.e) {
                    self.expr(a.e, ctx);
                }
            }
            return self.set(e, Ty::UNKNOWN, Ref::None);
        };
        let n_args = args.len();
        if n_args < usize::from(f.min) || (f.max != u8::MAX && n_args > usize::from(f.max)) {
            self.diag(
                DiagKind::ArgCount { min: f.min, max: f.max, got: n_args.min(255) as u8 },
                name_sp,
                None,
            );
        }
        // Kind rules.
        let windowed = call.over.is_some();
        let arg_ctx = match f.kind {
            Kind::Aggregate if !windowed => {
                if ctx.in_agg {
                    self.diag(DiagKind::NestedAggregate, name_sp, None);
                } else if !ctx.allow_agg {
                    self.diag(DiagKind::AggregateNotAllowed, name_sp, None);
                }
                self.s.bound.aggregates = true;
                Ctx { allow_agg: false, allow_window: false, in_agg: true, aliases: false }
            }
            Kind::Aggregate | Kind::Window => {
                if f.kind == Kind::Window && !windowed {
                    self.diag(DiagKind::WindowNeedsOver, name_sp, None);
                }
                if windowed && !ctx.allow_window {
                    self.diag(DiagKind::WindowNotAllowed, name_sp, None);
                }
                Ctx {
                    allow_agg: ctx.allow_agg,
                    allow_window: false,
                    in_agg: ctx.in_agg,
                    aliases: ctx.aliases,
                }
            }
            Kind::Scalar => {
                if windowed {
                    self.diag(DiagKind::OverNotAllowed, name_sp, None);
                }
                ctx
            }
        };
        self.s.tmp_tys.clear();
        let mut tys: Vec<Ty> = std::mem::take(&mut self.s.tmp_tys);
        for a in args {
            let t = if matches!(self.ast().exprs[a.e], Expr::Star) {
                if f.name != "COUNT" {
                    let sp = self.expr_sp(a.e);
                    self.diag(DiagKind::StarNotAllowed, sp, None);
                }
                Ty::UNKNOWN
            } else if self.date_part(a.e) {
                Ty::UNKNOWN
            } else {
                self.expr(a.e, arg_ctx)
            };
            tys.push(t);
        }
        if f.numeric_arg {
            if let (Some(&t), Some(a)) = (tys.first(), args.first()) {
                if !(t.numeric_or_open()
                    || (f.name == "SUM" || f.name == "AVG") && t.code == TypeCode::Interval)
                {
                    let sp = self.expr_sp(a.e);
                    self.diag(DiagKind::NotNumeric, sp, None);
                }
            }
        }
        for &o in self.ast().order_items.slice(call.order) {
            self.expr(o.e, arg_ctx);
        }
        if let Some(l) = call.limit {
            self.expr(l, WHERE);
        }
        if let Some(w) = call.over {
            self.window(w, ctx);
        }
        let t = funcs::result_type(&f, &tys);
        self.s.tmp_tys = tys;
        self.set(e, t, Ref::None)
    }

    fn window(&mut self, w: Idx<WindowSpec>, ctx: Ctx) {
        let spec = self.ast().windows[w];
        // Window specs see select-list aliases where the enclosing clause does
        // (QUALIFY ... OVER (ORDER BY alias)).
        let wctx = Ctx {
            allow_agg: ctx.allow_agg,
            allow_window: false,
            in_agg: false,
            aliases: ctx.aliases,
        };
        for &x in self.ast().expr_lists.slice(spec.partition) {
            self.expr(x, wctx);
        }
        for &o in self.ast().order_items.slice(spec.order) {
            self.expr(o.e, wctx);
        }
        if let Some(fr) = spec.frame {
            for b in [Some(fr.start), fr.end].into_iter().flatten() {
                if let FrameBound::Preceding(x) | FrameBound::Following(x) = b {
                    self.expr(x, WHERE);
                }
            }
        }
    }

    fn type_node(&mut self, t: TypeId) -> Ty {
        match self.ast().types[t] {
            TypeNode::Named { name, .. } => from_type_name(self.name_text(name)),
            TypeNode::Array(inner) => {
                let i = self.type_node(inner);
                Ty::array(i.code)
            }
            TypeNode::Struct(_) => Ty::scalar(TypeCode::Struct),
            TypeNode::Range(_) => Ty::scalar(TypeCode::Range),
        }
    }
}

/// A column's value type (arrays keep their element class and origin).
fn col_ty(c: &crate::catalog::ColumnRef<'_>) -> Ty {
    if c.code == TypeCode::Array {
        let elem = crate::catalog::types::array_element(c.data_type)
            .map_or(TypeCode::Unknown, TypeCode::of);
        Ty { col: Some(c.id), ..Ty::array(elem) }
    } else {
        Ty { col: Some(c.id), ..Ty::scalar(c.code) }
    }
}

/// Supertype for CASE / ARRAY / COALESCE-like merges.
fn widen(a: Ty, b: Ty) -> Ty {
    if a.is_open() {
        return Ty { col: None, ..b };
    }
    if b.is_open() {
        return a;
    }
    if a.code.is_numeric() && b.code.is_numeric() {
        return Ty::scalar(numeric_super(a.code, b.code));
    }
    Ty { col: None, ..a }
}

/// Result of resolving a column name in one relation.
#[derive(Clone, Copy, Debug)]
enum Found {
    No,
    Column(ColId),
    Derived(u32, Ty),
    Element(Ty),
    Offset,
    Opaque,
}
