// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Canonical GoogleSQL text from a parsed tree.
//!
//! Keywords upper-case, single spaces, `, ` separators, and parentheses only
//! where precedence requires them (plus those written in the source, which
//! the tree keeps). Identifiers and literals are copied from the source
//! verbatim, so emission never changes meaning. Two statements that differ
//! only in layout, keyword case or comments emit byte-identical text — which
//! is what makes BigQuery's result cache hit for re-issued queries.
//!
//! `emit(parse(emit(parse(s)))) == emit(parse(s))` for every accepted `s`
//! (property-tested).

use crate::ast::*;
use crate::parser::Parsed;
use std::fmt::Write as _;

/// Emission hooks: let a caller that knows more than the syntax (the
/// binder) substitute names — fully-qualified tables, canonical aliases,
/// qualified column references. Static dispatch; [`NoHooks`] is free.
pub trait Hooks {
    /// Write expression `e` yourself and return `true`, or return `false`
    /// for default emission.
    fn expr(&mut self, _e: ExprId, _out: &mut String) -> bool {
        false
    }
    /// Write the table path of FROM item `f` and return `true`, or `false`.
    fn table_path(&mut self, _f: FromId, _out: &mut String) -> bool {
        false
    }
    /// Write the (possibly dotted) name of call `c` and return `true`, or
    /// `false` to keep the written name.
    fn call_name(&mut self, _c: crab_core::Idx<Call>, _out: &mut String) -> bool {
        false
    }
    /// Write the `AS alias` of FROM item `f` (including the leading space)
    /// and return `true`, or `false` to keep the written alias.
    fn table_alias(&mut self, _f: FromId, _out: &mut String) -> bool {
        false
    }
}

/// No substitutions.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoHooks;

impl Hooks for NoHooks {}

/// Emit `parsed` as canonical GoogleSQL into `out` (appending).
pub fn emit(parsed: &Parsed<'_>, out: &mut String) {
    emit_with(parsed, &mut NoHooks, out);
}

/// Emit with substitutions from `hooks`.
pub fn emit_with<H: Hooks>(parsed: &Parsed<'_>, hooks: &mut H, out: &mut String) {
    Emitter { p: parsed, out, h: hooks }.query(parsed.root);
}

/// Emit into a fresh string.
#[must_use]
pub fn to_sql(parsed: &Parsed<'_>) -> String {
    let mut s = String::with_capacity(parsed.src.len() + 16);
    emit(parsed, &mut s);
    s
}

struct Emitter<'a, 'p, H: Hooks> {
    p: &'a Parsed<'p>,
    out: &'a mut String,
    h: &'a mut H,
}

/// Precedence of an expression for parenthesisation (atoms = 12).
fn prec(e: &Expr) -> u8 {
    match e {
        Expr::Binary { op, .. } => op.precedence(),
        Expr::Like { .. }
        | Expr::Between { .. }
        | Expr::InList { .. }
        | Expr::InQuery { .. }
        | Expr::InUnnest { .. }
        | Expr::Is { .. }
        | Expr::IsDistinct { .. } => 4,
        Expr::Unary { op: UnOp::Not, .. } => 3,
        Expr::Unary { .. } => 11,
        _ => 12,
    }
}

impl<'p, H: Hooks> Emitter<'_, 'p, H> {
    /// The pools, borrowed for `'p` (not `&self`), so emission can walk them
    /// while appending to `out`.
    fn ast(&self) -> &'p Ast {
        self.p.ast
    }

    fn text(&self, sp: crate::Sp) -> &'p str {
        sp.text(self.p.src)
    }

    fn s(&mut self, s: &str) {
        self.out.push_str(s);
    }

    fn name(&mut self, n: Name) {
        let text = self.text(n.sp);
        if n.quoted {
            self.out.push('`');
            self.out.push_str(text);
            self.out.push('`');
        } else {
            self.out.push_str(text);
        }
    }

    fn names(&mut self, names: crab_core::Span<Name>, sep: &str) {
        let list = self.ast().names.slice(names);
        for (i, &n) in list.iter().enumerate() {
            if i > 0 {
                self.s(sep);
            }
            self.name(n);
        }
    }

    fn expr_list(&mut self, list: crab_core::Span<ExprId>) {
        let items = self.ast().expr_lists.slice(list);
        for (i, &e) in items.iter().enumerate() {
            if i > 0 {
                self.s(", ");
            }
            self.expr(e);
        }
    }

    // ---- queries ---------------------------------------------------------

    fn query(&mut self, q: QueryId) {
        let q = self.ast().queries[q];
        if !q.ctes.is_empty() {
            self.s(if q.recursive { "WITH RECURSIVE " } else { "WITH " });
            let ctes = self.ast().ctes.slice(q.ctes);
            for (i, c) in ctes.iter().enumerate() {
                if i > 0 {
                    self.s(", ");
                }
                self.name(c.name);
                self.s(" AS (");
                self.query(c.q);
                self.s(")");
            }
            self.s(" ");
        }
        self.set_expr(q.body);
        self.order_by(q.order);
        if let Some(l) = q.limit {
            self.s(" LIMIT ");
            self.expr(l);
            if let Some(o) = q.offset {
                self.s(" OFFSET ");
                self.expr(o);
            }
        }
    }

    fn order_by(&mut self, order: crab_core::Span<OrderItem>) {
        if !order.is_empty() {
            self.s(" ORDER BY ");
            self.order_items(order);
        }
    }

    fn order_items(&mut self, order: crab_core::Span<OrderItem>) {
        let items = self.ast().order_items.slice(order);
        for (i, it) in items.iter().enumerate() {
            if i > 0 {
                self.s(", ");
            }
            self.expr(it.e);
            match it.desc {
                Some(true) => self.s(" DESC"),
                Some(false) => self.s(" ASC"),
                None => {}
            }
            match it.nulls_first {
                Some(true) => self.s(" NULLS FIRST"),
                Some(false) => self.s(" NULLS LAST"),
                None => {}
            }
        }
    }

    fn set_expr(&mut self, s: crab_core::Idx<SetExpr>) {
        match self.ast().set_exprs[s] {
            SetExpr::Select(sel) => self.select(sel),
            SetExpr::Paren(q) => {
                self.s("(");
                self.query(q);
                self.s(")");
            }
            SetExpr::SetOp { op, all, l, r } => {
                self.set_expr(l);
                self.s(match op {
                    SetOp::Union => " UNION ",
                    SetOp::Intersect => " INTERSECT ",
                    SetOp::Except => " EXCEPT ",
                });
                self.s(if all { "ALL " } else { "DISTINCT " });
                self.set_expr(r);
            }
        }
    }

    fn select(&mut self, s: crab_core::Idx<Select>) {
        let s = self.ast().selects[s];
        self.s("SELECT ");
        if s.distinct {
            self.s("DISTINCT ");
        }
        match s.as_ {
            SelectAs::Struct => self.s("AS STRUCT "),
            SelectAs::Value => self.s("AS VALUE "),
            SelectAs::None => {}
        }
        let items = self.ast().select_items.slice(s.items);
        for (i, it) in items.iter().enumerate() {
            if i > 0 {
                self.s(", ");
            }
            match *it {
                SelectItem::Expr { e, alias } => {
                    self.expr(e);
                    if let Some(a) = alias {
                        self.s(" AS ");
                        self.name(a);
                    }
                }
                SelectItem::Star { qualifier, except, replace } => {
                    if let Some(q) = qualifier {
                        self.postfix_base(q);
                        self.s(".");
                    }
                    self.s("*");
                    if !except.is_empty() {
                        self.s(" EXCEPT (");
                        self.names(except, ", ");
                        self.s(")");
                    }
                    if !replace.is_empty() {
                        self.s(" REPLACE (");
                        self.struct_fields(replace);
                        self.s(")");
                    }
                }
            }
        }
        if let Some(f) = s.from {
            self.s(" FROM ");
            self.from(f);
        }
        if let Some(w) = s.where_ {
            self.s(" WHERE ");
            self.expr(w);
        }
        match s.group {
            GroupBy::None => {}
            GroupBy::All => self.s(" GROUP BY ALL"),
            GroupBy::Items(items) => {
                self.s(" GROUP BY ");
                let items = self.ast().group_items.slice(items);
                for (i, g) in items.iter().enumerate() {
                    if i > 0 {
                        self.s(", ");
                    }
                    match *g {
                        GroupItem::Expr(e) => self.expr(e),
                        GroupItem::Rollup(l) => self.wrapped("ROLLUP (", l),
                        GroupItem::Cube(l) => self.wrapped("CUBE (", l),
                        GroupItem::GroupingSets(l) => self.wrapped("GROUPING SETS (", l),
                        GroupItem::Empty => self.s("()"),
                    }
                }
            }
        }
        if let Some(h) = s.having {
            self.s(" HAVING ");
            self.expr(h);
        }
        if let Some(q) = s.qualify {
            self.s(" QUALIFY ");
            self.expr(q);
        }
        if !s.windows.is_empty() {
            self.s(" WINDOW ");
            let ws = self.ast().named_windows.slice(s.windows);
            for (i, w) in ws.iter().enumerate() {
                if i > 0 {
                    self.s(", ");
                }
                self.name(w.name);
                self.s(" AS ");
                self.window(w.spec);
            }
        }
    }

    fn wrapped(&mut self, open: &str, l: crab_core::Span<ExprId>) {
        self.s(open);
        self.expr_list(l);
        self.s(")");
    }

    fn struct_fields(&mut self, fields: crab_core::Span<StructField>) {
        let fs = self.ast().struct_fields.slice(fields);
        for (i, f) in fs.iter().enumerate() {
            if i > 0 {
                self.s(", ");
            }
            self.expr(f.e);
            if let Some(a) = f.alias {
                self.s(" AS ");
                self.name(a);
            }
        }
    }

    fn alias(&mut self, a: Option<Name>) {
        if let Some(a) = a {
            self.s(" AS ");
            self.name(a);
        }
    }

    fn table_alias(&mut self, f: FromId, alias: Option<Name>) {
        if !self.h.table_alias(f, self.out) {
            self.alias(alias);
        }
    }

    fn offset(&mut self, o: Option<WithOffset>) {
        if let Some(o) = o {
            self.s(" WITH OFFSET");
            self.alias(o.alias);
        }
    }

    fn from(&mut self, f: FromId) {
        match self.ast().froms[f] {
            From::Table { path, alias, time_travel, offset, sample } => {
                if !self.h.table_path(f, self.out) {
                    self.names(path, ".");
                }
                if let Some(t) = time_travel {
                    self.s(" FOR SYSTEM_TIME AS OF ");
                    self.expr(t);
                }
                self.table_alias(f, alias);
                self.offset(offset);
                if let Some(s) = sample {
                    self.s(" TABLESAMPLE SYSTEM (");
                    self.expr(s);
                    self.s(" PERCENT)");
                }
            }
            From::Subquery { q, alias } => {
                self.s("(");
                self.query(q);
                self.s(")");
                self.table_alias(f, alias);
            }
            From::Unnest { e, alias, offset } => {
                self.s("UNNEST(");
                self.expr(e);
                self.s(")");
                self.table_alias(f, alias);
                self.offset(offset);
            }
            From::Join { kind, l, r, cond } => {
                self.from(l);
                self.s(match kind {
                    JoinKind::Inner => " JOIN ",
                    JoinKind::Left => " LEFT JOIN ",
                    JoinKind::Right => " RIGHT JOIN ",
                    JoinKind::Full => " FULL JOIN ",
                    JoinKind::Cross => " CROSS JOIN ",
                    JoinKind::Comma => ", ",
                });
                self.from(r);
                match cond {
                    JoinCond::None => {}
                    JoinCond::On(e) => {
                        self.s(" ON ");
                        self.expr(e);
                    }
                    JoinCond::Using(cols) => {
                        self.s(" USING (");
                        self.names(cols, ", ");
                        self.s(")");
                    }
                }
            }
            From::Paren(inner) => {
                self.s("(");
                self.from(inner);
                self.s(")");
            }
        }
    }

    // ---- expressions -----------------------------------------------------

    /// Emit `e`, parenthesised when its precedence is below `min`.
    fn expr_min(&mut self, e: ExprId, min: u8) {
        if prec(&self.ast().exprs[e]) < min {
            self.s("(");
            self.expr(e);
            self.s(")");
        } else {
            self.expr(e);
        }
    }

    fn postfix_base(&mut self, e: ExprId) {
        self.expr_min(e, 12);
    }

    #[allow(clippy::too_many_lines)] // the expression table
    fn expr(&mut self, e: ExprId) {
        if self.h.expr(e, self.out) {
            return;
        }
        match self.ast().exprs[e] {
            Expr::Lit { kind, sp } => match kind {
                LitKind::Bool(true) => self.s("TRUE"),
                LitKind::Bool(false) => self.s("FALSE"),
                LitKind::Null => self.s("NULL"),
                LitKind::Typed(str_sp) => {
                    let ty = self.text(sp).to_ascii_uppercase();
                    let lit = self.text(str_sp);
                    let _ = write!(self.out, "{ty} {lit}");
                }
                _ => {
                    let t = self.text(sp);
                    self.s(t);
                }
            },
            Expr::Ident(n) => self.name(n),
            Expr::Field { base, field } => {
                self.postfix_base(base);
                self.s(".");
                self.name(field);
            }
            Expr::Param(sp) => {
                self.s("@");
                let t = self.text(sp);
                self.s(t);
            }
            Expr::SysVar(sp) => {
                self.s("@@");
                let t = self.text(sp);
                self.s(t);
            }
            Expr::Positional(_) => self.s("?"),
            Expr::Star => self.s("*"),
            Expr::Unary { op, e } => match op {
                UnOp::Not => {
                    self.s("NOT ");
                    self.expr_min(e, 3);
                }
                UnOp::Neg | UnOp::Plus | UnOp::BitNot => {
                    self.s(match op {
                        UnOp::Neg => "-",
                        UnOp::Plus => "+",
                        _ => "~",
                    });
                    // Avoid `--x` (a comment) and `- -x` ambiguity.
                    if matches!(self.ast().exprs[e], Expr::Unary { op: UnOp::Neg | UnOp::Plus, .. })
                        || self.leading_minus(e)
                    {
                        self.s(" ");
                    }
                    self.expr_min(e, 11);
                }
            },
            Expr::Binary { op, l, r } => {
                let p = op.precedence();
                self.expr_min(l, p);
                self.s(" ");
                self.s(op.as_str());
                self.s(" ");
                self.expr_min(r, p + 1);
            }
            Expr::Like { e, pats, quant, not } => {
                self.expr_min(e, 4);
                self.s(if not { " NOT LIKE " } else { " LIKE " });
                if let Some(q) = quant {
                    self.s(match q {
                        Quant::Any => "ANY (",
                        Quant::Some => "SOME (",
                        Quant::All => "ALL (",
                    });
                    self.expr_list(pats);
                    self.s(")");
                } else {
                    let p = self.ast().expr_lists.slice(pats)[0];
                    self.expr_min(p, 5);
                }
            }
            Expr::Between { e, lo, hi, not } => {
                self.expr_min(e, 4);
                self.s(if not { " NOT BETWEEN " } else { " BETWEEN " });
                self.expr_min(lo, 5);
                self.s(" AND ");
                self.expr_min(hi, 5);
            }
            Expr::InList { e, list, not } => {
                self.expr_min(e, 4);
                self.s(if not { " NOT IN (" } else { " IN (" });
                self.expr_list(list);
                self.s(")");
            }
            Expr::InQuery { e, q, not } => {
                self.expr_min(e, 4);
                self.s(if not { " NOT IN (" } else { " IN (" });
                self.query(q);
                self.s(")");
            }
            Expr::InUnnest { e, arr, not } => {
                self.expr_min(e, 4);
                self.s(if not { " NOT IN UNNEST(" } else { " IN UNNEST(" });
                self.expr(arr);
                self.s(")");
            }
            Expr::Is { e, what, not } => {
                self.expr_min(e, 4);
                self.s(if not { " IS NOT " } else { " IS " });
                self.s(match what {
                    IsWhat::Null => "NULL",
                    IsWhat::True => "TRUE",
                    IsWhat::False => "FALSE",
                    IsWhat::Unknown => "UNKNOWN",
                });
            }
            Expr::IsDistinct { l, r, not } => {
                self.expr_min(l, 4);
                self.s(if not { " IS NOT DISTINCT FROM " } else { " IS DISTINCT FROM " });
                self.expr_min(r, 5);
            }
            Expr::Call(c) => self.call(c),
            Expr::Cast { e, ty, safe, format } => {
                self.s(if safe { "SAFE_CAST(" } else { "CAST(" });
                self.expr(e);
                self.s(" AS ");
                self.ty(ty);
                if let Some(f) = format {
                    self.s(" FORMAT ");
                    self.expr(f);
                }
                self.s(")");
            }
            Expr::Extract { part, e, tz } => {
                self.s("EXTRACT(");
                self.expr(part);
                self.s(" FROM ");
                self.expr(e);
                if let Some(t) = tz {
                    self.s(" AT TIME ZONE ");
                    self.expr(t);
                }
                self.s(")");
            }
            Expr::Case { operand, arms, else_ } => {
                self.s("CASE");
                if let Some(o) = operand {
                    self.s(" ");
                    self.expr(o);
                }
                let arms = self.ast().arms.slice(arms);
                for a in arms {
                    self.s(" WHEN ");
                    self.expr(a.when);
                    self.s(" THEN ");
                    self.expr(a.then);
                }
                if let Some(e) = else_ {
                    self.s(" ELSE ");
                    self.expr(e);
                }
                self.s(" END");
            }
            Expr::Array { ty, elems } => {
                if let Some(t) = ty {
                    self.ty(t);
                }
                self.s("[");
                self.expr_list(elems);
                self.s("]");
            }
            Expr::ArrayQuery(q) => {
                self.s("ARRAY(");
                self.query(q);
                self.s(")");
            }
            Expr::Struct { ty, fields } => {
                match ty {
                    Some(t) => self.ty(t),
                    None => self.s("STRUCT"),
                }
                self.s("(");
                self.struct_fields(fields);
                self.s(")");
            }
            Expr::Tuple(items) => {
                self.s("(");
                self.expr_list(items);
                self.s(")");
            }
            Expr::Index { e, idx, mode } => {
                self.postfix_base(e);
                let (open, close) = match mode {
                    IndexMode::Plain => ("[", "]"),
                    IndexMode::Offset => ("[OFFSET(", ")]"),
                    IndexMode::SafeOffset => ("[SAFE_OFFSET(", ")]"),
                    IndexMode::Ordinal => ("[ORDINAL(", ")]"),
                    IndexMode::SafeOrdinal => ("[SAFE_ORDINAL(", ")]"),
                };
                self.s(open);
                self.expr(idx);
                self.s(close);
            }
            Expr::Exists(q) => {
                self.s("EXISTS (");
                self.query(q);
                self.s(")");
            }
            Expr::Subquery(q) => {
                self.s("(");
                self.query(q);
                self.s(")");
            }
            Expr::Interval { e, unit, to } => {
                self.s("INTERVAL ");
                self.expr_min(e, 11);
                self.s(" ");
                self.name(unit);
                if let Some(t) = to {
                    self.s(" TO ");
                    self.name(t);
                }
            }
            Expr::Paren(e) => {
                self.s("(");
                self.expr(e);
                self.s(")");
            }
        }
    }

    /// Would `e` be emitted starting with a `-` (a negative literal)?
    fn leading_minus(&self, e: ExprId) -> bool {
        matches!(self.ast().exprs[e], Expr::Lit { kind: LitKind::Int | LitKind::Float, sp } if self.text(sp).starts_with('-'))
    }

    fn call(&mut self, id: crab_core::Idx<Call>) {
        let c = self.ast().calls[id];
        if !self.h.call_name(id, self.out) {
            self.names(c.name, ".");
        }
        self.s("(");
        if c.distinct {
            self.s("DISTINCT ");
        }
        let args = self.ast().args.slice(c.args);
        for (i, a) in args.iter().enumerate() {
            if i > 0 {
                self.s(", ");
            }
            if let Some(n) = a.name {
                self.name(n);
                self.s(" => ");
            }
            self.expr(a.e);
        }
        match c.nulls {
            Some(NullTreatment::Ignore) => self.s(" IGNORE NULLS"),
            Some(NullTreatment::Respect) => self.s(" RESPECT NULLS"),
            None => {}
        }
        self.order_by(c.order);
        if let Some(l) = c.limit {
            self.s(" LIMIT ");
            self.expr(l);
        }
        self.s(")");
        if let Some(w) = c.over {
            self.s(" OVER ");
            self.window(w);
        }
    }

    fn window(&mut self, w: crab_core::Idx<WindowSpec>) {
        let w = self.ast().windows[w];
        if w.bare {
            if let Some(b) = w.base {
                self.name(b);
            }
            return;
        }
        self.s("(");
        let mut first = true;
        let mut sep = |me: &mut Self| {
            if !first {
                me.s(" ");
            }
            first = false;
        };
        if let Some(b) = w.base {
            sep(self);
            self.name(b);
        }
        if !w.partition.is_empty() {
            sep(self);
            self.s("PARTITION BY ");
            self.expr_list(w.partition);
        }
        if !w.order.is_empty() {
            sep(self);
            self.s("ORDER BY ");
            self.order_items(w.order);
        }
        if let Some(f) = w.frame {
            sep(self);
            self.s(match f.unit {
                FrameUnit::Rows => "ROWS ",
                FrameUnit::Range => "RANGE ",
            });
            if let Some(end) = f.end {
                self.s("BETWEEN ");
                self.bound(f.start);
                self.s(" AND ");
                self.bound(end);
            } else {
                self.bound(f.start);
            }
        }
        self.s(")");
    }

    fn bound(&mut self, b: Bound) {
        match b {
            Bound::UnboundedPreceding => self.s("UNBOUNDED PRECEDING"),
            Bound::UnboundedFollowing => self.s("UNBOUNDED FOLLOWING"),
            Bound::CurrentRow => self.s("CURRENT ROW"),
            Bound::Preceding(e) => {
                self.expr_min(e, 5);
                self.s(" PRECEDING");
            }
            Bound::Following(e) => {
                self.expr_min(e, 5);
                self.s(" FOLLOWING");
            }
        }
    }

    fn ty(&mut self, t: TypeId) {
        match self.ast().types[t] {
            TypeNode::Named { name, params } => {
                if name.quoted {
                    self.name(name);
                } else {
                    let up = self.text(name.sp).to_ascii_uppercase();
                    self.s(&up);
                }
                if !params.is_empty() {
                    self.s("(");
                    self.expr_list(params);
                    self.s(")");
                }
            }
            TypeNode::Array(inner) => {
                self.s("ARRAY<");
                self.ty(inner);
                self.s(">");
            }
            TypeNode::Range(inner) => {
                self.s("RANGE<");
                self.ty(inner);
                self.s(">");
            }
            TypeNode::Struct(fields) => {
                self.s("STRUCT<");
                let fs = self.ast().type_fields.slice(fields);
                for (i, f) in fs.iter().enumerate() {
                    if i > 0 {
                        self.s(", ");
                    }
                    if let Some(n) = f.name {
                        self.name(n);
                        self.s(" ");
                    }
                    self.ty(f.ty);
                }
                self.s(">");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::{Mode, Parser};

    fn canon(sql: &str) -> String {
        let mut p = Parser::new();
        let parsed = p.parse(sql, Mode::Full).unwrap_or_else(|e| panic!("{sql}: {e}"));
        super::to_sql(&parsed)
    }

    fn fixed_point(sql: &str) {
        let once = canon(sql);
        let twice = canon(&once);
        assert_eq!(once, twice, "not a fixed point for {sql}");
    }

    #[test]
    fn canonical_layout() {
        assert_eq!(
            canon("select  a,b   as c from `p.d.t` t -- comment\n where a=1 and b in (1,2) order by a desc limit 5"),
            "SELECT a, b AS c FROM `p.d.t` AS t WHERE a = 1 AND b IN (1, 2) ORDER BY a DESC LIMIT 5"
        );
        assert_eq!(
            canon("SELECT date '2026-01-01', cast(x as int64)"),
            "SELECT DATE '2026-01-01', CAST(x AS INT64)"
        );
    }

    #[test]
    fn layout_variants_emit_identically() {
        let a = canon("SELECT o.id FROM shop.orders AS o WHERE o.status='shipped'");
        let b = canon("select o.id\nfrom shop.orders o\n-- x\nwhere (o.status = 'shipped')");
        // The second keeps its explicit parentheses (the tree does), so only
        // layout-level differences are erased:
        assert_eq!(
            a.replace("(o.status = 'shipped')", "o.status = 'shipped'"),
            b.replace("(o.status = 'shipped')", "o.status = 'shipped'")
        );
    }

    #[test]
    fn precedence_is_preserved_with_minimal_parens() {
        assert_eq!(
            canon("SELECT (a + b) * c, a + b * c, a - (b - c), (a - b) - c"),
            "SELECT (a + b) * c, a + b * c, a - (b - c), (a - b) - c"
        );
        assert_eq!(canon("SELECT NOT (a OR b), - -1, -(-a)"), "SELECT NOT (a OR b), - -1, -(-a)");
    }

    #[test]
    fn fixed_points() {
        for sql in [
            "WITH x AS (SELECT 1 AS a) SELECT a FROM x UNION ALL (SELECT 2) ORDER BY 1 LIMIT 1 OFFSET 0",
            "SELECT t.* EXCEPT (a) REPLACE (b + 1 AS b) FROM t LEFT JOIN u USING (k), UNNEST(t.arr) AS e WITH OFFSET AS i",
            "SELECT SUM(x) OVER (w ORDER BY y ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), RANK() OVER w FROM t WINDOW w AS (PARTITION BY k)",
            "SELECT CASE WHEN a BETWEEN 1 AND 2 THEN 'x' ELSE NULL END, a IS NOT DISTINCT FROM b, s NOT LIKE ANY ('a', 'b') FROM t",
            "SELECT ARRAY<STRUCT<a INT64, b ARRAY<STRING>>>[], STRUCT(1 AS a), [1, 2][OFFSET(0)], (SELECT 1), EXISTS (SELECT 1) FROM t",
            "SELECT EXTRACT(WEEK(MONDAY) FROM d AT TIME ZONE 'UTC'), INTERVAL 1 DAY, INTERVAL '1-2' YEAR TO MONTH FROM t",
            "SELECT * FROM `p.d.t` FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP() AS t TABLESAMPLE SYSTEM (5 PERCENT) GROUP BY ROLLUP (a), ()",
            "SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY y LIMIT 3), f(a => 1) FROM t QUALIFY x = 1",
        ] {
            fixed_point(sql);
        }
    }
}
