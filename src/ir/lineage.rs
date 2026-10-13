// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Column lineage: which catalog columns every output column of a query is
//! computed from, through which intermediate relations, and how.
//!
//! The walk follows the binder's resolutions, never names: an identifier in
//! a correlated subquery is credited to the relation it actually binds to, a
//! nested struct field is its own catalog column, a set operation's output
//! is traced into **every** branch, and a SELECT-list alias used later in
//! the same SELECT is followed to its expression. Each intermediate output
//! reached through a CTE, a FROM subquery or a set-operation branch is a
//! [`Hop`], carrying the SELECT it was computed in (its WHERE, GROUP BY,
//! HAVING and QUALIFY are the filters and grain that shaped the value).

use crate::base::pool::Idx;
use crate::catalog::ColId;
use crate::ir::bind::{Bound, Ref};
use crate::ir::funcs::{self, Kind};
use crate::sql::ast::{Expr, ExprId, From, QueryId, Select, SelectItem, SetExpr};
use crate::sql::{Parsed, Sp};
use std::collections::HashMap;

/// How an output column relates to its sources.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transform {
    /// Every hop selects a source column unchanged, under the same name.
    Passthrough,
    /// Every hop selects a source column unchanged, under another name.
    Rename,
    /// A (SAFE_)CAST of a source column, otherwise unchanged.
    Cast,
    /// An aggregate over the sources (`SUM`, `COUNT`, ...).
    Aggregate,
    /// A window function over the sources.
    Window,
    /// No source column at all (a literal, a parameterless function).
    Constant,
    /// Any other expression.
    Derived,
}

/// An intermediate output column on the way to the sources.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hop {
    /// Name of the relation it belongs to (CTE or subquery alias), when it
    /// has one.
    pub relation: Option<String>,
    /// The output column's name.
    pub column: Option<String>,
    /// The expression computing it (None: star expansion).
    pub expr: Option<Sp>,
    /// The SELECT it is computed in (for its filters and grain).
    pub select: Option<Idx<Select>>,
    /// Reached as a branch of a set operation.
    pub set_branch: bool,
}

/// The lineage of one output column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnLineage {
    /// Output position.
    pub index: u32,
    /// Output name (alias, implicit name or catalog column name).
    pub name: Option<String>,
    /// The top-level expression (None: star expansion).
    pub expr: Option<Sp>,
    /// The top-level SELECT computing it.
    pub select: Option<Idx<Select>>,
    /// Catalog columns read, sorted and unique.
    pub sources: Vec<ColId>,
    /// Intermediate outputs traversed, in discovery order (outermost first).
    pub hops: Vec<Hop>,
    /// References to unresolved (tolerated) tables.
    pub opaque: Vec<Sp>,
    /// How the value relates to its sources.
    pub transform: Transform,
    /// Sources read inside expression subqueries (scalar, IN, EXISTS,
    /// ARRAY): they shape the value without flowing into it directly.
    pub via_subquery: bool,
}

/// Lineage of every output column of a bound query.
#[must_use]
pub fn lineage(p: &Parsed<'_>, b: &Bound, cat: &crate::catalog::Catalog) -> Vec<ColumnLineage> {
    let w = Walker::new(p, b, cat);
    (b.output.0..b.output.1)
        .enumerate()
        .map(|(pos, i)| {
            let o = b.outs[i as usize];
            let mut acc = Acc::default();
            w.out(i, &mut acc, 0, false);
            acc.sources.sort_unstable();
            acc.sources.dedup();
            let top_expr = o.expr.or_else(|| o.union.and_then(|(l, _)| w.first_expr(l)));
            ColumnLineage {
                index: pos as u32,
                name: w.out_name(i),
                expr: top_expr.map(|e| crate::ir::walk::span(p, e)),
                select: top_expr.and_then(|e| w.select_of.get(&e).copied()),
                transform: acc.transform(&w, top_expr),
                sources: acc.sources,
                hops: acc.hops,
                opaque: acc.opaque,
                via_subquery: acc.via_subquery,
            }
        })
        .collect()
}

/// Catalog columns that decide **which rows** a query returns and how they
/// are grouped: every WHERE, JOIN ... ON, GROUP BY, HAVING and QUALIFY of
/// every SELECT in it (CTEs and subqueries included), resolved through
/// derived relations to physical columns, sorted and unique. A change to
/// one of these changes the rows of the result even when it feeds no output
/// value — the edges impact analysis needs besides value lineage.
#[must_use]
pub fn conditions(p: &Parsed<'_>, b: &Bound, cat: &crate::catalog::Catalog) -> Vec<ColId> {
    use crate::sql::ast::{GroupBy, GroupItem, JoinCond};
    let w = Walker::new(p, b, cat);
    let ast = p.ast;
    let mut acc = Acc::default();
    let take = |e: ExprId, acc: &mut Acc| {
        acc.seen.clear();
        w.expr(e, acc, 0, false);
    };
    for (_, sel) in ast.selects.iter() {
        for e in [sel.where_, sel.having, sel.qualify].into_iter().flatten() {
            take(e, &mut acc);
        }
        // `GROUP BY 2`: the second select-list expression.
        let items_of = ast.select_items.slice(sel.items);
        let ordinal = |e: ExprId| -> Option<ExprId> {
            let Expr::Lit { kind: crate::sql::ast::LitKind::Int, sp } = ast.exprs[e] else {
                return None;
            };
            let n: usize = p.text(sp).parse().ok()?;
            match items_of.get(n.checked_sub(1)?)? {
                SelectItem::Expr { e, .. } => Some(*e),
                SelectItem::Star { .. } => None,
            }
        };
        if let GroupBy::Items(items) = sel.group {
            for g in ast.group_items.slice(items) {
                match *g {
                    GroupItem::Expr(e) => take(ordinal(e).unwrap_or(e), &mut acc),
                    GroupItem::Rollup(es) | GroupItem::Cube(es) | GroupItem::GroupingSets(es) => {
                        for &e in ast.expr_lists.slice(es) {
                            take(e, &mut acc);
                        }
                    }
                    GroupItem::Empty => {}
                }
            }
        }
    }
    for (_, f) in ast.froms.iter() {
        if let From::Join { cond: JoinCond::On(e), .. } = *f {
            take(e, &mut acc);
        }
    }
    acc.sources.sort_unstable();
    acc.sources.dedup();
    acc.sources
}

/// Hops deeper than this are not followed (recursive CTEs).
const MAX_DEPTH: u32 = 64;

struct Walker<'a, 'p> {
    p: &'a Parsed<'p>,
    b: &'a Bound,
    cat: &'a crate::catalog::Catalog,
    /// Select-list expression → its SELECT.
    select_of: HashMap<ExprId, Idx<Select>>,
}

#[derive(Default)]
struct Acc {
    sources: Vec<ColId>,
    hops: Vec<Hop>,
    opaque: Vec<Sp>,
    via_subquery: bool,
    /// Expressions met on the path (top first), for classification.
    exprs: Vec<ExprId>,
    /// A hop renamed the value (output name differs from its source's).
    renamed: bool,
    /// Visited output columns (cycle guard).
    seen: Vec<u32>,
}

impl<'a, 'p> Walker<'a, 'p> {
    fn new(p: &'a Parsed<'p>, b: &'a Bound, cat: &'a crate::catalog::Catalog) -> Self {
        let mut select_of = HashMap::new();
        for (sel_id, sel) in p.ast.selects.iter() {
            for item in p.ast.select_items.slice(sel.items) {
                if let SelectItem::Expr { e, .. } = *item {
                    select_of.insert(e, sel_id);
                }
            }
        }
        Self { p, b, cat, select_of }
    }

    fn first_expr(&self, mut i: u32) -> Option<ExprId> {
        for _ in 0..MAX_DEPTH {
            let o = self.b.outs.get(i as usize)?;
            if let Some(e) = o.expr {
                return Some(e);
            }
            i = o.union?.0;
        }
        None
    }

    fn out_name(&self, i: u32) -> Option<String> {
        let o = self.b.outs.get(i as usize)?;
        if let Some(n) = o.name {
            return Some(self.p.text(n.sp).trim_matches('`').to_owned());
        }
        if let Some(c) = o.col_name.and_then(|c| self.cat.column(c)) {
            return Some(c.leaf().to_owned());
        }
        // An implicit name: the last identifier of a bare column reference.
        match o.expr.map(|e| self.p.ast.exprs[e]) {
            Some(Expr::Ident(n)) => Some(self.p.text(n.sp).trim_matches('`').to_owned()),
            Some(Expr::Field { field, .. }) => {
                Some(self.p.text(field.sp).trim_matches('`').to_owned())
            }
            _ => o.union.and_then(|(l, _)| self.out_name(l)),
        }
    }

    /// Trace output column `i` into `acc`.
    fn out(&self, i: u32, acc: &mut Acc, depth: u32, via_sub: bool) {
        if depth > MAX_DEPTH || acc.seen.contains(&i) {
            return;
        }
        acc.seen.push(i);
        let Some(o) = self.b.outs.get(i as usize).copied() else { return };
        if let Some((l, r)) = o.union {
            for (k, branch) in [l, r].into_iter().enumerate() {
                if depth > 0 || k > 0 {
                    self.hop(branch, None, true, acc);
                }
                self.out(branch, acc, depth + 1, via_sub);
            }
            return;
        }
        if let Some(e) = o.expr {
            acc.exprs.push(e);
            self.expr(e, acc, depth, via_sub);
        } else if let Some(c) = o.col_name {
            acc.sources.push(c);
        }
    }

    fn hop(&self, i: u32, relation: Option<String>, set_branch: bool, acc: &mut Acc) {
        let o = self.b.outs[i as usize];
        acc.hops.push(Hop {
            relation,
            column: self.out_name(i),
            expr: o.expr.map(|e| crate::ir::walk::span(self.p, e)),
            select: o.expr.and_then(|e| self.select_of.get(&e).copied()),
            set_branch,
        });
    }

    /// Trace every reference inside expression `e`.
    fn expr(&self, e: ExprId, acc: &mut Acc, depth: u32, via_sub: bool) {
        let ast = self.p.ast;
        crate::ir::walk::each_child(ast, e, &mut |x| {
            // `el.field` over `UNNEST(t.arr) AS el`: the nested catalog
            // column `arr.field`, not the whole array.
            if let Expr::Field { field, .. } = ast.exprs[x] {
                if let Some(c) = self.element_field(x, field) {
                    acc.sources.push(c);
                    return false;
                }
            }
            match self.b.refs.get(x.raw() as usize).copied().unwrap_or(Ref::None) {
                Ref::Column { col, .. } => {
                    acc.sources.push(col);
                    if via_sub {
                        acc.via_subquery = true;
                    }
                    // A nested field is its own column: its bases add nothing.
                    return false;
                }
                Ref::Derived { rel, index } => {
                    let r = self.b.rels[rel as usize];
                    let target = r.outs.0 + index;
                    let name = r.name.map(|n| self.p.text(n.sp).trim_matches('`').to_owned());
                    self.hop(target, name, false, acc);
                    if let (Some(src), Some(dst)) = (self.out_name(target), expr_name(self.p, x)) {
                        if !src.eq_ignore_ascii_case(&dst) {
                            acc.renamed = true;
                        }
                    }
                    self.out(target, acc, depth + 1, via_sub);
                    return false;
                }
                Ref::Element { rel } | Ref::Offset { rel } => {
                    let r = self.b.rels[rel as usize];
                    if let From::Unnest { e: arr, .. } = ast.froms[r.from] {
                        self.expr(arr, acc, depth + 1, via_sub);
                    }
                    return false;
                }
                Ref::Alias { expr } => {
                    self.expr(expr, acc, depth + 1, via_sub);
                    return false;
                }
                Ref::Opaque { .. } => {
                    acc.opaque.push(crate::ir::walk::span(self.p, x));
                    return false;
                }
                _ => {}
            }
            // Expression subqueries shape the value: trace their outputs and
            // filters too.
            match ast.exprs[x] {
                Expr::Subquery(q) | Expr::ArrayQuery(q) | Expr::Exists(q) => {
                    self.subquery(q, acc, depth + 1);
                }
                Expr::InQuery { q, .. } => self.subquery(q, acc, depth + 1),
                _ => {}
            }
            true
        });
    }

    /// The catalog column `arr.field` for `el.field` where `el` is the
    /// element of `UNNEST(t.arr)` (the binder resolves the whole `el.field`
    /// node to the element).
    fn element_field(&self, node: ExprId, field: crate::sql::ast::Name) -> Option<ColId> {
        let Ref::Element { rel } = self.b.refs.get(node.raw() as usize).copied()? else {
            return None;
        };
        let From::Unnest { e: arr, .. } = self.p.ast.froms[self.b.rels[rel as usize].from] else {
            return None;
        };
        let Ref::Column { col, .. } = self.b.refs.get(arr.raw() as usize).copied()? else {
            return None;
        };
        let c = self.cat.column(col)?;
        let want = format!("{}.{}", c.path, self.p.text(field.sp).trim_matches('`'));
        self.cat.columns(c.table).find(|x| x.path.eq_ignore_ascii_case(&want)).map(|x| x.id)
    }

    /// Every select-list, WHERE and HAVING expression of a subquery.
    fn subquery(&self, q: QueryId, acc: &mut Acc, depth: u32) {
        if depth > MAX_DEPTH {
            return;
        }
        let ast = self.p.ast;
        let mut stack = vec![ast.queries[q].body];
        while let Some(s) = stack.pop() {
            match ast.set_exprs[s] {
                SetExpr::Select(sel) => {
                    let sel = ast.selects[sel];
                    for item in ast.select_items.slice(sel.items) {
                        if let SelectItem::Expr { e, .. } = *item {
                            self.expr(e, acc, depth, true);
                        }
                    }
                    for e in [sel.where_, sel.having].into_iter().flatten() {
                        self.expr(e, acc, depth, true);
                    }
                }
                SetExpr::Paren(inner) => stack.push(ast.queries[inner].body),
                SetExpr::SetOp { l, r, .. } => stack.extend([l, r]),
            }
        }
    }
}

/// The name a bare reference is written with (`o.cust` → `cust`).
fn expr_name(p: &Parsed<'_>, e: ExprId) -> Option<String> {
    match p.ast.exprs[e] {
        Expr::Ident(n) => Some(p.text(n.sp).trim_matches('`').to_owned()),
        Expr::Field { field, .. } => Some(p.text(field.sp).trim_matches('`').to_owned()),
        _ => None,
    }
}

impl Acc {
    /// Classify from the expressions on the path.
    fn transform(&self, w: &Walker<'_, '_>, top: Option<ExprId>) -> Transform {
        let ast = w.p.ast;
        if self.sources.is_empty() && self.opaque.is_empty() {
            return Transform::Constant;
        }
        let mut cast = false;
        let mut kind = None;
        let mut bare = true;
        for &e in &self.exprs {
            match ast.exprs[e] {
                Expr::Ident(_) | Expr::Field { .. } => {}
                Expr::Cast { e: inner, .. }
                    if matches!(ast.exprs[inner], Expr::Ident(_) | Expr::Field { .. }) =>
                {
                    cast = true;
                }
                _ => bare = false,
            }
            crate::ir::walk::each_child(ast, e, &mut |x| {
                if let Expr::Call(c) = ast.exprs[x] {
                    let call = ast.calls[c];
                    if call.over.is_some() {
                        kind = Some(Transform::Window);
                    } else if kind.is_none() {
                        let name = ast.names.slice(call.name).last().map_or("", |n| w.p.text(n.sp));
                        if funcs::lookup(name).is_some_and(|f| f.kind == Kind::Aggregate) {
                            kind = Some(Transform::Aggregate);
                        }
                    }
                }
                true
            });
        }
        if let Some(k) = kind {
            return k;
        }
        if !bare || self.via_subquery || self.sources.len() + self.opaque.len() != 1 {
            return Transform::Derived;
        }
        if cast {
            return Transform::Cast;
        }
        // A bare reference: renamed if any hop or the top level changed the name.
        let top_renamed = top.is_some_and(|e| {
            let written = expr_name(w.p, e);
            let out = w.select_of.get(&e).and_then(|&sel| {
                ast.select_items.slice(ast.selects[sel].items).iter().find_map(|it| match *it {
                    SelectItem::Expr { e: x, alias: Some(a) } if x == e => {
                        Some(w.p.text(a.sp).trim_matches('`').to_owned())
                    }
                    _ => None,
                })
            });
            matches!((written, out), (Some(a), Some(b)) if !a.eq_ignore_ascii_case(&b))
        });
        let leaf_renamed = self.sources.first().and_then(|&c| w.cat.column(c)).is_some_and(|c| {
            self.exprs
                .last()
                .and_then(|&e| expr_name(w.p, e))
                .is_some_and(|n| !n.eq_ignore_ascii_case(c.leaf()))
        });
        if self.renamed || top_renamed || leaf_renamed {
            Transform::Rename
        } else {
            Transform::Passthrough
        }
    }
}
