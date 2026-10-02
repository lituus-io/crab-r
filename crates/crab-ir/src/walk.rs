// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Expression traversal and spans.

use crab_sql::ast::*;
use crab_sql::{Parsed, Sp};

/// Push the direct expression children of `e` (not into subqueries).
pub(crate) fn children(ast: &Ast, e: ExprId, out: &mut Vec<ExprId>) {
    let list = |l: crab_core::Span<ExprId>, out: &mut Vec<ExprId>| {
        out.extend_from_slice(ast.expr_lists.slice(l));
    };
    match ast.exprs[e] {
        // Leaves, and subqueries (walked by their own scope).
        Expr::Lit { .. }
        | Expr::Ident(_)
        | Expr::Param(_)
        | Expr::SysVar(_)
        | Expr::Positional(_)
        | Expr::Star
        | Expr::Exists(_)
        | Expr::Subquery(_)
        | Expr::ArrayQuery(_) => {}
        Expr::Field { base, .. } => out.push(base),
        Expr::Unary { e: x, .. }
        | Expr::Paren(x)
        | Expr::Interval { e: x, .. }
        | Expr::InQuery { e: x, .. }
        | Expr::Is { e: x, .. } => out.push(x),
        Expr::Binary { l, r, .. } | Expr::IsDistinct { l, r, .. } => {
            out.push(l);
            out.push(r);
        }
        Expr::Like { e: x, pats, .. } => {
            out.push(x);
            list(pats, out);
        }
        Expr::Between { e: x, lo, hi, .. } => out.extend([x, lo, hi]),
        Expr::InList { e: x, list: l, .. } => {
            out.push(x);
            list(l, out);
        }
        Expr::InUnnest { e: x, arr, .. } => out.extend([x, arr]),
        Expr::Call(c) => {
            let call = ast.calls[c];
            out.extend(ast.args.slice(call.args).iter().map(|a| a.e));
            out.extend(ast.order_items.slice(call.order).iter().map(|o| o.e));
            out.extend(call.limit);
            if let Some(w) = call.over {
                let w = ast.windows[w];
                list(w.partition, out);
                out.extend(ast.order_items.slice(w.order).iter().map(|o| o.e));
            }
        }
        Expr::Cast { e: x, format, .. } => {
            out.push(x);
            out.extend(format);
        }
        Expr::Extract { part, e: x, tz } => {
            out.extend([part, x]);
            out.extend(tz);
        }
        Expr::Case { operand, arms, else_ } => {
            out.extend(operand);
            for a in ast.arms.slice(arms) {
                out.extend([a.when, a.then]);
            }
            out.extend(else_);
        }
        Expr::Array { elems: l, .. } | Expr::Tuple(l) => list(l, out),
        Expr::Struct { fields, .. } => {
            out.extend(ast.struct_fields.slice(fields).iter().map(|f| f.e));
        }
        Expr::Index { e: x, idx, .. } => out.extend([x, idx]),
    }
}

/// Pre-order walk from `e` (not into subqueries): `f(node)` returns
/// whether to descend into it.
pub fn visit(ast: &Ast, e: ExprId, f: &mut impl FnMut(ExprId) -> bool) {
    each_child(ast, e, f);
}

/// Pre-order walk from `e`: `f(node)` returns whether to descend into it.
pub(crate) fn each_child(ast: &Ast, e: ExprId, f: &mut impl FnMut(ExprId) -> bool) {
    let mut stack = vec![e];
    let mut kids = Vec::new();
    while let Some(x) = stack.pop() {
        if !f(x) {
            continue;
        }
        kids.clear();
        children(ast, x, &mut kids);
        stack.extend(kids.iter().rev());
    }
}

/// The source span covered by an expression (from its names and literals).
#[must_use]
pub fn span(p: &Parsed<'_>, e: ExprId) -> Sp {
    span_in(p.ast, p.src, e)
}

/// [`span`] over any parse tree and its source (e.g. a script's statements).
#[must_use]
pub fn span_in(ast: &Ast, src: &str, e: ExprId) -> Sp {
    let (mut lo, mut hi) = (u32::MAX, 0u32);
    let mut take = |sp: Sp| {
        if sp.start < sp.end || sp.start > 0 {
            lo = lo.min(sp.start);
            hi = hi.max(sp.end);
        }
    };
    each_child(ast, e, &mut |x| {
        match ast.exprs[x] {
            Expr::Lit { sp, kind } => {
                take(sp);
                if let LitKind::Typed(s) = kind {
                    take(s);
                }
            }
            Expr::Ident(n) => take(n.sp),
            Expr::Field { field, .. } => take(field.sp),
            Expr::Param(sp) | Expr::SysVar(sp) => take(sp),
            Expr::Call(c) => {
                for n in ast.names.slice(ast.calls[c].name) {
                    take(n.sp);
                }
            }
            Expr::Interval { unit, to, .. } => {
                take(unit.sp);
                if let Some(t) = to {
                    take(t.sp);
                }
            }
            _ => {}
        }
        true
    });
    if lo == u32::MAX {
        Sp::default()
    } else {
        // Include a closing parenthesis or quote directly after a call.
        let mut end = hi as usize;
        let bytes = src.as_bytes();
        let mut depth = 0i32;
        let mut i = lo as usize;
        while i < end.min(bytes.len()) {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
        while depth > 0 && end < bytes.len() {
            if bytes[end] == b')' {
                depth -= 1;
            } else if bytes[end] == b'(' {
                depth += 1;
            }
            end += 1;
        }
        Sp { start: lo, end: end as u32 }
    }
}
