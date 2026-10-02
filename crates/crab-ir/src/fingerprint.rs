// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Canonical fingerprints of bound queries.
//!
//! Two candidate queries with the same [`fingerprint`] return the same
//! result, so the engine executes only one of them (fewer warehouse runs,
//! lower cost, faster voting). The hash is over the *meaning*, not the text:
//!
//! - names are hashed by what they resolved to (catalog key), not by alias,
//!   so `o.status` and `orders.status` agree;
//! - parentheses, keyword case, layout and select-list aliases are ignored;
//! - operands of commutative operators are order-independent, and chains of
//!   `AND` / `OR` are hashed as sets;
//! - `a > b` is hashed as `b < a` (and `>=` as `<=`);
//! - FROM items joined by inner / comma joins are a set; WHERE conjuncts are
//!   a set; GROUP BY keys are a set.
//!
//! [`template_fingerprint`] additionally replaces literal values by their
//! type, grouping queries that differ only in constants (the unit Part 2
//! clusters query logs by).

use crate::bind::{Bound, Ref};
use crab_catalog::{Catalog, TableId};
use crab_sql::ast::Bound as FrameBound;
use crab_sql::ast::*;
use crab_sql::Parsed;
use graph_r::hash::Hasher;

const SEED_TEMPLATE: u64 = 0x7E57_1A7E;

/// Meaning-level fingerprint of a bound statement.
#[must_use]
pub fn fingerprint(p: &Parsed<'_>, b: &Bound, cat: &Catalog) -> u128 {
    Fp { p, b, cat: Some(cat), template: false }.statement(0)
}

/// Like [`fingerprint`], with literal values replaced by their type.
#[must_use]
pub fn template_fingerprint(p: &Parsed<'_>, b: &Bound, cat: &Catalog) -> u128 {
    Fp { p, b, cat: Some(cat), template: true }.statement(SEED_TEMPLATE)
}

/// 64-bit canonical hash of one expression (used for GROUP BY matching).
#[must_use]
pub fn expr_hash(p: &Parsed<'_>, b: &Bound, e: ExprId) -> u64 {
    Fp { p, b, cat: None, template: false }.expr(e)
}

struct Fp<'a, 'p> {
    p: &'a Parsed<'p>,
    b: &'a Bound,
    cat: Option<&'a Catalog>,
    template: bool,
}

fn mix(tag: u64, parts: &[u64]) -> u64 {
    let mut h = Hasher::with_seed(tag);
    for x in parts {
        h.update(&x.to_le_bytes());
    }
    h.digest64()
}

fn text_hash(s: &str) -> u64 {
    graph_r::hash::xxh3_64(s.as_bytes())
}

fn lower_hash(s: &str) -> u64 {
    let mut h = Hasher::new();
    for b in s.bytes() {
        h.update(&[b.to_ascii_lowercase()]);
    }
    h.digest64()
}

impl Fp<'_, '_> {
    fn ast(&self) -> &Ast {
        self.p.ast
    }

    fn statement(&self, seed: u64) -> u128 {
        let h = self.query(self.p.root);
        let mut out = Hasher::with_seed(seed);
        out.update(&h.to_le_bytes());
        out.digest128()
    }

    /// Signature of a range variable: the table it scans (plus which
    /// occurrence, to keep self-joins apart), else its position.
    fn rel_sig(&self, rel: u32) -> u64 {
        let Some(r) = self.b.rels.get(rel as usize) else { return mix(1, &[u64::from(rel)]) };
        match (r.table, self.cat) {
            (Some(t), Some(cat)) => {
                let occurrence =
                    self.b.rels[..rel as usize].iter().filter(|x| x.table == Some(t)).count()
                        as u64;
                mix(2, &[cat.table(t).map_or(u64::from(t.0), |x| x.key.raw()), occurrence])
            }
            (Some(TableId(t)), None) => mix(2, &[u64::from(t), u64::from(rel)]),
            _ => mix(3, &[u64::from(rel)]),
        }
    }

    fn names(&self, s: crab_core::Span<Name>) -> u64 {
        let parts: Vec<u64> =
            self.ast().names.slice(s).iter().map(|n| lower_hash(n.sp.text(self.p.src))).collect();
        mix(4, &parts)
    }

    fn set(tag: u64, mut parts: Vec<u64>) -> u64 {
        parts.sort_unstable();
        mix(tag, &parts)
    }

    fn flatten(&self, e: ExprId, op: BinOp, out: &mut Vec<u64>) {
        match self.ast().exprs[e] {
            Expr::Binary { op: o, l, r } if o == op => {
                self.flatten(l, op, out);
                self.flatten(r, op, out);
            }
            Expr::Paren(x) => self.flatten(x, op, out),
            _ => out.push(self.expr(e)),
        }
    }

    #[allow(clippy::too_many_lines)] // one arm per expression kind
    fn expr(&self, e: ExprId) -> u64 {
        match self.b.refs.get(e.raw() as usize).copied().unwrap_or(Ref::None) {
            Ref::Column { rel, col } => {
                let key =
                    self.cat.and_then(|c| c.column(col)).map_or(u64::from(col.0), |c| c.key.raw());
                return mix(10, &[self.rel_sig(rel), key]);
            }
            Ref::Derived { rel, index } => return mix(11, &[self.rel_sig(rel), u64::from(index)]),
            Ref::Element { rel } => {
                // `it` or `it.a.b`: the element's relation plus the field path.
                let mut tail = Vec::new();
                let mut cur = e;
                while let Expr::Field { base, field } = self.ast().exprs[cur] {
                    tail.push(lower_hash(field.sp.text(self.p.src)));
                    cur = base;
                }
                tail.push(self.rel_sig(rel));
                return mix(12, &tail);
            }
            Ref::Offset { rel } => return mix(13, &[self.rel_sig(rel)]),
            Ref::Rel { rel } => return mix(14, &[self.rel_sig(rel)]),
            Ref::Alias { expr } => return self.expr(expr),
            _ => {}
        }
        let src = self.p.src;
        match self.ast().exprs[e] {
            Expr::Lit { kind, sp } => {
                let v = if self.template { 0 } else { text_hash(sp.text(src)) };
                match kind {
                    LitKind::Int => mix(20, &[v]),
                    LitKind::Float => mix(21, &[v]),
                    LitKind::Str => mix(22, &[v]),
                    LitKind::Bytes => mix(23, &[v]),
                    LitKind::Bool(b) => mix(24, &[u64::from(b)]),
                    LitKind::Null => mix(25, &[]),
                    LitKind::Typed(s) => mix(
                        26,
                        &[
                            lower_hash(sp.text(src)),
                            if self.template { 0 } else { text_hash(s.text(src)) },
                        ],
                    ),
                }
            }
            Expr::Ident(n) => mix(30, &[lower_hash(n.sp.text(src))]),
            Expr::Field { base, field } => {
                mix(31, &[self.expr(base), lower_hash(field.sp.text(src))])
            }
            Expr::Param(sp) => mix(32, &[lower_hash(sp.text(src))]),
            Expr::SysVar(sp) => mix(33, &[lower_hash(sp.text(src))]),
            Expr::Positional(i) => mix(34, &[u64::from(i)]),
            Expr::Star => mix(35, &[]),
            Expr::Paren(x) => self.expr(x),
            Expr::Unary { op, e: x } => mix(40 + op as u64, &[self.expr(x)]),
            Expr::Binary { op, l, r } => match op {
                BinOp::And | BinOp::Or => {
                    let mut parts = Vec::new();
                    self.flatten(e, op, &mut parts);
                    Self::set(50 + op as u64, parts)
                }
                BinOp::Eq
                | BinOp::Ne
                | BinOp::Add
                | BinOp::Mul
                | BinOp::BitAnd
                | BinOp::BitOr
                | BinOp::BitXor => Self::set(50 + op as u64, vec![self.expr(l), self.expr(r)]),
                BinOp::Gt => mix(50 + BinOp::Lt as u64, &[self.expr(r), self.expr(l)]),
                BinOp::Ge => mix(50 + BinOp::Le as u64, &[self.expr(r), self.expr(l)]),
                _ => mix(50 + op as u64, &[self.expr(l), self.expr(r)]),
            },
            Expr::Like { e: x, pats, quant, not } => {
                let ps: Vec<u64> =
                    self.ast().expr_lists.slice(pats).iter().map(|&p| self.expr(p)).collect();
                mix(
                    80,
                    &[
                        self.expr(x),
                        Self::set(81, ps),
                        quant.map_or(9, |q| q as u64),
                        u64::from(not),
                    ],
                )
            }
            Expr::Between { e: x, lo, hi, not } => {
                mix(82, &[self.expr(x), self.expr(lo), self.expr(hi), u64::from(not)])
            }
            Expr::InList { e: x, list, not } => {
                let items: Vec<u64> =
                    self.ast().expr_lists.slice(list).iter().map(|&i| self.expr(i)).collect();
                mix(83, &[self.expr(x), Self::set(84, items), u64::from(not)])
            }
            Expr::InQuery { e: x, q, not } => {
                mix(85, &[self.expr(x), self.query(q), u64::from(not)])
            }
            Expr::InUnnest { e: x, arr, not } => {
                mix(86, &[self.expr(x), self.expr(arr), u64::from(not)])
            }
            Expr::Is { e: x, what, not } => mix(87, &[self.expr(x), what as u64, u64::from(not)]),
            Expr::IsDistinct { l, r, not } => {
                mix(88, &[Self::set(89, vec![self.expr(l), self.expr(r)]), u64::from(not)])
            }
            Expr::Call(c) => self.call(c),
            Expr::Cast { e: x, ty, safe, format } => mix(
                90,
                &[self.expr(x), self.ty(ty), u64::from(safe), format.map_or(0, |f| self.expr(f))],
            ),
            Expr::Extract { part, e: x, tz } => {
                mix(91, &[self.expr(part), self.expr(x), tz.map_or(0, |t| self.expr(t))])
            }
            Expr::Case { operand, arms, else_ } => {
                let mut parts = vec![operand.map_or(0, |o| self.expr(o))];
                for a in self.ast().arms.slice(arms) {
                    parts.push(self.expr(a.when));
                    parts.push(self.expr(a.then));
                }
                parts.push(else_.map_or(0, |x| self.expr(x)));
                mix(92, &parts)
            }
            Expr::Array { elems, ty } => {
                let mut parts: Vec<u64> =
                    self.ast().expr_lists.slice(elems).iter().map(|&x| self.expr(x)).collect();
                parts.push(ty.map_or(0, |t| self.ty(t)));
                mix(93, &parts)
            }
            Expr::ArrayQuery(q) => mix(94, &[self.query(q)]),
            Expr::Struct { fields, .. } => {
                let parts: Vec<u64> = self
                    .ast()
                    .struct_fields
                    .slice(fields)
                    .iter()
                    .map(|f| {
                        mix(
                            95,
                            &[self.expr(f.e), f.alias.map_or(0, |a| lower_hash(a.sp.text(src)))],
                        )
                    })
                    .collect();
                mix(96, &parts)
            }
            Expr::Tuple(items) => {
                let parts: Vec<u64> =
                    self.ast().expr_lists.slice(items).iter().map(|&x| self.expr(x)).collect();
                mix(97, &parts)
            }
            Expr::Index { e: x, idx, mode } => {
                mix(98, &[self.expr(x), self.expr(idx), mode as u64])
            }
            Expr::Exists(q) => mix(99, &[self.query(q)]),
            Expr::Subquery(q) => mix(100, &[self.query(q)]),
            Expr::Interval { e: x, unit, to } => mix(
                101,
                &[
                    self.expr(x),
                    lower_hash(unit.sp.text(src)),
                    to.map_or(0, |t| lower_hash(t.sp.text(src))),
                ],
            ),
        }
    }

    fn call(&self, c: crab_core::Idx<Call>) -> u64 {
        let call = self.ast().calls[c];
        let name = {
            let parts = self.ast().names.slice(call.name);
            let joined: Vec<&str> = parts.iter().map(|n| n.sp.text(self.p.src)).collect();
            let joined = joined.join(".");
            crate::funcs::lookup(&joined).map_or_else(|| lower_hash(&joined), |f| text_hash(f.name))
        };
        let mut parts = vec![name, u64::from(call.distinct), call.nulls.map_or(9, |n| n as u64)];
        for a in self.ast().args.slice(call.args) {
            parts.push(mix(
                110,
                &[self.expr(a.e), a.name.map_or(0, |n| lower_hash(n.sp.text(self.p.src)))],
            ));
        }
        for o in self.ast().order_items.slice(call.order) {
            parts.push(self.order_item(*o));
        }
        parts.push(call.limit.map_or(0, |l| self.expr(l)));
        if let Some(w) = call.over {
            let w = self.ast().windows[w];
            let parts_p: Vec<u64> =
                self.ast().expr_lists.slice(w.partition).iter().map(|&x| self.expr(x)).collect();
            let order: Vec<u64> =
                self.ast().order_items.slice(w.order).iter().map(|o| self.order_item(*o)).collect();
            parts.push(mix(
                111,
                &[
                    Self::set(112, parts_p),
                    mix(113, &order),
                    w.base.map_or(0, |b| lower_hash(b.sp.text(self.p.src))),
                    w.frame.map_or(0, |f| self.frame(f)),
                ],
            ));
        }
        mix(114, &parts)
    }

    fn frame(&self, f: Frame) -> u64 {
        let bound = |b: FrameBound| match b {
            FrameBound::UnboundedPreceding => mix(120, &[]),
            FrameBound::Preceding(e) => mix(121, &[self.expr(e)]),
            FrameBound::CurrentRow => mix(122, &[]),
            FrameBound::Following(e) => mix(123, &[self.expr(e)]),
            FrameBound::UnboundedFollowing => mix(124, &[]),
        };
        mix(125, &[f.unit as u64, bound(f.start), f.end.map_or(0, bound)])
    }

    fn order_item(&self, o: OrderItem) -> u64 {
        // ASC is the default; NULLS FIRST is the default for ASC, LAST for DESC.
        let desc = o.desc.unwrap_or(false);
        let nulls_first = o.nulls_first.unwrap_or(!desc);
        mix(130, &[self.expr(o.e), u64::from(desc), u64::from(nulls_first)])
    }

    fn ty(&self, t: TypeId) -> u64 {
        match self.ast().types[t] {
            TypeNode::Named { name, params } => {
                let ps: Vec<u64> =
                    self.ast().expr_lists.slice(params).iter().map(|&x| self.expr(x)).collect();
                mix(
                    140,
                    &[crab_catalog::TypeCode::of(name.sp.text(self.p.src)) as u64, mix(141, &ps)],
                )
            }
            TypeNode::Array(i) => mix(142, &[self.ty(i)]),
            TypeNode::Struct(fields) => {
                let parts: Vec<u64> =
                    self.ast().type_fields.slice(fields).iter().map(|f| self.ty(f.ty)).collect();
                mix(143, &parts)
            }
            TypeNode::Range(i) => mix(144, &[self.ty(i)]),
        }
    }

    fn query(&self, q: QueryId) -> u64 {
        let q = self.ast().queries[q];
        let ctes: Vec<u64> =
            self.ast().ctes.slice(q.ctes).iter().map(|c| self.query(c.q)).collect();
        let order: Vec<u64> =
            self.ast().order_items.slice(q.order).iter().map(|o| self.order_item(*o)).collect();
        mix(
            150,
            &[
                mix(151, &ctes),
                u64::from(q.recursive),
                self.set_expr(q.body),
                mix(152, &order),
                q.limit.map_or(0, |l| self.expr(l)),
                q.offset.map_or(0, |o| self.expr(o)),
            ],
        )
    }

    fn set_expr(&self, s: crab_core::Idx<SetExpr>) -> u64 {
        match self.ast().set_exprs[s] {
            SetExpr::Select(sel) => self.select(sel),
            SetExpr::Paren(q) => self.query(q),
            SetExpr::SetOp { op, all, l, r } => {
                let (a, b) = (self.set_expr(l), self.set_expr(r));
                // UNION / INTERSECT are commutative; EXCEPT is not.
                if op == SetOp::Except {
                    mix(160, &[a, b, u64::from(all)])
                } else {
                    Self::set(161 + op as u64, vec![a, b, u64::from(all)])
                }
            }
        }
    }

    fn select(&self, s: crab_core::Idx<Select>) -> u64 {
        let s = self.ast().selects[s];
        let items: Vec<u64> = self
            .ast()
            .select_items
            .slice(s.items)
            .iter()
            .map(|it| match *it {
                SelectItem::Expr { e, .. } => self.expr(e),
                SelectItem::Star { qualifier, except, replace } => {
                    let rs: Vec<u64> = self
                        .ast()
                        .struct_fields
                        .slice(replace)
                        .iter()
                        .map(|f| self.expr(f.e))
                        .collect();
                    mix(
                        170,
                        &[qualifier.map_or(0, |q| self.expr(q)), self.names(except), mix(171, &rs)],
                    )
                }
            })
            .collect();
        let from = s.from.map_or(0, |f| self.from(f));
        let where_ = s.where_.map_or(0, |w| {
            let mut conj = Vec::new();
            self.flatten(w, BinOp::And, &mut conj);
            Self::set(172, conj)
        });
        let group = match s.group {
            GroupBy::None => 0,
            GroupBy::All => 1,
            GroupBy::Items(items) => {
                let keys: Vec<u64> = self
                    .ast()
                    .group_items
                    .slice(items)
                    .iter()
                    .map(|g| match *g {
                        GroupItem::Expr(e) => self.group_key(e, &s),
                        GroupItem::Rollup(l) => mix(173, &self.list(l)),
                        GroupItem::Cube(l) => mix(174, &self.list(l)),
                        GroupItem::GroupingSets(l) => mix(175, &self.list(l)),
                        GroupItem::Empty => mix(176, &[]),
                    })
                    .collect();
                Self::set(177, keys)
            }
        };
        mix(
            180,
            &[
                u64::from(s.distinct),
                s.as_ as u64,
                mix(181, &items),
                from,
                where_,
                group,
                s.having.map_or(0, |h| self.expr(h)),
                s.qualify.map_or(0, |q| self.expr(q)),
            ],
        )
    }

    /// GROUP BY ordinals hash as the select item they name.
    fn group_key(&self, e: ExprId, s: &Select) -> u64 {
        if let Expr::Lit { kind: LitKind::Int, sp } = self.ast().exprs[e] {
            let n: usize = sp.text(self.p.src).parse().unwrap_or(0);
            if let Some(SelectItem::Expr { e: item, .. }) =
                n.checked_sub(1).and_then(|i| self.ast().select_items.slice(s.items).get(i))
            {
                return self.expr(*item);
            }
        }
        self.expr(e)
    }

    fn list(&self, l: crab_core::Span<ExprId>) -> Vec<u64> {
        self.ast().expr_lists.slice(l).iter().map(|&x| self.expr(x)).collect()
    }

    /// FROM: inner and comma joins form a set of items plus a set of
    /// conditions; outer joins keep their shape.
    fn from(&self, f: FromId) -> u64 {
        let mut items = Vec::new();
        let mut conds = Vec::new();
        self.inner_block(f, &mut items, &mut conds);
        mix(190, &[Self::set(191, items), Self::set(192, conds)])
    }

    fn inner_block(&self, f: FromId, items: &mut Vec<u64>, conds: &mut Vec<u64>) {
        match self.ast().froms[f] {
            From::Join {
                kind: JoinKind::Inner | JoinKind::Cross | JoinKind::Comma,
                l,
                r,
                cond,
            } => {
                self.inner_block(l, items, conds);
                self.inner_block(r, items, conds);
                match cond {
                    JoinCond::On(e) => self.flatten(e, BinOp::And, conds),
                    JoinCond::Using(cols) => conds.push(self.names(cols)),
                    JoinCond::None => {}
                }
            }
            From::Join { kind, l, r, cond } => {
                let c = match cond {
                    JoinCond::On(e) => {
                        let mut v = Vec::new();
                        self.flatten(e, BinOp::And, &mut v);
                        Self::set(193, v)
                    }
                    JoinCond::Using(cols) => self.names(cols),
                    JoinCond::None => 0,
                };
                items.push(mix(194, &[kind as u64, self.from(l), self.from(r), c]));
            }
            From::Paren(i) => self.inner_block(i, items, conds),
            From::Table { time_travel, sample, .. } => {
                let rel = self.b.from_rel.get(f.raw() as usize).copied().unwrap_or(u32::MAX);
                let sig = if rel == u32::MAX { 0 } else { self.rel_sig(rel) };
                items.push(mix(
                    195,
                    &[
                        sig,
                        time_travel.map_or(0, |t| self.expr(t)),
                        sample.map_or(0, |s| self.expr(s)),
                    ],
                ));
            }
            From::Subquery { q, .. } => items.push(mix(196, &[self.query(q)])),
            From::Unnest { e, offset, .. } => {
                items.push(mix(197, &[self.expr(e), u64::from(offset.is_some())]));
            }
        }
    }
}
