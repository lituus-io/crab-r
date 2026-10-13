// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Canonical GoogleSQL for execution.
//!
//! The SQL sent to BigQuery is rewritten from the bound tree so that it is
//! unambiguous and stable:
//!
//! - catalog tables are fully qualified: `` `project.dataset.table` ``
//!   (independent of the session's defaults);
//! - every range variable gets a canonical alias `t{n}`;
//! - every column reference is qualified with that alias and spelled with the
//!   catalog's case (`t0.order_ts`), nested paths included;
//! - date-part keywords are upper-cased; layout, keyword case and comments are
//!   already canonical from the emitter.
//!
//! Candidates that differ only in aliases, qualification, case or layout
//! therefore produce byte-identical text — which is what BigQuery's free
//! 24-hour result cache keys on. Output column aliases written by the model
//! are kept: they are the answer's column names.

use crate::catalog::Catalog;
use crate::ir::bind::{Bound, FromBind, Ref};
use crate::sql::ast::{Expr, ExprId, From, FromId};
use crate::sql::emit::{emit_with, Hooks};
use crate::sql::Parsed;
use std::fmt::Write as _;

/// Canonical execution SQL for a bound statement, appended to `out`.
pub fn emit_canonical(p: &Parsed<'_>, b: &Bound, cat: &Catalog, out: &mut String) {
    let mut h = Canon { p, b, cat };
    emit_with(p, &mut h, out);
}

/// Canonical execution SQL as a new string.
#[must_use]
pub fn to_canonical_sql(p: &Parsed<'_>, b: &Bound, cat: &Catalog) -> String {
    let mut s = String::with_capacity(p.src.len() + 64);
    emit_canonical(p, b, cat, &mut s);
    s
}

struct Canon<'a, 'p> {
    p: &'a Parsed<'p>,
    b: &'a Bound,
    cat: &'a Catalog,
}

impl Canon<'_, '_> {
    fn alias(out: &mut String, rel: u32) {
        let _ = write!(out, "t{rel}");
    }

    /// Source text of the field names after the head of a `Field` chain.
    fn tail(&self, e: ExprId, out: &mut String) {
        let mut parts = Vec::new();
        let mut cur = e;
        while let Expr::Field { base, field } = self.p.ast.exprs[cur] {
            parts.push(field);
            cur = base;
        }
        for f in parts.iter().rev() {
            out.push('.');
            if f.quoted {
                out.push('`');
                out.push_str(f.sp.text(self.p.src));
                out.push('`');
            } else {
                out.push_str(f.sp.text(self.p.src));
            }
        }
    }

    fn quote_path(out: &mut String, path: &str) {
        crate::sql::emit::push_path(out, path);
    }
}

impl Hooks for Canon<'_, '_> {
    fn expr(&mut self, e: ExprId, out: &mut String) -> bool {
        let node = self.p.ast.exprs[e];
        let is_name = matches!(node, Expr::Ident(_) | Expr::Field { .. });
        match self.b.refs.get(e.raw() as usize).copied().unwrap_or(Ref::None) {
            Ref::Column { rel, col } if is_name => {
                let Some(c) = self.cat.column(col) else { return false };
                Self::alias(out, rel);
                out.push('.');
                Self::quote_path(out, c.path);
                true
            }
            Ref::Derived { rel, index } if is_name => {
                let r = self.b.rels[rel as usize];
                let o = self.b.outs[(r.outs.0 + index) as usize];
                let name = o
                    .name
                    .map(|n| n.sp.text(self.p.src))
                    .or_else(|| o.col_name.and_then(|c| self.cat.column(c)).map(|c| c.leaf()));
                let Some(name) = name else { return false };
                Self::alias(out, rel);
                out.push('.');
                Self::quote_path(out, name);
                if let Expr::Field { .. } = node {
                    // Nested navigation below a derived column.
                    let mut depth = 0;
                    let mut cur = e;
                    while let Expr::Field { base, .. } = self.p.ast.exprs[cur] {
                        depth += 1;
                        cur = base;
                    }
                    if depth > 1 {
                        return false;
                    }
                }
                true
            }
            Ref::Element { rel } if is_name => {
                Self::alias(out, rel);
                if let Expr::Field { .. } = node {
                    self.tail(e, out);
                }
                true
            }
            Ref::Opaque { rel } if is_name => {
                Self::alias(out, rel);
                out.push('.');
                let name = match node {
                    Expr::Ident(n) | Expr::Field { field: n, .. } => n.sp.text(self.p.src),
                    _ => return false,
                };
                Self::quote_path(out, name);
                true
            }
            Ref::Rel { rel } if matches!(node, Expr::Ident(_)) => {
                Self::alias(out, rel);
                true
            }
            Ref::DatePart if matches!(node, Expr::Ident(_)) => {
                let Expr::Ident(n) = node else { return false };
                out.push_str(&n.sp.text(self.p.src).to_ascii_uppercase());
                true
            }
            _ => false,
        }
    }

    fn call_name(&mut self, c: crate::base::Idx<crate::sql::ast::Call>, out: &mut String) -> bool {
        let call = self.p.ast.calls[c];
        let parts = self.p.ast.names.slice(call.name);
        if parts.iter().any(|n| n.quoted) {
            return false;
        }
        let joined: Vec<&str> = parts.iter().map(|n| n.sp.text(self.p.src)).collect();
        let joined = joined.join(".");
        if crate::ir::funcs::lookup(&joined).is_none() {
            return false;
        }
        out.push_str(&joined.to_ascii_uppercase());
        true
    }

    fn table_path(&mut self, f: FromId, out: &mut String) -> bool {
        match self.b.froms.get(f.raw() as usize).copied().unwrap_or(FromBind::None) {
            FromBind::Table(t) => {
                let Some(t) = self.cat.table(t) else { return false };
                let _ = write!(out, "`{}.{}.{}`", t.project, t.dataset_name, t.name);
                true
            }
            FromBind::ArrayPath => {
                // `o.items` → `t0.items` (head is the base relation).
                let From::Table { path, .. } = self.p.ast.froms[f] else { return false };
                let names = self.p.ast.names.slice(path);
                let Some(base) = names.first() else { return false };
                let head = base.sp.text(self.p.src);
                let Some(rel) = self.b.rels.iter().position(|r| {
                    r.name.is_some_and(|n| n.sp.text(self.p.src).eq_ignore_ascii_case(head))
                }) else {
                    return false;
                };
                Self::alias(out, rel as u32);
                for n in &names[1..] {
                    out.push('.');
                    Self::quote_path(out, n.sp.text(self.p.src));
                }
                true
            }
            _ => false,
        }
    }

    fn table_alias(&mut self, f: FromId, out: &mut String) -> bool {
        let rel = self.b.from_rel.get(f.raw() as usize).copied().unwrap_or(u32::MAX);
        if rel == u32::MAX {
            return false;
        }
        out.push_str(" AS ");
        Self::alias(out, rel);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogBuilder, ColumnSpec, TableKind, TableSpec};
    use crate::ir::bind::{BindOpts, Binder};
    use crate::sql::{Mode, Parser};

    fn cat() -> Catalog {
        let col = |p: &str, t: &str, o: u32| ColumnSpec {
            path: p.into(),
            data_type: t.into(),
            ordinal: o,
            nullable: true,
            partitioning: false,
            hidden: false,
            clustering: None,
            description: None,
        };
        let mut b = CatalogBuilder::new("proj");
        b.add_table(TableSpec {
            dataset: "shop".into(),
            name: "Orders".into(),
            kind: TableKind::Table,
            description: None,
            row_count: 0,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns: vec![
                col("Order_Id", "INT64", 1),
                col("status", "STRING", 2),
                col("items", "ARRAY<STRUCT<sku STRING>>", 3),
                col("items.sku", "STRING", 3),
                col("created", "DATE", 4),
            ],
        });
        Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
    }

    fn canon(c: &Catalog, sql: &str) -> String {
        let mut p = Parser::new();
        let parsed = p.parse(sql, Mode::Full).unwrap();
        let mut b = Binder::new();
        let bound = b.bind(
            &parsed,
            c,
            BindOpts { default_project: "proj", default_dataset: Some("shop"), mode: Mode::Strict },
        );
        assert!(
            bound.ok(),
            "{sql}: {:?}",
            bound.diags.iter().map(|d| d.message(sql)).collect::<Vec<_>>()
        );
        to_canonical_sql(&parsed, bound, c)
    }

    #[test]
    fn qualifies_tables_aliases_and_columns() {
        let c = cat();
        assert_eq!(
            canon(&c, "select order_id, o.STATUS as s from Orders o where o.status = 'x' order by s"),
            "SELECT t0.Order_Id, t0.status AS s FROM `proj.shop.Orders` AS t0 WHERE t0.status = 'x' ORDER BY s"
        );
    }

    #[test]
    fn equivalent_spellings_are_byte_identical() {
        let c = cat();
        let a = canon(&c, "SELECT o.status, COUNT(*) AS n FROM shop.Orders AS o GROUP BY o.status");
        for variant in [
            "select status, count(*) as n from Orders group by status",
            "SELECT orders.status, COUNT(*) AS n FROM `proj.shop.Orders` orders GROUP BY orders.status",
            "SELECT x.STATUS, COUNT(*) AS n FROM proj.shop.Orders x -- comment\n GROUP BY x.Status",
        ] {
            assert_eq!(canon(&c, variant), a, "{variant}");
        }
    }

    #[test]
    fn unnest_paths_and_date_parts() {
        let c = cat();
        assert_eq!(
            canon(&c, "SELECT it.sku, DATE_TRUNC(o.created, month) FROM Orders o, o.items AS it"),
            "SELECT t1.sku, DATE_TRUNC(t0.created, MONTH) FROM `proj.shop.Orders` AS t0, t0.items AS t1"
        );
        assert_eq!(
            canon(&c, "SELECT e.sku FROM Orders o CROSS JOIN UNNEST(o.items) e"),
            "SELECT t1.sku FROM `proj.shop.Orders` AS t0 CROSS JOIN UNNEST(t0.items) AS t1"
        );
    }

    #[test]
    fn ctes_and_subqueries_keep_names_consistent() {
        let c = cat();
        assert_eq!(
            canon(&c, "WITH s AS (SELECT status FROM Orders) SELECT q.status FROM s AS q"),
            "WITH s AS (SELECT t0.status FROM `proj.shop.Orders` AS t0) SELECT t1.status FROM s AS t1"
        );
        let sql = canon(&c, "SELECT o.* FROM Orders o");
        assert_eq!(sql, "SELECT t0.* FROM `proj.shop.Orders` AS t0");
    }

    #[test]
    fn canonical_sql_reparses_and_rebinds_to_the_same_fingerprint() {
        let c = cat();
        let sql = "SELECT o.status, COUNT(*) AS n FROM Orders o, UNNEST(o.items) AS i WHERE i.sku LIKE 'A%' GROUP BY 1";
        let once = canon(&c, sql);
        assert_eq!(canon(&c, &once), once, "canonical form is a fixed point");
        let fp = |s: &str| {
            let mut p = Parser::new();
            let parsed = p.parse(s, Mode::Full).unwrap();
            let mut b = Binder::new();
            let bound = b.bind(
                &parsed,
                &c,
                BindOpts {
                    default_project: "proj",
                    default_dataset: Some("shop"),
                    mode: Mode::Full,
                },
            );
            crate::ir::fingerprint::fingerprint(&parsed, bound, &c)
        };
        assert_eq!(fp(sql), fp(&once));
    }
}
