// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The tables a statement reads, resolved the way BigQuery resolves them.
//!
//! [`referenced_tables`] walks the parse tree with its scopes: a one-part
//! name is a common table expression only where that CTE is visible (after
//! its own body in a plain `WITH`, everywhere in its `WITH RECURSIVE`, never
//! outside the query that declares it), else a table in the default dataset.
//! A path whose first part is a range variable in scope (`FROM t, t.arr`)
//! is an array scan, not a table ([`RefKind::Path`]). `INFORMATION_SCHEMA`
//! views and the `__TABLES__` meta-table are told apart from tables.
//!
//! The walk is checked against the tree it walked: a FROM item or query the
//! walk did not reach is reported ([`Refs::complete`] false), so a caller
//! that must see every read (the request guard) refuses rather than trust a
//! partial answer.

use crate::base::pool::Idx;
use crate::sql::ast::*;
use crate::sql::parser::{ParseError, Parser, Script};
use crate::sql::Mode;

/// A dataset, optionally in a named project.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatasetRef<'a> {
    /// The project; `None` matches a dataset of this name in any project.
    pub project: Option<&'a str>,
    /// The dataset (case-sensitive, as BigQuery's are by default).
    pub dataset: &'a str,
}

impl<'a> DatasetRef<'a> {
    /// A dataset in any project.
    #[must_use]
    pub const fn any(dataset: &'a str) -> Self {
        Self { project: None, dataset }
    }

    /// `project.dataset` (split at the last dot, so a domain-scoped
    /// `example.com:p.d` keeps its project), or a bare `dataset`.
    #[must_use]
    pub fn parse(s: &'a str) -> Self {
        match s.rsplit_once('.') {
            Some((p, d)) if !p.is_empty() => Self { project: Some(p), dataset: d },
            _ => Self::any(s),
        }
    }
}

/// What a referenced name is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefKind {
    /// A table, view or wildcard table.
    Table,
    /// An `INFORMATION_SCHEMA` view (`dataset` holds its scope: a dataset or
    /// `region-xx`, `None` for a project-level view).
    InformationSchema,
    /// A dataset's `__TABLES__` / `__TABLES_SUMMARY__` meta-table.
    MetaTable,
    /// A path from a range variable in scope (`FROM t, t.arr`): an array
    /// scan of a row already read, not a table.
    Path,
}

/// One name a statement reads from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableRef {
    /// The project, when the name or the default dataset gives one.
    pub project: Option<String>,
    /// The dataset, when the name or the default dataset gives one.
    pub dataset: Option<String>,
    /// The last part: the table, view or meta-table name.
    pub table: String,
    /// What it is.
    pub kind: RefKind,
}

impl TableRef {
    /// `project.dataset.table`, when all three are known.
    #[must_use]
    pub fn fqn(&self) -> Option<String> {
        match (&self.project, &self.dataset) {
            (Some(p), Some(d)) => Some(format!("{p}.{d}.{}", self.table)),
            _ => None,
        }
    }
}

/// What [`walk_script`] found.
#[derive(Debug, Default)]
pub struct Refs {
    /// Every name read, in order of appearance, without duplicates.
    pub tables: Vec<TableRef>,
    /// The walk reached every FROM item and query of the tree. False means
    /// the tree holds a read this walk could not place in its scope.
    pub complete: bool,
}

/// Every table `sql` reads (a query or a script), resolving unqualified
/// names against `default_dataset`. Writes (`INSERT` and `MERGE` targets,
/// `CREATE` names) are not reads.
pub fn referenced_tables(
    sql: &str,
    default_dataset: Option<DatasetRef<'_>>,
) -> Result<Vec<TableRef>, ParseError> {
    referenced_tables_with(&mut Parser::new(), sql, default_dataset)
}

/// [`referenced_tables`] on a reused parser.
pub fn referenced_tables_with(
    p: &mut Parser,
    sql: &str,
    default_dataset: Option<DatasetRef<'_>>,
) -> Result<Vec<TableRef>, ParseError> {
    let script = p.parse_script(sql, Mode::Full)?;
    Ok(walk_script(&script, default_dataset).tables)
}

/// The reads of every statement of `script`. A FROM item outside every
/// statement's query (an `UPDATE`'s FROM, a subquery in a `MERGE`
/// condition) is read at the statement level; [`Refs::complete`] says
/// whether everything was placed.
#[must_use]
pub fn walk_script(script: &Script<'_>, default_dataset: Option<DatasetRef<'_>>) -> Refs {
    let ast = script.ast;
    let mut w = Walker {
        ast,
        src: script.src,
        default: default_dataset,
        scope: Vec::new(),
        seen_from: vec![false; ast.froms.len()],
        seen_query: vec![false; ast.queries.len()],
        out: Vec::new(),
    };
    for s in script.stmts {
        if let Some(q) = s.query {
            w.query(q);
        }
        if let Some(path) = s.source_table {
            w.table(path);
        }
    }
    // Reads no statement's query holds: placed with no scope at all.
    let complete = w.seen_query.iter().all(|&s| s) && w.seen_from.iter().all(|&s| s);
    if !complete {
        for (id, f) in ast.froms.iter() {
            if let From::Table { path, .. } = *f {
                if !w.seen_from[id.raw() as usize] {
                    w.table(path);
                }
            }
        }
    }
    Refs { tables: w.out, complete }
}

/// What a name in scope stands for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Bind {
    Cte,
    Range,
}

struct Walker<'p, 'd> {
    ast: &'p Ast,
    src: &'p str,
    default: Option<DatasetRef<'d>>,
    /// Names in scope, innermost last.
    scope: Vec<(&'p str, Bind)>,
    seen_from: Vec<bool>,
    seen_query: Vec<bool>,
    out: Vec<TableRef>,
}

/// An identifier's text without its backticks.
fn bare(s: &str) -> &str {
    s.trim_matches('`')
}

/// Split a dot-joined path into parts; a domain-scoped project
/// (`example.com:p`) stays one part.
fn parts(joined: &str) -> Vec<&str> {
    if let Some(colon) = joined.find(':') {
        if joined[..colon].contains('.') {
            return match joined[colon..].find('.') {
                Some(dot) => {
                    let (project, rest) = joined.split_at(colon + dot);
                    std::iter::once(project).chain(rest[1..].split('.')).collect()
                }
                None => vec![joined],
            };
        }
    }
    joined.split('.').collect()
}

impl<'p> Walker<'p, '_> {
    fn bound(&self, name: &str, kind: Bind) -> bool {
        self.scope.iter().rev().any(|(n, k)| *k == kind && n.eq_ignore_ascii_case(name))
    }

    fn push(&mut self, r: TableRef) {
        if !self.out.contains(&r) {
            self.out.push(r);
        }
    }

    fn name_text(&self, n: Name) -> &'p str {
        bare(n.sp.text(self.src))
    }

    /// A FROM path.
    fn table(&mut self, path: crate::base::pool::Span<Name>) {
        let names = self.ast.names.slice(path);
        let mut joined = String::new();
        for (i, n) in names.iter().enumerate() {
            if i > 0 {
                joined.push('.');
            }
            joined.push_str(self.name_text(*n));
        }
        let parts = parts(&joined);
        let Some((&last, init)) = parts.split_last() else { return };
        if init.is_empty() && self.bound(last, Bind::Cte) {
            return;
        }
        if !init.is_empty() && self.bound(init[0], Bind::Range) {
            let r = TableRef {
                project: None,
                dataset: None,
                table: joined.clone(),
                kind: RefKind::Path,
            };
            self.push(r);
            return;
        }
        let own = |s: &str| Some(s.to_owned());
        let dflt_project = self.default.and_then(|d| d.project).and_then(own);
        let dflt_dataset = self.default.map(|d| d.dataset.to_owned());
        let is_schema = init.last().is_some_and(|s| s.eq_ignore_ascii_case("INFORMATION_SCHEMA"));
        let r = if is_schema {
            let scope = &init[..init.len() - 1];
            let (project, dataset) = match scope {
                [] => (dflt_project, dflt_dataset),
                [s] => (dflt_project, own(s)),
                [p, s] => (own(p), own(s)),
                _ => (None, None),
            };
            TableRef { project, dataset, table: last.to_owned(), kind: RefKind::InformationSchema }
        } else {
            let meta = last.eq_ignore_ascii_case("__TABLES__")
                || last.eq_ignore_ascii_case("__TABLES_SUMMARY__");
            let kind = if meta { RefKind::MetaTable } else { RefKind::Table };
            let (project, dataset) = match init {
                [] => (dflt_project, dflt_dataset),
                [d] => (dflt_project, own(d)),
                [p, d] => (own(p), own(d)),
                // Deeper than project.dataset.table: not a name a table has.
                _ => (None, None),
            };
            let table = if init.len() > 2 { joined.clone() } else { last.to_owned() };
            TableRef { project, dataset, table, kind }
        };
        self.push(r);
    }

    fn query(&mut self, q: QueryId) {
        let Some(seen) = self.seen_query.get_mut(q.raw() as usize) else { return };
        if std::mem::replace(seen, true) {
            return;
        }
        let query = self.ast.queries[q];
        let base = self.scope.len();
        let ctes = self.ast.ctes.slice(query.ctes);
        if query.recursive {
            for c in ctes {
                let n = self.name_text(c.name);
                self.scope.push((n, Bind::Cte));
            }
        }
        for c in ctes {
            self.query(c.q);
            if !query.recursive {
                let n = self.name_text(c.name);
                self.scope.push((n, Bind::Cte));
            }
        }
        self.set_expr(query.body);
        for o in self.ast.order_items.slice(query.order) {
            self.expr(o.e);
        }
        for e in [query.limit, query.offset].into_iter().flatten() {
            self.expr(e);
        }
        self.scope.truncate(base);
    }

    fn set_expr(&mut self, s: Idx<SetExpr>) {
        match self.ast.set_exprs[s] {
            SetExpr::Select(sel) => self.select(sel),
            SetExpr::Paren(q) => self.query(q),
            SetExpr::SetOp { l, r, .. } => {
                self.set_expr(l);
                self.set_expr(r);
            }
        }
    }

    fn select(&mut self, sel: Idx<Select>) {
        let s = self.ast.selects[sel];
        let base = self.scope.len();
        if let Some(f) = s.from {
            self.from(f);
        }
        for item in self.ast.select_items.slice(s.items) {
            match *item {
                SelectItem::Expr { e, .. } => self.expr(e),
                SelectItem::Star { qualifier, replace, .. } => {
                    if let Some(q) = qualifier {
                        self.expr(q);
                    }
                    for f in self.ast.struct_fields.slice(replace) {
                        self.expr(f.e);
                    }
                }
            }
        }
        for e in [s.where_, s.having, s.qualify].into_iter().flatten() {
            self.expr(e);
        }
        if let GroupBy::Items(items) = s.group {
            for g in self.ast.group_items.slice(items) {
                match *g {
                    GroupItem::Expr(e) => self.expr(e),
                    GroupItem::Rollup(l) | GroupItem::Cube(l) | GroupItem::GroupingSets(l) => {
                        for &e in self.ast.expr_lists.slice(l) {
                            self.expr(e);
                        }
                    }
                    GroupItem::Empty => {}
                }
            }
        }
        for w in self.ast.named_windows.slice(s.windows) {
            self.window(w.spec);
        }
        // Range variables end with their SELECT; CTEs end with their query.
        self.scope.truncate(base);
    }

    fn range(&mut self, alias: Option<Name>, implicit: Option<&'p str>) {
        let name = alias.map(|a| self.name_text(a)).or(implicit);
        if let Some(n) = name {
            self.scope.push((n, Bind::Range));
        }
    }

    fn from(&mut self, f: FromId) {
        let Some(seen) = self.seen_from.get_mut(f.raw() as usize) else { return };
        *seen = true;
        match self.ast.froms[f] {
            From::Table { path, alias, time_travel, sample, .. } => {
                for e in [time_travel, sample].into_iter().flatten() {
                    self.expr(e);
                }
                self.table(path);
                let last = self.ast.names.slice(path).last().map(|n| self.name_text(*n));
                let implicit = last.map(|l| l.rsplit('.').next().unwrap_or(l));
                self.range(alias, implicit);
            }
            From::Subquery { q, alias } => {
                self.query(q);
                self.range(alias, None);
            }
            From::Unnest { e, alias, .. } => {
                self.expr(e);
                self.range(alias, None);
            }
            From::Join { l, r, cond, .. } => {
                self.from(l);
                self.from(r);
                if let JoinCond::On(e) = cond {
                    self.expr(e);
                }
            }
            From::Paren(inner) => self.from(inner),
        }
    }

    fn window(&mut self, w: Idx<WindowSpec>) {
        let spec = self.ast.windows[w];
        for &e in self.ast.expr_lists.slice(spec.partition) {
            self.expr(e);
        }
        for o in self.ast.order_items.slice(spec.order) {
            self.expr(o.e);
        }
        if let Some(fr) = spec.frame {
            for b in std::iter::once(fr.start).chain(fr.end) {
                if let Bound::Preceding(e) | Bound::Following(e) = b {
                    self.expr(e);
                }
            }
        }
    }

    fn expr(&mut self, e: ExprId) {
        let mut stack = vec![e];
        let mut kids = Vec::new();
        while let Some(x) = stack.pop() {
            match self.ast.exprs[x] {
                Expr::Exists(q)
                | Expr::Subquery(q)
                | Expr::ArrayQuery(q)
                | Expr::InQuery { q, .. } => self.query(q),
                Expr::Call(c) => {
                    if let Some(w) = self.ast.calls[c].over {
                        self.window(w);
                    }
                }
                _ => {}
            }
            kids.clear();
            crate::ir::walk::children(self.ast, x, &mut kids);
            stack.extend(kids.iter().rev());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refs(sql: &str, d: Option<&str>) -> Vec<(String, RefKind)> {
        let d = d.map(DatasetRef::parse);
        referenced_tables(sql, d)
            .unwrap()
            .into_iter()
            .map(|r| {
                let (p, d) = (r.project.as_deref(), r.dataset.as_deref());
                (format!("{}.{}.{}", p.unwrap_or("?"), d.unwrap_or("?"), r.table), r.kind)
            })
            .collect()
    }

    use RefKind::{InformationSchema as Is, MetaTable, Path, Table};

    #[test]
    fn qualified_unqualified_and_domain_scoped_names() {
        assert_eq!(
            refs("SELECT * FROM p.raw.orders JOIN `other.p2.t` USING (id), d.x", Some("q.dd")),
            [("p.raw.orders".into(), Table), ("other.p2.t".into(), Table), ("q.d.x".into(), Table)]
        );
        assert_eq!(refs("SELECT 1 FROM t", Some("p.d")), [("p.d.t".into(), Table)]);
        assert_eq!(refs("SELECT 1 FROM t", None), [("?.?.t".into(), Table)]);
        assert_eq!(
            refs("SELECT 1 FROM `example.com:proj.d.t`", None),
            [("example.com:proj.d.t".into(), Table)]
        );
        assert_eq!(
            refs("SELECT 1 FROM `example.com:proj`.d.t", None),
            [("example.com:proj.d.t".into(), Table)]
        );
    }

    #[test]
    fn information_schema_and_meta_tables() {
        let r = referenced_tables(
            "SELECT * FROM `p`.`region-us`.INFORMATION_SCHEMA.JOBS j, d.INFORMATION_SCHEMA.TABLES, ds.__TABLES__",
            None,
        )
        .unwrap();
        assert_eq!(r[0].kind, Is);
        assert_eq!(
            (r[0].project.as_deref(), r[0].dataset.as_deref()),
            (Some("p"), Some("region-us"))
        );
        assert_eq!((r[1].kind, r[1].dataset.as_deref()), (Is, Some("d")));
        assert_eq!(r[2].kind, MetaTable);
    }

    #[test]
    fn cte_scope_follows_bigquery() {
        // Visible after its own body, in later CTEs and the body.
        assert_eq!(
            refs("WITH a AS (SELECT 1 FROM x.y), b AS (SELECT * FROM a) SELECT * FROM b, a", None),
            [("?.x.y".into(), Table)]
        );
        // Not inside its own body (plain WITH), nor in an earlier CTE.
        assert_eq!(
            refs("WITH a AS (SELECT * FROM b), b AS (SELECT * FROM a) SELECT 1", Some("p.d")),
            [("p.d.b".into(), Table)]
        );
        // Recursive: visible in every body of its WITH.
        assert!(refs(
            "WITH RECURSIVE a AS (SELECT 1 UNION ALL SELECT * FROM a) SELECT * FROM a",
            None
        )
        .is_empty());
        // A CTE declared in a subquery ends with it.
        assert_eq!(
            refs(
                "SELECT (SELECT 1 FROM (WITH s AS (SELECT 1) SELECT * FROM s)), (SELECT 1 FROM s)",
                Some("p.d")
            ),
            [("p.d.s".into(), Table)]
        );
    }

    #[test]
    fn subqueries_everywhere_are_read() {
        let sql = "SELECT (SELECT MAX(a) FROM p.d.s1), ARRAY(SELECT 1 FROM p.d.s2) FROM p.d.t \
                   WHERE EXISTS (SELECT 1 FROM p.d.s3) AND x IN (SELECT y FROM p.d.s4) \
                   QUALIFY SUM(1) OVER (ORDER BY (SELECT 1 FROM p.d.s5)) > 0";
        let got: Vec<String> = refs(sql, None).into_iter().map(|r| r.0).collect();
        assert_eq!(got, ["p.d.t", "p.d.s1", "p.d.s2", "p.d.s3", "p.d.s4", "p.d.s5"]);
    }

    #[test]
    fn array_paths_from_range_variables_are_not_tables() {
        assert_eq!(
            refs("SELECT 1 FROM p.d.t AS r, r.items", None),
            [("p.d.t".into(), Table), ("?.?.r.items".into(), Path)]
        );
        // A range variable of another SELECT is not in scope.
        assert_eq!(
            refs("SELECT (SELECT 1 FROM p.d.t AS r), (SELECT 1 FROM r.items)", Some("q.z")),
            [("p.d.t".into(), Table), ("q.r.items".into(), Table)]
        );
    }

    #[test]
    fn scripts_read_every_statement_and_merge_sources() {
        let sql = "CREATE TEMP TABLE tmp AS SELECT * FROM p.a.x; \
                   MERGE p.b.target t USING p.a.src s ON t.k = s.k WHEN MATCHED THEN DELETE; \
                   INSERT INTO p.b.out SELECT * FROM tmp";
        let got: Vec<String> = refs(sql, Some("p.b")).into_iter().map(|r| r.0).collect();
        assert_eq!(got, ["p.a.x", "p.a.src", "p.b.tmp"]);
    }

    #[test]
    fn unparseable_is_an_error() {
        assert!(referenced_tables("SELECT FROM (", None).is_err());
    }
}
