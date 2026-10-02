// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! From a producer's SQL to per-column lineage of the table it writes.
//!
//! The statement is parsed (full GoogleSQL scripts) and its row-producing
//! query is bound against the catalog, so every column reference resolves
//! to the relation it really belongs to (crab-ir lineage). Output columns
//! are then mapped onto the target's columns:
//! - `CREATE TABLE ... AS`, `CREATE [MATERIALIZED] VIEW`: by name (a view's
//!   column list first), falling back to position;
//! - `INSERT t (cols) ...`: by position through the column list, or through
//!   the target's own column order when there is none;
//! - `MERGE`: through the actual assignments — `UPDATE SET col = expr` and
//!   `INSERT (cols) VALUES (exprs)` — each expression's references to the
//!   source alias resolved into the USING query's lineage.

use crab_catalog::Catalog;
use crab_ir::{BindOpts, Binder, ColumnLineage, Transform};
use crab_lineage::Column;
use crab_sql::ast::{Expr, ExprId, GroupBy, MergeAction, Select, StmtKind};
use crab_sql::{Mode, Parser, Script};
use std::collections::BTreeSet;
use std::fmt::Write as _;

/// Lineage of one target column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnTrace {
    /// Target column (lower-cased).
    pub column: String,
    /// The expression producing it, as written.
    pub expression: Option<String>,
    /// How it relates to its sources.
    pub transform: Transform,
    /// Physical source columns.
    pub sources: Vec<Column>,
    /// Intermediate steps, outermost first: `cte o: month := DATE_TRUNC(...) | WHERE ...`.
    pub hops: Vec<String>,
    /// Some sources shape the value through a subquery (filters, lookups).
    pub via_subquery: bool,
    /// References to tables the catalog does not know.
    pub unresolved: Vec<String>,
}

/// Lineage of a table from the SQL that produced it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableTrace {
    /// The statement kind.
    pub statement: StmtKind,
    /// Per target column (columns the SQL does not write are absent).
    pub columns: Vec<ColumnTrace>,
    /// The final query's WHERE, GROUP BY, HAVING, QUALIFY (grain and filters).
    pub filters: Vec<String>,
    /// Problems worth reporting (unresolved tables, unmapped outputs).
    pub warnings: Vec<String>,
    /// Temporary tables of the script the rows passed through (inlined).
    pub staged: Vec<String>,
}

impl TableTrace {
    /// Fold in the trace of another writer of the same table: per column,
    /// sources are unioned; the transform is kept when both agree, else the
    /// column is derived; the other expression and its steps are kept as
    /// hops (labelled with `label`). Columns only the other writer sets are
    /// added; its filters are kept, labelled.
    pub fn absorb(&mut self, other: TableTrace, label: &str) {
        for oc in other.columns {
            if let Some(c) = self.columns.iter_mut().find(|c| c.column == oc.column) {
                for s in oc.sources {
                    if !c.sources.contains(&s) {
                        c.sources.push(s);
                    }
                }
                if c.transform != oc.transform {
                    c.transform = Transform::Derived;
                }
                if let Some(e) = oc.expression.filter(|e| Some(e) != c.expression.as_ref()) {
                    c.hops.push(format!("{label}: {e}"));
                }
                c.hops.extend(oc.hops.into_iter().map(|h| format!("{label}: {h}")));
                c.via_subquery |= oc.via_subquery;
                for u in oc.unresolved {
                    if !c.unresolved.contains(&u) {
                        c.unresolved.push(u);
                    }
                }
            } else {
                let mut oc = oc;
                oc.hops.insert(0, format!("set only by {label}"));
                self.columns.push(oc);
            }
        }
        self.filters.extend(other.filters.into_iter().map(|f| format!("{label}: {f}")));
        self.warnings.extend(other.warnings.into_iter().map(|w| format!("{label}: {w}")));
        for s in other.staged {
            if !self.staged.contains(&s) {
                self.staged.push(s);
            }
        }
    }
}

/// `DECLARE a[, b] TYPE [DEFAULT expr]` statements of a script, as each
/// variable's name (lower-cased) and the expression standing for it: the
/// default, else a typed NULL.
fn script_variables(script: &Script<'_>) -> Vec<(String, String)> {
    use crab_sql::lexer::{tokenize, Kw, Punct, Tok};
    let mut out = Vec::new();
    let mut toks = Vec::new();
    for st in script.stmts.iter().filter(|s| s.kind == StmtKind::Other) {
        let text = script.text(st.sp);
        if tokenize(text, &mut toks).is_err() {
            continue;
        }
        let word = |i: usize| toks.get(i).map_or("", |t| t.sp.text(text));
        if !word(0).eq_ignore_ascii_case("DECLARE") {
            continue;
        }
        let mut names = Vec::new();
        let mut i = 1;
        while matches!(toks.get(i).map(|t| t.kind), Some(Tok::Ident | Tok::QuotedIdent)) {
            names.push(word(i).to_ascii_lowercase());
            i += 1;
            if toks.get(i).map(|t| t.kind) == Some(Tok::P(Punct::Comma)) {
                i += 1;
            } else {
                break;
            }
        }
        let default = toks.iter().position(|t| t.kind == Tok::Kw(Kw::Default));
        let end = toks
            .iter()
            .position(|t| t.kind == Tok::P(Punct::Semi))
            .map_or(text.len(), |k| toks[k].sp.start as usize);
        let ty_end = default.map_or(end, |d| toks[d].sp.start as usize);
        let ty_start = toks.get(i).map_or(ty_end, |t| t.sp.start as usize);
        let value = match default {
            Some(d) => format!("({})", text[toks[d].sp.end as usize..end].trim()),
            None => format!("CAST(NULL AS {})", text[ty_start..ty_end.max(ty_start)].trim()),
        };
        for n in names {
            out.push((n, value.clone()));
        }
    }
    out
}

/// The script with its DECLAREs blanked out and every reference to a
/// declared variable replaced by its value; `None` without DECLAREs. The
/// result declares nothing, so tracing it cannot substitute again.
fn substitute_variables(script: &Script<'_>) -> Option<String> {
    use crab_sql::lexer::{tokenize, Kw, Punct, Tok};
    let vars = script_variables(script);
    if vars.is_empty() {
        return None;
    }
    let mut src = script.src.to_owned();
    for s in script.stmts.iter().filter(|s| s.kind == StmtKind::Other) {
        let text = script.text(s.sp);
        if text.trim_start().get(..7).is_some_and(|w| w.eq_ignore_ascii_case("DECLARE")) {
            let (a, b) = (s.sp.start as usize, s.sp.end as usize);
            src.replace_range(a..b, &" ".repeat(b - a));
        }
    }
    let mut toks = Vec::new();
    tokenize(&src, &mut toks).ok()?;
    let mut out = String::with_capacity(src.len() + 64);
    let mut pos = 0usize;
    for (i, t) in toks.iter().enumerate() {
        if t.kind != Tok::Ident {
            continue;
        }
        let kind = |k: usize| toks.get(k).map(|t| t.kind);
        // Not a qualified name, an alias, or a function name.
        if i > 0 && matches!(kind(i - 1), Some(Tok::P(Punct::Dot) | Tok::Kw(Kw::As))) {
            continue;
        }
        if matches!(kind(i + 1), Some(Tok::P(Punct::Dot | Punct::LParen))) {
            continue;
        }
        let w = t.sp.text(&src);
        let Some((_, v)) = vars.iter().find(|(n, _)| n.eq_ignore_ascii_case(w)) else { continue };
        out.push_str(&src[pos..t.sp.start as usize]);
        out.push_str(v);
        pos = t.sp.end as usize;
    }
    out.push_str(&src[pos..]);
    Some(out)
}

/// Source span of a path, with the backticks of quoted names.
fn path_sp(
    script: &Script<'_>,
    names: crab_core::pool::Span<crab_sql::ast::Name>,
) -> Option<crab_sql::Sp> {
    let ns = script.ast.names.slice(names);
    let (first, last) = (ns.first()?, ns.last()?);
    let src = script.src.as_bytes();
    let mut sp = crab_sql::Sp { start: first.sp.start, end: last.sp.end };
    if sp.start > 0 && src.get(sp.start as usize - 1) == Some(&b'`') {
        sp.start -= 1;
    }
    if src.get(sp.end as usize) == Some(&b'`') {
        sp.end += 1;
    }
    Some(sp)
}

/// An UPDATE as the query computing its new values:
/// `SELECT expr AS col, ... FROM target [AS alias][, from items] [WHERE cond]`.
fn update_query(script: &Script<'_>, u: &crab_sql::ast::Stmt) -> Option<String> {
    let mut q = String::from("SELECT ");
    for (i, it) in script.set_items(u.clauses).iter().enumerate() {
        let col = script.ast.names.slice(it.col).first()?;
        let name = script.text(col.sp).trim_matches('`');
        let e = crab_ir::span_in(script.ast, script.src, it.value);
        if i > 0 {
            q.push_str(", ");
        }
        let _ = write!(q, "{} AS `{name}`", script.text(e));
    }
    let _ = write!(q, " FROM {}", script.text(path_sp(script, u.target?)?));
    if let Some(a) = u.target_alias {
        let _ = write!(q, " AS {}", script.text(a.sp));
    }
    if u.query_sp.end > u.query_sp.start {
        let _ = write!(q, ", {}", script.text(u.query_sp));
    }
    if let Some(w) = u.on {
        let _ = write!(q, " WHERE {}", script.text(crab_ir::span_in(script.ast, script.src, w)));
    }
    Some(q)
}

/// Largest script rewrite produced by inlining (bytes).
const MAX_INLINED: usize = 256 * 1024;

/// The script's final writing statement with every reference to a table an
/// earlier statement of the same script created (unqualified: temporary
/// tables) replaced by `(its query) AS name`, recursively. `None` when
/// nothing was staged, or the result would exceed [`MAX_INLINED`].
fn inline_script(script: &Script<'_>, last: &crab_sql::ast::Stmt) -> Option<(String, Vec<String>)> {
    use std::collections::HashMap;
    let mut defs: HashMap<String, String> = HashMap::new();
    let mut staged: Vec<String> = Vec::new();
    // Rewrite `sp` of the source with the staged tables known so far.
    let rewrite = |sp: crab_sql::Sp,
                   defs: &HashMap<String, String>,
                   staged: &mut Vec<String>|
     -> Option<String> {
        let mut edits: Vec<(u32, u32, String)> = Vec::new();
        let mut single = |names: crab_core::pool::Span<crab_sql::ast::Name>, alias: bool| {
            let ns = script.ast.names.slice(names);
            let ([n], Some(_)) = (ns, ns.first()) else { return };
            if n.sp.start < sp.start || n.sp.end > sp.end {
                return;
            }
            let name = script.text(n.sp).trim_matches('`').to_ascii_lowercase();
            if let Some(q) = defs.get(&name) {
                let text = if alias { format!("({q})") } else { format!("({q}) AS `{name}`") };
                // A quoted name's span excludes its backticks.
                let src = script.src.as_bytes();
                let (a, b) = (n.sp.start as usize, n.sp.end as usize);
                let quoted = a > 0 && src.get(a - 1) == Some(&b'`') && src.get(b) == Some(&b'`');
                let (a, b) =
                    if quoted { (n.sp.start - 1, n.sp.end + 1) } else { (n.sp.start, n.sp.end) };
                edits.push((a, b, text));
                if !staged.contains(&name) {
                    staged.push(name);
                }
            }
        };
        for (_, f) in script.ast.froms.iter() {
            if let crab_sql::ast::From::Table { path, alias, .. } = *f {
                single(path, alias.is_some());
            }
        }
        if let Some(src) = last.source_table.filter(|_| sp == last.sp) {
            single(src, last.source_alias.is_some());
        }
        if edits.is_empty() {
            return Some(script.text(sp).to_owned());
        }
        edits.sort_by_key(|e| e.0);
        let mut out = String::with_capacity(sp.end as usize - sp.start as usize + 256);
        let mut pos = sp.start;
        for (a, b, t) in edits {
            if a < pos {
                continue;
            }
            out.push_str(script.text(crab_sql::Sp { start: pos, end: a }));
            out.push_str(&t);
            pos = b;
            if out.len() > MAX_INLINED {
                return None;
            }
        }
        out.push_str(script.text(crab_sql::Sp { start: pos, end: sp.end }));
        Some(out)
    };
    for st in script.stmts {
        if st.sp == last.sp {
            break;
        }
        let Some(target) = st.target else { continue };
        if st.kind != StmtKind::CreateTable || st.query.is_none() {
            continue;
        }
        let names = script.ast.names.slice(target);
        let [n] = names else { continue };
        let name = script.text(n.sp).trim_matches('`').to_ascii_lowercase();
        let q = rewrite(st.query_sp, &defs, &mut Vec::new())?;
        if q.len() > MAX_INLINED {
            return None;
        }
        defs.insert(name, q);
    }
    if defs.is_empty() {
        return None;
    }
    let sql = rewrite(last.sp, &defs, &mut staged)?;
    (!staged.is_empty()).then_some((sql, staged))
}

/// Trace `sql` (the producer of `target`, `project.dataset.table`) onto
/// `target_columns` (top-level names, in table order).
pub fn trace(
    sql: &str,
    target: &str,
    target_columns: &[String],
    cat: &Catalog,
) -> Result<TableTrace, String> {
    let mut parser = Parser::new();
    let script = parser
        .parse_script(sql, Mode::Full)
        .map_err(|e| format!("producer SQL does not parse: {}", e.msg))?;
    // Script variables (`DECLARE cutoff DATE DEFAULT ...`) are not columns:
    // bind their references as the value they are declared with.
    if let Some(sql) = substitute_variables(&script) {
        return trace(&sql, target, target_columns, cat);
    }
    // A script may write several tables: the statements writing this one
    // (matched on the table name), else the script's last write.
    let name = target.rsplit('.').next().unwrap_or(target);
    let writes_target = |s: &&crab_sql::ast::Stmt| {
        s.target.is_some_and(|t| {
            let path = script.path(t);
            path.rsplit('.').next().is_some_and(|n| n.eq_ignore_ascii_case(name))
        })
    };
    let ours = script.stmts.iter().any(|s| writes_target(&s));
    let stmt = match script
        .stmts
        .iter()
        .rev()
        .filter(|s| !ours || writes_target(s))
        .find(|s| !matches!(s.kind, StmtKind::Query | StmtKind::Other | StmtKind::Update))
    {
        Some(s) => *s,
        None => *script.last_write().ok_or("producer SQL has no statement that writes rows")?,
    };
    // UPDATEs rewrite columns of rows another statement wrote: trace the
    // script's main write without them, then fold each in as a writer of the
    // columns it sets. A script of UPDATEs only is traced as those.
    let main = (stmt.kind != StmtKind::Update).then_some(stmt);
    let updates: Vec<crab_sql::ast::Stmt> = script
        .stmts
        .iter()
        .filter(|s| s.kind == StmtKind::Update && (!ours || writes_target(s)))
        .copied()
        .collect();
    if !updates.is_empty() {
        let mut out = match main {
            Some(_) => {
                // The script with its UPDATEs blanked out (same offsets).
                let mut rest = sql.to_owned();
                for u in &updates {
                    let (a, b) = (u.sp.start as usize, u.sp.end as usize);
                    rest.replace_range(a..b, &" ".repeat(b - a));
                }
                trace(&rest, target, target_columns, cat)?
            }
            None => TableTrace {
                statement: StmtKind::Update,
                columns: Vec::new(),
                filters: Vec::new(),
                warnings: Vec::new(),
                staged: Vec::new(),
            },
        };
        for (i, u) in updates.iter().enumerate() {
            let label =
                if updates.len() == 1 { "UPDATE".to_owned() } else { format!("UPDATE {}", i + 1) };
            match update_query(&script, u) {
                Some(q) => match trace(&q, target, target_columns, cat) {
                    Ok(x) if main.is_none() && i == 0 => {
                        out = TableTrace { statement: StmtKind::Update, ..x };
                    }
                    Ok(x) => out.absorb(x, &label),
                    Err(e) => out.warnings.push(format!("{label}: {e}")),
                },
                None => out.warnings.push(format!("{label}: not traceable")),
            }
        }
        return Ok(out);
    }
    // A script that stages rows in temporary tables first (dbt incremental
    // `CREATE TEMP TABLE x__dbt_tmp AS ...; MERGE ... USING x__dbt_tmp`):
    // trace the final statement with each staged table inlined as the query
    // that filled it, so lineage reaches the physical sources.
    if let Some((inlined, staged)) = inline_script(&script, &stmt) {
        let mut out = trace(&inlined, target, target_columns, cat)?;
        out.staged = staged;
        return Ok(out);
    }
    let mut parts = target.splitn(3, '.');
    let (project, dataset) = (parts.next().unwrap_or(""), parts.next());
    let mut out = TableTrace {
        statement: stmt.kind,
        columns: Vec::new(),
        filters: Vec::new(),
        warnings: Vec::new(),
        staged: Vec::new(),
    };
    let target_lc: Vec<String> = target_columns.iter().map(|c| c.to_ascii_lowercase()).collect();
    let mut binder = Binder::new();

    // The source rows: the statement's query, or `USING table` of a MERGE.
    let Some(parsed) = script.query(&stmt) else {
        if stmt.kind == StmtKind::Merge {
            if let Some(src) = stmt.source_table {
                merge_from_table(
                    &script,
                    &stmt,
                    &script.path(src),
                    &target_lc,
                    project,
                    dataset,
                    cat,
                    &mut out,
                );
                return Ok(out);
            }
        }
        return Err("producer statement carries no query (e.g. INSERT ... VALUES)".into());
    };
    let bound = binder.bind(
        &parsed,
        cat,
        BindOpts { default_project: project, default_dataset: dataset, mode: Mode::Full },
    );
    for d in bound.diags.iter().filter(|d| d.severity == crab_ir::Severity::Error) {
        out.warnings.push(d.message(sql));
    }
    let lin = crab_ir::lineage(&parsed, bound, cat);
    let render = |l: &ColumnLineage| column_trace(&script, cat, l);

    // Final query filters and grain.
    if let Some(sel) = lin.iter().find_map(|l| l.select) {
        out.filters = select_filters(&script, &script.ast.selects[sel]);
    }

    if stmt.kind == StmtKind::Merge {
        let alias =
            stmt.source_alias.map(|a| script.text(a.sp).trim_matches('`').to_ascii_lowercase());
        let by_name = |name: &str| {
            lin.iter().find(|l| l.name.as_deref().is_some_and(|n| n.eq_ignore_ascii_case(name)))
        };
        for (col, value) in merge_assignments(
            &script,
            &stmt,
            &lin.iter().filter_map(|l| l.name.clone()).collect::<Vec<_>>(),
        ) {
            if !target_lc.contains(&col) || out.columns.iter().any(|c| c.column == col) {
                continue;
            }
            let refs = source_refs(&script, value, alias.as_deref());
            let mut t = ColumnTrace {
                column: col.clone(),
                expression: Some(crab_ir::span(&parsed, value).text(sql).to_owned()),
                transform: Transform::Derived,
                sources: Vec::new(),
                hops: Vec::new(),
                via_subquery: false,
                unresolved: Vec::new(),
            };
            let bare = matches!(script.ast.exprs[value], Expr::Ident(_) | Expr::Field { .. });
            for r in &refs {
                match by_name(r) {
                    Some(l) => {
                        let inner = render(l);
                        if bare && refs.len() == 1 {
                            t.transform = if inner.transform == Transform::Passthrough
                                && !r.eq_ignore_ascii_case(&col)
                            {
                                Transform::Rename
                            } else {
                                inner.transform
                            };
                        }
                        t.sources.extend(inner.sources);
                        t.hops.push(format!(
                            "source `{r}` := {}",
                            inner.expression.unwrap_or_default()
                        ));
                        t.hops.extend(inner.hops);
                        t.via_subquery |= inner.via_subquery;
                        t.unresolved.extend(inner.unresolved);
                    }
                    None => {
                        t.unresolved.push(format!("{r}: not an output of the MERGE source"));
                    }
                }
            }
            if refs.is_empty() {
                t.transform = Transform::Constant;
            }
            dedup(&mut t.sources);
            out.columns.push(t);
        }
    } else {
        // Output name per position: a view's column list, an INSERT's
        // column list (or the target order), else the query's own names.
        let names: Vec<Option<String>> = match stmt.kind {
            StmtKind::Insert if stmt.columns.is_empty() => {
                (0..lin.len()).map(|i| target_lc.get(i).cloned()).collect()
            }
            StmtKind::Insert | StmtKind::CreateView | StmtKind::CreateMaterializedView
                if !stmt.columns.is_empty() =>
            {
                script
                    .ast
                    .names
                    .slice(stmt.columns)
                    .iter()
                    .map(|n| Some(script.text(n.sp).trim_matches('`').to_ascii_lowercase()))
                    .collect()
            }
            _ => lin.iter().map(|l| l.name.as_ref().map(|n| n.to_ascii_lowercase())).collect(),
        };
        let by_position = names.iter().flatten().filter(|n| target_lc.contains(n)).count() == 0
            && lin.len() == target_lc.len();
        for (i, l) in lin.iter().enumerate() {
            let name = if by_position {
                target_lc.get(i).cloned()
            } else {
                names.get(i).cloned().flatten()
            };
            let Some(name) = name else {
                out.warnings.push(format!("output #{} has no name", i + 1));
                continue;
            };
            if !target_lc.is_empty() && !target_lc.contains(&name) {
                out.warnings.push(format!("output `{name}` is not a column of {target}"));
                continue;
            }
            let mut t = render(l);
            if name != l.name.as_deref().unwrap_or("").to_ascii_lowercase()
                && t.transform == Transform::Passthrough
            {
                t.transform = Transform::Rename;
            }
            t.column = name;
            out.columns.push(t);
        }
    }
    Ok(out)
}

/// MERGE `USING table`: every assignment `col = s.x` reads `table.x`.
#[allow(clippy::too_many_arguments)]
fn merge_from_table(
    script: &Script<'_>,
    stmt: &crab_sql::ast::Stmt,
    source: &str,
    target: &[String],
    project: &str,
    dataset: Option<&str>,
    cat: &Catalog,
    out: &mut TableTrace,
) {
    let fqn = match source.split('.').count() {
        1 => format!("{project}.{}.{source}", dataset.unwrap_or("")),
        2 => format!("{project}.{source}"),
        _ => source.to_owned(),
    };
    let known: Vec<String> = cat
        .tables()
        .find(|t| format!("{}.{}.{}", t.project, t.dataset_name, t.name).eq_ignore_ascii_case(&fqn))
        .map(|t| {
            cat.columns(t.id)
                .filter(|c| c.depth == 0)
                .map(|c| c.path.to_ascii_lowercase())
                .collect()
        })
        .unwrap_or_default();
    let alias = stmt.source_alias.map(|a| script.text(a.sp).trim_matches('`').to_ascii_lowercase());
    for (col, value) in merge_assignments(script, stmt, &known) {
        if !target.contains(&col) || out.columns.iter().any(|c| c.column == col) {
            continue;
        }
        let refs = source_refs(script, value, alias.as_deref());
        let bare = matches!(script.ast.exprs[value], Expr::Ident(_) | Expr::Field { .. });
        let transform = match (refs.len(), bare) {
            (0, _) => Transform::Constant,
            (1, true) if refs[0].eq_ignore_ascii_case(&col) => Transform::Passthrough,
            (1, true) => Transform::Rename,
            _ => Transform::Derived,
        };
        out.columns.push(ColumnTrace {
            column: col,
            expression: None,
            transform,
            sources: refs.iter().map(|r| Column::new(fqn.clone(), r)).collect(),
            hops: Vec::new(),
            via_subquery: false,
            unresolved: if known.is_empty() {
                vec![format!("{fqn}: not in the catalog")]
            } else {
                Vec::new()
            },
        });
    }
}

/// `(target column, value expression)` of every MERGE assignment; `INSERT
/// ROW` maps the source columns (`source_names`) by name.
fn merge_assignments(
    script: &Script<'_>,
    stmt: &crab_sql::ast::Stmt,
    source_names: &[String],
) -> Vec<(String, ExprId)> {
    let mut out = Vec::new();
    for c in script.merge_clauses(stmt) {
        match c.action {
            MergeAction::Update(r) => {
                for item in script.set_items(r) {
                    let names = script.ast.names.slice(item.col);
                    if let Some(last) = names.last() {
                        out.push((
                            script.text(last.sp).trim_matches('`').to_ascii_lowercase(),
                            item.value,
                        ));
                    }
                }
            }
            MergeAction::Insert { cols, values } => {
                let vals = script.ast.expr_lists.slice(values);
                for (n, &v) in script.ast.names.slice(cols).iter().zip(vals) {
                    out.push((script.text(n.sp).trim_matches('`').to_ascii_lowercase(), v));
                }
            }
            MergeAction::InsertRow | MergeAction::Delete => {}
        }
    }
    // INSERT ROW (and unassigned columns) fall back to same-named outputs;
    // no value expression exists for them, so they are left to the caller.
    let _ = source_names;
    out
}

/// Names of source columns an assignment reads: `s.x` (with the source
/// alias) or a bare `x`.
fn source_refs(script: &Script<'_>, e: ExprId, alias: Option<&str>) -> Vec<String> {
    let ast = script.ast;
    let mut out: Vec<String> = Vec::new();
    let mut stack = vec![e];
    while let Some(x) = stack.pop() {
        match ast.exprs[x] {
            Expr::Field { base, field } => {
                if let Expr::Ident(b) = ast.exprs[base] {
                    let b = script.text(b.sp).trim_matches('`').to_ascii_lowercase();
                    if alias.is_none_or(|a| a == b) {
                        out.push(script.text(field.sp).trim_matches('`').to_ascii_lowercase());
                    }
                    continue;
                }
                stack.push(base);
            }
            Expr::Ident(n) => out.push(script.text(n.sp).trim_matches('`').to_ascii_lowercase()),
            _ => crab_ir::visit(ast, x, &mut |k| {
                if k == x {
                    return true;
                }
                stack.push(k);
                false
            }),
        }
    }
    out.sort();
    out.dedup();
    out
}

fn dedup(v: &mut Vec<Column>) {
    v.sort();
    v.dedup();
}

/// A column's trace from its crab-ir lineage.
fn column_trace(script: &Script<'_>, cat: &Catalog, l: &ColumnLineage) -> ColumnTrace {
    let mut sources: Vec<Column> = l
        .sources
        .iter()
        .filter_map(|&c| {
            let col = cat.column(c)?;
            let t = cat.table(col.table)?;
            Some(Column::new(format!("{}.{}.{}", t.project, t.dataset_name, t.name), col.path))
        })
        .collect();
    dedup(&mut sources);
    let mut seen = BTreeSet::new();
    let hops = l
        .hops
        .iter()
        .filter_map(|h| {
            let mut s = String::new();
            match (&h.relation, h.set_branch) {
                (Some(r), _) => {
                    let _ = write!(s, "`{r}`: ");
                }
                (None, true) => s.push_str("set-operation branch: "),
                (None, false) => s.push_str("subquery: "),
            }
            s.push_str(h.column.as_deref().unwrap_or("?"));
            if let Some(e) = h.expr {
                s.push_str(" := ");
                s.push_str(e.text(script.src));
            }
            if let Some(sel) = h.select {
                for f in select_filters(script, &script.ast.selects[sel]) {
                    s.push_str(" | ");
                    s.push_str(&f);
                }
            }
            seen.insert(s.clone()).then_some(s)
        })
        .collect();
    ColumnTrace {
        column: l.name.clone().unwrap_or_default().to_ascii_lowercase(),
        expression: l.expr.map(|sp| sp.text(script.src).to_owned()),
        transform: l.transform,
        sources,
        hops,
        via_subquery: l.via_subquery,
        unresolved: l.opaque.iter().map(|sp| sp.text(script.src).to_owned()).collect(),
    }
}

/// `WHERE ...`, `GROUP BY ...`, `HAVING ...`, `QUALIFY ...` of a SELECT, as written.
fn select_filters(script: &Script<'_>, sel: &Select) -> Vec<String> {
    let text = |e: ExprId| crab_ir::span_in(script.ast, script.src, e).text(script.src).to_owned();
    let mut out = Vec::new();
    if let Some(w) = sel.where_ {
        out.push(format!("WHERE {}", text(w)));
    }
    match sel.group {
        GroupBy::Items(items) => {
            let keys: Vec<String> = script
                .ast
                .group_items
                .slice(items)
                .iter()
                .filter_map(|g| match *g {
                    crab_sql::ast::GroupItem::Expr(e) => Some(text(e)),
                    _ => None,
                })
                .collect();
            out.push(format!("GROUP BY {}", keys.join(", ")));
        }
        GroupBy::All => out.push("GROUP BY ALL".into()),
        GroupBy::None => {}
    }
    if let Some(h) = sel.having {
        out.push(format!("HAVING {}", text(h)));
    }
    if let Some(q) = sel.qualify {
        out.push(format!("QUALIFY {}", text(q)));
    }
    out
}
