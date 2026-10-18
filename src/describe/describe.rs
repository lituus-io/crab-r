// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Describe a project, datasets, tables or a query's result: discovery,
//! evidence, generative validation. A run's scope is a list of [`Target`]s.
//!
//! 1. **Catalog**: INFORMATION_SCHEMA per dataset (existing descriptions,
//!    types, partitioning), value profiles, and the free `tables.get`
//!    resource of every table (etag, raw schema, view definitions).
//! 2. **Producers**: view definitions; for tables, the job history.
//! 3. **Upstream**: tables the producers read, in any dataset or project,
//!    are added from their resources, so the SQL binds and upstream
//!    descriptions are known.
//! 4. **Lineage**: every producer is traced onto its table's columns; the
//!    table-level DAG orders the work in levels, sources first — a table is
//!    described after everything it reads, and inherits what was verified
//!    upstream.
//! 5. **Per table** (tables of a level run concurrently): columns with a
//!    description are kept (or audited); pass-through and rename columns
//!    whose single source is documented inherit it verbatim (no model call);
//!    the rest get one **draft** call (descriptions plus the model's own
//!    validation checks), a guarded **check run** (one scan), and one
//!    **judge** call (verdicts, revisions, confidence), with at most one
//!    more round when the judge asks for more checks.
//! 6. Budgets bound model spend and warehouse bytes; tables left when a
//!    budget runs out are reported `pending`, never guessed.

use crate::base::{Budget, Spend};
use crate::catalog::{
    Catalog, CatalogBuilder, ColumnSpec, ProfileConfig, SyncConfig, TableKind, TableSpec,
};
use crate::describe::checks::{self, Check, CheckResult, CheckSql, Outcome, RunnerConfig};
use crate::describe::generate::{self, Draft, DraftCheck, Judgement, DRAFT_SYSTEM, JUDGE_SYSTEM};
use crate::describe::incremental::{self, KeyInputs, Prior, PriorTable};
use crate::describe::producers::{self, JobsConfig, Producer};
use crate::describe::trace::{self, ColumnTrace, TableTrace};
use crate::exec::{TableMeta, TableMetadata, Warehouse};
use crate::ir::Transform;
use crate::lineage::{Column, Graph, LineageSource};
use crate::llm::{Candidates, Constraint, GenRequest, Model, Part, Prompt};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

/// Version of what a describe run produces. Raised whenever tracing,
/// prompts or decisions change results, so reports written by older code
/// are described again instead of reused.
pub const STATE_VERSION: u64 = 3;

/// What a run describes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    /// Every dataset of a project in the run's region (one metadata pass).
    Project(String),
    /// One dataset, `project.dataset`.
    Dataset(String),
    /// One table, `project.dataset.table`; what it reads is evidence
    /// (and described too with [`DescribeConfig::upstream`]).
    Table(String),
    /// SQL text. With `target`, the SQL is that table's producer (CTAS,
    /// INSERT, MERGE, a view body): the table is described as the SQL
    /// fills it. Without, its result is described as a virtual table named
    /// `_query_<hash>` in `default_dataset` (columns from a free dry run;
    /// checks run over the query itself).
    Sql {
        /// The statement or script.
        sql: String,
        /// The table it writes, `project.dataset.table`.
        target: Option<String>,
        /// `project.dataset` for unqualified table names.
        default_dataset: Option<String>,
    },
}

impl Target {
    /// Classify a name: `project`, `project.dataset` or
    /// `project.dataset.table`. A project may carry a domain
    /// (`example.com:project`); datasets and tables never contain dots.
    #[must_use]
    pub fn parse(name: &str) -> Self {
        let project_end = match name.find(':') {
            Some(colon) => name[colon..].find('.').map(|i| colon + i),
            None => name.find('.'),
        };
        let Some(end) = project_end else { return Self::Project(name.to_owned()) };
        match name[end + 1..].matches('.').count() {
            0 => Self::Dataset(name.to_owned()),
            _ => Self::Table(name.to_owned()),
        }
    }

    /// The project the target lives in (for billing and the home catalog).
    #[must_use]
    pub fn project(&self) -> Option<&str> {
        match self {
            Self::Project(p) => Some(p),
            Self::Dataset(d) | Self::Sql { target: None, default_dataset: Some(d), .. } => {
                d.rsplit_once('.').map(|(p, _)| p)
            }
            Self::Table(t) | Self::Sql { target: Some(t), .. } => {
                crate::base::key::split_fqn(t).map(|(p, _, _)| p)
            }
            Self::Sql { .. } => None,
        }
    }
}

impl From<&str> for Target {
    fn from(name: &str) -> Self {
        Self::parse(name)
    }
}

/// Producer statement type of a virtual table (a query's result).
pub const VIRTUAL: &str = "QUERY";

/// Settings of a describe run.
#[derive(Clone, Debug)]
#[allow(clippy::struct_excessive_bools)] // independent switches, not a state machine
pub struct DescribeConfig {
    /// What to describe.
    pub targets: Vec<Target>,
    /// Levels of upstream tables (what [`Target::Table`]s read, then what
    /// those read, ...) described too; 0 keeps them as evidence only.
    pub upstream: u32,
    /// Region of the datasets and jobs (`US`, `EU`, `northamerica-northeast1`).
    pub region: String,
    /// Job-history discovery.
    pub jobs: JobsConfig,
    /// Profile column values (recommended: evidence for checks and drafts).
    pub profile: bool,
    /// Check execution limits.
    pub runner: RunnerConfig,
    /// Rounds of checks per table (1 = draft + judge; 2 allows one more).
    pub max_rounds: u8,
    /// Tables described at once.
    pub concurrency: usize,
    /// Audit existing descriptions with checks (else keep them as they are).
    pub audit_existing: bool,
    /// Regenerate columns that already have a description.
    pub overwrite: bool,
    /// Byte cap per metadata query.
    pub metadata_bytes: u64,
    /// Longest producer SQL shown to the model, characters.
    pub max_sql_chars: usize,
    /// Spending limits for the whole run.
    pub budget: Budget,
    /// Hops of external lineage followed upstream.
    pub lineage_depth: u32,
    /// A previous report: unchanged tables are reused from it (no profile,
    /// no model call, no check query). Empty: describe everything.
    pub prior: Prior,
    /// Also ask the lineage source for the consumers of every column
    /// outside the described datasets (impact analysis).
    pub downstream: bool,
}

impl DescribeConfig {
    /// Defaults for `targets` in `region`, with job history of `jobs_project`.
    #[must_use]
    pub fn new(targets: Vec<Target>, region: &str, jobs_project: &str) -> Self {
        Self {
            targets,
            upstream: 0,
            region: region.to_owned(),
            jobs: JobsConfig::new(vec![jobs_project.to_owned()], region),
            profile: true,
            runner: RunnerConfig::default(),
            max_rounds: 2,
            concurrency: 4,
            audit_existing: true,
            overwrite: false,
            metadata_bytes: 1 << 30,
            max_sql_chars: 12_000,
            budget: Budget::new(5_000_000, 100 << 30, 5_000_000, 3_600_000),
            lineage_depth: 10,
            prior: Prior::default(),
            downstream: false,
        }
    }

    /// The settings that change results, for incremental keys, with the
    /// [`STATE_VERSION`] of this code.
    fn settings(&self) -> u64 {
        STATE_VERSION << 32
            | u64::from(self.audit_existing)
            | u64::from(self.overwrite) << 1
            | u64::from(self.profile) << 2
            | u64::from(self.max_rounds) << 8
    }
}

/// What happened to a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Existing description kept (not audited).
    Keep,
    /// Existing description audited: the checks support it.
    Confirm,
    /// Existing description contradicted by a check: flagged, with a
    /// proposed correction (applied only when asked).
    Flag,
    /// Copied verbatim from its single documented source.
    Propagate,
    /// Generated and validated.
    Generate,
    /// No description could be produced.
    Skip,
}

impl Action {
    /// Lower-case name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Keep => "keep",
            Self::Confirm => "confirm",
            Self::Flag => "flag",
            Self::Propagate => "propagate",
            Self::Generate => "generate",
            Self::Skip => "skip",
        }
    }
}

/// Metadata of one column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnReport {
    /// Column path (lower-cased).
    pub column: String,
    /// Type.
    pub data_type: String,
    /// What happened.
    pub action: Action,
    /// The description the table had.
    pub existing: Option<String>,
    /// The description to use (for `Flag`: the proposed correction).
    pub description: Option<String>,
    /// Semantic type.
    pub semantic_type: Option<String>,
    /// Unit.
    pub unit: Option<String>,
    /// How the SQL computes it.
    pub transform: Option<Transform>,
    /// The producing expression.
    pub expression: Option<String>,
    /// Physical sources.
    pub sources: Vec<Column>,
    /// Intermediate steps.
    pub hops: Vec<String>,
    /// `high`, `medium` or `low`.
    pub confidence: Option<String>,
    /// Ids of checks about this column.
    pub checks: Vec<u32>,
    /// Columns outside the described datasets computed from this one
    /// (`project.dataset.table.column`), when downstream lineage was asked.
    pub consumers: Vec<Column>,
}

/// A check with its verdict.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckReport {
    /// The check and its outcome.
    pub result: CheckResult,
    /// Judge verdict (`supports`, `contradicts`, `inconclusive`).
    pub verdict: Option<String>,
    /// Why.
    pub reason: Option<String>,
    /// Round (1-based).
    pub round: u8,
    /// The judge and the mechanical comparison disagreed.
    pub disputed: bool,
}

/// Metadata of one table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableReport {
    /// `project.dataset.table`.
    pub table: String,
    /// `TABLE`, `VIEW`, ...
    pub kind: String,
    /// Rows.
    pub rows: u64,
    /// Logical bytes.
    pub bytes: u64,
    /// Last modification, epoch milliseconds.
    pub last_modified_ms: u64,
    /// How it is produced.
    pub producer: Option<Producer>,
    /// Existing table description.
    pub existing: Option<String>,
    /// Table description to use.
    pub description: Option<String>,
    /// Grain columns proposed.
    pub grain: Vec<String>,
    /// Filters and grain of the producing query.
    pub filters: Vec<String>,
    /// Temporary tables of the producing script the rows passed through.
    pub staged: Vec<String>,
    /// Columns of other tables that decide which rows this one holds
    /// (filters, join keys, grouping, MERGE matching), `project.dataset.table.column`.
    pub conditions: Vec<Column>,
    /// Columns.
    pub columns: Vec<ColumnReport>,
    /// Checks run.
    pub checks: Vec<CheckReport>,
    /// Model calls made.
    pub model_calls: u32,
    /// Problems (unparsable producer, rejected checks, ...).
    pub warnings: Vec<String>,
    /// Not described: a budget ran out first.
    pub pending: bool,
    /// Spend for this table.
    pub spend: Spend,
    /// The raw resource, for schema output and apply (not serialised).
    pub meta: Option<TableMeta>,
    /// Shape, data and producer key (see [`incremental`]).
    pub prekey: u64,
    /// Full key; `None` when the table is not complete (pending, or a
    /// model call failed), so a rerun describes it again.
    pub key: Option<u64>,
    /// Carried over unchanged from the previous report.
    pub reused: bool,
    /// The previous report's check evidence, as JSON, when reused.
    pub carried_checks: Option<String>,
}

/// The run's result.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// Tables, in description order.
    pub tables: Vec<TableReport>,
    /// Tables per lineage level (sources first).
    pub levels: Vec<Vec<String>>,
    /// Run-level problems.
    pub warnings: Vec<String>,
    /// Datasets whose catalog could not be read (see [`Discovery::unreadable`]).
    pub unreadable: Vec<String>,
    /// Total spend.
    pub spend: Spend,
    /// The store could not be read and was regenerated empty: this run
    /// reused nothing and recorded only what it described.
    pub regenerated: bool,
}

/// `project.dataset.table` of a catalog table.
fn fqn(t: &crate::catalog::TableRef<'_>) -> String {
    format!("{}.{}.{}", t.project, t.dataset_name, t.name)
}

/// A table resource's schema as catalog columns (nested paths, repeated
/// fields as arrays, legacy type names mapped to GoogleSQL).
pub(crate) fn schema_columns(meta: &TableMeta) -> Vec<ColumnSpec> {
    fn ty(v: &generate::Value) -> String {
        let base = match v.str("type").unwrap_or("STRING") {
            "INTEGER" => "INT64".to_owned(),
            "FLOAT" => "FLOAT64".to_owned(),
            "BOOLEAN" => "BOOL".to_owned(),
            "RECORD" | "STRUCT" => {
                let inner: Vec<String> = v
                    .arr("fields")
                    .iter()
                    .map(|f| format!("{} {}", f.str("name").unwrap_or("_"), ty(f)))
                    .collect();
                format!("STRUCT<{}>", inner.join(", "))
            }
            other => other.to_owned(),
        };
        if v.str("mode") == Some("REPEATED") {
            format!("ARRAY<{base}>")
        } else {
            base
        }
    }
    fn walk(
        fields: &[generate::Value],
        prefix: &str,
        ordinal: Option<u32>,
        out: &mut Vec<ColumnSpec>,
    ) {
        for (i, f) in fields.iter().enumerate() {
            let Some(name) = f.str("name") else { continue };
            let path = if prefix.is_empty() { name.to_owned() } else { format!("{prefix}.{name}") };
            let ord = ordinal.unwrap_or(i as u32 + 1);
            out.push(ColumnSpec {
                path: path.clone(),
                data_type: ty(f),
                ordinal: ord,
                nullable: f.str("mode") != Some("REQUIRED"),
                partitioning: false,
                hidden: false,
                clustering: None,
                description: f.str("description").map(str::to_owned),
            });
            walk(f.arr("fields"), &path, Some(ord), out);
        }
    }
    let mut out = Vec::new();
    if let Some(v) = generate::Value::parse(meta.schema_json()) {
        walk(v.arr("fields"), "", None, &mut out);
    }
    out
}

/// A catalog table spec from a table resource (for upstream tables outside
/// the synced datasets).
pub(crate) fn spec_from_meta(dataset: &str, name: &str, meta: &TableMeta) -> TableSpec {
    let kind = match meta.kind.as_str() {
        "VIEW" => TableKind::View,
        "MATERIALIZED_VIEW" => TableKind::MaterializedView,
        _ => TableKind::Table,
    };
    TableSpec {
        dataset: dataset.to_owned(),
        name: name.to_owned(),
        kind,
        description: meta.description.clone(),
        row_count: meta.num_rows,
        logical_bytes: 0,
        last_modified_ms: meta.last_modified_ms,
        require_partition_filter: false,
        columns: schema_columns(meta),
    }
}

/// Compact profile evidence for a column.
fn profile_line(cat: &Catalog, col: crate::catalog::ColId) -> Option<String> {
    let p = cat.profile(col)?;
    let mut s = String::new();
    let _ = write!(s, "{:.1}% NULL, ~{} distinct", p.null_fraction() * 100.0, p.distinct);
    if let (Some(lo), Some(hi)) = (p.min, p.max) {
        let _ = write!(s, ", min {lo}, max {hi}");
    }
    let vals: Vec<String> =
        p.values().take(8).map(|(v, n)| format!("{}×{n}", generate::quoted(v))).collect();
    if !vals.is_empty() {
        let _ = write!(
            s,
            ", top values{}: {}",
            if p.values_complete { " (all)" } else { "" },
            vals.join(", ")
        );
    }
    if p.sampled {
        s.push_str(" (sampled)");
    }
    Some(s)
}

/// What discovery found: the datasets' catalog (with profiles), their
/// table resources and producers. Hosts may build it themselves (e.g.
/// from a cached catalog) and call [`describe_discovered`].
#[derive(Debug)]
pub struct Discovery {
    /// Catalog of the datasets (project-qualified when not `home`).
    pub builder: CatalogBuilder,
    /// The builder's project.
    pub home: String,
    /// Tables to describe, `project.dataset.table`.
    pub in_scope: Vec<String>,
    /// Table resources by table.
    pub metas: HashMap<String, TableMeta>,
    /// Producers by table.
    pub producers: BTreeMap<String, Producer>,
    /// Problems met.
    pub warnings: Vec<String>,
    /// Datasets whose catalog could not be read (`project.dataset`): the
    /// run says nothing about their tables, so it must not replace what an
    /// earlier run knew about them.
    pub unreadable: Vec<String>,
    /// Warehouse spend so far.
    pub spend: Spend,
}

/// Describe `cfg.targets` (with [`DescribeConfig::upstream`] levels of what
/// they read): [`discover`], then [`describe_discovered`].
pub async fn describe<M: Model, W: Warehouse + TableMetadata, L: LineageSource>(
    model: &M,
    w: &W,
    lineage: &L,
    cfg: &DescribeConfig,
) -> Report {
    if cfg.upstream == 0 {
        let d = discover(w, cfg).await;
        return describe_discovered(model, w, lineage, cfg, d).await;
    }
    let (more, warnings) = expand_upstream(w, cfg, cfg.upstream).await;
    let mut wide = cfg.clone();
    wide.targets.extend(more);
    let mut d = discover(w, &wide).await;
    d.warnings.extend(warnings);
    describe_discovered(model, w, lineage, &wide, d).await
}

/// Prekey of `t` from its catalog spec, producer and resource.
fn prekey_of(
    cfg: &DescribeConfig,
    t: &str,
    spec: &TableSpec,
    producer: Option<&Producer>,
    meta: Option<&TableMeta>,
) -> u64 {
    // Every writer's SQL counts: a new or changed backfill changes the table.
    let joined: Option<String> = producer
        .filter(|p| !p.others.is_empty())
        .map(|p| p.all().map(|w| w.query.as_str()).collect::<Vec<_>>().join("\n;\n"));
    incremental::prekey(
        t,
        spec,
        KeyInputs {
            producer: producer.map(|p| {
                (
                    p.statement_type.as_str(),
                    joined.as_deref().unwrap_or(p.query.as_str()),
                    p.last_ms,
                )
            }),
            view_query: meta.and_then(|m| m.view_query.as_deref()),
            settings: cfg.settings(),
        },
    )
}

/// The previous record of `t` when its shape, data, producer and
/// descriptions are unchanged since.
fn unchanged<'p>(
    cfg: &'p DescribeConfig,
    t: &str,
    spec: &TableSpec,
    producer: Option<&Producer>,
    meta: Option<&TableMeta>,
) -> Option<&'p PriorTable> {
    let p = cfg.prior.get(t)?;
    (p.prekey == prekey_of(cfg, t, spec, producer, meta)
        && incremental::descriptions_compatible(spec, p))
    .then_some(p)
}

/// The datasets of one project a run covers: every one (`all`), or the
/// named ones, each whole (`None`) or narrowed to tables.
#[derive(Debug, Default)]
struct ProjectScope {
    all: bool,
    datasets: BTreeMap<String, Option<std::collections::BTreeSet<String>>>,
}

impl ProjectScope {
    fn add(&mut self, dataset: &str, table: Option<&str>) {
        let slot = self
            .datasets
            .entry(dataset.to_owned())
            .or_insert_with(|| Some(std::collections::BTreeSet::default()));
        match (slot, table) {
            (Some(set), Some(t)) => {
                set.insert(t.to_owned());
            }
            (slot, None) => *slot = None,
            (None, Some(_)) => {}
        }
    }

    fn covers(&self, dataset: &str, table: &str) -> bool {
        self.all
            || self
                .datasets
                .get(dataset)
                .is_some_and(|t| t.as_ref().is_none_or(|s| s.contains(table)))
    }
}

/// The scope of `targets` by project, and the problems met reading it.
fn scopes(targets: &[Target], warnings: &mut Vec<String>) -> BTreeMap<String, ProjectScope> {
    let mut out: BTreeMap<String, ProjectScope> = BTreeMap::new();
    for t in targets {
        match t {
            Target::Project(p) => out.entry(p.clone()).or_default().all = true,
            Target::Dataset(d) => match d.rsplit_once('.') {
                Some((p, ds)) if !p.is_empty() && !ds.is_empty() => {
                    out.entry(p.to_owned()).or_default().add(ds, None);
                }
                _ => warnings.push(format!("{d}: expected project.dataset")),
            },
            Target::Table(n) | Target::Sql { target: Some(n), .. } => {
                match crate::base::key::split_fqn(n) {
                    Some((p, d, tb)) => out.entry(p.to_owned()).or_default().add(d, Some(tb)),
                    None => warnings.push(format!("{n}: expected project.dataset.table")),
                }
            }
            Target::Sql { target: None, .. } => {}
        }
    }
    out
}

/// The table a query's result is described as: `_query_<hash>` in its
/// default dataset (else in `_query` of `project`).
fn virtual_name(sql: &str, default_dataset: Option<&str>, project: &str) -> String {
    let hash = graph_r::hash::xxh3_64(sql.as_bytes()) as u32;
    match default_dataset {
        Some(d) => format!("{d}._query_{hash:08x}"),
        None => format!("{project}._query._query_{hash:08x}"),
    }
}

/// Whether `table` is a query's result described as a virtual table.
#[must_use]
pub fn is_virtual(table: &str) -> bool {
    crate::base::key::split_fqn(table).is_some_and(|(_, _, n)| n.starts_with("_query_"))
}

/// Upstream tables of `targets`, `levels` deep: what their producers read
/// (job history and view definitions), then what those read. One job-history
/// query and free table reads per level.
pub async fn expand_upstream<W: Warehouse + TableMetadata>(
    w: &W,
    cfg: &DescribeConfig,
    levels: u32,
) -> (Vec<Target>, Vec<String>) {
    let mut warnings = Vec::new();
    let mut seen: std::collections::BTreeSet<String> = cfg
        .targets
        .iter()
        .filter_map(|t| match t {
            Target::Table(n) | Target::Sql { target: Some(n), .. } => Some(n.clone()),
            _ => None,
        })
        .collect();
    let mut frontier: Vec<String> = seen.iter().cloned().collect();
    let mut out = Vec::new();
    let mut parser = crate::sql::Parser::new();
    for _ in 0..levels {
        if frontier.is_empty() {
            break;
        }
        let datasets: Vec<String> = frontier
            .iter()
            .filter_map(|t| crate::base::key::split_fqn(t).map(|(p, d, _)| format!("{p}.{d}")))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let (producers, warns) = producers::discover(w, &cfg.jobs, &datasets).await;
        warnings.extend(warns);
        let views = objstore_r::join::join_all(frontier.iter().map(|t| async move {
            let (p, d, n) = crate::base::key::split_fqn(t)?;
            w.table_get(p, d, n).await.ok().flatten().and_then(|m| m.view_query)
        }))
        .await;
        let mut next = Vec::new();
        for (t, view) in frontier.iter().zip(views) {
            let sql =
                producers.get(t).map(|p| p.all().map(|w| w.query.clone()).collect::<Vec<_>>());
            for q in sql.into_iter().flatten().chain(view) {
                for r in reads_of(&mut parser, &q, t, false) {
                    if seen.insert(r.clone()) {
                        next.push(r);
                    }
                }
            }
        }
        out.extend(next.iter().cloned().map(Target::Table));
        frontier = next;
    }
    (out, warnings)
}

/// `sql` with its unqualified table names (not its CTEs) written as
/// `` `project.dataset.name` `` of `default_dataset`, so it runs anywhere —
/// a dry run, a check over `FROM (sql)` — without a session default.
fn qualify(sql: &str, default_dataset: &str) -> String {
    let mut p = crate::sql::Parser::new();
    let Ok(script) = p.parse_script(sql, crate::sql::Mode::Full) else { return sql.to_owned() };
    let ctes: Vec<String> =
        script.ast.ctes.iter().map(|(_, c)| script.text(c.name.sp).to_ascii_lowercase()).collect();
    let mut spans: Vec<(usize, usize, String)> = Vec::new();
    for (_, f) in script.ast.froms.iter() {
        if let crate::sql::ast::From::Table { path, .. } = *f {
            let names = script.ast.names.slice(path);
            let (Some(first), true) = (names.first(), names.len() == 1) else { continue };
            let raw = script.path(path);
            if raw.contains('.') || ctes.contains(&raw.to_ascii_lowercase()) {
                continue;
            }
            let (mut start, mut end) = (first.sp.start as usize, first.sp.end as usize);
            // A quoted name's span may hold only its text: take the quotes.
            if start > 0
                && sql.as_bytes().get(start - 1) == Some(&b'`')
                && sql.as_bytes().get(end) == Some(&b'`')
            {
                start -= 1;
                end += 1;
            }
            spans.push((start, end, format!("`{default_dataset}.{raw}`")));
        }
    }
    spans.sort_unstable_by_key(|s| s.0);
    let mut out = String::with_capacity(sql.len() + spans.len() * default_dataset.len());
    let mut at = 0;
    for (start, end, text) in spans {
        if start < at || end > sql.len() {
            continue;
        }
        out.push_str(&sql[at..start]);
        out.push_str(&text);
        at = end;
    }
    out.push_str(&sql[at..]);
    out
}

/// Tables `sql` (the producer of `table`) reads, `project.dataset.table`,
/// through [`crate::sql::refs::referenced_tables_with`]. Unqualified names
/// resolve in `table`'s dataset only when `unqualified` (a query's own CTEs
/// never count); a dataset-qualified name is in `table`'s project.
fn reads_of(p: &mut crate::sql::Parser, sql: &str, table: &str, unqualified: bool) -> Vec<String> {
    use crate::sql::refs::{referenced_tables_with, DatasetRef, RefKind};
    let Some((proj, ds, _)) = crate::base::key::split_fqn(table) else { return Vec::new() };
    let home = unqualified.then_some(DatasetRef { project: Some(proj), dataset: ds });
    let Ok(refs) = referenced_tables_with(p, sql, home) else { return Vec::new() };
    let mut out: Vec<String> = Vec::new();
    for r in refs.into_iter().filter(|r| r.kind == RefKind::Table) {
        let Some(d) = r.dataset else { continue };
        let full = format!("{}.{d}.{}", r.project.as_deref().unwrap_or(proj), r.table);
        if !out.contains(&full) {
            out.push(full);
        }
    }
    out
}

/// Catalog, profiles, table resources and producers of `cfg.targets`.
/// With a previous report, tables unchanged since are not profiled.
pub async fn discover<W: Warehouse + TableMetadata>(w: &W, cfg: &DescribeConfig) -> Discovery {
    let mut report = Report::default();
    let mut budget = cfg.budget;

    // 1. Catalog of the scope: one INFORMATION_SCHEMA sync per project
    //    (region-wide for a whole project).
    let scope = scopes(&cfg.targets, &mut report.warnings);
    let home = cfg
        .targets
        .iter()
        .find_map(Target::project)
        .or_else(|| cfg.jobs.job_projects.first().map(String::as_str))
        .unwrap_or("")
        .to_owned();
    let mut synced: Vec<(&str, CatalogBuilder)> = Vec::new();
    for (project, ps) in &scope {
        let mut sc = SyncConfig::new(project.as_str(), cfg.region.clone());
        if !ps.all {
            sc.datasets = Some(ps.datasets.keys().cloned().collect());
            sc.dataset_scoped = true;
        }
        sc.max_bytes_billed = cfg.metadata_bytes;
        match crate::catalog::sync(w, &sc).await {
            Ok((b, stats)) => {
                let _ = budget
                    .charge(Spend { warehouse_bytes: stats.bytes_billed, ..Spend::default() });
                synced.push((project, b));
            }
            Err(e) => {
                report.warnings.push(format!("catalog of {project}: {e}"));
                if ps.all {
                    report.unreadable.push(project.clone());
                } else {
                    report.unreadable.extend(ps.datasets.keys().map(|d| format!("{project}.{d}")));
                }
            }
        }
    }
    let mut in_scope: Vec<String> = synced
        .iter()
        .flat_map(|(p, b)| {
            let ps = &scope[*p];
            b.tables()
                .iter()
                .filter(|t| ps.covers(&t.dataset, &t.name))
                .map(move |t| format!("{p}.{}.{}", t.dataset, t.name))
        })
        .collect();
    for t in &cfg.targets {
        if let Target::Sql { target: Some(n), .. } = t {
            if !in_scope.contains(n) {
                report.warnings.push(format!(
                    "{n}: not found; describe the query alone by leaving out its target"
                ));
            }
        }
    }

    // 2. Table resources (free), concurrently.
    let metas: Vec<Option<TableMeta>> =
        objstore_r::join::join_all(in_scope.iter().map(|t| async move {
            let (p, d, n) = crate::base::key::split_fqn(t)?;
            w.table_get(p, d, n).await.ok().flatten()
        }))
        .await;
    let meta_of: HashMap<String, TableMeta> =
        in_scope.iter().cloned().zip(metas).filter_map(|(t, m)| m.map(|m| (t, m))).collect();

    // 3. Producers: job history of the datasets in scope, view definitions,
    //    then SQL given for a table.
    let datasets: Vec<String> = in_scope
        .iter()
        .filter_map(|t| crate::base::key::split_fqn(t).map(|(p, d, _)| format!("{p}.{d}")))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let (mut producers, warns) = if datasets.is_empty() {
        (BTreeMap::new(), Vec::new())
    } else {
        producers::discover(w, &cfg.jobs, &datasets).await
    };
    producers.retain(|t, _| in_scope.contains(t));
    report.warnings.extend(warns);
    for (t, m) in &meta_of {
        if let Some(q) = &m.view_query {
            producers.insert(
                t.clone(),
                Producer {
                    table: t.clone(),
                    statement_type: "VIEW".into(),
                    query: q.clone(),
                    writes: 0,
                    writers: 0,
                    first_ms: 0,
                    last_ms: m.last_modified_ms,
                    others: Vec::new(),
                },
            );
        }
    }
    let given = |sql: &str, table: &str| Producer {
        table: table.to_owned(),
        statement_type: "SQL".into(),
        query: sql.to_owned(),
        writes: 0,
        writers: 0,
        first_ms: 0,
        last_ms: 0,
        others: Vec::new(),
    };
    for t in &cfg.targets {
        if let Target::Sql { sql, target: Some(n), .. } = t {
            if in_scope.contains(n) {
                producers.insert(n.clone(), given(sql, n));
            }
        }
    }

    // 4. Profiles of the in-scope tables that changed since the previous
    //    report.
    let mut builder = CatalogBuilder::new(home.clone());
    for (project, mut b) in synced {
        if cfg.profile {
            let changed: Vec<String> = b
                .tables()
                .iter()
                .filter(|s| {
                    let t = format!("{project}.{}.{}", s.dataset, s.name);
                    in_scope.contains(&t)
                        && unchanged(cfg, &t, s, producers.get(&t), meta_of.get(&t)).is_none()
                })
                .map(|s| format!("{}.{}", s.dataset, s.name))
                .collect();
            if !changed.is_empty() {
                let datasets: Vec<String> = changed
                    .iter()
                    .filter_map(|t| t.split_once('.').map(|(d, _)| d.to_owned()))
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let pc = ProfileConfig {
                    datasets: Some(datasets),
                    tables: Some(changed),
                    ..ProfileConfig::default()
                };
                match crate::catalog::profile(w, &mut b, project, &pc).await {
                    Ok(ps) => {
                        let _ = budget
                            .charge(Spend { warehouse_bytes: ps.bytes_billed, ..Spend::default() });
                    }
                    Err(e) => report.warnings.push(format!("profiling {project}: {e}")),
                }
            }
        }
        builder.absorb(b);
    }

    // 5. Query results described as virtual tables: columns from a dry run
    //    (free), the query as the producer.
    for t in &cfg.targets {
        let Target::Sql { sql, target: None, default_dataset } = t else { continue };
        let fqn = virtual_name(sql, default_dataset.as_deref(), &home);
        let qualified;
        let sql = match default_dataset {
            Some(d) => {
                qualified = qualify(sql, d);
                &qualified
            }
            None => sql,
        };
        let Some((p, d, n)) = crate::base::key::split_fqn(&fqn) else { continue };
        let dry = match w.dry_run(sql).await {
            Ok(dry) => dry,
            Err(e) => {
                report.warnings.push(format!("query does not run ({e}); not described"));
                continue;
            }
        };
        let columns = dry
            .schema
            .iter()
            .enumerate()
            .map(|(i, c)| ColumnSpec {
                path: c.name.clone(),
                data_type: c.sql_type.clone(),
                ordinal: i as u32 + 1,
                nullable: true,
                partitioning: false,
                hidden: false,
                clustering: None,
                description: None,
            })
            .collect();
        builder.add_table(TableSpec {
            dataset: if p == home { d.to_owned() } else { format!("{p}.{d}") },
            name: n.to_owned(),
            kind: TableKind::View,
            description: None,
            row_count: 0,
            // What the query scans: sizes its checks' byte caps.
            logical_bytes: dry.bytes_processed,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns,
        });
        let mut prod = given(sql, &fqn);
        prod.statement_type = VIRTUAL.into();
        producers.insert(fqn.clone(), prod);
        in_scope.push(fqn);
    }
    Discovery {
        builder,
        home,
        in_scope,
        metas: meta_of,
        producers,
        warnings: report.warnings,
        unreadable: report.unreadable,
        spend: budget.spent(),
    }
}

/// Describe what [`discover`] found.
#[allow(clippy::too_many_lines)] // the run's stages, in order
pub async fn describe_discovered<M: Model, W: Warehouse + TableMetadata, L: LineageSource>(
    model: &M,
    w: &W,
    lineage: &L,
    cfg: &DescribeConfig,
    d: Discovery,
) -> Report {
    let Discovery {
        mut builder,
        home,
        in_scope,
        metas: mut meta_of,
        producers,
        warnings,
        unreadable,
        spend,
    } = d;
    let home = home.as_str();
    let mut report = Report { warnings, unreadable, ..Report::default() };
    let mut budget = cfg.budget;
    let _ = budget.charge(spend);

    // 4. Upstream tables the producers read, from their resources (those
    //    the catalog already holds need no read).
    let in_builder = |t: &str| {
        crate::base::key::split_fqn(t).is_some_and(|(p, d, n)| {
            let qualified = format!("{p}.{d}");
            builder
                .tables()
                .iter()
                .any(|s| s.name == n && (s.dataset == qualified || (p == home && s.dataset == d)))
        })
    };
    let mut upstream: Vec<String> = Vec::new();
    let mut reads: HashMap<&str, Vec<String>> = HashMap::new();
    {
        let mut p = crate::sql::Parser::new();
        for (t, prod) in producers.iter().flat_map(|(t, p)| p.all().map(move |w| (t, w))) {
            // A query's own unqualified names resolve in its default dataset.
            let unqualified = prod.statement_type == VIRTUAL;
            for full in reads_of(&mut p, &prod.query, t, unqualified) {
                if !in_scope.contains(&full) && !upstream.contains(&full) && !in_builder(&full) {
                    upstream.push(full.clone());
                }
                let r = reads.entry(t.as_str()).or_default();
                if !r.contains(&full) {
                    r.push(full);
                }
            }
        }
    }
    let ups: Vec<Option<TableMeta>> =
        objstore_r::join::join_all(upstream.iter().map(|t| async move {
            let (p, d, n) = crate::base::key::split_fqn(t)?;
            w.table_get(p, d, n).await.ok().flatten()
        }))
        .await;
    for (t, m) in upstream.iter().zip(ups) {
        let (Some(m), Some((p, d, n))) = (m, crate::base::key::split_fqn(t)) else {
            report.warnings.push(format!("upstream table {t} not readable"));
            continue;
        };
        let qualified = if p == home { d.to_owned() } else { format!("{p}.{d}") };
        builder.add_table(spec_from_meta(&qualified, n, &m));
        meta_of.insert(t.clone(), m);
    }

    // Incremental keys, and the tables reused from the previous report:
    // unchanged themselves, upstream text unchanged, and everything they
    // read in scope reused too (so what they inherited is unchanged).
    let specs: HashMap<(&str, &str), &TableSpec> =
        builder.tables().iter().map(|s| ((s.dataset.as_str(), s.name.as_str()), s)).collect();
    let spec_of = |t: &str| -> Option<&TableSpec> {
        let (p, d, n) = crate::base::key::split_fqn(t)?;
        if p == home {
            if let Some(s) = specs.get(&(d, n)) {
                return Some(s);
            }
        }
        specs.get(&(format!("{p}.{d}").as_str(), n)).copied()
    };
    let model_id = model.caps().id.clone();
    let mut keys: HashMap<&str, (u64, u64)> = HashMap::new();
    let mut reuse: HashMap<&str, &PriorTable> = HashMap::new();
    let mut unprofiled: Vec<&str> = Vec::new();
    for t in &in_scope {
        let Some(spec) = spec_of(t) else { continue };
        let pre = prekey_of(cfg, t, spec, producers.get(t), meta_of.get(t));
        let ups: Vec<&TableSpec> = reads
            .get(t.as_str())
            .into_iter()
            .flatten()
            .filter(|r| !in_scope.contains(r))
            .filter_map(|r| spec_of(r))
            .collect();
        let k = incremental::key(pre, &ups, &model_id);
        keys.insert(t, (pre, k));
        if let Some(p) = unchanged(cfg, t, spec, producers.get(t), meta_of.get(t)) {
            if p.key == k {
                reuse.insert(t, p);
            } else {
                unprofiled.push(t);
            }
        }
    }
    loop {
        let stale: Vec<&str> = reuse
            .keys()
            .copied()
            .filter(|t| {
                reads.get(t).into_iter().flatten().any(|r| {
                    in_scope.contains(r) && r.as_str() != *t && !reuse.contains_key(r.as_str())
                })
            })
            .collect();
        if stale.is_empty() {
            break;
        }
        for t in stale {
            reuse.remove(t);
            unprofiled.push(t);
        }
    }
    // Unchanged tables that must be described again after all (their
    // upstream changed) were not profiled in discovery: profile them now.
    if cfg.profile && !unprofiled.is_empty() {
        let pc = ProfileConfig {
            tables: Some(
                unprofiled
                    .iter()
                    .filter_map(|t| spec_of(t).map(|s| format!("{}.{}", s.dataset, s.name)))
                    .collect(),
            ),
            ..ProfileConfig::default()
        };
        match crate::catalog::profile(w, &mut builder, home, &pc).await {
            Ok(ps) => {
                let _ =
                    budget.charge(Spend { warehouse_bytes: ps.bytes_billed, ..Spend::default() });
            }
            Err(e) => report.warnings.push(format!("profiling: {e}")),
        }
    }
    let cat =
        match builder.build(1, 0).and_then(|img| Catalog::from_bytes(objstore_r::Bytes::from(img)))
        {
            Ok(c) => c,
            Err(e) => {
                report.warnings.push(format!("catalog: {e}"));
                return report;
            }
        };

    // 5. Lineage from the producers, and the order of work.
    let mut traces: HashMap<String, Result<TableTrace, String>> = HashMap::new();
    let mut graph = Graph::new();
    for t in &in_scope {
        let Some(prod) = producers.get(t) else { continue };
        let Some(id) =
            crate::base::key::split_fqn(t).and_then(|(p, d, n)| cat.find_table_in(p, d, n))
        else {
            continue;
        };
        let cols: Vec<String> =
            cat.columns(id).filter(|c| c.depth == 0).map(|c| c.path.to_ascii_lowercase()).collect();
        let mut tr = trace::trace(&prod.query, t, &cols, &cat);
        // Other writers of the table: their lineage is part of its lineage.
        if let Ok(main) = &mut tr {
            for (i, o) in prod.others.iter().enumerate() {
                let label = format!("writer {} ({})", i + 2, o.statement_type);
                match trace::trace(&o.query, t, &cols, &cat) {
                    Ok(x) => main.absorb(x, &label),
                    Err(e) => main.warnings.push(format!("{label}: {e}")),
                }
            }
        }
        if let Ok(tr) = &tr {
            for c in &tr.columns {
                let exact = matches!(c.transform, Transform::Passthrough | Transform::Rename)
                    && c.sources.len() == 1;
                for s in &c.sources {
                    graph.add(Column::new(t.clone(), &c.column), s.clone(), exact);
                }
            }
        }
        traces.insert(t.clone(), tr);
    }
    // Known descriptions: every documented column in the catalog.
    let mut known: HashMap<Column, String> = HashMap::new();
    for t in cat.tables() {
        for c in cat.columns(t.id) {
            if !c.description.trim().is_empty() {
                known.insert(Column::new(fqn(&t), c.path), c.description.to_owned());
            }
        }
    }
    // External lineage: undocumented sources outside the dataset, and the
    // columns of tables whose producing SQL was not found (outside the job
    // history window, loaded, or written from another project) — for them
    // the lineage service is the only record of where the data came from.
    let mut roots: std::collections::BTreeSet<Column> = traces
        .values()
        .filter_map(|r| r.as_ref().ok())
        .flat_map(|tr| tr.columns.iter().flat_map(|c| c.sources.iter()))
        .filter(|s| !known.contains_key(s) && !in_scope.contains(&s.table))
        .cloned()
        .collect();
    for t in &in_scope {
        // (Reused tables too: the call is free, and their lineage belongs
        // in the report and the metadata image.)
        if producers.contains_key(t) {
            continue;
        }
        let Some(id) =
            crate::base::key::split_fqn(t).and_then(|(p, d, n)| cat.find_table_in(p, d, n))
        else {
            continue;
        };
        roots.extend(
            cat.columns(id).filter(|c| c.depth == 0).map(|c| Column::new(t.clone(), c.path)),
        );
    }
    let roots: Vec<Column> = roots.into_iter().collect();
    if !roots.is_empty() {
        match lineage.upstream(&roots, cfg.lineage_depth).await {
            Ok(g) => {
                if !g.unreachable.is_empty() {
                    report
                        .warnings
                        .push(format!("lineage incomplete; unreachable: {:?}", g.unreachable));
                }
                // Descriptions of the tables it reached that the catalog
                // does not hold (free resource reads), so they can be
                // inherited.
                let extra: Vec<String> = g
                    .tables()
                    .into_iter()
                    .filter(|t| !meta_of.contains_key(*t))
                    .map(str::to_owned)
                    .collect();
                let metas: Vec<Option<TableMeta>> =
                    objstore_r::join::join_all(extra.iter().map(|t| async move {
                        let (p, d, n) = crate::base::key::split_fqn(t)?;
                        w.table_get(p, d, n).await.ok().flatten()
                    }))
                    .await;
                for (t, m) in extra.iter().zip(metas) {
                    let Some(m) = m else { continue };
                    for c in schema_columns(&m) {
                        if let Some(d) = c.description.filter(|d| !d.trim().is_empty()) {
                            known.entry(Column::new(t.clone(), &c.path)).or_insert(d);
                        }
                    }
                }
                graph.merge(g);
            }
            Err(e) => report.warnings.push(format!("external lineage unavailable: {e}")),
        }
    }
    report.levels = graph.table_levels(&in_scope);

    // 6. Describe level by level; tables of a level concurrently.
    let run = Run { model, w, cat: &cat, cfg };
    for level in report.levels.clone() {
        for chunk in level.chunks(cfg.concurrency.max(1)) {
            let jobs = chunk.iter().map(|t| {
                let prior = reuse.get(t.as_str()).copied();
                let pending = prior.is_none() && budget.check(Spend::default()).is_err();
                let key = keys.get(t.as_str()).copied();
                let tr = traces.get(t).cloned();
                let prod = producers.get(t).cloned();
                let meta = meta_of.get(t).cloned();
                let known = &known;
                let graph = &graph;
                let run = &run;
                async move {
                    let mut rep = run.table(t, tr, prod, meta, known, graph, pending, prior).await;
                    if let Some((pre, k)) = key {
                        rep.prekey = pre;
                        rep.key = rep.key.map(|_| k);
                    } else {
                        rep.key = None;
                    }
                    rep
                }
            });
            for tr in objstore_r::join::join_all(jobs).await {
                let _ = budget.charge(tr.spend);
                // Verified results are known to the next levels; a flagged
                // description is replaced by its evidence-backed correction,
                // so nothing downstream inherits what the data contradicts.
                for c in &tr.columns {
                    let key = Column::new(tr.table.clone(), &c.column);
                    match (&c.description, c.action) {
                        (
                            Some(d),
                            Action::Propagate
                            | Action::Generate
                            | Action::Confirm
                            | Action::Keep
                            | Action::Flag,
                        ) => {
                            known.insert(key, d.clone());
                        }
                        _ => {
                            known.remove(&key);
                        }
                    }
                }
                report.tables.push(tr);
            }
        }
    }
    // 7. Impact: consumers of the described columns outside the datasets.
    if cfg.downstream {
        let roots: Vec<Column> = report
            .tables
            .iter()
            .flat_map(|t| t.columns.iter().map(|c| Column::new(t.table.clone(), &c.column)))
            .collect();
        match lineage.downstream(&roots, cfg.lineage_depth).await {
            Ok(g) => {
                for (child, parent, _) in g.edges() {
                    if in_scope.contains(&child.table) {
                        continue;
                    }
                    let Some(t) = report.tables.iter_mut().find(|t| t.table == parent.table) else {
                        continue;
                    };
                    if let Some(c) = t.columns.iter_mut().find(|c| c.column == parent.column) {
                        if !c.consumers.contains(child) {
                            c.consumers.push(child.clone());
                        }
                    }
                }
                for t in &mut report.tables {
                    for c in &mut t.columns {
                        c.consumers.sort_unstable();
                    }
                }
            }
            Err(e) => report.warnings.push(format!("downstream lineage unavailable: {e}")),
        }
    }
    report.spend = budget.spent();
    report
}

struct Run<'a, M: Model, W: Warehouse> {
    model: &'a M,
    w: &'a W,
    cat: &'a Catalog,
    cfg: &'a DescribeConfig,
}

impl<M: Model, W: Warehouse> Run<'_, M, W> {
    /// One model call; returns the reply text and its spend.
    async fn ask(
        &self,
        system: &str,
        user: &str,
        schema: &str,
        max_out: u32,
        temperature: f32,
    ) -> Result<(String, Spend), String> {
        let parts = [Part::text(user)];
        let constraint = if self.model.caps().constraints.json_schema {
            Constraint::JsonSchema(schema)
        } else {
            Constraint::None
        };
        let req = GenRequest {
            constraint,
            max_output_tokens: max_out,
            temperature,
            independent: true,
            ..GenRequest::new(Prompt { system, parts: &parts })
        };
        let mut out = Candidates::new();
        let usage =
            self.model.generate(&req, &mut out, &mut ()).await.map_err(|e| e.to_string())?;
        Ok((out.text(0).to_owned(), usage.spend()))
    }

    /// A model call whose reply must parse. An unusable reply (cut off, or
    /// looping until the token cap) is retried once, sampled differently so
    /// it does not repeat; calls and spend are charged to `rep`.
    #[allow(clippy::too_many_arguments)]
    async fn ask_parsed<T>(
        &self,
        rep: &mut TableReport,
        what: &str,
        system: &str,
        user: &str,
        schema: &str,
        max_out: u32,
        parse: impl Fn(&str) -> Option<T>,
    ) -> Option<T> {
        for temperature in [0.0, 0.7] {
            match self.ask(system, user, schema, max_out, temperature).await {
                Ok((text, spend)) => {
                    rep.model_calls += 1;
                    rep.spend = rep.spend.plus(spend);
                    if let Some(v) = parse(&text) {
                        return Some(v);
                    }
                }
                Err(e) => {
                    rep.warnings.push(format!("{what} call failed: {e}"));
                    rep.key = None;
                    return None;
                }
            }
        }
        rep.warnings.push(format!("{what} reply was not usable JSON"));
        rep.key = None;
        None
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)] // one table's whole loop reads best in order
    async fn table(
        &self,
        t: &str,
        tr: Option<Result<TableTrace, String>>,
        producer: Option<Producer>,
        meta: Option<TableMeta>,
        known: &HashMap<Column, String>,
        graph: &Graph,
        pending: bool,
        prior: Option<&PriorTable>,
    ) -> TableReport {
        let cat = self.cat;
        // A virtual table's checks run over the query it stands for.
        let virtual_sql: Option<String> =
            producer.as_ref().filter(|p| p.statement_type == VIRTUAL).map(|p| p.query.clone());
        let id = crate::base::key::split_fqn(t).and_then(|(p, d, n)| cat.find_table_in(p, d, n));
        let tb = id.and_then(|i| cat.table(i));
        let mut rep = TableReport {
            table: t.to_owned(),
            kind: meta.as_ref().map(|m| m.kind.clone()).unwrap_or_default(),
            rows: tb.as_ref().map_or(0, |x| x.row_count),
            bytes: tb.as_ref().map_or(0, |x| x.logical_bytes),
            last_modified_ms: tb.as_ref().map_or(0, |x| x.last_modified_ms),
            producer,
            existing: tb
                .as_ref()
                .map(|x| x.description.trim().to_owned())
                .filter(|d| !d.is_empty()),
            description: None,
            grain: Vec::new(),
            filters: Vec::new(),
            staged: Vec::new(),
            conditions: Vec::new(),
            columns: Vec::new(),
            checks: Vec::new(),
            model_calls: 0,
            warnings: Vec::new(),
            pending,
            spend: Spend::default(),
            meta,
            prekey: 0,
            key: (!pending).then_some(0),
            reused: false,
            carried_checks: None,
        };
        let (Some(id), Some(tb)) = (id, tb) else {
            rep.warnings.push("not in the catalog".into());
            rep.key = None;
            return rep;
        };
        let traces: Vec<ColumnTrace> = match tr {
            Some(Ok(tr)) => {
                rep.filters = tr.filters.clone();
                rep.staged = tr.staged.clone();
                rep.conditions = tr.conditions.iter().filter(|c| c.table != t).cloned().collect();
                rep.warnings.extend(tr.warnings.iter().cloned());
                tr.columns
            }
            Some(Err(e)) => {
                rep.warnings.push(e);
                Vec::new()
            }
            None => {
                if rep.producer.is_none() {
                    rep.warnings.push("no producing SQL found (job history window, or loaded rather than queried)".into());
                }
                Vec::new()
            }
        };

        // Deterministic decisions first.
        let mut to_describe: Vec<String> = Vec::new();
        // Without producing SQL, the lineage service's dependencies are the
        // only sign of a filter: it reports a filter column as an `OTHER`
        // dependency of every column (and folds it into the copy edge of the
        // column it also copies), so any such dependency anywhere in the
        // table counts as a filter on every column's path.
        let lineage_filtered = traces.is_empty()
            && cat
                .columns(id)
                .filter(|c| c.depth == 0)
                .any(|c| graph.parents(&Column::new(t, c.path)).iter().any(|(_, exact)| !exact));
        let mut to_audit: Vec<String> = Vec::new();
        for c in cat.columns(id).filter(|c| c.depth == 0) {
            let name = c.path.to_ascii_lowercase();
            let trace = traces.iter().find(|x| x.column == name);
            let existing = Some(c.description.trim().to_owned()).filter(|d| !d.is_empty());
            let mut col = ColumnReport {
                column: name.clone(),
                data_type: c.data_type.to_owned(),
                action: Action::Skip,
                existing: existing.clone(),
                description: None,
                semantic_type: None,
                unit: None,
                transform: trace.map(|x| x.transform),
                expression: trace.and_then(|x| x.expression.clone()),
                // From the producing SQL, else the lineage service's record.
                sources: match trace {
                    Some(x) => x.sources.clone(),
                    None => graph
                        .parents(&Column::new(t, &name))
                        .iter()
                        .map(|(p, _)| p.clone())
                        .collect(),
                },
                hops: trace.map(|x| x.hops.clone()).unwrap_or_default(),
                confidence: None,
                checks: Vec::new(),
                consumers: Vec::new(),
            };
            if let (Some(e), false) = (&existing, self.cfg.overwrite) {
                col.description = Some(e.clone());
                if self.cfg.audit_existing {
                    col.action = Action::Confirm;
                    to_audit.push(name.clone());
                } else {
                    col.action = Action::Keep;
                }
            } else if let Some((src, filtered)) = trace
                .filter(|x| {
                    matches!(x.transform, Transform::Passthrough | Transform::Rename)
                        && x.sources.len() == 1
                })
                .map(|x| {
                    // A filter on the path (WHERE, HAVING, QUALIFY) can change
                    // the value domain and NULL share.
                    let filtered = rep.filters.iter().any(|f| !f.starts_with("GROUP BY"))
                        || x.hops.iter().any(|h| {
                            h.contains(" | WHERE ")
                                || h.contains(" | HAVING ")
                                || h.contains(" | QUALIFY ")
                        });
                    (x.sources[0].clone(), filtered)
                })
                .or_else(|| {
                    // No producing SQL: the lineage service's record. A
                    // single exact-copy parent is the source.
                    if trace.is_some() {
                        return None;
                    }
                    let ps = graph.parents(&Column::new(t, &name));
                    match ps.iter().filter(|(_, e)| *e).collect::<Vec<_>>().as_slice() {
                        [(p, _)] => Some((p.clone(), lineage_filtered)),
                        _ => None,
                    }
                })
            {
                let src = &src;
                let inherited = known.get(src).cloned().or_else(|| {
                    let hits = graph.nearest_documented(src, |c| known.get(c).cloned());
                    match hits.as_slice() {
                        [one] if one.exact_copy_chain => Some(one.description.clone()),
                        _ => None,
                    }
                });
                // Through a filter only a description that states nothing
                // checkable survives verbatim; the rest is regenerated from
                // it and checked.
                match inherited {
                    Some(d) if !filtered || !generate::testable(&d) => {
                        col.action = Action::Propagate;
                        col.description = Some(d);
                        col.confidence = Some("high".into());
                    }
                    _ => to_describe.push(name.clone()),
                }
            } else {
                to_describe.push(name.clone());
            }
            rep.columns.push(col);
        }
        rep.description.clone_from(&rep.existing);
        if let Some(p) = prior {
            // Unchanged since the previous report: carry its results.
            for col in &mut rep.columns {
                if let Some(pc) = p.columns.iter().find(|x| x.column == col.column) {
                    col.action = pc.action;
                    col.description.clone_from(&pc.description);
                    col.semantic_type.clone_from(&pc.semantic_type);
                    col.unit.clone_from(&pc.unit);
                    col.confidence.clone_from(&pc.confidence);
                    col.checks.clone_from(&pc.checks);
                }
            }
            if p.description.is_some() {
                rep.description.clone_from(&p.description);
            }
            rep.grain.clone_from(&p.grain);
            rep.reused = true;
            rep.carried_checks = Some(p.checks_json.clone());
            return rep;
        }
        let needs_table = rep.existing.is_none() || self.cfg.overwrite;
        if pending || (to_describe.is_empty() && to_audit.is_empty() && !needs_table) {
            return rep;
        }

        // Evidence for the model.
        let columns: Vec<String> = to_describe.iter().chain(to_audit.iter()).cloned().collect();
        let mut ev = String::with_capacity(8 * 1024);
        let _ = writeln!(
            ev,
            "TABLE `{t}` ({}; ~{} rows)",
            if rep.kind.is_empty() { "TABLE" } else { &rep.kind },
            tb.row_count
        );
        if let Some(d) = &rep.existing {
            let _ = writeln!(ev, "Existing table description: {d}");
        }
        if let Some(p) = &rep.producer {
            let _ = writeln!(
                ev,
                "Produced by: {} (latest of {} writes by {} principals)",
                p.statement_type, p.writes, p.writers
            );
            let sql: String = p.query.chars().take(self.cfg.max_sql_chars).collect();
            let _ = writeln!(ev, "Producer SQL:\n```sql\n{sql}\n```");
            // Other writers share what is left of the SQL budget.
            let mut left = self.cfg.max_sql_chars.saturating_sub(sql.len());
            for (i, o) in p.others.iter().enumerate() {
                if left < 200 {
                    break;
                }
                let sql: String = o.query.chars().take(left).collect();
                left -= sql.len();
                let _ = writeln!(
                    ev,
                    "Writer {} ({}, {} writes):\n```sql\n{sql}\n```",
                    i + 2,
                    o.statement_type,
                    o.writes
                );
            }
        }
        if !rep.filters.is_empty() {
            let _ = writeln!(ev, "Final query filters and grain: {}", rep.filters.join(" | "));
        }
        if !rep.staged.is_empty() {
            let _ = writeln!(
                ev,
                "Rows staged through temporary tables (traced through): {}",
                rep.staged.join(", ")
            );
        }
        ev.push_str("COLUMNS:\n");
        for col in &rep.columns {
            let _ = write!(ev, "- {} {}", col.column, col.data_type);
            if let Some(e) = &col.existing {
                let _ = write!(ev, " | existing: {}", generate::quoted(e));
            }
            if let Some(x) = &col.expression {
                let _ = write!(
                    ev,
                    " | computed as `{x}` ({:?})",
                    col.transform.unwrap_or(Transform::Derived)
                );
            }
            for s in &col.sources {
                match known.get(s) {
                    Some(d) => {
                        let _ = write!(ev, " | from {s}: {}", generate::quoted(d));
                    }
                    None => {
                        let _ = write!(ev, " | from {s}");
                    }
                }
            }
            for h in col.hops.iter().take(4) {
                let _ = write!(ev, " | via {h}");
            }
            // Blind audit: an audited column's checks must come from what its
            // description asserts, so the data is withheld from the draft
            // (the judge sees it).
            if to_audit.contains(&col.column) {
                ev.push_str(" | (audit: profile withheld)");
            } else if let Some(p) =
                cat.find_column(id, &col.column).and_then(|cid| profile_line(cat, cid))
            {
                let _ = write!(ev, " | profile: {p}");
            }
            ev.push('\n');
        }
        let _ = writeln!(
            ev,
            "DESCRIBE: {}",
            if to_describe.is_empty() { "(none)".into() } else { to_describe.join(", ") }
        );
        let _ = writeln!(
            ev,
            "AUDIT: {}",
            if to_audit.is_empty() { "(none)".into() } else { to_audit.join(", ") }
        );

        // Round 1: draft and check-writing.
        let all: Vec<String> = rep.columns.iter().map(|c| c.column.clone()).collect();
        let existing_descriptions: Vec<(String, String)> = rep
            .columns
            .iter()
            .filter(|c| to_audit.contains(&c.column))
            .filter_map(|c| c.existing.clone().map(|e| (c.column.clone(), e)))
            .collect();
        let schema = generate::draft_schema(&columns, &all);
        let Some(draft): Option<Draft> = self
            .ask_parsed(&mut rep, "draft", DRAFT_SYSTEM, &ev, &schema, 8_192, |text| {
                generate::parse_draft(text, &columns, &all, &existing_descriptions)
            })
            .await
        else {
            return rep;
        };
        rep.grain.clone_from(&draft.grain);
        let mut current: HashMap<String, String> =
            draft.columns.iter().map(|c| (c.column.clone(), c.description.clone())).collect();
        for d in &draft.columns {
            if let Some(col) = rep.columns.iter_mut().find(|c| c.column == d.column) {
                col.semantic_type = Some(d.semantic_type.clone());
                col.unit.clone_from(&d.unit);
            }
        }
        let mut table_desc = draft.table_description.clone();
        let mut pending_checks: Vec<DraftCheck> = draft.checks.clone();
        let mut next_id = 1u32;
        let source = virtual_sql.as_deref();
        let target = checks::Target { id, fqn: t, bytes: tb.logical_bytes, source };
        let mut judged: Option<Judgement> = None;
        for round in 1..=self.cfg.max_rounds.max(1) {
            if pending_checks.is_empty() && round > 1 {
                break;
            }
            // Guarded run: one scan for every aggregate.
            let checks: Vec<Check> = pending_checks
                .drain(..)
                .map(|d| {
                    let c = Check {
                        id: next_id,
                        column: d.column,
                        claim: d.claim,
                        sql: if d.query {
                            CheckSql::Query(d.sql)
                        } else {
                            CheckSql::Aggregate(d.sql)
                        },
                        expect: d.expect,
                        tests_existing: d.tests_existing,
                    };
                    next_id += 1;
                    c
                })
                .collect();
            let (results, billed) =
                checks::run(self.w, cat, target, &checks, self.cfg.runner).await;
            rep.spend = rep.spend.plus(Spend { warehouse_bytes: billed, ..Spend::default() });
            rep.checks.extend(results.into_iter().map(|r| CheckReport {
                result: r,
                verdict: None,
                reason: None,
                round,
                disputed: false,
            }));

            // Judge and revise.
            let mut ju = String::with_capacity(4 * 1024);
            let _ = writeln!(
                ju,
                "TABLE `{t}` (~{} rows){}",
                tb.row_count,
                if rep.checks.iter().any(|c| c.result.sampled) {
                    "; some checks ran on a sample of rows"
                } else {
                    ""
                }
            );
            if let Some(d) = &table_desc {
                let _ = writeln!(ju, "Draft table description: {d}");
            }
            ju.push_str("DRAFTS:\n");
            for c in &columns {
                let existing =
                    rep.columns.iter().find(|x| &x.column == c).and_then(|x| x.existing.clone());
                let d = current.get(c).cloned().or(existing).unwrap_or_default();
                let prof = cat
                    .find_column(id, c)
                    .and_then(|cid| profile_line(cat, cid))
                    .unwrap_or_default();
                let _ = writeln!(ju, "- {c}: {} | profile: {prof}", generate::quoted(&d));
            }
            ju.push_str("CHECK RESULTS:\n");
            for cr in &rep.checks {
                let r = &cr.result;
                let sql = match &r.check.sql {
                    CheckSql::Aggregate(s) | CheckSql::Query(s) => s,
                };
                let outcome = match &r.outcome {
                    Outcome::Value(v) => format!("result {v}"),
                    Outcome::Rows(rows) => format!(
                        "rows {}",
                        rows.iter().take(6).map(|r| r.join(" | ")).collect::<Vec<_>>().join(" ; ")
                    ),
                    Outcome::Rejected(why) => format!("REJECTED: {why}"),
                    Outcome::Failed(why) => format!("FAILED: {why}"),
                };
                let prior = cr
                    .verdict
                    .as_ref()
                    .map(|v| format!(" [earlier verdict: {v}]"))
                    .unwrap_or_default();
                let expect = r
                    .check
                    .expect
                    .as_deref()
                    .map(|e| format!(" | expected {e}"))
                    .unwrap_or_default();
                let mech = match r.holds {
                    Some(true) => " -> HOLDS",
                    Some(false) => " -> VIOLATED",
                    None => "",
                };
                let audit = if r.check.tests_existing { " [tests existing]" } else { "" };
                let _ = writeln!(
                    ju,
                    "#{} [{}]{audit} claim: {} | sql: {sql}{expect} | {outcome}{}{mech}{prior}",
                    r.check.id,
                    r.check.column.as_deref().unwrap_or("table"),
                    generate::quoted(&r.check.claim),
                    if r.sampled { " (sampled)" } else { "" }
                );
            }
            let ids: Vec<u32> = rep.checks.iter().map(|c| c.result.check.id).collect();
            let schema = generate::judge_schema(&columns, &ids);
            let Some(j) = self
                .ask_parsed(&mut rep, "judge", JUDGE_SYSTEM, &ju, &schema, 4_096, |text| {
                    generate::parse_judgement(text, &columns, &ids)
                })
                .await
            else {
                break;
            };
            for v in &j.verdicts {
                if let Some(cr) = rep.checks.iter_mut().find(|c| c.result.check.id == v.check) {
                    cr.verdict = Some(v.verdict.clone());
                    cr.reason = Some(v.reason.clone());
                }
            }
            // Reconcile each check's two readings: the mechanical comparison
            // of its result with the model's own expectation, and the judge.
            // Agreement, or a judge that is unsure, gives the mechanical
            // verdict. Opposite readings mean either a misread number or a
            // mis-stated expectation; neither can be trusted alone, so the
            // check is disputed (inconclusive) and left for review.
            for cr in &mut rep.checks {
                let Some(h) = cr.result.holds else { continue };
                let want = if h { "supports" } else { "contradicts" };
                if cr.verdict.as_deref() == Some(want) {
                    continue;
                }
                let got = match &cr.result.outcome {
                    Outcome::Value(v) => v.clone(),
                    Outcome::Rows(rows) => {
                        rows.get(1).and_then(|r| r.first()).cloned().unwrap_or_default()
                    }
                    _ => String::new(),
                };
                let note = format!(
                    "result {got} vs expected {}",
                    cr.result.check.expect.as_deref().unwrap_or("?")
                );
                if let Some(judge @ ("supports" | "contradicts")) = cr.verdict.as_deref() {
                    rep.warnings.push(format!(
                        "check #{}: disputed — judge said {judge}, but {note}",
                        cr.result.check.id
                    ));
                    cr.reason = Some(format!("disputed: judge said {judge}; {note}"));
                    cr.verdict = Some("inconclusive".into());
                    cr.disputed = true;
                } else {
                    cr.verdict = Some(want.to_owned());
                    cr.reason = Some(note);
                }
            }
            for c in &j.columns {
                current.insert(c.column.clone(), c.description.clone());
            }
            if j.table_description.is_some() {
                table_desc.clone_from(&j.table_description);
            }
            pending_checks.clone_from(&j.more_checks);
            judged = Some(j);
        }

        // Final decisions per column.
        let conf_of = |col: &str, judged_conf: Option<&str>, checks: &[CheckReport]| -> String {
            let mine: Vec<&CheckReport> =
                checks.iter().filter(|c| c.result.check.column.as_deref() == Some(col)).collect();
            let supports = mine.iter().filter(|c| c.verdict.as_deref() == Some("supports")).count();
            let contradicts =
                mine.iter().filter(|c| c.verdict.as_deref() == Some("contradicts")).count();
            match (supports, contradicts, judged_conf) {
                (s, 0, Some("high")) if s > 0 => "high".into(),
                (0, _, Some("high")) => "medium".into(),
                (_, _, Some(c)) => c.to_owned(),
                (s, 0, None) if s > 0 => "medium".into(),
                _ => "low".into(),
            }
        };
        let checks_snapshot = rep.checks.clone();
        for col in &mut rep.columns {
            col.checks = checks_snapshot
                .iter()
                .filter(|c| c.result.check.column.as_deref() == Some(col.column.as_str()))
                .map(|c| c.result.check.id)
                .collect();
            // Only a check of the existing description's own claims can flag it.
            let mine = |c: &&CheckReport| {
                c.result.check.column.as_deref() == Some(col.column.as_str())
                    && c.result.check.tests_existing
            };
            let contradicted = checks_snapshot
                .iter()
                .filter(mine)
                .any(|c| c.verdict.as_deref() == Some("contradicts"));
            let tested = checks_snapshot.iter().filter(mine).any(|c| c.verdict.is_some());
            let disputed = checks_snapshot.iter().filter(mine).any(|c| c.disputed);
            let jc = judged
                .as_ref()
                .and_then(|j| j.columns.iter().find(|x| x.column == col.column))
                .map(|x| x.confidence.as_str());
            match col.action {
                Action::Confirm => {
                    let proposed = current.get(&col.column).cloned();
                    let changed =
                        proposed.as_deref().is_some_and(|p| Some(p) != col.existing.as_deref());
                    if contradicted {
                        // Flagged with the proposed correction (or, without
                        // one, the existing text until reviewed).
                        col.action = Action::Flag;
                        if changed {
                            col.description = proposed;
                            col.confidence = Some(conf_of(&col.column, jc, &checks_snapshot));
                        } else {
                            col.confidence = Some("low".into());
                        }
                    } else if disputed {
                        // A check of its own claims could not be settled:
                        // keep the text, flag it for a person to decide.
                        col.action = Action::Flag;
                        col.confidence = Some("low".into());
                    } else {
                        // Unchanged: judge edits to text the checks did not
                        // contradict are ignored; confidence only from checks
                        // of the description's own claims.
                        col.confidence = Some(if tested {
                            conf_of(&col.column, jc, &checks_snapshot)
                        } else {
                            "medium".into()
                        });
                    }
                }
                Action::Skip => {
                    if let Some(d) = current.get(&col.column) {
                        col.action = Action::Generate;
                        col.description = Some(d.clone());
                        col.confidence = Some(conf_of(&col.column, jc, &checks_snapshot));
                    }
                }
                _ => {}
            }
        }
        if needs_table {
            rep.description = table_desc.or(rep.description.take());
        }
        rep
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_classify_by_their_dots() {
        assert_eq!(Target::parse("p"), Target::Project("p".into()));
        assert_eq!(Target::parse("p.d"), Target::Dataset("p.d".into()));
        assert_eq!(Target::parse("p.d.t"), Target::Table("p.d.t".into()));
        assert_eq!(Target::parse("example.com:p"), Target::Project("example.com:p".into()));
        assert_eq!(Target::parse("example.com:p.d"), Target::Dataset("example.com:p.d".into()));
        assert_eq!(Target::parse("example.com:p.d.t").project(), Some("example.com:p"));
        assert_eq!(Target::parse("example.com:p.d").project(), Some("example.com:p"));
        let q = |t: Option<&str>, d: Option<&str>| Target::Sql {
            sql: "SELECT 1".into(),
            target: t.map(Into::into),
            default_dataset: d.map(Into::into),
        };
        assert_eq!(q(Some("p.d.t"), None).project(), Some("p"));
        assert_eq!(q(None, Some("p.d")).project(), Some("p"));
        assert_eq!(q(None, None).project(), None);
    }

    #[test]
    fn scopes_merge_narrow_and_widen() {
        let mut warnings = Vec::new();
        let targets = [
            Target::parse("p.a.t1"),
            Target::parse("p.a.t2"),
            Target::parse("p.b.x"),
            Target::parse("p.b"),
            Target::parse("p.b.y"),
            Target::parse("q"),
            Target::parse("q.c.z"),
            Target::Sql {
                sql: "INSERT ...".into(),
                target: Some("p.a.t3".into()),
                default_dataset: None,
            },
            Target::Dataset("bad".into()),
        ];
        let s = scopes(&targets, &mut warnings);
        let p = &s["p"];
        assert!(!p.all && p.covers("a", "t1") && p.covers("a", "t3") && !p.covers("a", "other"));
        assert!(p.covers("b", "anything"), "a whole dataset wins over its tables");
        assert!(!p.covers("c", "t1"));
        assert!(s["q"].all && s["q"].covers("any", "thing"));
        assert_eq!(warnings.len(), 1, "{warnings:?}");
    }

    #[test]
    fn query_results_get_stable_virtual_names() {
        let a = virtual_name("SELECT 1", Some("p.d"), "home");
        assert_eq!(a, virtual_name("SELECT 1", Some("p.d"), "other"));
        assert!(a.starts_with("p.d._query_") && is_virtual(&a));
        assert!(virtual_name("SELECT 1", None, "home").starts_with("home._query._query_"));
        assert_ne!(a, virtual_name("SELECT 2", Some("p.d"), "home"));
        assert!(!is_virtual("p.d.orders"));
    }

    #[test]
    fn reads_resolve_names_and_skip_ctes() {
        let mut p = crate::sql::Parser::new();
        let sql = "WITH recent AS (SELECT * FROM raw.orders) \
                   SELECT * FROM recent JOIN users USING (id) JOIN other.p2.t USING (id)";
        assert_eq!(reads_of(&mut p, sql, "p.d.v", false), ["p.raw.orders", "other.p2.t"]);
        assert_eq!(
            reads_of(&mut p, sql, "p.d.v", true),
            ["p.raw.orders", "p.d.users", "other.p2.t"],
            "a query's unqualified names live in its default dataset; its CTEs never"
        );
        assert!(reads_of(&mut p, "not sql", "p.d.v", true).is_empty());
    }

    #[test]
    fn unqualified_names_are_qualified_and_ctes_kept() {
        let sql = "WITH r AS (SELECT * FROM orders) SELECT * FROM r JOIN `users` u USING (id) \
                   JOIN raw.x USING (id)";
        assert_eq!(
            qualify(sql, "p.d"),
            "WITH r AS (SELECT * FROM `p.d.orders`) SELECT * FROM r JOIN `p.d.users` u USING (id) \
             JOIN raw.x USING (id)"
        );
        assert_eq!(qualify("not sql", "p.d"), "not sql");
    }
}
