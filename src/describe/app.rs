// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! The `crab-describe` command (feature `cli`): option parsing, a full run
//! with its store, publish and apply steps, and the store lookups — shared
//! by the binary and the Python package.
//!
//! `crab-describe`: evidence-driven metadata for BigQuery.
//!
//! ```text
//! crab-describe --dataset my-proj.mart[,my-proj.stg] [--region US]
//!     [--jobs-project my-proj] [--lookback-days 90] [--billing-project my-proj]
//!     [--vertex-project my-proj] [--vertex-location us-central1] [--model gemini-2.5-flash]
//!     [--lineage-location us]            # add Dataplex lineage (Data Lineage API)
//!     [--downstream]                     # with it: consumers outside the datasets
//!     [--no-profile] [--no-audit] [--overwrite] [--rounds 2] [--concurrency 4]
//!     [--budget-usd 5] [--out report.json] [--emit-schema DIR]
//!     [--store DIR]                      # graph-r store: state + metadata (reuse unchanged tables)
//!     [--publish gs://bucket/prefix]     # publish the store (or to a local dir), fenced
//!     [--apply] [--correct]
//! ```
//!
//! With `--store DIR`, the run's state and metadata live in a graph-r store:
//! tables unchanged since an earlier run — shape, rows, producing SQL,
//! upstream descriptions — are carried over with no profile, model call or
//! check query. `--publish` ships the store through objstore-r's graph-r mirror (only
//! what changed; a second writer is fenced out).
//!
//! Lookups read a store, no network:
//!
//! ```text
//! crab-describe table      DIR project.dataset.table
//! crab-describe lookup     DIR project.dataset.table.column
//! crab-describe upstream   DIR project.dataset.table.column [DEPTH]
//! crab-describe downstream DIR project.dataset.table.column [DEPTH]
//! crab-describe due        DIR [N]
//! crab-describe pull       gs://bucket/prefix DIR   # replica: fetch, or refresh, a published store
//! ```
//!
//! Credentials: Application Default Credentials. Nothing is written to the
//! warehouse unless `--apply` is given; then only empty descriptions are
//! filled (flagged corrections too with `--correct`), each table under
//! `If-Match` of the resource it read.

use crate::describe::describe::{describe, is_virtual, DescribeConfig, Report, Target};
use crate::describe::report::{self, Applied};
use crate::exec::bigquery::{BigQuery, BigQueryConfig};
use crate::lineage::dataplex::{Dataplex, DataplexConfig};
use crate::lineage::NoLineage;
use crate::llm::vertex::{ExplicitCache, Vertex, VertexConfig};
use crate::llm::{CacheSupport, Caps, ConstraintSupport, Price};
use objstore_r::http::{Client, HttpConfig};
use objstore_r::token::Adc;
use std::fmt::Write as _;

/// Parsed command-line options of a describe run (see [`USAGE`]).
#[derive(Clone, Debug)]
#[allow(clippy::struct_excessive_bools)] // independent switches
pub struct Options {
    targets: Vec<Target>,
    sql: Option<String>,
    sql_target: Option<String>,
    default_dataset: Option<String>,
    upstream: u32,
    region: String,
    jobs_project: Option<String>,
    billing_project: Option<String>,
    lookback_days: u32,
    vertex_project: Option<String>,
    vertex_location: String,
    model: String,
    lineage_location: Option<String>,
    downstream: bool,
    profile: bool,
    audit: bool,
    overwrite: bool,
    rounds: u8,
    concurrency: usize,
    budget_usd: f64,
    /// Where the report JSON is written (`--out`); `None` leaves it to the
    /// caller.
    pub out: Option<String>,
    store: Option<String>,
    publish: Option<String>,
    emit_schema: Option<String>,
    apply: bool,
    correct: bool,
}

/// Usage of a describe run.
pub const USAGE: &str = "usage: crab-describe [PROJECT | PROJECT.DATASET | PROJECT.DATASET.TABLE]... \
[--sql FILE|- [--target PROJECT.DATASET.TABLE] [--default-dataset PROJECT.DATASET]] [--upstream N] [--region US] [--jobs-project P] [--billing-project P] [--lookback-days N] \
[--vertex-project P] [--vertex-location L] [--model M] [--lineage-location L] [--downstream] [--no-profile] [--no-audit] [--overwrite] \
[--rounds N] [--concurrency N] [--budget-usd X] [--out FILE] [--store DIR] [--publish gs://B/PREFIX|DIR] [--emit-schema DIR] [--apply] [--correct]";

/// Parse the options of a describe run (the CLI's arguments after the
/// program name).
pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Options, String> {
    let mut a = Options {
        targets: Vec::new(),
        sql: None,
        sql_target: None,
        default_dataset: None,
        upstream: 0,
        region: "US".into(),
        jobs_project: None,
        billing_project: None,
        lookback_days: 90,
        vertex_project: None,
        vertex_location: "us-central1".into(),
        model: "gemini-2.5-flash".into(),
        lineage_location: None,
        downstream: false,
        profile: true,
        audit: true,
        overwrite: false,
        rounds: 2,
        concurrency: 4,
        budget_usd: 5.0,
        out: None,
        store: None,
        publish: None,
        emit_schema: None,
        apply: false,
        correct: false,
    };
    let mut it = args.into_iter();
    while let Some(flag) = it.next() {
        let mut val = |name: &str| it.next().ok_or_else(|| format!("{name} needs a value"));
        match flag.as_str() {
            "--project" | "--dataset" | "--datasets" | "--table" => a.targets.extend(
                val(&flag)?.split(',').map(str::trim).filter(|s| !s.is_empty()).map(Target::parse),
            ),
            "--sql" => a.sql = Some(val(&flag)?),
            "--target" => a.sql_target = Some(val(&flag)?),
            "--default-dataset" => a.default_dataset = Some(val(&flag)?),
            "--upstream" => a.upstream = val(&flag)?.parse().map_err(|_| "--upstream: a number")?,
            "--region" => a.region = val(&flag)?,
            "--jobs-project" => a.jobs_project = Some(val(&flag)?),
            "--billing-project" => a.billing_project = Some(val(&flag)?),
            "--lookback-days" => {
                a.lookback_days = val(&flag)?.parse().map_err(|_| "--lookback-days: a number")?;
            }
            "--vertex-project" => a.vertex_project = Some(val(&flag)?),
            "--vertex-location" => a.vertex_location = val(&flag)?,
            "--model" => a.model = val(&flag)?,
            "--lineage-location" => a.lineage_location = Some(val(&flag)?),
            "--downstream" => a.downstream = true,
            "--no-profile" => a.profile = false,
            "--no-audit" => a.audit = false,
            "--overwrite" => a.overwrite = true,
            "--rounds" => a.rounds = val(&flag)?.parse().map_err(|_| "--rounds: a number")?,
            "--concurrency" => {
                a.concurrency = val(&flag)?.parse().map_err(|_| "--concurrency: a number")?;
            }
            "--budget-usd" => {
                a.budget_usd = val(&flag)?.parse().map_err(|_| "--budget-usd: a number")?;
            }
            "--out" => a.out = Some(val(&flag)?),
            "--store" => a.store = Some(val(&flag)?),
            "--publish" => a.publish = Some(val(&flag)?),
            "--emit-schema" => a.emit_schema = Some(val(&flag)?),
            "--apply" => a.apply = true,
            "--correct" => a.correct = true,
            "-h" | "--help" => return Err(USAGE.into()),
            other if other.starts_with('-') && other != "-" => {
                return Err(format!("unknown argument {other}\n{USAGE}"))
            }
            name => a.targets.push(Target::parse(name)),
        }
    }
    if let Some(src) = a.sql.take() {
        let sql = if src == "-" {
            let mut s = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut s)
                .map_err(|e| format!("--sql -: {e}"))?;
            s
        } else {
            std::fs::read_to_string(&src).map_err(|e| format!("--sql {src}: {e}"))?
        };
        a.targets.push(Target::Sql {
            sql,
            target: a.sql_target.take(),
            default_dataset: a.default_dataset.take(),
        });
    } else if a.sql_target.is_some() || a.default_dataset.is_some() {
        return Err(format!("--target and --default-dataset go with --sql\n{USAGE}"));
    }
    if a.targets.is_empty() {
        return Err(format!("name a project, dataset or table, or give --sql\n{USAGE}"));
    }
    if a.targets.iter().any(|t| matches!(t, Target::Project(p) if p.is_empty())) {
        return Err(format!("an empty name is not a project\n{USAGE}"));
    }
    Ok(a)
}

/// Publish the store at `dir` to `gs://bucket/prefix` (ADC) or a local
/// directory through objstore-r's graph-r mirror: only what changed is uploaded, and a
/// second writer is fenced out.
async fn publish(
    dest: &str,
    store: &graph_r::Store,
) -> Result<objstore_r::mirror::Published, String> {
    async fn ship<O: objstore_r::mirror::ObjectStore>(
        objects: &O,
        prefix: &str,
        store: &graph_r::Store,
    ) -> Result<objstore_r::mirror::Published, String> {
        let m = objstore_r::mirror::Mirror::new(objects, prefix).map_err(|e| e.to_string())?;
        m.publish(store).await.map_err(|e| e.to_string())
    }
    if let Some(rest) = dest.strip_prefix("gs://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let tokens = Adc::discover().map_err(|e| format!("credentials: {e}"))?;
        let gcs = objstore_r::Gcs::new(bucket, tokens, objstore_r::GcsConfig::default())
            .map_err(|e| format!("{dest}: {e}"))?;
        ship(&gcs, prefix, store).await
    } else {
        let objects = objstore_r::LocalDir::open(dest).map_err(|e| format!("{dest}: {e}"))?;
        ship(&objects, "", store).await
    }
}

/// Fetch a published store into `dir` (first time), or bring it up to date
/// with only what changed since.
pub async fn pull(src: &str, dir: &str) -> Result<String, String> {
    async fn sync<O: objstore_r::mirror::ObjectStore>(
        objects: &O,
        prefix: &str,
        dir: &str,
    ) -> Result<String, String> {
        let m = objstore_r::mirror::Mirror::new(objects, prefix).map_err(|e| e.to_string())?;
        let path = std::path::Path::new(dir);
        if path.join(graph_r::store::BASE_FILE).exists() {
            let store = graph_r::Store::open_read_only(path).map_err(|e| e.to_string())?;
            let before = store.generation();
            let changed = m.refresh_into(&store).await.map_err(|e| e.to_string())?;
            Ok(format!(
                "refreshed {dir}: {} (generation {before} -> {})",
                if changed { "updated" } else { "unchanged" },
                store.generation()
            ))
        } else {
            match m.pull_into(path).await.map_err(|e| e.to_string())? {
                Some(p) => {
                    Ok(format!("pulled generation {} into {dir} ({} bytes)", p.generation, p.bytes))
                }
                None => Err("nothing published there".into()),
            }
        }
    }
    if let Some(rest) = src.strip_prefix("gs://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let tokens = Adc::discover().map_err(|e| format!("credentials: {e}"))?;
        let gcs = objstore_r::Gcs::new(bucket, tokens, objstore_r::GcsConfig::default())
            .map_err(|e| format!("{src}: {e}"))?;
        sync(&gcs, prefix, dir).await
    } else {
        let objects = objstore_r::LocalDir::open(src).map_err(|e| format!("{src}: {e}"))?;
        sync(&objects, "", dir).await
    }
}

/// The lookup subcommands.
pub const LOOKUPS: [&str; 5] = ["table", "lookup", "upstream", "downstream", "due"];

/// A lookup subcommand over the store in `args[0]`: its text, or why
/// not. Reads only the store, no network.
pub fn lookup(cmd: &str, args: &[String]) -> Result<String, String> {
    use crate::describe::state::attr;
    use graph_r::{Direction, EdgeType, UrlKey};
    let mut out = String::new();
    let (Some(dir), name) = (args.first(), args.get(1).map_or("", String::as_str)) else {
        return Err(format!("usage: crab-describe {cmd} STORE_DIR NAME"));
    };
    let store = graph_r::Store::open_read_only(dir).map_err(|e| format!("{dir}: {e}"))?;
    let snap = store.snapshot();
    let mut uri = crate::base::key::UriBuf::new();
    let key_of = |uri: &mut crate::base::key::UriBuf, name: &str, column: bool| -> Option<UrlKey> {
        if column {
            let (t, c) = name.rsplit_once('.')?;
            let (p, d, t) = crate::base::key::split_fqn(t)?;
            Some(UrlKey::of(uri.column(p, d, t, c)))
        } else {
            let (p, d, t) = crate::base::key::split_fqn(name)?;
            Some(UrlKey::of(uri.table(p, d, t)))
        }
    };
    let label = |k: UrlKey| {
        snap.node(k).map_or_else(
            || format!("{:016x}", k.0),
            |n| n.url.trim_start_matches(crate::describe::state::ROOT).to_owned(),
        )
    };
    let line = |k: UrlKey| {
        let n = snap.node(k);
        let d = n.and_then(|n| n.snippet).unwrap_or("");
        let prov = snap.attr(k, attr::ACTION).unwrap_or("");
        format!(
            "{}{}{}",
            label(k),
            if prov.is_empty() { String::new() } else { format!(" [{prov}]") },
            if d.is_empty() { String::new() } else { format!(" — {d}") }
        )
    };
    let depth: u32 = args.get(2).and_then(|d| d.parse().ok()).unwrap_or(10);
    match cmd {
        "due" => {
            let n: usize = args.get(1).and_then(|d| d.parse().ok()).unwrap_or(20);
            for d in
                snap.due(graph_r::fsio::now_ms(), n).into_iter().filter(|d| !d.url.contains('#'))
            {
                writeln!(
                    out,
                    "{} (overdue {} s)",
                    d.url.trim_start_matches(crate::describe::state::ROOT),
                    d.overdue_ms / 1000
                )
                .unwrap_or_default();
            }
        }
        "table" => {
            let Some(k) = key_of(&mut uri, name, false).filter(|k| snap.node(*k).is_some()) else {
                return Err(format!("no table {name}"));
            };
            writeln!(out, "{}", line(k)).unwrap_or_default();
            for (a, what) in
                [(attr::TYPE, "kind"), (attr::STATEMENT, "produced by"), (attr::ROWS, "rows")]
            {
                if let Some(v) = snap.attr(k, a) {
                    writeln!(out, "  {what}: {v}").unwrap_or_default();
                }
            }
            let mut it = snap.neighbors(k);
            while let Some(e) = graph_r::LendingIterator::next(&mut it) {
                match e.etype {
                    EdgeType::Contains => {
                        writeln!(out, "  {}", line(e.dst_key)).unwrap_or_default();
                    }
                    EdgeType::Conditions => {
                        writeln!(out, "  rows decided by {}", label(e.dst_key)).unwrap_or_default();
                    }
                    _ => {}
                }
            }
        }
        "lookup" => {
            let Some(k) = key_of(&mut uri, name, true).filter(|k| snap.node(*k).is_some()) else {
                return Err(format!("no column {name}"));
            };
            writeln!(out, "{}", line(k)).unwrap_or_default();
            for (a, what) in [
                (attr::TYPE, "type"),
                (attr::CONFIDENCE, "confidence"),
                (attr::TRANSFORM, "transform"),
                (attr::EXPRESSION, "expression"),
            ] {
                if let Some(v) = snap.attr(k, a).filter(|v| !v.is_empty()) {
                    writeln!(out, "  {what}: {v}").unwrap_or_default();
                }
            }
            let mut it = snap.neighbors(k);
            while let Some(e) = graph_r::LendingIterator::next(&mut it) {
                let exact = e.flags & graph_r::format::base::eflags::EXACT != 0;
                writeln!(
                    out,
                    "  from {}{}",
                    label(e.dst_key),
                    if exact { " (exact copy)" } else { "" }
                )
                .unwrap_or_default();
            }
            snap.in_edges(k, |src, t, _, _| match t {
                EdgeType::Derives => writeln!(out, "  feeds {}", label(src)).unwrap_or_default(),
                EdgeType::Conditions => {
                    writeln!(out, "  decides which rows {} holds", label(src)).unwrap_or_default();
                }
                _ => {}
            });
        }
        _ => {
            let Some(k) = key_of(&mut uri, name, true).filter(|k| snap.node(*k).is_some()) else {
                return Err(format!("no column {name}"));
            };
            let (dir, follow): (Direction, fn(EdgeType) -> bool) = if cmd == "upstream" {
                (Direction::Out, |t| t == EdgeType::Derives)
            } else {
                (Direction::In, |t| matches!(t, EdgeType::Derives | EdgeType::Conditions))
            };
            writeln!(out, "{}", line(k)).unwrap_or_default();
            snap.walk(k, dir, depth, follow, &mut graph_r::Walk::new(), |h| {
                let how = match (h.etype, h.exact_chain) {
                    (EdgeType::Conditions, _) => "  (rows)",
                    (_, true) => "  (exact copy)",
                    _ => "",
                };
                writeln!(out, "{}{}{how}", "  ".repeat(h.depth as usize), line(h.key))
                    .unwrap_or_default();
                // A table reached by its rows is reported, not walked through.
                h.etype != EdgeType::Conditions
            });
        }
    }
    Ok(out)
}

/// Run a describe: discover, describe, then what the options ask (store,
/// publish, output file, schema files, apply). Progress lines go to `log`.
#[allow(clippy::too_many_lines)] // the run's stages, in order
pub async fn run(a: &Options, log: &mut impl FnMut(&str)) -> Result<Report, String> {
    let home = a
        .targets
        .iter()
        .find_map(Target::project)
        .map(str::to_owned)
        .or_else(|| a.billing_project.clone())
        .ok_or("a query with no table names needs --default-dataset or --billing-project")?;
    // Queries are billed to (and metadata read through) the billing project;
    // job history is read from the jobs project, which may differ.
    let billing = a.billing_project.clone().unwrap_or_else(|| home.clone());
    let jobs_project = a.jobs_project.clone().unwrap_or_else(|| home.clone());
    let http = || {
        Client::new(
            Adc::discover().map_err(|e| format!("credentials: {e}"))?,
            HttpConfig::default(),
        )
        .map_err(|e| format!("http: {e}"))
    };
    let bq = BigQuery::new(
        http()?,
        &BigQueryConfig { project: billing.clone(), location: a.region.clone(), endpoint: None },
    );
    let vp = a.vertex_project.clone().unwrap_or_else(|| billing.clone());
    let model = Vertex::new(
        http()?,
        VertexConfig {
            project: vp,
            location: a.vertex_location.clone(),
            model: a.model.clone(),
            caps: Caps {
                id: a.model.clone(),
                constraints: ConstraintSupport { lark: false, gbnf: false, json_schema: true },
                max_candidates: 8,
                max_stream_candidates: 1,
                cache: CacheSupport::Explicit,
                context_tokens: 1_048_576,
                max_output_tokens: 8_192,
                streaming: true,
                price: Price { input: 300_000, cached_input: 30_000, output: 2_500_000 },
            },
            thinking_budget: Some(0),
            endpoint: None,
            cache: Some(ExplicitCache::default()),
        },
    );
    let mut cfg = DescribeConfig::new(a.targets.clone(), &a.region, &jobs_project);
    cfg.upstream = a.upstream;
    cfg.jobs.lookback_days = a.lookback_days;
    cfg.profile = a.profile;
    cfg.audit_existing = a.audit;
    cfg.overwrite = a.overwrite;
    cfg.max_rounds = a.rounds;
    cfg.downstream = a.downstream;
    cfg.concurrency = a.concurrency;
    cfg.budget =
        crate::base::Budget::new(50_000_000, 200 << 30, (a.budget_usd * 1e6) as u64, 6 * 3_600_000);
    let store = match &a.store {
        Some(dir) => {
            let exists = std::path::Path::new(dir).join(graph_r::store::BASE_FILE).exists();
            let s = if exists {
                graph_r::Store::open(dir)
            } else {
                graph_r::Store::create(dir, graph_r::Config::default())
            };
            Some(s.map_err(|e| format!("{dir}: {e}"))?)
        }
        None => None,
    };
    if let Some(s) = &store {
        cfg.prior = crate::describe::state::load(s);
        log(&format!("store: {} tables recorded for reuse", cfg.prior.len()));
    }
    let report = match &a.lineage_location {
        Some(loc) => {
            let dp = Dataplex::new(
                http()?,
                DataplexConfig {
                    project: billing.clone(),
                    location: loc.clone(),
                    locations: vec![],
                    exact_copy: true,
                    endpoint: None,
                },
            );
            describe(&model, &bq, &dp, &cfg).await
        }
        None => describe(&model, &bq, &NoLineage, &cfg).await,
    };

    let json = report::to_json(&report);
    for w in &report.warnings {
        log(&format!("warning: {w}"));
    }
    if let Some(p) = &a.out {
        graph_r::fsio::write_atomic(std::path::Path::new(p), &[json.as_bytes()])
            .map_err(|e| format!("{p}: {e}"))?;
    }
    if let Some(s) = &store {
        // Tables of a dataset this run could not read keep what earlier
        // runs recorded: only covered tables are written.
        crate::describe::state::save(s, &report, graph_r::fsio::now_ms())
            .map_err(|e| format!("store: {e}"))?;
        if !report.unreadable.is_empty() {
            log(&format!("store kept as it was for {}", report.unreadable.join(", ")));
        }
        if let Some(dest) = &a.publish {
            let p = publish(dest, s).await?;
            log(&format!(
                "published {dest}: generation {}, {} bytes uploaded",
                p.generation, p.uploaded
            ));
        }
    } else if a.publish.is_some() {
        return Err("--publish needs --store".into());
    }
    if let Some(dir) = &a.emit_schema {
        std::fs::create_dir_all(dir).map_err(|e| format!("{dir}: {e}"))?;
        for t in &report.tables {
            if let Some(s) = report::schema_file(t, a.correct) {
                let path = format!("{dir}/{}.schema.json", t.table);
                graph_r::fsio::write_atomic(std::path::Path::new(&path), &[s.as_bytes()])
                    .map_err(|e| format!("{path}: {e}"))?;
            }
        }
    }
    // Summary on stderr.
    let (mut gen, mut prop, mut flag, mut confirm) = (0, 0, 0, 0);
    for t in &report.tables {
        for c in &t.columns {
            match c.action {
                crate::describe::describe::Action::Generate => gen += 1,
                crate::describe::describe::Action::Propagate => prop += 1,
                crate::describe::describe::Action::Flag => flag += 1,
                crate::describe::describe::Action::Confirm => confirm += 1,
                _ => {}
            }
        }
    }
    let reused = report.tables.iter().filter(|t| t.reused).count();
    log(&format!(
        "crab-describe: {} tables in {} levels ({reused} unchanged, reused); columns generated {gen}, propagated {prop}, confirmed {confirm}, flagged {flag}; ${:.4}, {} MiB billed, {} warnings",
        report.tables.len(),
        report.levels.len(),
        report.spend.micro_usd as f64 / 1e6,
        report.spend.warehouse_bytes >> 20,
        report.warnings.len() + report.tables.iter().map(|t| t.warnings.len()).sum::<usize>()
    ));
    if a.apply {
        // A query's result is no table: there is nothing to write back.
        for t in report.tables.iter().filter(|t| !is_virtual(&t.table)) {
            match report::apply(&bq, t, a.correct).await {
                Applied::Unchanged => {}
                Applied::Written { columns, table } => log(&format!(
                    "applied {}: {columns} columns{}",
                    t.table,
                    if table { " + table description" } else { "" }
                )),
                Applied::Conflict => {
                    log(&format!("skipped {}: changed while applying; re-run describe", t.table));
                }
                Applied::Failed(e) => log(&format!("failed {}: {e}", t.table)),
            }
        }
    }
    Ok(report)
}
