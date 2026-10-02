// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! `crab-describe`: evidence-driven metadata for BigQuery datasets.
//!
//! ```text
//! crab-describe --dataset my-proj.mart[,my-proj.stg] [--region US]
//!     [--jobs-project my-proj] [--lookback-days 90] [--billing-project my-proj]
//!     [--vertex-project my-proj] [--vertex-location us-central1] [--model gemini-2.5-flash]
//!     [--lineage-location us]            # add Dataplex lineage (Data Lineage API)
//!     [--downstream]                     # with it: consumers outside the datasets
//!     [--no-profile] [--no-audit] [--overwrite] [--rounds 2] [--concurrency 4]
//!     [--budget-usd 5] [--out report.json] [--emit-schema DIR]
//!     [--state report.json]              # incremental: reuse unchanged tables
//!     [--emit-image meta.img]            # metadata image for crab-meta lookups
//!     [--apply] [--correct]
//! ```
//!
//! With `--state FILE`, the previous report in `FILE` (when present) is the
//! run's state: tables unchanged since — shape, rows, producing SQL,
//! upstream descriptions — are carried over with no profile, model call or
//! check query, and the new report is written back to `FILE`.
//!
//! Credentials: Application Default Credentials. Nothing is written to the
//! warehouse unless `--apply` is given; then only empty descriptions are
//! filled (flagged corrections too with `--correct`), each table under
//! `If-Match` of the resource it read.

use crab_describe::describe::{describe, DescribeConfig};
use crab_describe::report::{self, Applied};
use crab_exec::bigquery::{BigQuery, BigQueryConfig};
use crab_lineage::dataplex::{Dataplex, DataplexConfig};
use crab_lineage::NoLineage;
use crab_llm::vertex::{ExplicitCache, Vertex, VertexConfig};
use crab_llm::{CacheSupport, Caps, ConstraintSupport, Price};
use objstore_r::http::{Client, HttpConfig};
use objstore_r::token::Adc;
use std::process::ExitCode;

struct Args {
    datasets: Vec<String>,
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
    out: Option<String>,
    state: Option<String>,
    emit_image: Option<String>,
    emit_schema: Option<String>,
    apply: bool,
    correct: bool,
}

const USAGE: &str = "usage: crab-describe --dataset PROJECT.DATASET[,...] [--region US] [--jobs-project P] [--billing-project P] [--lookback-days N] \
[--vertex-project P] [--vertex-location L] [--model M] [--lineage-location L] [--downstream] [--no-profile] [--no-audit] [--overwrite] \
[--rounds N] [--concurrency N] [--budget-usd X] [--out FILE] [--state FILE] [--emit-image FILE] [--emit-schema DIR] [--apply] [--correct]";

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        datasets: Vec::new(),
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
        state: None,
        emit_image: None,
        emit_schema: None,
        apply: false,
        correct: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = |name: &str| it.next().ok_or_else(|| format!("{name} needs a value"));
        match flag.as_str() {
            "--dataset" | "--datasets" => a.datasets.extend(
                val(&flag)?.split(',').map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()),
            ),
            "--region" => a.region = val(&flag)?,
            "--jobs-project" => a.jobs_project = Some(val(&flag)?),
            "--billing-project" => a.billing_project = Some(val(&flag)?),
            "--lookback-days" => {
                a.lookback_days = val(&flag)?.parse().map_err(|_| "--lookback-days: a number")?
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
                a.concurrency = val(&flag)?.parse().map_err(|_| "--concurrency: a number")?
            }
            "--budget-usd" => {
                a.budget_usd = val(&flag)?.parse().map_err(|_| "--budget-usd: a number")?
            }
            "--out" => a.out = Some(val(&flag)?),
            "--state" => a.state = Some(val(&flag)?),
            "--emit-image" => a.emit_image = Some(val(&flag)?),
            "--emit-schema" => a.emit_schema = Some(val(&flag)?),
            "--apply" => a.apply = true,
            "--correct" => a.correct = true,
            "-h" | "--help" => return Err(USAGE.into()),
            other => return Err(format!("unknown argument {other}\n{USAGE}")),
        }
    }
    if a.datasets.is_empty() || !a.datasets.iter().all(|d| d.contains('.')) {
        return Err(format!("--dataset PROJECT.DATASET is required\n{USAGE}"));
    }
    Ok(a)
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    match objstore_r::http::block_on(run(args)) {
        Ok(Ok(code)) => code,
        Ok(Err(e)) => {
            eprintln!("crab-describe: {e}");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("crab-describe: runtime: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(a: Args) -> Result<ExitCode, String> {
    let home = a.datasets[0].split_once('.').map(|(p, _)| p.to_owned()).unwrap_or_default();
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
    let mut cfg = DescribeConfig::new(a.datasets.clone(), &a.region, &jobs_project);
    cfg.jobs.lookback_days = a.lookback_days;
    cfg.profile = a.profile;
    cfg.audit_existing = a.audit;
    cfg.overwrite = a.overwrite;
    cfg.max_rounds = a.rounds;
    cfg.downstream = a.downstream;
    cfg.concurrency = a.concurrency;
    cfg.budget =
        crab_core::Budget::new(50_000_000, 200 << 30, (a.budget_usd * 1e6) as u64, 6 * 3_600_000);
    if let Some(p) = &a.state {
        match std::fs::read_to_string(p) {
            Ok(text) => cfg.prior = crab_describe::incremental::Prior::from_json(&text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("{p}: {e}")),
        }
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
    match a.out.as_ref().or(a.state.as_ref()) {
        Some(p) => std::fs::write(p, &json).map_err(|e| format!("{p}: {e}"))?,
        None => println!("{json}"),
    }
    if let (Some(out), Some(state)) = (&a.out, &a.state) {
        if out != state {
            std::fs::write(state, &json).map_err(|e| format!("{state}: {e}"))?;
        }
    }
    if let Some(p) = &a.emit_image {
        let img = report::meta_image(&report).map_err(|e| format!("metadata image: {e}"))?;
        std::fs::write(p, img).map_err(|e| format!("{p}: {e}"))?;
    }
    if let Some(dir) = &a.emit_schema {
        std::fs::create_dir_all(dir).map_err(|e| format!("{dir}: {e}"))?;
        for t in &report.tables {
            if let Some(s) = report::schema_file(t, a.correct) {
                let path = format!("{dir}/{}.schema.json", t.table);
                std::fs::write(&path, s).map_err(|e| format!("{path}: {e}"))?;
            }
        }
    }
    // Summary on stderr.
    let (mut gen, mut prop, mut flag, mut confirm) = (0, 0, 0, 0);
    for t in &report.tables {
        for c in &t.columns {
            match c.action {
                crab_describe::describe::Action::Generate => gen += 1,
                crab_describe::describe::Action::Propagate => prop += 1,
                crab_describe::describe::Action::Flag => flag += 1,
                crab_describe::describe::Action::Confirm => confirm += 1,
                _ => {}
            }
        }
    }
    let reused = report.tables.iter().filter(|t| t.reused).count();
    eprintln!(
        "crab-describe: {} tables in {} levels ({reused} unchanged, reused); columns generated {gen}, propagated {prop}, confirmed {confirm}, flagged {flag}; ${:.4}, {} MiB billed, {} warnings",
        report.tables.len(),
        report.levels.len(),
        report.spend.micro_usd as f64 / 1e6,
        report.spend.warehouse_bytes >> 20,
        report.warnings.len() + report.tables.iter().map(|t| t.warnings.len()).sum::<usize>()
    );
    if a.apply {
        for t in &report.tables {
            match report::apply(&bq, t, a.correct).await {
                Applied::Unchanged => {}
                Applied::Written { columns, table } => eprintln!(
                    "applied {}: {columns} columns{}",
                    t.table,
                    if table { " + table description" } else { "" }
                ),
                Applied::Conflict => {
                    eprintln!("skipped {}: changed while applying; re-run describe", t.table)
                }
                Applied::Failed(e) => eprintln!("failed {}: {e}", t.table),
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}
