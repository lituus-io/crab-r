// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// The live S2 baseline: catalog sync → engine (Vertex Gemini + BigQuery) →
// evaluation against gold SQL → scorecard. Costs real money (a few cents):
// ignored by default. Run with
//
//   CRAB_BQ_PROJECT=my-project \
//   CRAB_EVAL=evals/thelook_smoke.json \
//     cargo test --test baseline_live -- --ignored --nocapture
//
// Optional: CRAB_DATA_PROJECT (default bigquery-public-data),
// CRAB_DATASETS (default thelook_ecommerce), CRAB_VERTEX_MODEL
// (default gemini-2.5-flash), CRAB_VERTEX_LOCATION (default us-central1),
// CRAB_PROFILE=0 (skip column profiling), CRAB_CATALOG_CACHE=<path>
// (reuse a synced + profiled image across runs), CRAB_GROUNDING=0 (A/B:
// no profile hints, value grounding or literal checks), CRAB_SAMPLES=<n>
// (first-batch samples; 1 = single greedy sample), CRAB_REPEAT=1 (a second
// pass served by the verified-answer cache), CRAB_PROFILE_DATASETS=a,b
// (profile only these), CRAB_EXPLICIT_CACHE=0 (implicit prompt caching only), CRAB_SCHEMA_BUDGET=<tokens> (default 12000; larger
// schemas are linked), CRAB_LINK_MODEL=0 (lexical linking only).
// Credentials: Application Default Credentials.

use crab_r::base::date::Date;
use crab_r::catalog::{profile, sync, Catalog, ProfileConfig, SyncConfig};
use crab_r::engine::{Engine, EngineConfig};
use crab_r::eval::run::{evaluate, RunConfig};
use crab_r::eval::{Dataset, Scorecard};
use crab_r::exec::bigquery::{BigQuery, BigQueryConfig};
use crab_r::exec::Warehouse;
use crab_r::llm::openai::{OpenAi, OpenAiConfig};
use crab_r::llm::vertex::{ExplicitCache, Vertex, VertexConfig};
use crab_r::llm::Model;
use crab_r::llm::{CacheSupport, Caps, ConstraintSupport, Price};
use objstore_r::http::{Client, HttpConfig};
use objstore_r::token::Adc;

fn env(k: &str, default: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| default.to_owned())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "calls live Vertex AI and BigQuery"]
async fn live_baseline_scorecard() {
    let Ok(project) = std::env::var("CRAB_BQ_PROJECT") else {
        eprintln!("CRAB_BQ_PROJECT not set; skipping");
        return;
    };
    let data_project = env("CRAB_DATA_PROJECT", "bigquery-public-data");
    let datasets: Vec<String> =
        env("CRAB_DATASETS", "thelook_ecommerce").split(',').map(str::to_owned).collect();
    let eval_path = env("CRAB_EVAL", "evals/thelook_smoke.json");
    let src =
        std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(&eval_path))
            .expect("eval file");
    let ds = Dataset::from_json_array("eval", &src).expect("dataset");

    let http =
        || Client::new(Adc::discover().expect("ADC"), HttpConfig::default()).expect("client");
    let bq = BigQuery::new(
        http(),
        &BigQueryConfig { project: project.clone(), location: "US".into(), endpoint: None },
    );

    // 1. Catalog (dataset-scoped: public projects cannot be listed
    //    region-wide), profiled, cached on disk when asked.
    let cache = std::env::var("CRAB_CATALOG_CACHE").ok().map(std::path::PathBuf::from);
    let t = std::time::Instant::now();
    let image = match cache.as_deref().and_then(|p| std::fs::read(p).ok()) {
        Some(img) => {
            eprintln!("catalog: cached image ({} bytes)", img.len());
            img
        }
        None => {
            let mut cfg = SyncConfig::new(&data_project, "US");
            cfg.datasets = Some(datasets.clone());
            cfg.dataset_scoped = true;
            let (mut builder, stats) = sync(&bq, &cfg).await.expect("sync");
            eprintln!("sync: {stats:?} in {:?}", t.elapsed());
            if env("CRAB_PROFILE", "1") != "0" {
                let t = std::time::Instant::now();
                let pcfg = ProfileConfig {
                    datasets: std::env::var("CRAB_PROFILE_DATASETS")
                        .ok()
                        .map(|d| d.split(',').map(str::to_owned).collect()),
                    ..ProfileConfig::default()
                };
                let ps = profile(&bq, &mut builder, &data_project, &pcfg).await.expect("profile");
                eprintln!("profile: {ps:?} in {:?}", t.elapsed());
            }
            let img = builder.build(1, 0).expect("build");
            if let Some(p) = &cache {
                std::fs::write(p, &img).expect("write catalog cache");
            }
            img
        }
    };
    let catalog = Catalog::from_bytes(objstore_r::Bytes::from(image)).expect("image");
    eprintln!("catalog: {:?} in {:?}", catalog.counts(), t.elapsed());

    // 2. Model: static dispatch per backend, the rest is generic.
    let model_id = env("CRAB_VERTEX_MODEL", "gemini-2.5-flash");
    let location = env("CRAB_VERTEX_LOCATION", "us-central1");
    let caps = Caps {
        id: model_id.clone(),
        constraints: ConstraintSupport { lark: false, gbnf: false, json_schema: true },
        max_candidates: 8,
        max_stream_candidates: 1,
        cache: CacheSupport::Implicit,
        context_tokens: 1_048_576,
        max_output_tokens: 8192,
        streaming: true,
        price: Price { input: 300_000, cached_input: 30_000, output: 2_500_000 },
    };
    let gold = BigQuery::new(
        http(),
        &BigQueryConfig { project: project.clone(), location: "US".into(), endpoint: None },
    );
    let run = Run { data_project, datasets, catalog, gold, items: ds.items };
    if env("CRAB_BACKEND", "vertex") == "openai" {
        let mut cfg = OpenAiConfig::vertex(&project, &location, &model_id, caps);
        cfg.extra = std::env::var("CRAB_OPENAI_EXTRA")
            .ok()
            .or_else(|| Some(OpenAiConfig::VERTEX_NO_THINKING.into()));
        eprintln!("backend: openai-compatible {}", cfg.url);
        run.go(OpenAi::new(http(), cfg), bq).await;
    } else {
        let vertex = Vertex::new(
            http(),
            VertexConfig {
                project,
                location,
                model: model_id,
                caps,
                thinking_budget: Some(0),
                endpoint: None,
                cache: (env("CRAB_EXPLICIT_CACHE", "1") != "0").then(ExplicitCache::default),
            },
        );
        run.go(vertex, bq).await;
    }
}

struct Run<W: Warehouse> {
    data_project: String,
    datasets: Vec<String>,
    catalog: Catalog,
    gold: W,
    items: Vec<crab_r::eval::Item>,
}

impl<W: Warehouse> Run<W> {
    async fn go<M: Model, X: Warehouse>(self, model: M, bq: X) {
        let Self { data_project, datasets, catalog, gold, items } = self;
        let mut ecfg = EngineConfig::new(&data_project);
        ecfg.default_dataset = datasets.first().cloned();
        ecfg.samples = env("CRAB_SAMPLES", "2").parse().expect("CRAB_SAMPLES");
        ecfg.schema_token_budget =
            env("CRAB_SCHEMA_BUDGET", "12000").parse().expect("CRAB_SCHEMA_BUDGET");
        ecfg.link.model = env("CRAB_LINK_MODEL", "1") != "0";
        if env("CRAB_GROUNDING", "1") == "0" {
            ecfg.render.profiles = false;
            ecfg.max_grounded = 0;
            ecfg.check_literals = false;
        }
        let engine = Engine::new(model, bq, catalog, ecfg)
            .with_cache(crab_r::engine::MemoryCache::new(1024));
        eprintln!(
            "schema context: {} tables, ~{} tokens, {} indexed values",
            engine.schema().blocks().len(),
            engine.schema().estimated_tokens(),
            engine.values().len()
        );
        if std::env::var("CRAB_SHOW_SCHEMA").is_ok() {
            eprintln!("{}", engine.schema().text());
        }

        // 3. Evaluate (gold runs on the same warehouse; reruns hit its cache).
        let today = Date::parse("2026-09-29").ok();
        let passes = if env("CRAB_REPEAT", "0") == "1" { 2 } else { 1 };
        for pass in 1..=passes {
            let t = std::time::Instant::now();
            let traces = evaluate(
                &engine,
                &gold,
                &items,
                RunConfig { concurrency: 4, today, ..RunConfig::default() },
            )
            .await;
            eprintln!("\n=== pass {pass}");
            for tr in &traces {
                eprintln!(
                    "{:>6} {:?} {:.2} {:>6} ms {:>6} µ$  {}  {}",
                    tr.record.item_id,
                    tr.record.verdict,
                    tr.record.confidence,
                    tr.record.latency_ms,
                    tr.record.spend.micro_usd,
                    tr.sql.as_deref().unwrap_or("-"),
                    tr.note.as_deref().unwrap_or("")
                );
            }
            let records: Vec<_> = traces.into_iter().map(|t| t.record).collect();
            let card = Scorecard::from_records(&records);
            eprintln!("\n{card:#?}\nwall: {:?}", t.elapsed());
        }
    }
}
