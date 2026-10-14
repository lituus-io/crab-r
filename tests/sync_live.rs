// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Live catalog sync against BigQuery (bills six minimum-sized metadata
// queries): ignored by default. Run with
//
//   CRAB_BQ_PROJECT=my-project CRAB_BQ_REGION=US \
//     cargo test --test sync_live -- --ignored --nocapture
//
// Credentials come from Application Default Credentials.

use crab_r::catalog::{sync, Catalog, SyncConfig};
use crab_r::exec::bigquery::{BigQuery, BigQueryConfig};
use objstore_r::http::{Client, HttpConfig};
use objstore_r::token::Adc;

#[tokio::test(flavor = "current_thread")]
#[ignore = "calls the live BigQuery API"]
async fn live_sync_builds_a_valid_image() {
    let Ok(project) = std::env::var("CRAB_BQ_PROJECT") else {
        eprintln!("CRAB_BQ_PROJECT not set; skipping");
        return;
    };
    let region = std::env::var("CRAB_BQ_REGION").unwrap_or_else(|_| "US".into());
    let client = Client::new(Adc::discover().expect("ADC"), HttpConfig::default()).expect("client");
    let bq = BigQuery::new(
        client,
        &BigQueryConfig { project: project.clone(), location: region.clone(), endpoint: None },
    );
    let t = std::time::Instant::now();
    let (builder, stats) = sync(&bq, &SyncConfig::new(&project, &region)).await.expect("sync");
    let img = builder.build(1, 0).expect("build");
    let cat = Catalog::from_bytes(objstore_r::Bytes::from(img.clone())).expect("valid image");
    let (d, tb, c) = cat.counts();
    eprintln!(
        "{stats:?}\nimage: {d} datasets, {tb} tables, {c} columns, {} bytes, total {:?}",
        img.len(),
        t.elapsed()
    );
    for table in cat.tables().take(5) {
        let cols: Vec<&str> = cat.columns(table.id).take(8).map(|c| c.path).collect();
        eprintln!(
            "  {}.{} ({:?}, {} rows): {cols:?}",
            table.dataset_name, table.name, table.kind, table.row_count
        );
    }
}
