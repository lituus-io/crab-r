// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Live column lineage from the Data Lineage API (free to call; the tables
// must have been written by BigQuery jobs after the API was enabled):
// ignored by default. Run with
//
//   CRAB_LINEAGE_PROJECT=my-project CRAB_LINEAGE_TABLE=my-project.crab_mock.stg_orders \
//   CRAB_LINEAGE_COLUMNS=order_id,order_status,returned_at CRAB_LINEAGE_LOCATION=us \
//     cargo test --features dataplex --test dataplex_live -- --ignored --nocapture
//
// Credentials come from Application Default Credentials.

#![cfg(feature = "dataplex")]

use crab_r::lineage::dataplex::{Dataplex, DataplexConfig};
use crab_r::lineage::{Column, LineageSource};
use objstore_r::http::{Client, HttpConfig};
use objstore_r::token::Adc;

#[tokio::test(flavor = "current_thread")]
#[ignore = "calls the live Data Lineage API"]
async fn live_upstream_marks_exact_copies_in_one_call_per_batch() {
    let (Ok(project), Ok(table), Ok(cols)) = (
        std::env::var("CRAB_LINEAGE_PROJECT"),
        std::env::var("CRAB_LINEAGE_TABLE"),
        std::env::var("CRAB_LINEAGE_COLUMNS"),
    ) else {
        eprintln!("CRAB_LINEAGE_PROJECT / _TABLE / _COLUMNS not set; skipping");
        return;
    };
    let location = std::env::var("CRAB_LINEAGE_LOCATION").unwrap_or_else(|_| "us".into());
    let client = Client::new(Adc::discover().expect("ADC"), HttpConfig::default()).expect("client");
    let d = Dataplex::new(
        client,
        DataplexConfig { project, location, locations: vec![], exact_copy: true, endpoint: None },
    );
    let roots: Vec<Column> =
        cols.split(',').map(|c| Column::new(table.clone(), c.trim())).collect();
    let t = std::time::Instant::now();
    let g = d.upstream(&roots, 10).await.expect("lineage");
    eprintln!("{} edges in {:?}; unreachable {:?}", g.edge_count(), t.elapsed(), g.unreachable);
    assert!(g.edge_count() > 0, "no lineage recorded for {table}");
    for r in &roots {
        for (p, exact) in g.parents(r) {
            eprintln!(
                "  {}.{} <- {}.{}{}",
                r.table,
                r.column,
                p.table,
                p.column,
                if *exact { " (exact copy)" } else { "" }
            );
        }
    }
    assert!(roots.iter().any(|r| g.parents(r).iter().any(|(_, e)| *e)), "no exact copy marked");
}
