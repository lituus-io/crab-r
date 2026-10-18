// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// S16: lookups answer within a token budget (graph-r's TokenBudget):
// whole lines, nearest first, a `truncated` line when some did not fit, and
// never more than the budget; `search` is graph-r's budgeted query.

#![cfg(feature = "cli")]

use crab_r::describe::app::{lookup, lookup_within, TRUNCATED};
use crab_r::describe::ingest::ingest_dir;
use graph_r::TokenBudget;

const NODES: &str = include_str!("data/static_lineage/nodes.json");
const EDGES: &str = include_str!("data/static_lineage/edges.json");

fn store(name: &str) -> String {
    let d = std::env::temp_dir().join(format!("crab-budget-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    let d = d.to_string_lossy().into_owned();
    ingest_dir(&d, NODES, EDGES).unwrap();
    d
}

fn tokens(s: &str) -> u64 {
    s.lines().map(|l| graph_r::text::estimate_tokens(l.len() + 1)).sum()
}

#[test]
fn lookups_fit_their_budget() {
    let d = store("lookups");
    let args = |name: &str| vec![d.clone(), name.to_owned(), "10".to_owned()];
    let full = lookup("upstream", &args("proj.mart.orders_t.doubled")).unwrap();
    assert!(full.lines().count() >= 2, "{full}");
    assert!(!full.contains(TRUNCATED));
    // A budget the whole answer fits is the whole answer.
    let roomy =
        lookup_within("upstream", &args("proj.mart.orders_t.doubled"), Some(TokenBudget(10_000)));
    assert_eq!(roomy.unwrap(), full);
    // A budget of one line: that line, then how much was left out.
    let first = full.lines().next().unwrap();
    let b = graph_r::text::estimate_tokens(first.len() + 1) as u32;
    let cut = lookup_within("upstream", &args("proj.mart.orders_t.doubled"), Some(TokenBudget(b)))
        .unwrap();
    let mut lines = cut.lines();
    assert_eq!(lines.next(), Some(first));
    let last = lines.next().unwrap();
    assert!(last.starts_with(TRUNCATED), "{cut}");
    assert!(tokens(first) <= u64::from(b));
    // Zero: nothing but the note.
    let none = lookup_within("table", &args("proj.mart.orders_t"), Some(TokenBudget(0))).unwrap();
    assert!(none.starts_with(TRUNCATED), "{none}");
}

#[test]
fn search_is_graph_rs_budgeted_query() {
    let d = store("search");
    let hits =
        lookup_within("search", &[d.clone(), "orders".into()], Some(TokenBudget(2_000))).unwrap();
    assert!(hits.contains("orders"), "{hits}");
    let small = lookup_within("search", &[d, "orders".into()], Some(TokenBudget(20))).unwrap();
    assert!(tokens(&small) <= 20 + 8, "{small}");
    assert!(small.lines().count() < hits.lines().count().max(2));
}
