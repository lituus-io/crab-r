// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Property tests for BigQuery names and static lineage ingest: any text
// parses as a name or is refused, never a panic; an accepted name reads the
// same in all three spellings and addresses one store node; any input to
// ingest is ingested or refused, never a panic and never a half-written
// store; and a second ingest of the same export writes nothing.

use crab_r::base::key::{BqName, UriBuf};
use crab_r::describe::ingest::ingest;
use proptest::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};

fn store() -> (std::path::PathBuf, graph_r::Store) {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!(
        "crab-ingest-prop-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&d);
    let s = graph_r::Store::create(&d, graph_r::Config::default()).unwrap();
    (d, s)
}

/// Name-ish text: mostly the characters names are made of, plus every
/// separator and some that must be refused.
fn nameish() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-zA-Z0-9_:./#-]{0,24}",
        "bq://[a-zA-Z0-9_:./#-]{0,24}",
        "bq://[a-z]{1,3}(/[a-zA-Z_]{1,3}){0,4}(#[A-Za-z]{1,3})?",
        "[a-z]{1,3}(\\.[a-zA-Z_]{1,3}){0,4}",
        any::<String>(),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4096))]

    #[test]
    fn names_parse_or_are_refused_and_every_spelling_agrees(s in nameish()) {
        let Ok(n) = BqName::parse(&s) else { return Ok(()) };
        let (id, path, dotted) = (n.bq_id(), n.bq_path(), n.dotted());
        prop_assert!(!path.contains('#'), "{path}");
        let mut uri = UriBuf::new();
        let want = n.uri(&mut uri).map(str::to_owned);
        for spelled in [&id, &path, &dotted] {
            let again = BqName::parse(spelled);
            prop_assert!(again.is_ok(), "{s:?} -> {spelled:?} does not parse");
            let again = again.unwrap();
            prop_assert_eq!(again.level(), n.level());
            prop_assert_eq!(again.uri(&mut uri).map(str::to_owned), want.clone());
            // Each spelling is a fixed point.
            prop_assert_eq!(&again.bq_id(), &id);
            prop_assert_eq!(&again.bq_path(), &path);
            prop_assert_eq!(&again.dotted(), &dotted);
        }
    }
}

/// An id a lineage export could hold, or one it should not.
fn id() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => "bq://[pq](/[de]{1,2}(/[tu]{1,2}([#/][cC]{1,2})?)?)?",
        1 => "bq-routine://p/d/r",
        1 => "urn:pulumi:x",
        1 => "bq://p/d/t[`'\" ./#]{1,3}",
    ]
}

fn kind() -> impl Strategy<Value = &'static str> {
    prop::sample::select(vec!["project", "dataset", "table", "view", "column", "routine"])
}

fn rel() -> impl Strategy<Value = &'static str> {
    prop::sample::select(vec![
        "contains",
        "derives_from",
        "column_derives_from",
        "writes_to",
        "defined_by",
    ])
}

fn quoted(s: &str) -> String {
    let mut out = String::new();
    crab_r::base::json::escape_into(s, &mut out); // quotes included
    out
}

fn export() -> impl Strategy<Value = (String, String, usize, usize)> {
    let nodes = prop::collection::vec((id(), kind(), prop::option::of("[A-Z]{1,6}")), 0..24);
    let edges = prop::collection::vec((rel(), id(), id()), 0..24);
    (nodes, edges).prop_map(|(nodes, edges)| {
        let n: Vec<String> = nodes
            .iter()
            .map(|(id, kind, t)| match t {
                Some(t) => format!(
                    r#"{{"id":{},"kind":"{kind}","data_type":"{t}","extra":[1,{{"a":null}}]}}"#,
                    quoted(id)
                ),
                None => format!(r#"{{"id":{},"kind":"{kind}"}}"#, quoted(id)),
            })
            .collect();
        let e: Vec<String> = edges
            .iter()
            .map(|(r, s, t)| {
                format!(
                    r#"{{"relationship":"{r}","source_id":{},"target_id":{}}}"#,
                    quoted(s),
                    quoted(t)
                )
            })
            .collect();
        (format!("[{}]", n.join(",")), format!("[{}]", e.join(",")), nodes.len(), edges.len())
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn any_text_is_ingested_or_refused_and_never_half_written(
        nodes in prop_oneof![any::<String>(), "\\[[\\[\\]{}\":,a-z0-9 ]{0,40}"],
        edges in prop_oneof![any::<String>(), Just("[]".to_owned())],
    ) {
        let (d, s) = store();
        let r = ingest(&s, &nodes, &edges, 1);
        if r.is_err() {
            prop_assert!(s.snapshot().node(graph_r::UrlKey::of(crab_r::describe::state::ROOT)).is_none());
        }
        // Whatever happened, the store still takes a clean ingest.
        let next = ingest(&s, r#"[{"id":"bq://z/y","kind":"dataset"}]"#, "[]", 2);
        prop_assert!(next.is_ok());
        drop(s);
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn an_export_ingests_once_and_reports_every_refusal_by_position(
        (nodes, edges, n, e) in export()
    ) {
        let (d, s) = store();
        let first = ingest(&s, &nodes, &edges, 1).unwrap();
        for r in &first.rejected {
            let len = match r.side {
                crab_r::describe::ingest::Side::Node => n,
                crab_r::describe::ingest::Side::Edge => e,
            };
            prop_assert!(r.index < len);
        }
        let accounted = first.rejected.len() + first.skipped.values().sum::<usize>();
        prop_assert!(accounted <= n + e);
        prop_assert_eq!(first.committed, first.created + first.updated + first.edges > 0);
        let second = ingest(&s, &nodes, &edges, 2).unwrap();
        prop_assert!(!second.committed, "a repeat ingest wrote: {second:?}");
        prop_assert_eq!(&second.rejected, &first.rejected);
        drop(s);
        // And the store reopens: nothing written was torn.
        let reopened = graph_r::Store::open(&d);
        prop_assert!(reopened.is_ok());
        drop(reopened);
        let _ = std::fs::remove_dir_all(d);
    }
}
