// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Property tests of the MetadataGuard (feature `http`):
//
// - random SQL assembled from tricky pieces (comments, quoted identifiers,
//   several statements, CTE shadowing, array paths, EXTERNAL_QUERY) is
//   never accepted when it reads a table outside INFORMATION_SCHEMA and
//   the graph datasets — checked by an independent, textual oracle on the
//   pieces used;
// - no method × host × path outside the allow list passes, and a table
//   read passes only with view=BASIC.

#![cfg(feature = "http")]

use crab_r::exec::guard::MetadataGuard;
use objstore_r::policy::{RequestPolicy, Url};
use objstore_r::transport::Method;
use proptest::prelude::*;

/// (text, reads a forbidden table when it parses as a query)
const PIECES: [(&str, bool); 18] = [
    ("SELECT * FROM ops.runs", false),
    ("SELECT * FROM `p.region-us.INFORMATION_SCHEMA.JOBS`", false),
    ("SELECT * FROM infra.nodes n JOIN ops.edges e ON n.k = e.k", false),
    ("SELECT * FROM `p.mart.orders`", true),
    ("SELECT * FROM `p`.`mart`.orders", true),
    ("SELECT * FROM orders", true),
    ("SELECT * FROM ops.runs r, r.items", true),
    ("SELECT * FROM `ops.runs` WHERE EXISTS (SELECT 1 FROM secret.t)", true),
    ("SELECT (SELECT MAX(x) FROM p.d.t) FROM ops.runs", true),
    ("WITH ops AS (SELECT 1) SELECT * FROM ops, mart.t", true),
    ("WITH s AS (SELECT * FROM ops.runs) SELECT * FROM s", false),
    ("WITH s AS (SELECT * FROM s) SELECT 1", true),
    ("SELECT * FROM EXTERNAL_QUERY('c', 'SELECT 1')", true),
    ("SELECT p.d.fn(1) FROM ops.runs", true),
    ("SELECT * FROM `OPS`.runs", true),
    ("SELECT * FROM ops.runs /* ; SELECT * FROM p.d.t */", false),
    ("SELECT * FROM ops.runs -- ; SELECT * FROM p.d.t\n", false),
    ("SELECT * FROM ops.runs UNION ALL SELECT * FROM p.d.t", true),
];

const GLUE: [&str; 6] = [";", " ", "\n", "; ", " UNION ALL ", ";\nDROP TABLE ops.runs;"];

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4096))]

    #[test]
    fn nothing_reading_outside_the_graph_is_accepted(
        picks in prop::collection::vec((0..PIECES.len(), 0..GLUE.len()), 1..4)
    ) {
        let mut sql = String::new();
        let mut forbidden = false;
        for (i, (p, g)) in picks.iter().enumerate() {
            if i > 0 {
                sql.push_str(GLUE[*g]);
            }
            sql.push_str(PIECES[*p].0);
            forbidden |= PIECES[*p].1;
        }
        let ok = MetadataGuard::default().check_sql(&sql).is_ok();
        prop_assert!(!(ok && forbidden), "accepted a forbidden read: {sql:?}");
        if picks.len() > 1 && GLUE[picks[1].1].contains(';') {
            prop_assert!(!ok, "accepted more than one statement: {sql:?}");
        }
    }

    #[test]
    fn no_endpoint_off_the_list_passes(
        m in 0..5usize,
        host in prop::sample::select(vec!["bigquery.googleapis.com", "datalineage.googleapis.com",
            "storage.googleapis.com", "metadata.google.internal", "bigquery.googleapis.com.evil.example",
            "BIGQUERY.googleapis.com"]),
        segs in prop::collection::vec(prop::sample::select(vec!["bigquery", "v2", "projects", "p", "datasets",
            "d", "tables", "t", "queries", "jobs", "data", "v1", "locations", "us:searchLinks", "processes",
            "x", "..", "%2F", "insertAll"]), 1..10),
        query in prop::sample::select(vec!["", "?view=BASIC", "?view=FULL", "?alt=media", "?view=BASIC&x=1"]),
    ) {
        let method = [Method::Get, Method::Post, Method::Put, Method::Delete, Method::Patch][m];
        let url = format!("https://{host}/{}{query}", segs.join("/"));
        let Ok(u) = Url::parse(&url) else { return Ok(()) };
        if MetadataGuard::default().check(method, &u).is_err() {
            return Ok(());
        }
        let h = u.host_str().unwrap_or("");
        prop_assert!(h == "bigquery.googleapis.com" || h == "datalineage.googleapis.com", "{url}");
        prop_assert!(matches!(method, Method::Get | Method::Post), "{url}");
        let p = u.path();
        prop_assert!(!p.contains("..") && !p.contains("/data") && !p.contains("insertAll") && !p.contains("/jobs"), "{url}");
        if p.contains("/tables/") {
            prop_assert_eq!(u.query(), Some("view=BASIC"), "{}", url);
        } else {
            prop_assert!(u.query().is_none(), "{url}");
        }
        prop_assert!(!p.contains("/queries/"), "a job result with no job issued: {url}");
    }
}
