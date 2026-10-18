// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// S6 conformance: every spelling in tests/data/ids_conformance.tsv (the id
// strings shared with pulumi-rs-yaml's sql_lineage::ids) reaches the same
// canonical id through BqName, through the SQL reference walk, and through
// the store key, so a lineage export's ids join the store's nodes.

use crab_r::base::key::{BqName, UriBuf};
use crab_r::sql::referenced_tables;

const TABLE: &str = include_str!("data/ids_conformance.tsv");

fn rows() -> impl Iterator<Item = (&'static str, &'static str)> {
    TABLE
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .map(|l| l.split_once('\t').expect("input<TAB>id"))
}

fn canonical(input: &str) -> Option<String> {
    if let Some(from) = input.strip_prefix("sql:") {
        let refs = referenced_tables(&format!("SELECT 1 FROM {from}"), None).ok()?;
        let fqn = refs.first()?.fqn()?;
        return BqName::parse(&fqn).ok().map(|n| n.bq_id());
    }
    BqName::parse(input).ok().map(|n| n.bq_id())
}

#[test]
fn every_spelling_reaches_the_shared_canonical_id() {
    let mut n = 0;
    for (input, want) in rows() {
        let got = canonical(input);
        if want == "-" {
            assert_eq!(got, None, "{input} must be refused");
        } else {
            assert_eq!(got.as_deref(), Some(want), "{input}");
        }
        n += 1;
    }
    assert!(n >= 20, "the table is read");
}

#[test]
fn the_store_key_of_every_spelling_is_the_canonical_ones() {
    let mut a = UriBuf::new();
    let mut b = UriBuf::new();
    for (input, want) in rows().filter(|(i, w)| *w != "-" && !i.starts_with("sql:")) {
        let (Ok(n), Ok(c)) = (BqName::parse(input), BqName::parse(want)) else { continue };
        assert_eq!(n.uri(&mut a), c.uri(&mut b), "{input}");
        assert_eq!(n, c, "{input}");
    }
}
