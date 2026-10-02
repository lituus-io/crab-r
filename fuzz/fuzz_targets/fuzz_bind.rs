// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// The binder on arbitrary SQL, against a small catalog with nested and
// repeated columns: binding never panics in either mode; any statement that
// binds cleanly has canonical SQL that re-parses, re-binds cleanly to the
// same fingerprint, and is a fixed point of canonicalisation. Column lineage
// over any binding never panics and covers exactly the outputs.

#![no_main]

use crab_catalog::{Catalog, CatalogBuilder, ColumnSpec, TableKind, TableSpec};
use crab_ir::canon::to_canonical_sql;
use crab_ir::fingerprint::fingerprint;
use crab_ir::{BindOpts, Binder};
use crab_sql::{Mode, Parser};
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

fn catalog() -> &'static Catalog {
    static C: OnceLock<Catalog> = OnceLock::new();
    C.get_or_init(|| {
        let col = |p: &str, t: &str, o: u32| ColumnSpec {
            path: p.into(),
            data_type: t.into(),
            ordinal: o,
            nullable: true,
            partitioning: false,
            hidden: false,
            clustering: None,
            description: None,
        };
        let spec = |name: &str, columns: Vec<ColumnSpec>| TableSpec {
            dataset: "d".into(),
            name: name.into(),
            kind: TableKind::Table,
            description: None,
            row_count: 0,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns,
        };
        let mut b = CatalogBuilder::new("p");
        b.add_table(spec(
            "t",
            vec![
                col("a", "INT64", 1),
                col("b", "STRING", 2),
                col("c", "DATE", 3),
                col("s", "STRUCT<x INT64, y STRING>", 4),
                col("s.x", "INT64", 4),
                col("s.y", "STRING", 4),
                col("arr", "ARRAY<STRUCT<k STRING, v FLOAT64>>", 5),
                col("arr.k", "STRING", 5),
                col("arr.v", "FLOAT64", 5),
            ],
        ));
        b.add_table(spec("u", vec![col("a", "INT64", 1), col("n", "NUMERIC", 2)]));
        Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
    })
}

fuzz_target!(|data: &[u8]| {
    let Ok(src) = std::str::from_utf8(data) else { return };
    if src.len() > 2048 {
        return;
    }
    let cat = catalog();
    let mut p = Parser::new();
    let Ok(parsed) = p.parse(src, Mode::Full) else { return };
    let mut b = Binder::new();
    for mode in [Mode::Full, Mode::Strict] {
        let bound = b.bind(
            &parsed,
            cat,
            BindOpts { default_project: "p", default_dataset: Some("d"), mode },
        );
        for d in &bound.diags {
            let _ = d.message(src);
        }
        // Lineage never panics, and names exactly the statement's outputs.
        let lin = crab_ir::lineage(&parsed, bound, cat);
        assert_eq!(lin.len() as u32, bound.output.1 - bound.output.0);
    }
    let bound = b.bind(
        &parsed,
        cat,
        BindOpts { default_project: "p", default_dataset: Some("d"), mode: Mode::Strict },
    );
    if !bound.ok() {
        return;
    }
    let fp = fingerprint(&parsed, bound, cat);
    let canon = to_canonical_sql(&parsed, bound, cat);
    let mut p2 = Parser::new();
    let parsed2 = p2
        .parse(&canon, Mode::Full)
        .unwrap_or_else(|e| panic!("canonical SQL does not parse: {e}\n{canon}"));
    let mut b2 = Binder::new();
    let bound2 = b2.bind(
        &parsed2,
        cat,
        BindOpts { default_project: "p", default_dataset: Some("d"), mode: Mode::Strict },
    );
    assert!(
        bound2.ok(),
        "canonical SQL does not bind: {:?}\n{canon}",
        bound2.diags.iter().map(|d| d.message(&canon)).collect::<Vec<_>>()
    );
    assert_eq!(
        fingerprint(&parsed2, bound2, cat),
        fp,
        "fingerprint changed by canonicalisation\n{src}\n{canon}"
    );
    assert_eq!(
        to_canonical_sql(&parsed2, bound2, cat),
        canon,
        "canonical SQL is not a fixed point"
    );
});
