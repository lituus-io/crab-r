// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Questions and SQL literals are user and model text. Grounding and literal
// checks must never panic on any input: the first half of the input is
// indexed as column values, the second half is the question and literal.

#![no_main]

use crab_catalog::{Catalog, CatalogBuilder, ColumnSpec, ProfileSpec, TableKind, TableSpec};
use crab_values::ValueIndex;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else { return };
    let mut mid = text.len() / 2;
    while !text.is_char_boundary(mid) {
        mid -= 1;
    }
    let (values, question) = text.split_at(mid);
    let mut b = CatalogBuilder::new("p");
    b.add_table(TableSpec {
        dataset: "d".into(),
        name: "t".into(),
        kind: TableKind::Table,
        description: None,
        row_count: 1,
        logical_bytes: 0,
        last_modified_ms: 0,
        require_partition_filter: false,
        columns: vec![ColumnSpec {
            path: "c".into(),
            data_type: "STRING".into(),
            ordinal: 1,
            nullable: true,
            partitioning: false,
            hidden: false,
            clustering: None,
            description: None,
        }],
    });
    let vals: Vec<(String, u64)> =
        values.split('\n').take(64).enumerate().map(|(i, v)| (v.to_owned(), i as u64)).collect();
    b.set_profile(
        "d",
        "t",
        "c",
        ProfileSpec {
            rows: 1,
            nulls: 0,
            distinct: vals.len() as u64,
            min: None,
            max: None,
            values: vals,
            values_complete: true,
            sampled: false,
        },
    );
    let Ok(img) = b.build(1, 0) else { return };
    let Ok(c) = Catalog::from_bytes(objstore_r::Bytes::from(img)) else { return };
    let idx = ValueIndex::build(&c);
    let g = idx.ground(question, 8);
    assert!(g.len() <= 8);
    let col = c.find_column(c.find_table("d", "t").unwrap(), "c").unwrap();
    let _ = idx.check(col, question);
});
