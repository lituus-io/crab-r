// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Catalog images arrive from disk and object storage. With the trailer
// checksum recomputed (so structural corruption reaches validation), any
// input must be rejected with a typed error or, if accepted, every accessor
// must work without panicking.

#![no_main]

use crab_r::catalog::{Catalog, ColId, TableId};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut img = data.to_vec();
    if img.len() >= 72 {
        let n = img.len() - 8;
        let sum = graph_r::hash::xxh3_64(&img[..n]);
        img[n..].copy_from_slice(&sum.to_le_bytes());
    }
    let Ok(c) = Catalog::from_bytes(objstore_r::Bytes::from(img)) else { return };
    for d in c.datasets() {
        let _ = (d.name.len(), d.tables.len());
    }
    for t in c.tables() {
        let _ = c.find_table(t.dataset_name, t.name);
        let _ = c.table_by_key(t.key);
        for col in c.columns(t.id) {
            let _ = (col.leaf(), col.in_array(), col.parent.and_then(|p| c.column(p)));
            let _ = c.find_column(t.id, col.path);
            let _ = c.column_by_key(col.key);
            if let Some(p) = c.profile(col.id) {
                let _ = (p.values().count(), p.null_fraction(), p.min, p.max);
            }
        }
    }
    // The value index is built from whatever profiles the image carries.
    let idx = crab_r::values::ValueIndex::build(&c);
    let _ = idx.ground("How many Shipped orders in Sao Paulo?", 8);
    let _ = c.table(TableId(u32::MAX));
    let _ = c.column(ColId(u32::MAX));
});
