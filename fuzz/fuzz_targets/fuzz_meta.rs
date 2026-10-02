// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Metadata images come from disk and object stores: opening any bytes must
// return a typed error or an image whose every accessor and walk stays in
// bounds and terminates — never a panic.

#![no_main]

use crab_meta::{Bytes, ColumnId, MetaImage, TableId, Walk};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Mutating a valid header gets past the magic far more often.
    let mut bytes = b"CRABMETA\x01\x00\x00\x00".to_vec();
    bytes.extend_from_slice(data);
    for b in [Bytes::from(data.to_vec()), Bytes::from(bytes)] {
        let Ok(img) = MetaImage::from_bytes(b) else { continue };
        let mut w = Walk::new();
        for t in img.tables().take(64) {
            let _ =
                (t.name(), t.kind(), t.description(), t.producer_sql(), t.rows(), t.provenance());
            let _ = img.table(t.name());
            for c in t.columns().take(64) {
                let _ = (
                    c.path(),
                    c.data_type(),
                    c.description(),
                    c.expression(),
                    c.confidence(),
                    c.transform(),
                );
                let _ = img.column(t.name(), c.path());
                let _ = c.parents().count() + c.children().count();
            }
        }
        let n = img.column_count().min(64) as u32;
        for i in 0..n {
            img.upstream(ColumnId(i), 16, &mut w, |_| true);
            img.downstream(ColumnId(i), 16, &mut w, |_| true);
            let _ = img.nearest_described(ColumnId(i), 16, &mut w);
        }
        let _ = img.table_at(TableId(u32::MAX));
        let _ = img.column_by_name("a.b.c.d");
    }
});
