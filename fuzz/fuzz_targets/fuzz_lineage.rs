// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Lineage responses come from a Preview API: any bytes must decode to a
// graph or a typed error, never a panic, and graph queries over whatever
// was decoded must terminate.

#![no_main]

use crab_r::lineage::{dataplex, Column};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(g) = dataplex::parse_response(data, false) else { return };
    let tables = vec!["p.a.b".to_owned(), "p.a.c".to_owned()];
    let _ = g.table_levels(&tables);
    let _ = g.nearest_documented(&Column::new("p.a.c", "x"), |c| {
        (c.column.len() % 3 == 0).then(|| "d".to_owned())
    });
});
