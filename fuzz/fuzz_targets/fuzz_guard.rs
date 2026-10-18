// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// SQL bodies through the MetadataGuard: whatever the text — comments,
// quoted identifiers, several statements, scripts — the guard never panics,
// and whatever it accepts reads only INFORMATION_SCHEMA views, meta-tables
// and the graph datasets. The invariant is checked by an independent,
// cruder reading: every FROM-like path in an accepted text that is not
// INFORMATION_SCHEMA/__TABLES__ must name `infra` or `ops` as its dataset,
// and the text holds exactly one statement.

#![no_main]

use crab_r::exec::guard::MetadataGuard;
use crab_r::sql::refs::{walk_script, RefKind};
use crab_r::sql::{Mode, Parser};
use libfuzzer_sys::fuzz_target;

/// Fragments the fuzzer splices between its own bytes, so tricks are
/// reached quickly.
const PIECES: [&str; 16] = [
    "SELECT * FROM ",
    "ops.runs",
    "`p.mart.orders`",
    "infra.nodes",
    "`region-us`.INFORMATION_SCHEMA.JOBS",
    " /* ; */ ",
    "; DROP TABLE ops.runs",
    " -- \n",
    "WITH s AS (SELECT 1) ",
    " UNION ALL SELECT * FROM ",
    "EXTERNAL_QUERY('c','q')",
    " WHERE EXISTS (SELECT 1 FROM ",
    ")",
    "`ops`.`runs`",
    "secret.t",
    "r, r.items",
];

fuzz_target!(|data: &[u8]| {
    let mut sql = String::new();
    for chunk in data.chunks(4) {
        match chunk[0] {
            b if b < 16 => sql.push_str(PIECES[b as usize]),
            _ => sql.push_str(&String::from_utf8_lossy(chunk)),
        }
    }
    let guard = MetadataGuard::default();
    if guard.check_sql(&sql).is_err() {
        return;
    }
    // Accepted: re-read independently and check what it reads.
    let mut p = Parser::new();
    let script = p.parse_script(&sql, Mode::Full).expect("accepted text parses");
    assert_eq!(script.stmts.len(), 1, "accepted more than one statement: {sql:?}");
    let refs = walk_script(&script, None);
    assert!(refs.complete);
    for r in &refs.tables {
        match r.kind {
            RefKind::InformationSchema | RefKind::MetaTable => {}
            RefKind::Table => assert!(
                matches!(r.dataset.as_deref(), Some("infra" | "ops")),
                "accepted a read outside the graph datasets: {sql:?} -> {r:?}"
            ),
            RefKind::Path => panic!("accepted an array path: {sql:?}"),
        }
    }
});
