// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Grammar ⊆ parser, at depth: the input picks a seed, nesting depth and
// spacing; the derived sentence must parse in strict mode (a sentence the
// grammar allows but the parser rejects would make constrained decoding
// produce SQL the engine cannot use).

#![no_main]

use crab_r::grammar::{Grammar, Table};
use crab_r::sql::{Mode, Parser};
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

fn grammar() -> &'static Grammar {
    static G: OnceLock<Grammar> = OnceLock::new();
    G.get_or_init(|| {
        let tables = [
            Table {
                fqn: "p.shop.orders",
                alias: "o",
                columns: vec!["order_id", "status", "items"],
            },
            Table { fqn: "p.shop.users", alias: "u", columns: vec!["id", "addr.city"] },
        ];
        Grammar::sql(&tables, crab_r::ir::funcs::NAMES)
    })
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 9 {
        return;
    }
    let seed = u64::from_le_bytes(data[..8].try_into().unwrap());
    let depth = 1 + u32::from(data[8] % 5);
    let tight = data[8] & 0x80 != 0;
    let sql = grammar().derive(seed, depth, tight);
    if sql.starts_with("CANNOT_ANSWER:") {
        return;
    }
    let mut p = Parser::new();
    if let Err(e) = p.parse(&sql, Mode::Strict) {
        panic!("grammar sentence rejected: {} :: {sql}", e.msg);
    }
});
