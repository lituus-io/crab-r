// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Grammar ⊆ parser: every random derivation of the schema grammar — in the
// loose (Lark) and tight (GBNF) spacings, queries and expressions nested
// five deep — is accepted by the CrabSQL strict parser, so constrained
// decoding can never produce text the engine rejects on syntax. (This test
// found: `a = NOT b`, `SELECT * EXCEPT` read as the star modifier,
// INTERVAL operands, and `--` from adjacent minus signs in tight spacing.)
// Also: catalog identifiers appear only as declared.

use crab_r::grammar::{Grammar, Table};
use crab_r::sql::{Mode, Parser};

fn grammar() -> Grammar {
    let tables = [
        Table {
            fqn: "p.shop.orders",
            alias: "o",
            columns: vec!["order_id", "status", "created_at", "items"],
        },
        Table { fqn: "p.shop.users", alias: "u", columns: vec!["id", "country", "addr.city"] },
    ];
    Grammar::sql(&tables, crab_r::ir::funcs::NAMES)
}

#[test]
fn every_derivation_parses_in_strict_mode() {
    let g = grammar();
    let mut p = Parser::new();
    let mut failures = Vec::new();
    // Depth 4 broadly, depth 5 sampled (the fuzz target `fuzz_grammar` runs
    // millions of deeper derivations).
    for seed in 0..2_400u64 {
        for tight in [false, true] {
            let depth = if seed % 8 == 0 { 5 } else { 4 };
            let sql = g.derive(seed, depth, tight);
            if sql.starts_with("CANNOT_ANSWER:") {
                continue; // a declared non-answer, handled before parsing
            }
            if let Err(e) = p.parse(&sql, Mode::Strict) {
                failures.push(format!("seed {seed} tight {tight}: {} :: {sql}", e.msg));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures, first: {:#?}",
        failures.len(),
        &failures[..failures.len().min(8)]
    );
}

#[test]
fn derivations_are_deep_and_varied() {
    let g = grammar();
    let all: Vec<String> = (0..8_000).map(|s| g.derive(s, 4, false)).collect();
    let distinct: std::collections::HashSet<&String> = all.iter().collect();
    assert!(distinct.len() > 3_600, "about half are the fixed non-answer: {}", distinct.len());
    for needle in [
        "JOIN",
        "GROUP BY",
        "WITH cte_",
        "UNNEST",
        "CASE",
        "OVER (",
        "o.status",
        "`p.shop.users` AS u",
        "CANNOT_ANSWER:",
    ] {
        assert!(all.iter().any(|s| s.contains(needle)), "no derivation contains {needle}");
    }
}

#[test]
fn catalog_identifiers_only_as_declared() {
    let g = grammar();
    let lark = g.lark();
    assert!(lark.contains("\"o.status\"") && lark.contains("\"u.addr.city\""));
    assert!(lark.contains("\"`p.shop.orders`\" \"AS\" \"o\""));
    assert!(!lark.contains("o.bogus"));
    assert!(lark.starts_with("start: query | \"CANNOT_ANSWER:\" LINE\n"), "{}", &lark[..80]);
    let gbnf = g.gbnf();
    assert!(gbnf.starts_with("root ::= r-query | \"CANNOT_ANSWER:\" ws line\n"), "{}", &gbnf[..80]);
    // Two words never touch without whitespace; punctuation may.
    assert!(gbnf.contains("\"SELECT\" (sp r-distinct)? sp r-select-items"), "{gbnf}");
    assert!(gbnf.contains("r-fname ws \"(\""), "{gbnf}");
    // `- -` must not become `--` (a comment).
    assert!(gbnf.contains("r-unary ::= \"-\" sp r-unary"), "{gbnf}");
}
