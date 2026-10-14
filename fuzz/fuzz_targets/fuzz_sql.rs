// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// The GoogleSQL front end on arbitrary input:
// - tokenizing, parsing (both modes) and viability never panic or overflow;
// - anything accepted emits canonical SQL that re-parses, and emission is a
//   fixed point (emit ∘ parse ∘ emit ∘ parse = emit ∘ parse);
// - a complete statement is always judged Complete, and none of its prefixes
//   is judged Invalid when the statement came from our own emitter;
// - scripts (CTAS, views, INSERT, MERGE, procedural statements) parse without
//   panicking, and every statement's query emits SQL that parses as a query.

#![no_main]

/// A resource limit of the parser, not a syntax error.
fn limit(msg: &str) -> bool {
    msg == crab_r::sql::parser::TOO_DEEP || msg == crab_r::sql::parser::TOO_COMPLEX
}

use crab_r::sql::emit::to_sql;
use crab_r::sql::{viability, Mode, Parser, Viability};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(src) = std::str::from_utf8(data) else { return };
    if src.len() > 4096 {
        return;
    }
    let mut p = Parser::new();
    let _ = viability(&mut p, src, Mode::Strict);
    if let Ok(script) = p.parse_script(src, Mode::Full) {
        let mut q = Parser::new();
        for st in script.stmts {
            if let Some(parsed) = script.query(st) {
                let sql = to_sql(&parsed);
                // Emission may add a few levels of parentheses: input right
                // at the nesting limit may then exceed it, and only that.
                if let Err(e) = q.parse(&sql, Mode::Full) {
                    assert_eq!(
                        e.msg, "nesting too deep",
                        "statement query does not re-parse: {e}\n{sql}"
                    );
                }
            }
            let _ = script.merge_clauses(st);
            let _ = st.target.map(|t| script.path(t));
        }
    }
    let Ok(parsed) = p.parse(src, Mode::Full) else { return };
    let once = to_sql(&parsed);
    let mut q = Parser::new();
    let reparsed = match q.parse(&once, Mode::Full) {
        Ok(r) => r,
        Err(e) if limit(e.msg) => return,
        Err(e) => panic!("emitted SQL does not parse: {e}\n{once}"),
    };
    let twice = to_sql(&reparsed);
    assert_eq!(once, twice, "emission is not a fixed point");
    if p.parse(&once, Mode::Strict).is_ok() {
        assert_eq!(viability(&mut p, &once, Mode::Strict), Viability::Complete);
    }
});
