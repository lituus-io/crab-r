// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// The strict-schema rewrite on arbitrary text: a typed error or a complete
// JSON document — never a panic, a hang or unbounded memory — and the
// rewrite of its own output is that output (one pass reaches the fixed point).

#![no_main]

use crab_r::base::json::Scanner;
use crab_r::llm::strict::strict;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else { return };
    let Ok(out) = strict(text) else { return };
    let mut s = Scanner::new(&out);
    while let Some(_) = s.next_event().expect("the rewrite is valid JSON") {}
    assert_eq!(strict(&out).expect("its output rewrites"), out, "idempotent");
});
