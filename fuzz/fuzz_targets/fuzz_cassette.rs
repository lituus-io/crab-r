// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Cassette files are read from disk or object storage: arbitrary input must
// parse or fail with a typed error, and anything accepted must round-trip.

#![no_main]

use crab_r::llm::replay::Cassette;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else { return };
    if let Ok(c) = Cassette::from_jsonl(text) {
        let again = Cassette::from_jsonl(&c.to_jsonl()).expect("own output parses");
        assert_eq!(again, c);
    }
});
