// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Schema files and table resources are read from disk and from the API:
// splicing descriptions into any bytes must return a typed error or valid
// output, never panic; splicing the same updates again is a no-op; and the
// result still parses as JSON.

#![no_main]

use crab_r::base::json::Scanner;
use crab_r::describe::splice::splice;
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

fuzz_target!(|data: &[u8]| {
    let Ok(src) = std::str::from_utf8(data) else { return };
    let mut up = BTreeMap::new();
    for (k, v) in [("a", "A \"quoted\"\n"), ("id", ""), ("rec.x", "nested"), ("b", "\u{1F980}")] {
        up.insert(k.to_owned(), v.to_owned());
    }
    let Ok((out, _)) = splice(src, &up) else { return };
    let (again, _) = splice(&out, &up).expect("splicing its own output");
    assert_eq!(again, out, "splicing twice changed the output");
    let mut s = Scanner::new(&out);
    if let Ok(first) = s.expect() {
        s.skip(first).expect("output is valid JSON");
    }
});
