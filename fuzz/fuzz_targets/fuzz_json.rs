// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Arbitrary bytes must produce events or a typed error — never a panic, a
// hang, or unbounded memory. Every string is also force-decoded.

#![no_main]

use crab_r::base::json::{Event, Scanner};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(mut s) = Scanner::from_bytes(data) else { return };
    let mut buf = String::new();
    let mut steps = 0usize;
    while let Ok(Some(ev)) = s.next_event() {
        steps += 1;
        assert!(steps <= data.len() + 1, "scanner must make progress");
        if let Event::Str(v) | Event::Key(v) = ev {
            let _ = v.decode(&mut buf);
        }
    }
});
