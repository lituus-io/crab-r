// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Date parsing and arithmetic must never panic; a parsed date must print back
// to the same literal.

#![no_main]

use crab_r::base::date::{Date, Grain};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else { return };
    if let Ok(d) = Date::parse(text) {
        assert_eq!(d.to_string(), text);
        for g in
            [Grain::Day, Grain::Week, Grain::IsoWeek, Grain::Month, Grain::Quarter, Grain::Year]
        {
            let _ = d.trunc(g);
            let _ = d.add(g, i32::from(data[0]) - 128);
            let _ = d.previous_period(g);
        }
    }
});
