// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Property tests for the crab-core codecs.

use crab_core::date::{Date, Grain};
use crab_core::json::{escape_into, Event, Scanner};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4096))]

    /// Any string escaped by the writer scans back to exactly itself.
    #[test]
    fn json_string_round_trip(s in any::<String>()) {
        let mut doc = String::new();
        escape_into(&s, &mut doc);
        let mut sc = Scanner::new(&doc);
        let Event::Str(v) = sc.expect().unwrap() else { panic!("not a string") };
        let mut buf = String::new();
        prop_assert_eq!(v.decode(&mut buf).unwrap(), s.as_str());
        prop_assert!(sc.next_event().unwrap().is_none());
    }

    /// The scanner never panics on arbitrary text.
    #[test]
    fn json_scanner_total(s in ".{0,256}") {
        let mut sc = Scanner::new(&s);
        let mut buf = String::new();
        while let Ok(Some(ev)) = sc.next_event() {
            if let Event::Str(v) | Event::Key(v) = ev {
                let _ = v.decode(&mut buf);
            }
        }
    }

    /// Adding and subtracting months or days is consistent with truncation.
    #[test]
    fn date_arithmetic_laws(days in -700_000i32..2_900_000, n in -600i32..600) {
        let d = Date::from_days(days).unwrap();
        if let Ok(e) = d.add_days(n) {
            prop_assert_eq!(e.add_days(-n).unwrap(), d);
        }
        for g in [Grain::Month, Grain::Quarter, Grain::Year, Grain::Week, Grain::IsoWeek] {
            let t = d.trunc(g);
            prop_assert!(t <= d);
            prop_assert_eq!(t.trunc(g), t, "truncation is idempotent");
        }
        let m = d.trunc(Grain::Month);
        if let Ok(x) = m.add_months(n) {
            prop_assert_eq!(x.add_months(-n).unwrap(), m, "month starts round-trip");
        }
    }
}
