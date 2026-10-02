// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Arbitrary bytes, split at arbitrary points, must decode to events or a
// typed error; the same bytes in one chunk or byte-by-byte give the same
// events.

#![no_main]

use crab_llm::sse::SseDecoder;
use libfuzzer_sys::fuzz_target;

fn events(chunks: &mut dyn Iterator<Item = &[u8]>) -> Option<Vec<String>> {
    let mut d = SseDecoder::new();
    let mut out = Vec::new();
    for c in chunks {
        d.push(c).ok()?;
        while let Some(e) = d.next_data().ok()? {
            out.push(e.to_owned());
        }
    }
    Some(out)
}

fuzz_target!(|data: &[u8]| {
    let whole = events(&mut std::iter::once(data));
    let split = events(&mut data.chunks(1));
    if let (Some(a), Some(b)) = (whole, split) {
        assert_eq!(a, b, "chunking must not change the events");
    }
});
