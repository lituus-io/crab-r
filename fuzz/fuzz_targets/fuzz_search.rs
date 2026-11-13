// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Catalog search responses through search's reader, and search text through
// its URL builder: any bytes give a typed error or an array that is valid
// JSON with at most `limit` entries, never a panic; any text gives a refusal
// or a URL whose path is the one search method and whose query string has
// exactly the three parameters.

#![no_main]

use crab_r::base::json::Scanner;
use crab_r::exec::search::{entries, request_url};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(found) = entries(data, 3, 16) {
        assert!(found.count <= 3);
        let mut s = Scanner::new(&found.json);
        let first = s.expect().expect("entries are JSON");
        s.skip(first).expect("entries are JSON");
        assert!(s.next_event().expect("entries are JSON").is_none());
    }
    let Ok(text) = std::str::from_utf8(data) else { return };
    if let Ok(url) = request_url("https://h", "p", text, 5) {
        let (path, query) = url.split_once('?').expect("a search has a query string");
        assert_eq!(path, "https://h/v1/projects/p/locations/global:searchEntries");
        assert_eq!(query.matches('&').count(), 2, "{query}");
        assert!(query.starts_with("query=") && query.ends_with("&pageSize=1000&orderBy=relevance"));
        assert!(!query.contains(['#', '/', '?', ' ']), "{query}");
    }
});
