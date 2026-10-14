// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Arbitrary option vectors (NUL-separated) parse or are refused, never
// panic; an accepted OpenAI-compatible model URL is a chat-completions URL
// with no credentials, query or fragment.

#![no_main]

use crab_r::describe::app;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else { return };
    let argv: Vec<String> = text.split('\0').map(str::to_owned).collect();
    // `--sql` reads a file or stdin: not the parser's logic.
    if argv.iter().any(|a| a == "--sql") {
        return;
    }
    if let Ok(opts) = app::parse(argv) {
        if let Some(o) = opts.openai() {
            let url = o.url();
            assert!(url.ends_with("/chat/completions"), "{url}");
            assert!(url.starts_with("https://") || url.starts_with("http://"), "{url}");
            assert!(!url.contains(['@', '?', '#']), "{url}");
        }
    }
});
