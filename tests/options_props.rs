// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Property tests for the describe options: any option vector parses or is
// refused — never a panic; an accepted OpenAI-compatible URL is always a
// chat-completions URL the transport accepts and carries no credentials;
// a key placed where the key or its variable's name or the URL belongs is
// never repeated in a refusal.

#![cfg(feature = "cli")]

use crab_r::describe::app;
use proptest::prelude::*;

/// Every option (`--sql` excluded: it reads a file or stdin).
const FLAGS: [&str; 34] = [
    "--project",
    "--dataset",
    "--table",
    "--target",
    "--default-dataset",
    "--upstream",
    "--region",
    "--jobs-project",
    "--billing-project",
    "--lookback-days",
    "--vertex-project",
    "--vertex-location",
    "--model",
    "--lineage-location",
    "--downstream",
    "--no-profile",
    "--no-audit",
    "--overwrite",
    "--rounds",
    "--concurrency",
    "--budget-usd",
    "--out",
    "--openai-url",
    "--openai-model",
    "--openai-key-env",
    "--openai-stream",
    "--openai-context-tokens",
    "--openai-max-output-tokens",
    "--price-input-usd-per-mtok",
    "--price-output-usd-per-mtok",
    "--openai-key",
    "--apply",
    "--correct",
    "--emit-schema",
];

/// Values worth meeting: URLs good and bad, names, numbers, empties.
const VALUES: [&str; 22] = [
    "https://h/v1",
    "https://h/v1/chat/completions",
    "http://127.0.0.1:8/v1",
    "http://h/v1",
    "https://u:p@h/v1",
    "https://h/v1?k=1",
    "https:///v1",
    "https://",
    "gpt-4o",
    "gpt-4o-mini",
    "llama",
    "OPENAI_API_KEY",
    "lower",
    "0",
    "1",
    "-1",
    "1e9",
    "NaN",
    "",
    " ",
    "p.d",
    "p.d.t",
];

fn token() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => proptest::sample::select(&FLAGS[..]).prop_map(str::to_owned),
        3 => proptest::sample::select(&VALUES[..]).prop_map(str::to_owned),
        1 => "[^\u{0}]{0,24}".prop_filter("not --sql", |s| s != "--sql"),
    ]
}

/// URLs near the rules' edges: either scheme, loopback or not, with or
/// without credentials, port (empty too), path, query and fragment.
fn url() -> impl Strategy<Value = String> {
    (
        proptest::sample::select(&["https://", "http://", "ftp://", ""][..]),
        proptest::option::of("[a-z]{1,6}(:[a-z0-9]{0,6})?@"),
        proptest::sample::select(
            &["h.example", "127.0.0.1", "localhost", "[::1]", "127.0.0.1.h.example", ""][..],
        ),
        proptest::option::of(":[0-9]{0,5}"),
        proptest::sample::select(&["", "/", "/v1", "/v1/", "/v1/chat/completions", "//x"][..]),
        proptest::option::of("[?#][a-z=0-9-]{0,12}"),
    )
        .prop_map(|(scheme, user, host, port, path, tail)| {
            format!(
                "{scheme}{}{host}{}{path}{}",
                user.unwrap_or_default(),
                port.unwrap_or_default(),
                tail.unwrap_or_default()
            )
        })
}

/// The argv of an OpenAI-compatible run, then arbitrary options.
fn openai_argv() -> impl Strategy<Value = Vec<String>> {
    (
        url(),
        proptest::sample::select(&["gpt-4o", "gpt-4.1-nano", "llama", ""][..]),
        proptest::collection::vec(token(), 0..8),
    )
        .prop_map(|(url, model, tail)| {
            let head = ["p.d", "--openai-url", &url, "--openai-model", model].map(str::to_owned);
            head.into_iter().chain(tail).collect()
        })
}

/// The transport's rules hold for an accepted URL.
fn sound(url: &str) -> Result<(), TestCaseError> {
    prop_assert!(url.ends_with("/chat/completions"), "{}", url);
    let loopback = ["http://127.0.0.1:", "http://127.0.0.1/", "http://localhost:", "http://[::1]:"];
    prop_assert!(
        url.starts_with("https://") || loopback.iter().any(|p| url.starts_with(p)),
        "{}",
        url
    );
    let authority = url.split_once("://").map_or("", |(_, r)| r.split('/').next().unwrap_or(""));
    prop_assert!(!authority.is_empty() && !authority.contains('@'), "{}", url);
    prop_assert!(!url.contains(['?', '#', ' ']), "{}", url);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4096))]

    /// Parsing is total, and an accepted model URL keeps the transport's rules.
    #[test]
    fn options_never_panic_and_accepted_urls_are_sound(
        argv in prop_oneof![proptest::collection::vec(token(), 0..14), openai_argv()]
    ) {
        if let Ok(opts) = app::parse(argv) {
            if let Some(o) = opts.openai() {
                sound(o.url())?;
            }
        }
    }

    /// A key given as (or in) `--openai-key`, as `--openai-key-env` or as
    /// `--openai-url` is never repeated: not by the refusal, nor by the
    /// connection that reads the variable.
    #[test]
    fn a_misplaced_key_is_never_echoed(
        argv in openai_argv(),
        at in 0usize..14,
        how in 0usize..4,
        key in "sk-[A-Za-z0-9]{24}",
    ) {
        let mut argv = argv;
        // After the program's own head, at a boundary between options.
        let at = (5 + at).min(argv.len());
        let pair = match how {
            0 => vec!["--openai-key".to_owned(), key.clone()],
            1 => vec![format!("--openai-key={key}")],
            2 => vec!["--openai-key-env".to_owned(), key.clone()],
            _ => vec!["--openai-url".to_owned(), key.clone()],
        };
        // Only where the pair is read as option and value: a preceding
        // option that takes a value would swallow it.
        let swallowed = at > 5
            && argv[at - 1].starts_with("--")
            && !matches!(
                argv[at - 1].as_str(),
                "--downstream" | "--no-profile" | "--no-audit" | "--overwrite" | "--openai-stream"
                    | "--apply" | "--correct"
            );
        argv.splice(at..at, pair);
        match app::parse(argv) {
            Err(e) if !swallowed => prop_assert!(!e.contains(&key), "{}", e),
            Ok(opts) => {
                if let Some(Err(e)) = opts.openai().map(app::OpenAiOptions::connect) {
                    prop_assert!(!e.contains(&key), "{}", e);
                }
            }
            Err(_) => {}
        }
    }
}
