// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The `crab-describe` command (feature `cli`): option parsing, a full run
//! with its store, publish and apply steps, and the store lookups — shared
//! by the binary and the Python package.
//!
//! `crab-describe`: evidence-driven metadata for BigQuery.
//!
//! ```text
//! crab-describe --dataset my-proj.mart[,my-proj.stg] [--region US]
//!     [--jobs-project my-proj] [--lookback-days 90] [--billing-project my-proj]
//!     [--vertex-project my-proj] [--vertex-location us-central1] [--model gemini-2.5-flash]
//!     [--openai-url https://host/v1 --openai-model M]  # an OpenAI-compatible server instead
//!         [--openai-key-env OPENAI_API_KEY] [--openai-stream]
//!         [--openai-context-tokens 128000] [--openai-max-output-tokens 16384]
//!         [--price-input-usd-per-mtok X --price-output-usd-per-mtok X]
//!     [--lineage-location us]            # add Dataplex lineage (Data Lineage API)
//!     [--downstream]                     # with it: consumers outside the datasets
//!     [--no-profile] [--no-audit] [--overwrite] [--rounds 2] [--concurrency 4]
//!     [--budget-usd 5] [--out report.json] [--emit-schema DIR]
//!     [--store DIR]                      # graph-r store: state + metadata (reuse unchanged tables)
//!     [--publish gs://bucket/prefix]     # publish the store (or to a local dir), fenced
//!     [--apply] [--correct]
//! ```
//!
//! With `--store DIR`, the run's state and metadata live in a graph-r store:
//! tables unchanged since an earlier run — shape, rows, producing SQL,
//! upstream descriptions — are carried over with no profile, model call or
//! check query. `--publish` ships the store through objstore-r's graph-r mirror (only
//! what changed; a second writer is fenced out).
//!
//! Lookups read a store, no network:
//!
//! ```text
//! crab-describe table      DIR project.dataset.table
//! crab-describe lookup     DIR project.dataset.table.column
//! crab-describe upstream   DIR project.dataset.table.column [DEPTH]
//! crab-describe downstream DIR project.dataset.table.column [DEPTH]
//! crab-describe due        DIR [N]
//! crab-describe pull       gs://bucket/prefix DIR   # replica: fetch, or refresh, a published store
//! crab-describe prune      DIR DAYS                 # drop tables not described or ingested in DAYS
//! ```
//!
//! A store that cannot be read (damaged, or written in a format this build
//! does not read) is regenerated, not reported: a run, an ingest or a prune
//! rebuilds it empty (the report's `"regenerated": true`), and a lookup
//! answers `regenerate_needed: nothing recorded ...` with success (see
//! [`crate::describe::lifecycle`]).
//!
//! Credentials: the caller's own token when one is supplied
//! ([`Options::with_credentials`], `access_token=` in the Python package):
//! then no credential is discovered and the metadata server is never asked.
//! Otherwise Application Default Credentials, with the instance metadata
//! server forbidden under `--no-metadata-server`. Nothing is written to the
//! warehouse unless `--apply` is given; then only empty descriptions are
//! filled (flagged corrections too with `--correct`), each table under
//! `If-Match` of the resource it read.
//!
//! With `--openai-url` the model is any OpenAI-compatible chat-completions
//! server instead of Vertex AI; BigQuery and Dataplex stay on ADC. Its bearer
//! key is read from the environment variable `--openai-key-env` names, never
//! from the command line: argv is visible to every local process, and this
//! command echoes argv-derived text in its usage and errors. Requests are
//! unary unless `--openai-stream` is given, because a gateway may refuse
//! streamed structured output while serving it unary (`n` candidates still
//! share one request). `--budget-usd` is enforced from the model's price, so
//! a run with no price for its model refuses to start rather than run with
//! an inert budget.

use crate::describe::describe::{describe, is_virtual, DescribeConfig, Report, Target};
use crate::describe::report::{self, Applied};
use crate::exec::bigquery::{BigQuery, BigQueryConfig};
use crate::exec::{TableMetadata, Warehouse};
use crate::lineage::dataplex::{Dataplex, DataplexConfig};
use crate::lineage::{LineageSource, NoLineage};
use crate::llm::openai::{OpenAi, OpenAiConfig};
use crate::llm::vertex::{ExplicitCache, Vertex, VertexConfig};
use crate::llm::{CacheSupport, Caps, ConstraintSupport, Model, Price};
use objstore_r::http::{Client, HttpConfig};
use objstore_r::token::{Adc, MetadataPolicy, Static};
use std::fmt::Write as _;

/// A caller's access token: never printed, not even by `Debug`.
#[derive(Clone)]
pub struct Secret(String);

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// Where a run's Google credential comes from.
#[derive(Clone, Debug, Default)]
pub struct Credentials {
    token: Option<Secret>,
    metadata: MetadataPolicy,
}

impl Credentials {
    /// The caller's `token` when given (used as is, nothing discovered),
    /// else discovery with the metadata server allowed or not.
    #[must_use]
    pub fn new(token: Option<String>, metadata: MetadataPolicy) -> Self {
        Self { token: token.map(Secret), metadata }
    }

    /// The credential.
    pub fn adc(&self) -> Result<Adc, String> {
        match &self.token {
            Some(Secret(t)) => Adc::from_access_token(t.clone()),
            None => Adc::discover_with(self.metadata),
        }
        .map_err(|e| format!("credentials: {e}"))
    }
}

/// Parsed command-line options of a describe run (see [`USAGE`]).
#[derive(Clone, Debug)]
#[allow(clippy::struct_excessive_bools)] // independent switches
pub struct Options {
    targets: Vec<Target>,
    sql: Option<String>,
    sql_target: Option<String>,
    default_dataset: Option<String>,
    upstream: u32,
    region: String,
    jobs_project: Option<String>,
    billing_project: Option<String>,
    lookback_days: u32,
    vertex_project: Option<String>,
    vertex_location: String,
    model: String,
    lineage_location: Option<String>,
    downstream: bool,
    profile: bool,
    audit: bool,
    overwrite: bool,
    rounds: u8,
    concurrency: usize,
    budget_usd: f64,
    /// Where the report JSON is written (`--out`); `None` leaves it to the
    /// caller.
    pub out: Option<String>,
    store: Option<String>,
    publish: Option<String>,
    emit_schema: Option<String>,
    apply: bool,
    correct: bool,
    /// The OpenAI-compatible model (`--openai-url`); `None` is Vertex AI.
    openai: Option<OpenAiOptions>,
    /// Where the Google credential comes from (never parsed from argv).
    creds: Credentials,
}

/// The variable `--openai-key-env` names when it is not given.
pub const DEFAULT_KEY_ENV: &str = "OPENAI_API_KEY";
/// Context window assumed without `--openai-context-tokens` (a gpt-4o-class
/// model).
pub const DEFAULT_CONTEXT_TOKENS: u32 = 128_000;
/// Output cap per candidate without `--openai-max-output-tokens` (a
/// gpt-4o-class model).
pub const DEFAULT_MAX_OUTPUT_TOKENS: u32 = 16_384;

/// Public list prices, micro-dollars per million tokens (uncached input,
/// cached input, output), for the exact model names whose price is not in
/// doubt. A dated snapshot or a provider-prefixed name is not listed: it may
/// be priced differently, and a wrong price would mis-enforce the budget.
/// Anything else needs both `--price-*` flags.
const LIST_PRICES: [(&str, Price); 5] = [
    ("gpt-4o", Price { input: 2_500_000, cached_input: 1_250_000, output: 10_000_000 }),
    ("gpt-4o-mini", Price { input: 150_000, cached_input: 75_000, output: 600_000 }),
    ("gpt-4.1", Price { input: 2_000_000, cached_input: 500_000, output: 8_000_000 }),
    ("gpt-4.1-mini", Price { input: 400_000, cached_input: 100_000, output: 1_600_000 }),
    ("gpt-4.1-nano", Price { input: 100_000, cached_input: 25_000, output: 400_000 }),
];

/// Where an OpenAI-compatible model's price came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PriceFrom {
    /// `--price-input-usd-per-mtok` / `--price-output-usd-per-mtok`.
    Flags,
    /// The public list price of the named model ([`LIST_PRICES`]).
    ListPrice,
}

/// The OpenAI-compatible model of a run, resolved when the options are
/// parsed — except its key, which is read from its variable only when the
/// run connects, so parsed options (cloned, debug-printed) never hold it.
#[derive(Clone, Debug)]
pub struct OpenAiOptions {
    url: String,
    model: String,
    key_env: String,
    stream: bool,
    context_tokens: u32,
    max_output_tokens: u32,
    price: Price,
    price_from: PriceFrom,
}

impl OpenAiOptions {
    /// The chat-completions URL requests go to.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The environment variable the key is read from.
    #[must_use]
    pub fn key_env(&self) -> &str {
        &self.key_env
    }

    /// Where the price came from.
    #[must_use]
    pub fn price_from(&self) -> PriceFrom {
        self.price_from
    }

    /// The model's capabilities. JSON schema is the only constraint an
    /// OpenAI-compatible API is known to accept; `n` candidates come from
    /// one request, streamed only for a single candidate (the protocol
    /// interleaves streamed candidates, but a server that streams at all is
    /// not thereby known to stream several), and no explicit prefix cache.
    #[must_use]
    pub fn caps(&self) -> Caps {
        Caps {
            id: self.model.clone(),
            constraints: ConstraintSupport { lark: false, gbnf: false, json_schema: true },
            max_candidates: 8,
            max_stream_candidates: 1,
            cache: CacheSupport::None,
            context_tokens: self.context_tokens,
            max_output_tokens: self.max_output_tokens,
            streaming: self.stream,
            price: self.price,
        }
    }

    /// The backend's settings.
    #[must_use]
    pub fn config(&self) -> OpenAiConfig {
        OpenAiConfig {
            url: self.url.clone(),
            model: self.model.clone(),
            caps: self.caps(),
            extra: None,
        }
    }

    /// The model, authorised with the key read from [`key_env`](Self::key_env).
    pub fn connect(&self) -> Result<OpenAi<Client<Static>>, String> {
        self.connect_with(std::env::var_os(&self.key_env))
    }

    /// [`connect`](Self::connect) with the variable's value as read.
    fn connect_with(
        &self,
        value: Option<std::ffi::OsString>,
    ) -> Result<OpenAi<Client<Static>>, String> {
        let key = api_key(&self.key_env, value)?;
        let client = Client::new(Static::new(key), HttpConfig::default())
            .map_err(|e| format!("http: {e}"))?;
        Ok(OpenAi::new(client, self.config()))
    }

    /// The model's host (no port).
    #[must_use]
    pub fn host(&self) -> &str {
        let rest = self.url.split_once("://").map_or(self.url.as_str(), |(_, r)| r);
        let auth = rest.split(['/', '?', '#']).next().unwrap_or("");
        let auth = auth.rsplit_once('@').map_or(auth, |(_, h)| h);
        match auth.rsplit_once(':') {
            Some((h, port)) if port.bytes().all(|b| b.is_ascii_digit()) => h,
            _ => auth,
        }
    }

    /// The model for a drafting run that reaches nothing else: a client
    /// that may send only to the model's own host, never to a Google host.
    pub fn connect_offline(
        &self,
    ) -> Result<OpenAi<Client<Static, crate::describe::offline::ModelOnly<'_>>>, String> {
        let policy = crate::describe::offline::ModelOnly::new(self.host())
            .ok_or("a drafting run's model must not be a Google host")?;
        let key = api_key(&self.key_env, std::env::var_os(&self.key_env))?;
        let client = Client::with_policy(Static::new(key), policy, HttpConfig::default())
            .map_err(|e| format!("http: {e}"))?;
        Ok(OpenAi::new(client, self.config()))
    }

    /// The run-log line naming the model, the host, the request mode and the
    /// price the budget is enforced from (never the key, nor the path).
    #[must_use]
    pub fn summary(&self) -> String {
        let host = self.url.split_once("://").map_or("", |(_, r)| r);
        let host = host.split('/').next().unwrap_or("");
        let usd = |micro: u64| micro as f64 / 1e6;
        let source = match self.price_from {
            PriceFrom::Flags => "given".to_owned(),
            PriceFrom::ListPrice => format!(
                "the public list price of {}; a gateway may bill otherwise (--price-input-usd-per-mtok / --price-output-usd-per-mtok override it)",
                self.model
            ),
        };
        format!(
            "model: {} (OpenAI-compatible) at {host}, {} requests; price ${:.4} input / ${:.4} output per million tokens: {source}",
            self.model,
            if self.stream { "streamed" } else { "unary" },
            usd(self.price.input),
            usd(self.price.output),
        )
    }
}

/// The key from `var`'s value. Only the variable's name ever appears in an
/// error; surrounding whitespace (a trailing newline from a file) is dropped.
fn api_key(var: &str, value: Option<std::ffi::OsString>) -> Result<String, String> {
    let Some(value) = value else {
        return Err(format!("{var} is not set: it must hold the model's API key (--openai-key-env names the variable)"));
    };
    let Ok(value) = value.into_string() else {
        return Err(format!("{var} is not valid UTF-8"));
    };
    let key = value.trim();
    if key.is_empty() {
        return Err(format!("{var} is empty: it must hold the model's API key"));
    }
    if key.chars().any(char::is_control) || key.contains(' ') {
        return Err(format!("{var} holds control characters or spaces: not an API key"));
    }
    Ok(key.to_owned())
}

/// `NAME` is an environment-variable name. Upper case only: a key passed
/// here by mistake (it would then be echoed as a name) is almost always
/// refused by this rule.
fn is_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_uppercase() || c == '_')
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        && name.len() <= 128
}

/// The chat-completions URL for `base`: `/chat/completions` appended unless
/// present. HTTPS, or plain HTTP to loopback (the transport's own rule,
/// applied before anything else runs). Credentials, a query or a fragment in
/// the URL are refused — a key must never travel in it — and no error
/// repeats the URL.
fn chat_url(base: &str) -> Result<String, String> {
    if base.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err("--openai-url: whitespace or control characters".into());
    }
    let rest = base
        .strip_prefix("https://")
        .or_else(|| base.strip_prefix("http://"))
        .ok_or("--openai-url: must be https:// (http:// only to loopback)")?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.contains('@') {
        return Err(
            "--openai-url: credentials in the URL are refused; put the key in a variable named by --openai-key-env"
                .into(),
        );
    }
    if rest.contains(['?', '#']) {
        return Err(
            "--openai-url: a query or fragment is refused (no key travels in the URL)".into()
        );
    }
    if authority.is_empty() || authority.starts_with(':') {
        return Err("--openai-url: no host".into());
    }
    let base = base.trim_end_matches('/');
    let url = if base.ends_with("/chat/completions") {
        base.to_owned()
    } else {
        format!("{base}/chat/completions")
    };
    let loopback = ["http://127.0.0.1:", "http://127.0.0.1/", "http://localhost:", "http://[::1]:"];
    if url.starts_with("http://") && !loopback.iter().any(|p| url.starts_with(p)) {
        return Err("--openai-url: must be https:// (http:// only to loopback)".into());
    }
    Ok(url)
}

/// A `--price-*-usd-per-mtok` value in micro-dollars per million tokens.
fn usd_per_mtok(flag: &str, v: &str) -> Result<u64, String> {
    match v.parse::<f64>() {
        // A million dollars per million tokens is far past any real price;
        // beyond it is a typo, and the product must stay exact in u64.
        Ok(x) if x.is_finite() && (0.0..=1e6).contains(&x) => Ok((x * 1e6).round() as u64),
        _ => Err(format!("{flag}: US dollars per million tokens, 0 to 1000000")),
    }
}

/// The `--openai-*` and `--price-*` options as given, before they are
/// checked together.
#[derive(Default)]
struct OpenAiArgs {
    url: Option<String>,
    model: Option<String>,
    key_env: Option<String>,
    stream: bool,
    context_tokens: Option<u32>,
    max_output_tokens: Option<u32>,
    price_input: Option<u64>,
    price_output: Option<u64>,
}

impl OpenAiArgs {
    /// Check the options together. `vertex` is the first Vertex-only option
    /// given, if any: never silently ignored beside `--openai-url`.
    fn resolve(self, vertex: Option<&str>) -> Result<Option<OpenAiOptions>, String> {
        let Some(base) = self.url else {
            let given = [
                ("--openai-model", self.model.is_some()),
                ("--openai-key-env", self.key_env.is_some()),
                ("--openai-stream", self.stream),
                ("--openai-context-tokens", self.context_tokens.is_some()),
                ("--openai-max-output-tokens", self.max_output_tokens.is_some()),
                ("--price-input-usd-per-mtok", self.price_input.is_some()),
                ("--price-output-usd-per-mtok", self.price_output.is_some()),
            ];
            return match given.iter().find(|(_, g)| *g) {
                Some((flag, _)) => Err(format!(
                    "{flag} goes with --openai-url (the Vertex AI model takes no such option)"
                )),
                None => Ok(None),
            };
        };
        if let Some(flag) = vertex {
            return Err(format!(
                "{flag} configures Vertex AI, which --openai-url replaces (name the model with --openai-model)"
            ));
        }
        let url = chat_url(&base)?;
        let model = self
            .model
            .filter(|m| !m.trim().is_empty())
            .ok_or("--openai-url needs --openai-model (the model as the server names it)")?;
        let key_env = self.key_env.unwrap_or_else(|| DEFAULT_KEY_ENV.into());
        if !is_env_name(&key_env) {
            return Err(
                "--openai-key-env: the NAME of an environment variable (A-Z, 0-9, _), never the key itself"
                    .into(),
            );
        }
        let context_tokens = self.context_tokens.unwrap_or(DEFAULT_CONTEXT_TOKENS);
        let max_output_tokens = self.max_output_tokens.unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS);
        if context_tokens == 0 || max_output_tokens == 0 {
            return Err("--openai-context-tokens and --openai-max-output-tokens: above 0".into());
        }
        // `--budget-usd` is enforced from this price: without one the budget
        // would be inert, so the run refuses rather than guesses. No cached
        // price flag exists, so cached input is charged as uncached — the
        // budget may over-count, never under-count.
        let (price, price_from) = match (self.price_input, self.price_output) {
            (Some(input), Some(output)) => {
                (Price { input, cached_input: input, output }, PriceFrom::Flags)
            }
            (None, None) => match LIST_PRICES.iter().find(|(name, _)| *name == model) {
                Some(&(_, p)) => (p, PriceFrom::ListPrice),
                None => {
                    return Err(format!(
                        "no known price for model {model}: --budget-usd cannot be enforced without --price-input-usd-per-mtok and --price-output-usd-per-mtok"
                    ))
                }
            },
            _ => {
                return Err(
                    "--price-input-usd-per-mtok and --price-output-usd-per-mtok go together".into()
                )
            }
        };
        Ok(Some(OpenAiOptions {
            url,
            model,
            key_env,
            stream: self.stream,
            context_tokens,
            max_output_tokens,
            price,
            price_from,
        }))
    }
}

impl Options {
    /// These options with the run's Google credential from `creds`.
    #[must_use]
    pub fn with_credentials(mut self, creds: Credentials) -> Self {
        self.creds = creds;
        self
    }

    /// The OpenAI-compatible model, when `--openai-url` chose one.
    #[must_use]
    pub fn openai(&self) -> Option<&OpenAiOptions> {
        self.openai.as_ref()
    }
}

/// Usage of a describe run.
pub const USAGE: &str = "usage: crab-describe [PROJECT | PROJECT.DATASET | PROJECT.DATASET.TABLE]... \
[--sql FILE|- [--target PROJECT.DATASET.TABLE] [--default-dataset PROJECT.DATASET]] [--upstream N] [--region US] [--jobs-project P] [--billing-project P] [--lookback-days N] \
[--vertex-project P] [--vertex-location L] [--model M] \
[--openai-url URL --openai-model M [--openai-key-env NAME] [--openai-stream] [--openai-context-tokens N] [--openai-max-output-tokens N] \
[--price-input-usd-per-mtok X --price-output-usd-per-mtok X]] [--lineage-location L] [--downstream] [--no-profile] [--no-audit] [--overwrite] \
[--rounds N] [--concurrency N] [--budget-usd X] [--out FILE] [--store DIR] [--publish gs://B/PREFIX|DIR] [--emit-schema DIR] [--apply] [--correct] \
[--no-metadata-server]";

/// Parse the options of a describe run (the CLI's arguments after the
/// program name).
pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Options, String> {
    let mut a = Options {
        targets: Vec::new(),
        sql: None,
        sql_target: None,
        default_dataset: None,
        upstream: 0,
        region: "US".into(),
        jobs_project: None,
        billing_project: None,
        lookback_days: 90,
        vertex_project: None,
        vertex_location: "us-central1".into(),
        model: "gemini-2.5-flash".into(),
        lineage_location: None,
        downstream: false,
        profile: true,
        audit: true,
        overwrite: false,
        rounds: 2,
        concurrency: 4,
        budget_usd: 5.0,
        out: None,
        store: None,
        publish: None,
        emit_schema: None,
        apply: false,
        correct: false,
        openai: None,
        creds: Credentials::default(),
    };
    let mut oa = OpenAiArgs::default();
    // The first Vertex-only option given (refused beside `--openai-url`).
    let mut vertex: Option<&'static str> = None;
    let mut it = args.into_iter();
    while let Some(flag) = it.next() {
        let mut val = |name: &str| it.next().ok_or_else(|| format!("{name} needs a value"));
        match flag.as_str() {
            "--project" | "--dataset" | "--datasets" | "--table" => a.targets.extend(
                val(&flag)?.split(',').map(str::trim).filter(|s| !s.is_empty()).map(Target::parse),
            ),
            "--sql" => a.sql = Some(val(&flag)?),
            "--target" => a.sql_target = Some(val(&flag)?),
            "--default-dataset" => a.default_dataset = Some(val(&flag)?),
            "--upstream" => a.upstream = val(&flag)?.parse().map_err(|_| "--upstream: a number")?,
            "--region" => a.region = val(&flag)?,
            "--jobs-project" => a.jobs_project = Some(val(&flag)?),
            "--billing-project" => a.billing_project = Some(val(&flag)?),
            "--lookback-days" => {
                a.lookback_days = val(&flag)?.parse().map_err(|_| "--lookback-days: a number")?;
            }
            "--vertex-project" => {
                a.vertex_project = Some(val(&flag)?);
                vertex = vertex.or(Some("--vertex-project"));
            }
            "--vertex-location" => {
                a.vertex_location = val(&flag)?;
                vertex = vertex.or(Some("--vertex-location"));
            }
            "--model" => {
                a.model = val(&flag)?;
                vertex = vertex.or(Some("--model"));
            }
            "--openai-url" => oa.url = Some(val(&flag)?),
            "--openai-model" => oa.model = Some(val(&flag)?),
            "--openai-key-env" => oa.key_env = Some(val(&flag)?),
            "--openai-stream" => oa.stream = true,
            "--openai-context-tokens" => {
                oa.context_tokens =
                    Some(val(&flag)?.parse().map_err(|_| "--openai-context-tokens: a number")?);
            }
            "--openai-max-output-tokens" => {
                oa.max_output_tokens =
                    Some(val(&flag)?.parse().map_err(|_| "--openai-max-output-tokens: a number")?);
            }
            "--price-input-usd-per-mtok" => {
                oa.price_input = Some(usd_per_mtok(&flag, &val(&flag)?)?);
            }
            "--price-output-usd-per-mtok" => {
                oa.price_output = Some(usd_per_mtok(&flag, &val(&flag)?)?);
            }
            // The key itself is never an option: argv is visible to every
            // local process. Refused without repeating the argument.
            key if key == "--openai-key" || key.starts_with("--openai-key=") => {
                return Err("the API key is never taken on the command line: put it in an environment variable and name that with --openai-key-env".into());
            }
            "--lineage-location" => a.lineage_location = Some(val(&flag)?),
            "--downstream" => a.downstream = true,
            "--no-profile" => a.profile = false,
            "--no-audit" => a.audit = false,
            "--overwrite" => a.overwrite = true,
            "--rounds" => a.rounds = val(&flag)?.parse().map_err(|_| "--rounds: a number")?,
            "--concurrency" => {
                a.concurrency = val(&flag)?.parse().map_err(|_| "--concurrency: a number")?;
            }
            "--budget-usd" => {
                a.budget_usd = val(&flag)?.parse().map_err(|_| "--budget-usd: a number")?;
            }
            "--out" => a.out = Some(val(&flag)?),
            "--store" => a.store = Some(val(&flag)?),
            "--publish" => a.publish = Some(val(&flag)?),
            "--emit-schema" => a.emit_schema = Some(val(&flag)?),
            "--apply" => a.apply = true,
            "--no-metadata-server" => a.creds.metadata = MetadataPolicy::Forbid,
            "--correct" => a.correct = true,
            "-h" | "--help" => return Err(USAGE.into()),
            other if other.starts_with('-') && other != "-" => {
                return Err(format!("unknown argument {other}\n{USAGE}"))
            }
            name => a.targets.push(Target::parse(name)),
        }
    }
    // Before `--sql` is read: a refused option fails before stdin is taken.
    a.openai = oa.resolve(vertex)?;
    if let Some(src) = a.sql.take() {
        let sql = if src == "-" {
            let mut s = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut s)
                .map_err(|e| format!("--sql -: {e}"))?;
            s
        } else {
            std::fs::read_to_string(&src).map_err(|e| format!("--sql {src}: {e}"))?
        };
        a.targets.push(Target::Sql {
            sql,
            target: a.sql_target.take(),
            default_dataset: a.default_dataset.take(),
        });
    } else if a.sql_target.is_some() || a.default_dataset.is_some() {
        return Err(format!("--target and --default-dataset go with --sql\n{USAGE}"));
    }
    if a.targets.is_empty() {
        return Err(format!("name a project, dataset or table, or give --sql\n{USAGE}"));
    }
    if a.targets.iter().any(|t| matches!(t, Target::Project(p) if p.is_empty())) {
        return Err(format!("an empty name is not a project\n{USAGE}"));
    }
    Ok(a)
}

/// Publish the store at `dir` to `gs://bucket/prefix` (ADC) or a local
/// directory through objstore-r's graph-r mirror: only what changed is uploaded, and a
/// second writer is fenced out.
async fn publish(
    creds: &Credentials,
    dest: &str,
    store: &graph_r::Store,
) -> Result<objstore_r::mirror::Published, String> {
    async fn ship<O: objstore_r::mirror::ObjectStore>(
        objects: &O,
        prefix: &str,
        store: &graph_r::Store,
    ) -> Result<objstore_r::mirror::Published, String> {
        let m = objstore_r::mirror::Mirror::new(objects, prefix).map_err(|e| e.to_string())?;
        m.publish(store).await.map_err(|e| e.to_string())
    }
    if let Some(rest) = dest.strip_prefix("gs://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let gcs = objstore_r::Gcs::new(bucket, creds.adc()?, objstore_r::GcsConfig::default())
            .map_err(|e| format!("{dest}: {e}"))?;
        ship(&gcs, prefix, store).await
    } else {
        let objects = objstore_r::LocalDir::open(dest).map_err(|e| format!("{dest}: {e}"))?;
        ship(&objects, "", store).await
    }
}

/// Fetch a published store into `dir` (first time), or bring it up to date
/// with only what changed since; a `gs://` source is read with `creds`.
pub async fn pull(src: &str, dir: &str, creds: &Credentials) -> Result<String, String> {
    async fn sync<O: objstore_r::mirror::ObjectStore>(
        objects: &O,
        prefix: &str,
        dir: &str,
    ) -> Result<String, String> {
        let m = objstore_r::mirror::Mirror::new(objects, prefix).map_err(|e| e.to_string())?;
        let path = std::path::Path::new(dir);
        let local = if path.join(graph_r::store::BASE_FILE).exists() {
            crate::describe::lifecycle::open_for_lookup(path).map_err(|e| e.to_string())?
        } else {
            None
        };
        if let Some(store) = local {
            let before = store.generation();
            let changed = m.refresh_into(&store).await.map_err(|e| e.to_string())?;
            Ok(format!(
                "refreshed {dir}: {} (generation {before} -> {})",
                if changed { "updated" } else { "unchanged" },
                store.generation()
            ))
        } else {
            // First time, or a damaged replica: a full pull replaces both
            // data files (never through a link).
            let damaged = path.join(graph_r::store::BASE_FILE).exists();
            if damaged {
                crate::describe::lifecycle::refuse_links(path).map_err(|e| e.to_string())?;
            }
            match m.pull_into(path).await.map_err(|e| e.to_string())? {
                Some(p) => Ok(format!(
                    "{} generation {} into {dir} ({} bytes)",
                    if damaged { "regenerated: pulled" } else { "pulled" },
                    p.generation,
                    p.bytes
                )),
                None => Err("nothing published there".into()),
            }
        }
    }
    // A mirror sync's state (manifest, segments, a resumable transfer) runs
    // to tens of kilobytes: it lives on the heap for the one pull a command
    // makes, not on the caller's stack.
    if let Some(rest) = src.strip_prefix("gs://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let gcs = objstore_r::Gcs::new(bucket, creds.adc()?, objstore_r::GcsConfig::default())
            .map_err(|e| format!("{src}: {e}"))?;
        Box::pin(sync(&gcs, prefix, dir)).await
    } else {
        let objects = objstore_r::LocalDir::open(src).map_err(|e| format!("{src}: {e}"))?;
        Box::pin(sync(&objects, "", dir)).await
    }
}

/// The lookup subcommands.
pub const LOOKUPS: [&str; 6] = ["table", "lookup", "upstream", "downstream", "due", "search"];

/// The last line of an answer cut to its token budget.
pub const TRUNCATED: &str = "truncated";

/// A lookup subcommand over the store in `args[0]`: its text, or why
/// not. Reads only the store, no network.
pub fn lookup(cmd: &str, args: &[String]) -> Result<String, String> {
    lookup_within(cmd, args, None)
}

/// [`lookup`] answering within `budget` tokens (graph-r's
/// [`graph_r::TokenBudget`], about four bytes a token): whole lines, nearest
/// first, then one `truncated: N more lines` line when some did not fit.
/// `search STORE TEXT [N]` is graph-r's own ranked, budgeted query over the
/// store's names and descriptions.
pub fn lookup_within(
    cmd: &str,
    args: &[String],
    budget: Option<graph_r::TokenBudget>,
) -> Result<String, String> {
    let full = lookup_all(cmd, args, budget)?;
    Ok(match budget {
        Some(b) => within(&full, b),
        None => full,
    })
}

/// `text` cut, at a line, to `budget` tokens.
fn within(text: &str, budget: graph_r::TokenBudget) -> String {
    let mut out = String::new();
    let mut spent = 0u64;
    let lines: Vec<&str> = text.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        let cost = graph_r::text::estimate_tokens(line.len() + 1);
        if spent + cost > u64::from(budget.0) {
            let _ = writeln!(out, "{TRUNCATED}: {} more lines", lines.len() - i);
            return out;
        }
        spent += cost;
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn lookup_all(
    cmd: &str,
    args: &[String],
    budget: Option<graph_r::TokenBudget>,
) -> Result<String, String> {
    use crate::describe::state::attr;
    use graph_r::{Direction, EdgeType, UrlKey};
    let mut out = String::new();
    let (Some(dir), name) = (args.first(), args.get(1).map_or("", String::as_str)) else {
        return Err(format!("usage: crab-describe {cmd} STORE_DIR NAME"));
    };
    // A damaged store answers as one with nothing recorded, flagged.
    let Some(store) =
        crate::describe::lifecycle::open_for_lookup(dir).map_err(|e| format!("{dir}: {e}"))?
    else {
        return Ok(crate::describe::lifecycle::nothing_recorded(dir));
    };
    let snap = store.snapshot();
    let mut uri = crate::base::key::UriBuf::new();
    // A `bq://` name, in either of its forms, is read by the one parser;
    // a dotted one keeps the right-to-left split, which also takes the
    // domain-scoped projects (`example.com:p`) the parser refuses.
    let key_of = |uri: &mut crate::base::key::UriBuf, name: &str, column: bool| -> Option<UrlKey> {
        use crate::base::key::{BqName, Level, BQ_SCHEME};
        if name.starts_with(BQ_SCHEME) {
            let n = BqName::parse(name).ok()?;
            let want = if column { Level::Column } else { Level::Table };
            return if n.level() == want { n.uri(uri).map(UrlKey::of) } else { None };
        }
        if column {
            let (t, c) = name.rsplit_once('.')?;
            let (p, d, t) = crate::base::key::split_fqn(t)?;
            Some(UrlKey::of(uri.column(p, d, t, c)))
        } else {
            let (p, d, t) = crate::base::key::split_fqn(name)?;
            Some(UrlKey::of(uri.table(p, d, t)))
        }
    };
    let label = |k: UrlKey| {
        snap.node(k).map_or_else(
            || format!("{:016x}", k.0),
            |n| n.url.trim_start_matches(crate::describe::state::ROOT).to_owned(),
        )
    };
    let line = |k: UrlKey| {
        let n = snap.node(k);
        let d = n.and_then(|n| n.snippet).unwrap_or("");
        // What a describe did, else that a lineage export declared it.
        let prov = snap
            .attr(k, attr::ACTION)
            .or_else(|| snap.attr(k, attr::STATIC).map(|_| "static"))
            .unwrap_or("");
        format!(
            "{}{}{}",
            label(k),
            if prov.is_empty() { String::new() } else { format!(" [{prov}]") },
            if d.is_empty() { String::new() } else { format!(" — {d}") }
        )
    };
    let depth: u32 = args.get(2).and_then(|d| d.parse().ok()).unwrap_or(10);
    match cmd {
        "search" => {
            let opts = graph_r::QueryOpts {
                limit: args.get(2).and_then(|n| n.parse().ok()).unwrap_or(20),
                budget: budget.unwrap_or_default(),
                ..graph_r::QueryOpts::default()
            };
            for h in snap.query(name, &opts) {
                writeln!(out, "{}", line(h.key)).unwrap_or_default();
            }
        }
        "due" => {
            let n: usize = args.get(1).and_then(|d| d.parse().ok()).unwrap_or(20);
            for d in
                snap.due(graph_r::fsio::now_ms(), n).into_iter().filter(|d| !d.url.contains('#'))
            {
                writeln!(
                    out,
                    "{} (overdue {} s)",
                    d.url.trim_start_matches(crate::describe::state::ROOT),
                    d.overdue_ms / 1000
                )
                .unwrap_or_default();
            }
        }
        "table" => {
            let Some(k) = key_of(&mut uri, name, false).filter(|k| snap.node(*k).is_some()) else {
                return Err(format!("no table {name}"));
            };
            writeln!(out, "{}", line(k)).unwrap_or_default();
            for (a, what) in
                [(attr::TYPE, "kind"), (attr::STATEMENT, "produced by"), (attr::ROWS, "rows")]
            {
                if let Some(v) = snap.attr(k, a) {
                    writeln!(out, "  {what}: {v}").unwrap_or_default();
                }
            }
            let mut it = snap.neighbors(k);
            while let Some(e) = graph_r::LendingIterator::next(&mut it) {
                match e.etype {
                    EdgeType::Contains => {
                        writeln!(out, "  {}", line(e.dst_key)).unwrap_or_default();
                    }
                    EdgeType::Conditions => {
                        writeln!(out, "  rows decided by {}", label(e.dst_key)).unwrap_or_default();
                    }
                    EdgeType::Derives => {
                        writeln!(out, "  from {}", label(e.dst_key)).unwrap_or_default();
                    }
                    _ => {}
                }
            }
        }
        "lookup" => {
            let Some(k) = key_of(&mut uri, name, true).filter(|k| snap.node(*k).is_some()) else {
                return Err(format!("no column {name}"));
            };
            writeln!(out, "{}", line(k)).unwrap_or_default();
            for (a, what) in [
                (attr::TYPE, "type"),
                (attr::CONFIDENCE, "confidence"),
                (attr::TRANSFORM, "transform"),
                (attr::EXPRESSION, "expression"),
            ] {
                if let Some(v) = snap.attr(k, a).filter(|v| !v.is_empty()) {
                    writeln!(out, "  {what}: {v}").unwrap_or_default();
                }
            }
            let mut it = snap.neighbors(k);
            while let Some(e) = graph_r::LendingIterator::next(&mut it) {
                let exact = e.flags & graph_r::format::base::eflags::EXACT != 0;
                writeln!(
                    out,
                    "  from {}{}",
                    label(e.dst_key),
                    if exact { " (exact copy)" } else { "" }
                )
                .unwrap_or_default();
            }
            snap.in_edges(k, |src, t, _, _| match t {
                EdgeType::Derives => writeln!(out, "  feeds {}", label(src)).unwrap_or_default(),
                EdgeType::Conditions => {
                    writeln!(out, "  decides which rows {} holds", label(src)).unwrap_or_default();
                }
                _ => {}
            });
        }
        _ => {
            // A column, else a table: a table's lineage is the tables it
            // reads, as a lineage export declares them.
            let Some(k) = key_of(&mut uri, name, true)
                .filter(|k| snap.node(*k).is_some())
                .or_else(|| key_of(&mut uri, name, false).filter(|k| snap.node(*k).is_some()))
            else {
                return Err(format!("no column or table {name}"));
            };
            let (dir, follow): (Direction, fn(EdgeType) -> bool) = if cmd == "upstream" {
                (Direction::Out, |t| t == EdgeType::Derives)
            } else {
                (Direction::In, |t| matches!(t, EdgeType::Derives | EdgeType::Conditions))
            };
            writeln!(out, "{}", line(k)).unwrap_or_default();
            snap.walk(k, dir, depth, follow, &mut graph_r::Walk::new(), |h| {
                let how = match (h.etype, h.exact_chain) {
                    (EdgeType::Conditions, _) => "  (rows)",
                    (_, true) => "  (exact copy)",
                    _ => "",
                };
                writeln!(out, "{}{}{how}", "  ".repeat(h.depth as usize), line(h.key))
                    .unwrap_or_default();
                // A table reached by its rows is reported, not walked through.
                h.etype != EdgeType::Conditions
            });
        }
    }
    Ok(out)
}

/// `prune STORE_DIR DAYS`: remove the tables no describe or ingest has
/// written or touched in `DAYS` days, with their columns; one line saying
/// what went.
pub fn prune(args: &[String]) -> Result<String, String> {
    let (Some(dir), Some(days)) = (args.first(), args.get(1).and_then(|d| d.parse().ok())) else {
        return Err("usage: crab-describe prune STORE_DIR DAYS".into());
    };
    use crate::describe::lifecycle::Regen;
    let (p, regen) =
        crate::describe::lifecycle::prune(dir, days).map_err(|e| format!("{dir}: {e}"))?;
    Ok(match regen {
        Regen::Done => format!("{dir} could not be read and was regenerated; nothing to prune\n"),
        Regen::Pending => format!(
            "{dir} could not be read and another process holds it: regenerate_pending, nothing pruned\n"
        ),
        Regen::No => format!("pruned {} tables, {} columns from {dir}\n", p.tables, p.columns),
    })
}

/// Tables a drafting run takes at once.
pub const MAX_OFFLINE_TABLES: usize = 256;

/// Draft descriptions from `catalog_json` (table metadata the caller read:
/// [`crate::exec::table_meta`] answers or table resources) with no Google
/// client at all: the model must be OpenAI-compatible (`--openai-url`) and
/// its client reaches only its own host. `--store` is reused (unchanged
/// tables are not drafted again, the store's lineage and descriptions are
/// inherited) and the drafts are recorded in it; `--out` and
/// `--emit-schema` are written. Nothing is written to the warehouse:
/// `--apply`, `--publish` and `--lineage-location` are refused, since each
/// reaches Google.
pub async fn run_offline(
    a: &Options,
    catalog_json: &str,
    log: &mut impl FnMut(&str),
) -> Result<Report, String> {
    if a.apply || a.publish.is_some() || a.lineage_location.is_some() {
        return Err(
            "an offline run reaches no Google service: --apply, --publish and --lineage-location are refused"
                .into(),
        );
    }
    let o = a.openai.as_ref().ok_or(
        "an offline run needs an OpenAI-compatible model (--openai-url): Vertex AI is a Google host",
    )?;
    let model = o.connect_offline()?;
    log(&o.summary());
    let supplied = crate::describe::offline::parse_catalog(catalog_json, MAX_OFFLINE_TABLES)?;
    let mut regenerated = false;
    let store = match &a.store {
        Some(dir) => {
            let opened = crate::describe::lifecycle::open_or_regenerate(dir)
                .map_err(|e| format!("{dir}: {e}"))?;
            regenerated = opened.regenerated();
            if opened.pending() {
                log(&format!(
                    "warning: store {dir} needs regenerating and another process holds it \
                     (regenerate_pending); this run records nothing in it"
                ));
            }
            opened.store()
        }
        None => None,
    };
    let mut cfg = DescribeConfig::new(Vec::new(), &a.region, "");
    cfg.audit_existing = a.audit;
    cfg.overwrite = a.overwrite;
    cfg.max_rounds = a.rounds;
    cfg.concurrency = a.concurrency;
    cfg.budget =
        crate::base::Budget::new(50_000_000, 0, (a.budget_usd * 1e6) as u64, 6 * 3_600_000);
    if let Some(s) = &store {
        cfg.prior = crate::describe::state::load(s);
    }
    let mut report =
        crate::describe::offline::describe_offline(&model, &supplied, store.as_ref(), &cfg).await;
    report.regenerated = regenerated;
    if let Some(s) = &store {
        crate::describe::state::save(s, &report, graph_r::fsio::now_ms())
            .map_err(|e| format!("store: {e}"))?;
    }
    if let Some(p) = &a.out {
        let json = report::to_json(&report);
        graph_r::fsio::write_atomic(std::path::Path::new(p), &[json.as_bytes()])
            .map_err(|e| format!("{p}: {e}"))?;
    }
    if let Some(dir) = &a.emit_schema {
        std::fs::create_dir_all(dir).map_err(|e| format!("{dir}: {e}"))?;
        for t in &report.tables {
            if let Some(s) = report::schema_file(t, a.correct) {
                let path = format!("{dir}/{}.schema.json", t.table);
                graph_r::fsio::write_atomic(std::path::Path::new(&path), &[s.as_bytes()])
                    .map_err(|e| format!("{path}: {e}"))?;
            }
        }
    }
    Ok(report)
}

/// [`describe`] with the lineage source when there is one.
async fn describe_with<M: Model, W: Warehouse + TableMetadata, L: LineageSource>(
    model: &M,
    w: &W,
    lineage: Option<&L>,
    cfg: &DescribeConfig,
) -> Report {
    match lineage {
        Some(l) => describe(model, w, l, cfg).await,
        None => describe(model, w, &NoLineage, cfg).await,
    }
}

/// Run a describe: discover, describe, then what the options ask (store,
/// publish, output file, schema files, apply). Progress lines go to `log`.
#[allow(clippy::too_many_lines)] // the run's stages, in order
pub async fn run(a: &Options, log: &mut impl FnMut(&str)) -> Result<Report, String> {
    // The model's key first: a missing variable fails before any credential
    // lookup or warehouse call.
    let openai = match &a.openai {
        Some(o) => {
            let model = o.connect()?;
            log(&o.summary());
            Some(model)
        }
        None => None,
    };
    let home = a
        .targets
        .iter()
        .find_map(Target::project)
        .map(str::to_owned)
        .or_else(|| a.billing_project.clone())
        .ok_or("a query with no table names needs --default-dataset or --billing-project")?;
    // Queries are billed to (and metadata read through) the billing project;
    // job history is read from the jobs project, which may differ.
    let billing = a.billing_project.clone().unwrap_or_else(|| home.clone());
    let jobs_project = a.jobs_project.clone().unwrap_or_else(|| home.clone());
    let http =
        || Client::new(a.creds.adc()?, HttpConfig::default()).map_err(|e| format!("http: {e}"));
    let bq = BigQuery::new(
        http()?,
        &BigQueryConfig { project: billing.clone(), location: a.region.clone(), endpoint: None },
    );
    let mut cfg = DescribeConfig::new(a.targets.clone(), &a.region, &jobs_project);
    cfg.upstream = a.upstream;
    cfg.jobs.lookback_days = a.lookback_days;
    cfg.profile = a.profile;
    cfg.audit_existing = a.audit;
    cfg.overwrite = a.overwrite;
    cfg.max_rounds = a.rounds;
    cfg.downstream = a.downstream;
    cfg.concurrency = a.concurrency;
    cfg.budget =
        crate::base::Budget::new(50_000_000, 200 << 30, (a.budget_usd * 1e6) as u64, 6 * 3_600_000);
    let mut regenerated = false;
    let store = match &a.store {
        Some(dir) => {
            let opened = crate::describe::lifecycle::open_or_regenerate(dir)
                .map_err(|e| format!("{dir}: {e}"))?;
            regenerated = opened.regenerated();
            if regenerated {
                log(&format!("warning: store {dir} could not be read; regenerated empty"));
            }
            if opened.pending() {
                log(&format!(
                    "warning: store {dir} needs regenerating and another process holds it \
                     (regenerate_pending); this run records nothing in it"
                ));
            }
            opened.store()
        }
        None => None,
    };
    if let Some(s) = &store {
        cfg.prior = crate::describe::state::load(s);
        log(&format!("store: {} tables recorded for reuse", cfg.prior.len()));
    }
    let dp = match &a.lineage_location {
        Some(loc) => Some(Dataplex::new(
            http()?,
            DataplexConfig {
                project: billing.clone(),
                location: loc.clone(),
                locations: vec![],
                exact_copy: true,
                endpoint: None,
            },
        )),
        None => None,
    };
    // One monomorphised describe per model: no boxing, no dynamic dispatch.
    let report = if let Some(model) = &openai {
        describe_with(model, &bq, dp.as_ref(), &cfg).await
    } else {
        let vp = a.vertex_project.clone().unwrap_or_else(|| billing.clone());
        let model = Vertex::new(
            http()?,
            VertexConfig {
                project: vp,
                location: a.vertex_location.clone(),
                model: a.model.clone(),
                caps: Caps {
                    id: a.model.clone(),
                    constraints: ConstraintSupport { lark: false, gbnf: false, json_schema: true },
                    max_candidates: 8,
                    max_stream_candidates: 1,
                    cache: CacheSupport::Explicit,
                    context_tokens: 1_048_576,
                    max_output_tokens: 8_192,
                    streaming: true,
                    price: Price { input: 300_000, cached_input: 30_000, output: 2_500_000 },
                },
                thinking_budget: Some(0),
                endpoint: None,
                cache: Some(ExplicitCache::default()),
            },
        );
        describe_with(&model, &bq, dp.as_ref(), &cfg).await
    };
    let mut report = report;
    report.regenerated = regenerated;

    let json = report::to_json(&report);
    for w in &report.warnings {
        log(&format!("warning: {w}"));
    }
    if let Some(p) = &a.out {
        graph_r::fsio::write_atomic(std::path::Path::new(p), &[json.as_bytes()])
            .map_err(|e| format!("{p}: {e}"))?;
    }
    if let Some(s) = &store {
        // Tables of a dataset this run could not read keep what earlier
        // runs recorded: only covered tables are written.
        crate::describe::state::save(s, &report, graph_r::fsio::now_ms())
            .map_err(|e| format!("store: {e}"))?;
        if !report.unreadable.is_empty() {
            log(&format!("store kept as it was for {}", report.unreadable.join(", ")));
        }
        if let Some(dest) = &a.publish {
            let p = publish(&a.creds, dest, s).await?;
            log(&format!(
                "published {dest}: generation {}, {} bytes uploaded",
                p.generation, p.uploaded
            ));
        }
    } else if a.publish.is_some() {
        return Err("--publish needs --store".into());
    }
    if let Some(dir) = &a.emit_schema {
        std::fs::create_dir_all(dir).map_err(|e| format!("{dir}: {e}"))?;
        for t in &report.tables {
            if let Some(s) = report::schema_file(t, a.correct) {
                let path = format!("{dir}/{}.schema.json", t.table);
                graph_r::fsio::write_atomic(std::path::Path::new(&path), &[s.as_bytes()])
                    .map_err(|e| format!("{path}: {e}"))?;
            }
        }
    }
    // Summary on stderr.
    let (mut gen, mut prop, mut flag, mut confirm) = (0, 0, 0, 0);
    for t in &report.tables {
        for c in &t.columns {
            match c.action {
                crate::describe::describe::Action::Generate => gen += 1,
                crate::describe::describe::Action::Propagate => prop += 1,
                crate::describe::describe::Action::Flag => flag += 1,
                crate::describe::describe::Action::Confirm => confirm += 1,
                _ => {}
            }
        }
    }
    let reused = report.tables.iter().filter(|t| t.reused).count();
    log(&format!(
        "crab-describe: {} tables in {} levels ({reused} unchanged, reused); columns generated {gen}, propagated {prop}, confirmed {confirm}, flagged {flag}; ${:.4}, {} MiB billed, {} warnings",
        report.tables.len(),
        report.levels.len(),
        report.spend.micro_usd as f64 / 1e6,
        report.spend.warehouse_bytes >> 20,
        report.warnings.len() + report.tables.iter().map(|t| t.warnings.len()).sum::<usize>()
    ));
    if a.apply {
        // A query's result is no table: there is nothing to write back.
        for t in report.tables.iter().filter(|t| !is_virtual(&t.table)) {
            match report::apply(&bq, t, a.correct).await {
                Applied::Unchanged => {}
                Applied::Written { columns, table } => log(&format!(
                    "applied {}: {columns} columns{}",
                    t.table,
                    if table { " + table description" } else { "" }
                )),
                Applied::Conflict => {
                    log(&format!("skipped {}: changed while applying; re-run describe", t.table));
                }
                Applied::Failed(e) => log(&format!("failed {}: {e}", t.table)),
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_of(flags: &[&str]) -> Result<Options, String> {
        parse(std::iter::once("p.d").chain(flags.iter().copied()).map(str::to_owned))
    }

    fn openai_of(flags: &[&str]) -> OpenAiOptions {
        parse_of(flags).unwrap().openai.expect("an OpenAI-compatible model")
    }

    const BASE: [&str; 4] = ["--openai-url", "https://gw.example/v1", "--openai-model", "gpt-4o"];

    #[test]
    fn without_openai_url_the_model_is_vertex() {
        let o = parse_of(&["--model", "gemini-2.5-pro", "--vertex-location", "europe-west4"]);
        assert!(o.unwrap().openai.is_none());
    }

    #[test]
    fn every_openai_option_is_parsed() {
        let o = openai_of(&[
            "--openai-url",
            "https://gw.example/v1",
            "--openai-model",
            "local-model",
            "--openai-key-env",
            "GATEWAY_KEY",
            "--openai-stream",
            "--openai-context-tokens",
            "32000",
            "--openai-max-output-tokens",
            "2048",
            "--price-input-usd-per-mtok",
            "0.5",
            "--price-output-usd-per-mtok",
            "1.25",
        ]);
        assert_eq!(o.url(), "https://gw.example/v1/chat/completions");
        assert_eq!(o.key_env(), "GATEWAY_KEY");
        assert_eq!(o.price_from(), PriceFrom::Flags);
        let c = o.caps();
        assert_eq!(c.id, "local-model");
        assert!(c.streaming);
        assert_eq!((c.context_tokens, c.max_output_tokens), (32_000, 2_048));
        assert_eq!(c.price, Price { input: 500_000, cached_input: 500_000, output: 1_250_000 });
        assert_eq!(o.config().model, "local-model");
    }

    #[test]
    fn defaults_are_unary_with_stated_limits() {
        let o = openai_of(&BASE);
        assert_eq!(o.key_env(), DEFAULT_KEY_ENV);
        let c = o.caps();
        assert!(!c.streaming, "unary unless --openai-stream");
        assert_eq!((c.context_tokens, c.max_output_tokens), (128_000, 16_384));
        assert_eq!(
            c.constraints,
            ConstraintSupport { lark: false, gbnf: false, json_schema: true }
        );
        assert_eq!((c.max_candidates, c.max_stream_candidates), (8, 1));
        assert_eq!(c.cache, CacheSupport::None);
        assert_eq!(o.config().extra, None);
    }

    #[test]
    fn chat_completions_is_appended_once() {
        for (base, url) in [
            ("https://h/v1", "https://h/v1/chat/completions"),
            ("https://h/v1/", "https://h/v1/chat/completions"),
            ("https://h/v1/chat/completions", "https://h/v1/chat/completions"),
            ("https://h:8443", "https://h:8443/chat/completions"),
            ("http://127.0.0.1:9/v1", "http://127.0.0.1:9/v1/chat/completions"),
            ("http://localhost:9/v1", "http://localhost:9/v1/chat/completions"),
        ] {
            assert_eq!(chat_url(base).as_deref(), Ok(url), "{base}");
        }
    }

    #[test]
    fn unsafe_or_malformed_urls_are_refused_without_being_repeated() {
        for (base, why) in [
            ("https://user:hunter2@h/v1", "credentials"),
            ("https://hunter2@h/v1", "credentials"),
            ("https://h/v1?api-key=hunter2", "query"),
            ("https://h/v1#hunter2", "query"),
            ("http://h.example/v1", "loopback"),
            ("http://127.0.0.1.hunter2.example/v1", "loopback"),
            ("ftp://h/v1", "https://"),
            ("https:///v1", "no host"),
            ("https://h/v1 hunter2", "whitespace"),
        ] {
            let e = chat_url(base).unwrap_err();
            assert!(e.contains(why) && !e.contains("hunter2"), "{base}: {e}");
            let e = parse_of(&["--openai-url", base, "--openai-model", "gpt-4o"]).unwrap_err();
            assert!(e.contains(why) && !e.contains("hunter2"), "{base}: {e}");
        }
    }

    #[test]
    fn openai_options_without_openai_url_are_refused() {
        for flags in [
            &["--openai-model", "m"][..],
            &["--openai-key-env", "K"],
            &["--openai-stream"],
            &["--openai-context-tokens", "1"],
            &["--openai-max-output-tokens", "1"],
            &["--price-input-usd-per-mtok", "1"],
            &["--price-output-usd-per-mtok", "1"],
        ] {
            let e = parse_of(flags).unwrap_err();
            assert!(e.starts_with(flags[0]) && e.contains("goes with --openai-url"), "{e}");
            assert!(!e.contains('\n'), "one line: {e}");
        }
    }

    #[test]
    fn vertex_options_beside_openai_url_are_refused() {
        for flags in [
            &["--vertex-project", "p"][..],
            &["--vertex-location", "us-east1"],
            &["--model", "gemini-2.5-pro"],
        ] {
            let e = parse_of(&[&BASE[..], flags].concat()).unwrap_err();
            assert!(e.starts_with(flags[0]) && e.contains("--openai-url"), "{e}");
            assert!(!e.contains('\n'), "one line: {e}");
        }
    }

    #[test]
    fn openai_url_needs_a_model() {
        let e = parse_of(&["--openai-url", "https://h/v1"]).unwrap_err();
        assert!(e.contains("--openai-model"), "{e}");
        let e = parse_of(&["--openai-url", "https://h/v1", "--openai-model", " "]).unwrap_err();
        assert!(e.contains("--openai-model"), "{e}");
    }

    #[test]
    fn the_key_is_never_an_option_and_never_echoed() {
        let secret = "sk-test-0123456789abcdef";
        for flags in [
            vec!["--openai-key".to_owned(), secret.to_owned()],
            vec![format!("--openai-key={secret}")],
        ] {
            let mut all: Vec<String> = BASE.iter().map(|s| (*s).to_owned()).collect();
            all.extend(flags);
            let e = parse(std::iter::once("p.d".to_owned()).chain(all)).unwrap_err();
            assert!(e.contains("--openai-key-env") && !e.contains(secret), "{e}");
        }
        // A key passed where its variable's name belongs is refused unrepeated.
        for v in [secret, "lower_case", "1ABC", "", "A-B"] {
            let e = parse_of(&[&BASE[..], &["--openai-key-env", v]].concat()).unwrap_err();
            assert!(e.contains("NAME") && (v.is_empty() || !e.contains(v)), "{v}: {e}");
        }
        assert!(!USAGE.contains("sk-"));
    }

    #[test]
    fn the_key_comes_from_the_named_variable_and_only_its_name_is_reported() {
        let secret = "sk-test-0123456789abcdef";
        let var = "CRAB_TEST_KEY";
        assert_eq!(api_key(var, Some(secret.into())).as_deref(), Ok(secret));
        assert_eq!(api_key(var, Some(format!("{secret}\n").into())).as_deref(), Ok(secret));
        for value in [None, Some(String::new()), Some("  \n".into())] {
            let e = api_key(var, value.map(Into::into)).unwrap_err();
            assert!(e.starts_with(var), "{e}");
        }
        for bad in [format!("{secret} x"), format!("{secret}\u{7}")] {
            let e = api_key(var, Some(bad.into())).unwrap_err();
            assert!(e.starts_with(var) && !e.contains(secret), "{e}");
        }
        // Through the environment: an unset variable fails the connection,
        // naming the variable (setting one is left to the integration tests,
        // which do it once before any thread reads the environment).
        let name = format!("CRAB_UNIT_UNSET_KEY_{}", std::process::id());
        let o = openai_of(&[&BASE[..], &["--openai-key-env", &name]].concat());
        let e = o.connect().unwrap_err();
        assert!(e.starts_with(&name), "{e}");
        let m = o.connect_with(Some(secret.into())).unwrap();
        assert_eq!(m.caps().id, "gpt-4o");
        assert!(!format!("{m:?}").contains(secret), "the client's debug form redacts the key");
        assert!(!format!("{o:?}").contains(secret) && !o.summary().contains(secret));
    }

    #[test]
    fn price_given_known_or_refused() {
        // Given: both flags, cached input charged in full.
        let o = openai_of(&[
            "--openai-url",
            "https://h/v1",
            "--openai-model",
            "gpt-4o",
            "--price-input-usd-per-mtok",
            "3",
            "--price-output-usd-per-mtok",
            "12.5",
        ]);
        assert_eq!(o.price_from(), PriceFrom::Flags);
        assert_eq!(
            o.caps().price,
            Price { input: 3_000_000, cached_input: 3_000_000, output: 12_500_000 }
        );
        assert!(o.summary().contains("given"), "{}", o.summary());
        // Known: the list price, and the run log says so.
        let o = openai_of(&BASE);
        assert_eq!(o.price_from(), PriceFrom::ListPrice);
        assert_eq!(
            o.caps().price,
            Price { input: 2_500_000, cached_input: 1_250_000, output: 10_000_000 }
        );
        let line = o.summary();
        assert!(
            line.contains("public list price of gpt-4o")
                && line.contains("$2.5000 input / $10.0000 output")
                && line.contains("unary")
                && line.contains("gw.example")
                && !line.contains("/v1"),
            "{line}"
        );
        // Unknown (a dated or prefixed name is not assumed): refused.
        for model in ["gpt-4o-2024-05-13", "openai/gpt-4o", "llama-3"] {
            let e =
                parse_of(&["--openai-url", "https://h/v1", "--openai-model", model]).unwrap_err();
            assert!(
                e.contains("--budget-usd cannot be enforced")
                    && e.contains("--price-input-usd-per-mtok")
                    && e.contains("--price-output-usd-per-mtok"),
                "{e}"
            );
        }
        // Half a price, or a value that is no price, is refused.
        let e = parse_of(&[&BASE[..], &["--price-input-usd-per-mtok", "1"]].concat()).unwrap_err();
        assert!(e.contains("go together"), "{e}");
        for v in ["-1", "NaN", "inf", "1e7", "cheap"] {
            let e =
                parse_of(&[&BASE[..], &["--price-input-usd-per-mtok", v]].concat()).unwrap_err();
            assert!(e.starts_with("--price-input-usd-per-mtok"), "{v}: {e}");
        }
        let zero = openai_of(
            &[&BASE[..], &["--price-input-usd-per-mtok", "0", "--price-output-usd-per-mtok", "0"]]
                .concat(),
        );
        assert_eq!(zero.caps().price, Price::default());
    }

    #[test]
    fn limits_must_be_positive_numbers() {
        for flags in [
            &["--openai-context-tokens", "0"][..],
            &["--openai-max-output-tokens", "0"],
            &["--openai-context-tokens", "many"],
            &["--openai-max-output-tokens", "-1"],
        ] {
            assert!(parse_of(&[&BASE[..], flags].concat()).is_err(), "{flags:?}");
        }
    }
}
