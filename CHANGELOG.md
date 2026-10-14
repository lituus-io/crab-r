# Changelog

All notable changes to crab-r are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
[SemVer](https://semver.org/spec/v2.0.0.html).

## 0.2.2

- **An OpenAI-compatible model for `describe`**: `--openai-url https://host/v1`
  (`/chat/completions` appended) and `--openai-model M` replace Vertex AI with
  any OpenAI-compatible chat-completions server; BigQuery and Dataplex stay on
  ADC. Python: `crab_r.describe(..., openai_url=..., openai_model=...)`.
- The key is read only from the environment variable `--openai-key-env`
  names (default `OPENAI_API_KEY`) — never from the command line, which other
  processes can read and which this command echoes in its errors. A missing
  or empty variable is an error naming the variable. A URL carrying
  credentials, a query or a fragment is refused.
- Unary by default: an OpenAI-compatible gateway may refuse streamed
  structured output while serving it unary, and `n` candidates still share
  one request (one prefill). `--openai-stream` streams single-candidate
  requests. `--openai-context-tokens` (default 128000) and
  `--openai-max-output-tokens` (default 16384) set the limits.
- `--budget-usd` stays enforced: the price comes from
  `--price-input-usd-per-mtok` and `--price-output-usd-per-mtok`, or from the
  public list price of `gpt-4o`, `gpt-4o-mini`, `gpt-4.1`, `gpt-4.1-mini` or
  `gpt-4.1-nano` (named in the run log). Any other model without both flags
  refuses to start rather than run with an inert budget.
- Options that would be ignored are refused instead: `--openai-*` and
  `--price-*` without `--openai-url`; `--vertex-project`,
  `--vertex-location` and `--model` with it.
- The `cli` feature (and so the Python wheel) now includes `openai`.

## 0.2.1

- Security: every lockfile (crate, Python binding, fuzz) refreshed to
  patched releases, among them rustls 0.23.46 (RUSTSEC-2026-0346, -0347,
  -0348, -0349) and the earlier rustls and quinn-proto fixes
  (RUSTSEC-2026-0285, -0338 to -0342). The PyPI wheels are built from these.
- Builds against uu-graph-r 0.4.1, uu-objstore-r 0.2.1 and uu-link-r 0.4.1.
- New Security workflow: a weekly advisory audit of all three lockfiles.

## 0.2.0

- **One crate, `uu-crab-r`** (`use crab_r::...`), on crates.io and docs.rs;
  the former workspace crates are its modules (`crab_sql` → `crab_r::sql`,
  `crab_describe` → `crab_r::describe`, `crab_core` → `crab_r::base`, ...).
- **Describe any scope**: projects (one metadata pass per region), datasets,
  tables (`--upstream N` describes what they read too), and SQL text — with
  `--target` as that table's producer, or alone as a virtual table whose
  checks run over the query itself (columns from a free dry run).
- **State in a graph-r store** (`--store`), published through objstore-r's
  graph-r mirror (`--publish gs://…`), with `pull` and local lookups
  (`table`, `lookup`, `upstream`, `downstream`, `due`). An unchanged rerun
  reuses every table at $0 in model spend.
- **Python**: `pip install uu-crab-r` (`import crab_r`): the same command as
  functions.
- Shared machinery comes from the sibling libraries rather than local
  copies: tokenizer, token estimate and hashing (graph-r), Google error
  envelopes, SSE, `join_all` and retry policy (objstore-r), BM25 formula
  (link-r).
- Fixed: check byte caps allow BigQuery's 10 MiB minimum per table read;
  Dataplex-only lineage treats filter columns as row conditions; a
  backtick-quoted `UPDATE … FROM` table now traces.

## 0.1.0

BigQuery metadata, lineage and text-to-SQL library (initial version).
