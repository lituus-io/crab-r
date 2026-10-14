# Changelog

All notable changes to crab-r are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
[SemVer](https://semver.org/spec/v2.0.0.html).

## 0.2.3

- Fixed: `ingest_lineage` kept the promise that a repeat of the same export
  writes nothing, except for a column derived from itself (`SET c = f(c)`,
  or one column named in both the `#c` and the path spelling). The store
  keeps no edge from a node to itself, so the staged edge never read back
  and every repeat committed it again. A self-derivation is now skipped
  when the plan is built: it adds no lineage.

## 0.2.2

- **Built on uu-graph-r 0.4.2 and uu-objstore-r 0.2.3.** The store core this crate
  embeds now refuses an attribute set larger than one log record before staging
  it; in 0.4.0–0.4.1 such a set committed, then the next writable open truncated
  the log there and lost that commit and every later one. The dependency floors
  rise to match, so no build of this crate links the earlier core.
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
- **Static lineage into a store**: `crab_r.ingest_lineage(store, nodes,
  edges)` (Rust: `describe::ingest`) takes a deployment program's SQL lineage
  export as it is emitted — `bq://` ids, `contains`, `derives_from` and
  `column_derives_from` edges — and writes the nodes a describe run keys, so
  `table`, `lookup`, `upstream` and `downstream` answer for declared tables
  offline. It only adds (a recorded description, type or edge is kept),
  writes in one commit or none, and a repeat of the same export writes
  nothing. Kinds and relationships crab-r does not model are counted by name
  in `skipped`; unusable entries are listed in `rejected` by position and
  never repeated.
- A later `describe` enriches the same nodes and keeps what was declared:
  the static mark (attribute 16, the declaring resource) and the declared
  `Derives` edges survive beside what the run traced. A declared node carries
  no incremental key, so its first describe does the work in full.
- **One BigQuery name, three spellings**: `bq://p/d/t#column` (the export's
  ids), `bq://p/d/t/column` (a path form for stores whose url
  canonicalisation drops a `#fragment`, which would collapse a column onto
  its table) and `p.d.t.column`. `base::key::BqName` parses any of them;
  Python: `bq_id`, `bq_path`, `dotted`. A component with a dot, quote,
  backtick, slash, whitespace or control character is refused, not guessed.
- Lookups take `bq://` names in either form, and `upstream` / `downstream`
  of a table name walk table-level lineage.

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
