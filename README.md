# crab-r

[![CI](https://github.com/lituus-io/crab-r/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/lituus-io/crab-r/actions/workflows/ci.yml) [![Security](https://github.com/lituus-io/crab-r/actions/workflows/security.yml/badge.svg?branch=main)](https://github.com/lituus-io/crab-r/actions/workflows/security.yml) [![Release](https://github.com/lituus-io/crab-r/actions/workflows/release.yaml/badge.svg)](https://github.com/lituus-io/crab-r/releases/latest) [![crates.io](https://img.shields.io/crates/v/uu-crab-r?label=crates.io)](https://crates.io/crates/uu-crab-r) [![docs.rs](https://img.shields.io/docsrs/uu-crab-r?label=docs.rs)](https://docs.rs/uu-crab-r) [![PyPI](https://img.shields.io/pypi/v/uu-crab-r?label=PyPI)](https://pypi.org/project/uu-crab-r/) [![Python](https://img.shields.io/pypi/pyversions/uu-crab-r)](https://pypi.org/project/uu-crab-r/) [![License](https://img.shields.io/badge/license-AGPL--3.0--or--later-blue)](LICENSE)

Generative technical metadata for BigQuery. Name a **project**, a **dataset**,
a **table**, or hand over **SQL**: crab-r reads the catalog, the queries that
produce each table (job history, view definitions, or your SQL), Dataplex
lineage and the values themselves, and writes column and table descriptions
that are checked against the data before they are trusted.

- **Evidence first.** Every producing statement is parsed, bound and traced to
  column lineage: pass-throughs and renames inherit their source's description
  verbatim (no model call), filters and joins are recorded as row conditions,
  and only what is genuinely new is drafted.
- **Validated, not guessed.** Each draft comes with the model's own checks
  (aggregates and queries), run as one guarded scan; a judge keeps, revises or
  flags each description on the results. Planted or stale descriptions are
  flagged with a correction.
- **Incremental.** With `--store`, state lives in a graph-r store. A table
  whose shape, rows, producing SQL and upstream descriptions are unchanged is
  carried over with no profile, model call or check: an unchanged rerun costs
  $0 in model spend.
- **Shareable.** `--publish gs://bucket/prefix` ships the store (only what
  changed, single fenced writer); `crab-describe pull` keeps replicas current.
  Lookups read a store locally.
- **Self-healing, bounded.** A store that cannot be read (damaged, or in a
  format this build does not read) is regenerated, never an error: a run, an
  ingest or a prune rebuilds it empty and says so (`"regenerated": true`),
  and a lookup answers `regenerate_needed: nothing recorded ...` with success.
  Only the store's own data files are deleted, never through a symbolic link;
  a locked or busy store is never touched. `crab-describe prune DIR DAYS`
  drops the tables no describe or ingest has touched in `DAYS` days, in one
  transaction and one compaction; pinned nodes stay.

## Install

| | |
|---|---|
| Rust | `cargo add uu-crab-r` |
| Python ≥3.12 (abi3 wheels: Linux x86_64/aarch64, macOS x86_64/arm64, Windows x86_64) | `pip install uu-crab-r` → `import crab_r` |

## Command

```text
crab-describe my-project                          # every dataset of a project (one region)
crab-describe my-project.mart my-project.stg      # datasets
crab-describe my-project.mart.orders --upstream 1 # a table, and what it reads one level up
crab-describe --sql query.sql --default-dataset my-project.mart
                                                  # a query's result, as a virtual table
crab-describe --sql ctas.sql --target my-project.mart.orders
                                                  # a table as this SQL fills it

    [--region US] [--billing-project P] [--jobs-project P] [--lineage-location us]
    [--store DIR] [--publish gs://bucket/prefix] [--budget-usd 5] [--out report.json]
    [--emit-schema DIR] [--apply [--correct]]
    [--openai-url https://host/v1 --openai-model M [--openai-key-env OPENAI_API_KEY]
     [--openai-stream] [--price-input-usd-per-mtok X --price-output-usd-per-mtok X]]

crab-describe lookup     DIR my-project.mart.orders.status
crab-describe upstream   DIR my-project.mart.orders.status [DEPTH]
crab-describe downstream DIR my-project.mart.orders.status [DEPTH]
crab-describe table      DIR my-project.mart.orders
crab-describe due        DIR [N]
crab-describe pull       gs://bucket/prefix DIR
crab-describe prune      DIR DAYS
```

Credentials come from Application Default Credentials. Nothing is written to
the warehouse unless `--apply` is given; then only empty descriptions are
filled (flagged ones too with `--correct`), each table guarded by the ETag it
was read at.

The model is Gemini on Vertex AI unless `--openai-url` names an
OpenAI-compatible chat-completions server (BigQuery and Dataplex stay on
ADC). Its API key is read from the environment variable `--openai-key-env`
names (default `OPENAI_API_KEY`) and is never accepted on the command line.
Requests are unary unless `--openai-stream` is given: some gateways refuse
streamed structured output, and `n` candidates still share one request.
`--budget-usd` is enforced from the model's price — the public list price for
`gpt-4o`, `gpt-4o-mini`, `gpt-4.1`, `gpt-4.1-mini` and `gpt-4.1-nano`, or both
`--price-*-usd-per-mtok` flags; with neither, the run refuses to start.

## Python

`pip install uu-crab-r` → `import crab_r`. The same options as keyword
arguments; see [`bindings/python`](bindings/python/README.md).

```python
import crab_r
report = crab_r.describe("my-project.mart.orders", upstream=1, store="state", access_token=token)
print(crab_r.lookup("state", "my-project.mart.orders.status", budget_tokens=500))

# Metadata only: tables.get?view=BASIC under the MetadataGuard, one typed outcome per table.
meta = crab_r.table_meta(["my-project.mart.orders"], omit_columns=["email"], access_token=token)
# Drafts from that metadata with no Google client at all (an OpenAI-compatible model).
drafts = crab_r.describe_offline("state", meta, openai_url=url, openai_model=m, openai_key_env="KEY",
                                 price_input_usd_per_mtok=0.15, price_output_usd_per_mtok=0.6)
```

`table_meta_async` and `describe_offline_async` are awaitables on one
process-wide runtime; cancelling one cancels its requests.

## Layout

One crate, `uu-crab-r` (`use crab_r::...`), on crates.io and docs.rs.


| Module | Role |
|---|---|
| `sql` | GoogleSQL lexer, parser and emitter |
| `ir` | Binder, diagnostics, column lineage and row conditions |
| `catalog` | Zero-copy catalog image, INFORMATION_SCHEMA sync, value profiles |
| `exec` | Warehouse trait; BigQuery client with dry-run cost guards |
| `lineage` | Column lineage graph; Dataplex lineage client |
| `llm` | Model trait; Vertex AI (Gemini), OpenAI-compatible, replay |
| `describe` | Describe: discovery, tracing, drafting, checks, judging, state |
| `engine`, `prompt`, `link`, `values`, `grammar`, `verify`, `eval` | Text-to-SQL (schema linking on link-r, value grounding, constrained generation, result verification, evaluation) |
| `base` | Keys, typed arenas, JSON scanner and writer, batches, dates, budgets |

crab-r builds on three sibling libraries, each with one job:
[graph-r](https://github.com/lituus-io/graph-r) (storage, graph, shared core),
[link-r](https://github.com/lituus-io/link-r) (acquisition and retrieval) and
[objstore-r](https://github.com/lituus-io/objstore-r) (object stores, HTTPS,
auth). They are path dependencies: check them out beside crab-r at the refs
in [`.github/siblings`](.github/siblings), or run `scripts/fetch-siblings.sh`.
No other third-party crate is a dependency (`scripts/check_deps.py`).

## Development

```text
scripts/fetch-siblings.sh
cargo test --all-features
cargo clippy --all-features --all-targets
python3 scripts/check_deps.py
```

Live tests are environment-gated and skip unless configured, for example
`CRAB_BQ_PROJECT`, `CRAB_LINEAGE_PROJECT`, `CRAB_VERTEX_PROJECT`, and
`OBJSTORE_LIVE_BUCKET` for the sibling libraries' Cloud Storage suites.
`tests/live/` holds a mock warehouse
(`run_sql.py crab_mock.sql PROJECT DATASET`) with two planted wrong
descriptions for Describe to flag.

## License

AGPL-3.0-or-later, or a commercial license from lituus-io.
