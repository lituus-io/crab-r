# Changelog

All notable changes to crab-r are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
[SemVer](https://semver.org/spec/v2.0.0.html).

## 0.5.2

- **Fix: structured output on strict OpenAI-compatible servers.** The JSON
  schemas the crate sends as `response_format` (`describe`'s draft and judge
  replies, the engine's table-linking reply) were built for Vertex AI's
  schema subset: open objects and optional fields. Sent with
  `"strict": true`, a server that enforces OpenAI's strict structured-output
  rules refused every one with `400 Invalid schema for response_format
  'output': ... 'additionalProperties' is required to be supplied and to be
  false`, so `describe` drafted nothing through it.
- **Strict by construction, in one place.** `llm::strict::write_strict`
  rewrites a schema as it is serialised: every object node, at every depth,
  gets `"additionalProperties": false` and `required` listing every
  property; a property that was optional becomes nullable (`[type, "null"]`,
  with `null` added to an `enum`, or `anyOf` with `{"type": "null"}` when
  the node has no `type`); sub-schemas are walked through `properties`,
  `items`, `anyOf` / `oneOf` / `allOf`, `prefixItems` and `$defs`. The
  OpenAI backend applies it to every schema it sends, so a schema built
  anywhere, now or later, reaches the wire compliant. The rewrite borrows
  the source text, is idempotent, and refuses rather than sends text that
  is not one JSON document, an object with a duplicate key (parsers differ
  on which one wins) or a `properties` that is not an object. The reply parsers already read `null` as absent. Requests
  stay unary unless `--openai-stream` is given.
- The Vertex AI backend is unchanged: its native `responseSchema` has no
  strict mode and accepts optional fields.
- Tests: a property test over arbitrary column names and check ids that
  every schema the crate can send comes out strict (closed objects,
  `required` equal to the property keys), idempotent, and with every enum
  value kept; the schemas as built are shown non-compliant; an offline
  gateway that refuses non-strict schemas with OpenAI's 400 runs a whole
  `describe` (draft and judge) and parses replies that use `null` for every
  optional field; a live check of the real draft and judge schemas against
  an OpenAI-compatible server; a fuzz target (`fuzz_strict`): arbitrary
  text gives an error or valid JSON that is its own rewrite.

## 0.5.1

- **`exec::query` and Python `crab_r.query` / `query_async`: one guarded,
  read-only query.** The `MetadataGuard`'s `check_query` is the only gate,
  asked before a credential is touched and again by the transport for the
  body it sends: a statement reading a table outside `INFORMATION_SCHEMA`,
  `__TABLES__` and the graph datasets (default `ops`, `infra`; a
  `project.dataset` entry scopes one to a project) is refused with nothing
  sent. The run is `Warehouse::run` over objstore-r's client under the
  guard, with `maximumBytesBilled` (default 1 GiB), a row cap (default
  1,000) and a timeout (default 30 s); the job's own results are read
  through the `Tracked` transport, only as many rows as are still wanted.
- **Rows are plain data with an explicit cut.** Python gets a `QueryRows`
  list of dicts of str, int, float, bool and None (TIMESTAMP as ISO 8601
  UTC, DATE/DATETIME/TIME as ISO 8601, NUMERIC as exact decimal text, BYTES
  as base64, STRUCT/ARRAY/JSON as compact JSON text). `budget_tokens` keeps
  whole rows, in order, that fit (about four bytes a token, each row
  measured as compact JSON); `truncated` is set when the row cap or the
  budget cut any, beside `total_rows`, `bytes_processed`, `bytes_billed` and
  `cache_hit`.
- **Every failure is typed.** One `QueryError` (a `CrabError`) with
  `.reason`: `refused_by_guard`, `no_credentials`, `expired`, `no_access`,
  `not_found`, `bytes_limit`, `invalid_query`, `timeout`, `unreachable`. Its
  text never holds the credential: a message BigQuery wrote has the token
  it was sent removed. Credentials follow `table_meta`: `access_token=` as
  given, else discovery with the metadata server forbidden unless
  `metadata_server=True`.
- **The GIL is released and the process-wide runtime shared**; cancelling
  `query_async` drops the request.
- `exec::Error::Unauthenticated` (a 401) is told apart from `Denied` (a
  403). `MetadataGuard::with_own_jobs`. `BigQueryConfig::location` may be
  empty, leaving the job's placement to BigQuery.
- Tests: decoding and error mapping (unit); generated SQL through the
  query path, never reaching a stub BigQuery when it reads a forbidden
  table (property); `jobs.query` + `getQueryResults` against a stub with
  long polls, paging, truncation and cancellation (integration); counted
  requests and budgets, the GIL released, one runtime (performance); a
  decoy metadata server never asked and an echoing server never leaking
  the token into an error (security). The binding gains a `testing`
  feature (`_set_test_origin`) for its stub tests; CI builds it for
  pytest.

## 0.5.0

- **Built on uu-objstore-r 0.4.0 and uu-link-r 0.4.4** (graph-r 0.6.0 as
  before). objstore-r 0.4 gives the request-policy generic `http::Client<T,
  P: RequestPolicy>`, `Adc::from_access_token` and `MetadataPolicy::Forbid`.
- **`sql::refs::referenced_tables`: the one answer to "which tables does this
  SQL read".** A scoped walk of the parse tree: a CTE is visible where
  BigQuery sees it (after its own body in a plain `WITH`, anywhere in its
  `WITH RECURSIVE`, never outside the query that declares it), a path from
  a range variable in scope is an array scan, `INFORMATION_SCHEMA` views and
  `__TABLES__` are told apart, and a read the walk could not place is
  reported. The describe pipeline's upstream expansion rests on it.
- **`exec::guard::MetadataGuard`, the request policy for metadata reads.**
  Only `tables.get?view=BASIC`, `datasets.get`, `jobs.query`, the results of
  jobs this process issued (`OwnJobs`, recorded by the `Tracked`
  transport) and Dataplex lineage reads. Every query body (objstore-r hands
  each one to `check_query`) must be one query statement whose every read is
  an `INFORMATION_SCHEMA` view, a meta-table, or a table in the graph
  datasets (default `infra` and `ops`). DML, DDL, scripts, several
  statements, `EXTERNAL_QUERY`, functions outside the built-in namespaces,
  unqualified names (they would resolve against a request's
  `defaultDataset`), array paths and unplaceable reads are refused.
- **`exec::table_meta`: exact metadata, many tables at once, typed
  outcomes.** One `tables.get?view=BASIC` per table, all in flight together
  (twenty tables in about one round trip). Each table is `ok`, `no_access`,
  `not_found`, `no_credentials`, `expired`, `unreachable` or
  `invalid_name`; a batch never fails and nothing is repeated on a 4xx. The
  schema is BigQuery's own bytes, spliced: `omit_columns` removed, at most
  2,000 columns, 15 levels and 1,000-character descriptions, with policy
  tags, collation, defaults and unknown members kept. `MetaClient` binds it
  to the caller's credential under the guard.
- **`describe::offline`: drafting from supplied metadata with no Google
  client.** The `Offline` warehouse refuses every query and answers table
  reads from the supplied metadata and the store; the store's column
  lineage is the lineage source (pass-through and renamed columns inherit);
  a view's SQL is its producer; unchanged tables are reused.
  `app::run_offline` drives it with an OpenAI-compatible model whose client
  (`ModelOnly`) reaches only the model's host, never a Google one, and
  refuses `--apply`, `--publish` and `--lineage-location`.
- **Credentials are the caller's.** `app::Credentials`: a supplied token is
  used as is (nothing discovered, the metadata server never asked), else
  discovery, with the metadata server forbidden under
  `--no-metadata-server`. `app::pull` takes them.
- **Identity follows the spec shared with pulumi-rs-yaml
  (`sql_lineage::ids`).** `BqName` and every store key lowercase the
  project, accept a domain-scoped `example.com:proj` in every spelling, and
  drop a table's `$partition` decorator; `tests/data/ids_conformance.tsv`
  holds the shared id strings. A store keyed by the old spelling (a project
  with capitals) is regenerated through the lifecycle, and lookups answer
  `regenerate_needed` until then.
- **`producers::jobs_projection_sql`: the one `INFORMATION_SCHEMA.JOBS`
  projection.** Per query job: its resource key
  (`//bigquery.googleapis.com/projects/P/locations/L/jobs/J`), ids, state,
  times, error reason and message, labels, destination and referenced
  tables, the SQL and its shape, and a fingerprint of the principal (never
  the address). `jobs_query` reads through it byte for byte, so the
  `ops.observed_lineage` / `ops.runs` views generated from it cannot drift.
  With `JobsConfig::ops_dataset`, discovery reads a project's
  `observed_lineage` view and falls back to JOBS.
- **Regeneration around a store another process maps (Windows, os error
  1224).** `lifecycle::open_or_regenerate` now returns `Opened`: a data file
  that cannot be deleted is renamed aside to a temporary the next
  regeneration deletes, and the store is built fresh; when the rename is
  refused too it is a soft `RegeneratePending`, never an error (ingest
  reports `"regenerate_pending"`, `prune` returns `Regen`).
- **Lookups within a token budget.** `app::lookup_within` answers inside a
  graph-r `TokenBudget` (whole lines, then `truncated: N more lines`); a new
  `search` lookup is graph-r's ranked, budgeted query.
- **Python.** One lazily created process-wide multi-thread runtime replaces
  a runtime per call. New `table_meta`, `table_meta_async`,
  `describe_offline`, `describe_offline_async`, `jobs_projection_sql`,
  `search`; every lookup takes `budget_tokens`; `describe` and `pull` take
  `access_token=` and, without it, forbid the metadata server unless
  `metadata_server=True` (**changed:** 0.4 fell through to it). The
  awaitables use `pyo3-async-runtimes` (+78 KB on the macOS arm64 wheel);
  cancelling one drops the run and its requests.

## 0.4.0

- **Built on uu-graph-r 0.6.0, uu-objstore-r 0.3.0 and uu-link-r 0.4.3.**
  graph-r 0.6 writes each transaction as one checksummed log record and
  replays whole records only, so a describe save or a lineage ingest cut
  short by a crash or a power cut is absent, never half there: the store is
  exactly what it was before the run (the crash-cut tests now assert that,
  at every cut point inside the record). A store written by crab-r 0.3.x is
  graph-r format 2; this release reads format 3, so the first writable open
  regenerates it (below) and lookups answer `regenerate_needed` until then.
  objstore-r 0.3 bounds `pull` and `--publish` transfers by stalls rather
  than sizes and resumes them after a dropped connection.
- **A store that cannot be read regenerates instead of erroring.** The store
  is a cache of what describe runs and lineage ingests found, so damage is
  rebuilt, not reported.
  - `describe::lifecycle::open_or_regenerate` is the one writable open of
    `describe`, `ingest` and `prune`. On graph-r's `Version`, `Corrupt` or
    `Format`, or a base or log that cannot be read, it deletes the store's
    own data files (`graph.base`, `graph.wal` and graph-r's temporaries of
    them, by name) under the commit lock and creates a fresh store. The run
    then records what it describes.
  - The describe report and the ingest result carry `"regenerated":
    true|false`; the run logs one `warning:` line, the Python package one
    warning on the `crab_r` logger.
  - `Locked` and `Busy` are not damage: nothing is deleted. A regeneration
    that finds the commit lock held waits for it, then refuses with
    `Locked`.
  - Refused with a typed error, nothing deleted, when the store directory or
    one of its own files is a symbolic link or not a regular file. Nothing
    else in the directory is touched. The lock files stay: they hold no
    data, and replacing one a waiter holds open would let two writers hold
    the lock at once.
- **Lookups never error on a damaged store.** `table`, `lookup`, `upstream`,
  `downstream` and `due` answer one line, `regenerate_needed: nothing
  recorded; ...`, and succeed (exit 0). `lifecycle::regenerate_needed(dir)`
  and Python's `crab_r.regenerate_needed(store)` ask directly. A missing
  store is still an error. `pull` into a damaged replica regenerates it by a
  full pull.
- **TTL prune.** `lifecycle::prune(dir, max_age_days)`, `crab-describe prune
  DIR DAYS` and `crab_r.prune(store, max_age_days)` remove the table subtrees
  (the table, its columns, their attributes and edges) no describe, reuse or
  ingest has written or touched since the cutoff. The datasets that held them
  and the root are rewritten, a dataset left empty is dropped, and lineage
  edges into a removed column are dropped. One transaction, then one
  compaction, however many tables go; nothing that old writes nothing.
  Pinned nodes, and tables with a pinned column, are never pruned.
- New tests: every damage to either data file (garbage, truncated, bit-flipped
  header, wrong version, empty base) regenerates on the describe and ingest
  paths and answers "nothing recorded" on every lookup; a locked or busy
  store keeps its bytes; links and non-files are refused with their targets
  untouched; prune removes exactly the stale subtrees, counted as one
  transaction and one compaction for 1, 7 and 64 tables. Fuzz target
  `fuzz_store`: arbitrary bytes as both data files through every lookup and
  the regenerating open.

## 0.3.1

- **Built on uu-graph-r 0.5.1**, whose commit lock is fair: a writer
  committing back to back no longer starves the others. Beside a busy
  writer, a describe save or a lineage ingest could otherwise wait seconds
  and be refused with `Locked` after spending its model budget.
- Fixed: a save cut short (a crash or power cut mid-append keeps a prefix of
  the transaction's log frames) could leave a table offered for reuse as a
  mixture of two runs: the new table record with the old key and the old
  columns. The next describe trusted it and the missing columns never came
  back. A table's key, which is what makes its record reusable, is now
  written last, and an earlier record loses its key first, so a cut anywhere
  leaves the previous complete record or none, and the next run re-describes
  it.
- New tests: a save and an ingest cut at every frame boundary and inside
  every frame read cleanly, never offer a partial record, and heal to
  exactly the uninterrupted store; separate processes saving, ingesting and
  reading through forced compactions lose nothing; writers killed mid-run
  cost the others nothing. All fail on the 0.2.x write paths.

## 0.3.0

- **Built on uu-graph-r 0.5.0, uu-link-r 0.4.2 and uu-objstore-r 0.2.4.** The
  embedded store core takes its writer lock per commit instead of for a
  writable handle's life, so several processes can write one describe store
  at once. The public API names graph-r 0.5 types, hence the minor bump.
- Fixed for that: a describe's save and a static-lineage ingest each read the
  store, then replaced whole containment edge sets from what they read. With
  writers no longer excluded, a writer whose view predated another's commit
  overwrote it: eight concurrent saves into three datasets left one dataset
  linked. Both now run as one graph-r transaction, reading under the commit
  lock, so a concurrent writer is built on, never overwritten. A save or
  ingest with nothing new still writes nothing.
- A store is opened with graph-r's race-safe `open_or_create`; checking for
  the base file and then creating it let one of several racing creators lose
  its first save.

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
