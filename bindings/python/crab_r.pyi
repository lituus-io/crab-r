# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
"""Generative technical metadata for BigQuery (the `crab-describe` command)."""

from typing import Any

__version__: str

class CrabError(Exception):
    """A run or lookup failed (credentials, warehouse, store)."""

class QueryError(CrabError):
    """A `query` failed. The text never holds the token."""

    reason: str
    """`refused_by_guard`, `no_credentials`, `expired`, `no_access`, `not_found`, `bytes_limit`,
    `invalid_query`, `timeout` or `unreachable`."""

class QueryRows(list[dict[str, Any]]):
    """A query's rows: dicts of plain scalars (str, int, float, bool, None; TIMESTAMP as ISO 8601 UTC text ending
    `Z`, DATE/DATETIME/TIME as ISO 8601, NUMERIC as exact decimal text, BYTES as base64, STRUCT/ARRAY/JSON as
    compact JSON text)."""

    truncated: bool
    """Rows exist beyond these: `max_rows` or `budget_tokens` cut some."""
    total_rows: int
    bytes_processed: int
    bytes_billed: int
    cache_hit: bool

def describe(*targets: str, sql: str | None = None, **options: Any) -> dict[str, Any]:
    """Describe projects, datasets (`"p.d"`), tables (`"p.d.t"`) or SQL.

    Options are the command's flags: `target=`, `default_dataset=`,
    `upstream=`, `region=`, `store=`, `publish=`, `budget_usd=`,
    `lineage_location=`, `profile=False`, `apply=True`, ...; an
    OpenAI-compatible model with `openai_url=`, `openai_model=` and
    `openai_key_env=` (the NAME of the variable holding the key, never the
    key), `openai_stream=True`, `price_input_usd_per_mtok=`,
    `price_output_usd_per_mtok=`. Credentials: `access_token=` (used as given; nothing is discovered and the
    metadata server is never asked), else Application Default Credentials with the metadata server forbidden
    unless `metadata_server=True`. Progress goes to the `crab_r` logger. Raises ValueError for invalid options, CrabError
    when the run fails. Returns the report; its `"regenerated"` is True when `store=` could not be read and was rebuilt
    empty (nothing reused, only what this run described recorded).
    """

# Lookups (`table`, `lookup`, `upstream`, `downstream`, `due`) never raise for a store that exists but cannot be read
# (damaged, or another format version): they return one line starting `regenerate_needed:` that says nothing is
# recorded, and the next `describe`, `ingest_lineage` or `prune` regenerates the store. `regenerate_needed(store)` is
# the same check as a bool. A missing store still raises CrabError.

def regenerate_needed(store: str) -> bool:
    """True when `store` exists but cannot be read: lookups answer nothing recorded until it is regenerated."""

def prune(store: str, max_age_days: int) -> dict[str, Any]:
    """Remove the tables no `describe` or `ingest_lineage` has written or touched in `max_age_days`, with their columns.

    One transaction and one compaction however many go; nothing is written when nothing is that old, and pinned nodes
    stay. A store that cannot be read is regenerated instead. Returns `{"tables": int, "columns": int,
    "regenerated": bool, "regenerate_pending": bool}` (pending: it needs regenerating but another process holds
    its files; nothing was changed). Raises CrabError when the store cannot be opened (locked, a symbolic link, ...).
    """

# Every lookup takes `budget_tokens=`: the answer then fits that many tokens (about four bytes each), whole lines
# nearest first, ending `truncated: N more lines` when some did not fit.

def table(store: str, name: str, budget_tokens: int | None = None) -> str:
    """A recorded table (`"p.d.t"` or `"bq://p/d/t"`): description, kind, producer, columns, lineage."""

def lookup(store: str, column: str, budget_tokens: int | None = None) -> str:
    """A recorded column (`"p.d.t.column"`, `"bq://p/d/t#column"` or `"bq://p/d/t/column"`): description, type,
    provenance."""

def upstream(store: str, column: str, depth: int = 10, budget_tokens: int | None = None) -> str:
    """Where a column's values come from; for a table name, the tables it reads."""

def downstream(store: str, column: str, depth: int = 10, budget_tokens: int | None = None) -> str:
    """What a column's values and rows feed; for a table name, the tables that read it."""

def due(store: str, n: int = 20, budget_tokens: int | None = None) -> str:
    """The tables most overdue for a re-describe."""

def search(store: str, text: str, n: int = 20, budget_tokens: int | None = None) -> str:
    """The recorded tables and columns best matching `text` (names and descriptions), ranked, within the budget."""

def pull(source: str, directory: str, access_token: str | None = None, metadata_server: bool = False) -> str:
    """Fetch (or refresh) a published store into `directory`."""

def table_meta(tables: list[str], include_view_sql: bool = False, omit_columns: list[str] | None = None,
               access_token: str | None = None, metadata_server: bool = False) -> list[dict[str, Any]]:
    """Exact metadata of `tables` (`"p.d.t"`), read at once through `tables.get?view=BASIC` (no statistics, no rows).

    One dict per table, in order: `{"table", "available", "reason", ...}`. `reason` is `"ok"` (with `type`,
    `location`, `description`, `labels`, `time_partitioning`, `range_partitioning`, `clustering`, `schema` — BigQuery's
    own JSON with `omit_columns` removed, policy tags, collation, defaults and unknown members kept — `columns`,
    `omitted`, `truncated_descriptions`, `etag`, and `view_sql` when asked), else one of `"no_access"`,
    `"not_found"`, `"no_credentials"`, `"expired"`, `"unreachable"`, `"invalid_name"` with a `message`. At most
    2,000 columns, 15 levels deep, 1,000-character descriptions. Never raises for a table; nothing is retried on a 4xx.
    """

async def table_meta_async(tables: list[str], include_view_sql: bool = False, omit_columns: list[str] | None = None,
                           access_token: str | None = None, metadata_server: bool = False) -> list[dict[str, Any]]:
    """`table_meta` as an awaitable on the shared runtime; cancelling it cancels the requests."""

def query(sql: str, project: str, location: str | None = None, access_token: str | None = None,
          metadata_server: bool = False, graph_datasets: list[str] | tuple[str, ...] = ("ops", "infra"),
          max_bytes_billed: int = 1 << 30, timeout_ms: int = 30_000, max_rows: int = 1_000,
          budget_tokens: int | None = None) -> QueryRows:
    """Run one read-only query in `project` (which runs and pays for the job).

    Only `INFORMATION_SCHEMA` views, `__TABLES__` and the `graph_datasets` (`"ops"`, or `"project.ops"` for one
    project's) may be read; one query statement, no DML, DDL, scripts, `EXTERNAL_QUERY` or unqualified names.
    Anything else raises QueryError(`refused_by_guard`) before a credential is touched or a byte is sent. The job
    bills at most `max_bytes_billed`; at most `max_rows` rows come back, then only the whole rows that fit
    `budget_tokens` (about four bytes a token, each row measured as compact JSON); `truncated` says whether either
    cut. `location=None` lets BigQuery place the job with its data. Credentials: `access_token=` as given, else
    Application Default Credentials with the metadata server forbidden unless `metadata_server=True`. Raises
    ValueError for invalid arguments.
    """

async def query_async(sql: str, project: str, location: str | None = None, access_token: str | None = None,
                      metadata_server: bool = False, graph_datasets: list[str] | tuple[str, ...] = ("ops", "infra"),
                      max_bytes_billed: int = 1 << 30, timeout_ms: int = 30_000, max_rows: int = 1_000,
                      budget_tokens: int | None = None) -> QueryRows:
    """`query` as an awaitable on the shared runtime; cancelling it cancels the request."""

def describe_offline(store: str | None, catalog_metadata: list[dict[str, Any]] | str, **options: Any) -> dict[str, Any]:
    """Draft descriptions for the tables of `catalog_metadata` (what `table_meta` returned) with no Google client.

    The model is OpenAI-compatible (`openai_url=`, `openai_model=`, `openai_key_env=`, prices), reached through a
    client that may contact only its own host and never a Google one. `store` is reused — unchanged tables are not
    drafted again, its lineage and descriptions are inherited — and the drafts are recorded in it. Nothing is written
    anywhere else. Returns the report; raises ValueError for invalid options, CrabError when the run fails.
    """

async def describe_offline_async(store: str | None, catalog_metadata: list[dict[str, Any]] | str,
                                 **options: Any) -> dict[str, Any]:
    """`describe_offline` as an awaitable on the shared runtime; cancelling it cancels the model's requests."""

def jobs_projection_sql(jobs_project: str, region: str, lookback_days: int = 180) -> str:
    """The one INFORMATION_SCHEMA.JOBS projection: the SQL the `ops.observed_lineage` and `ops.runs` views are
    generated from. Raises ValueError for a project or region that is not a plain name."""

def ingest_lineage(store: str, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, Any]:
    """Write a static lineage export's `nodes` and `edges`, as the export has them, into `store` (created when absent).

    Lookups then answer for the declared tables with no network, and a later `describe` enriches the same nodes.
    Adds only — a description, type or edge already recorded is kept — and writes in one commit or none, so a
    repeat ingest of the same export writes nothing. Returns `{"committed": bool, "created": int, "updated": int,
    "edges": int, "skipped": {name: count}, "rejected": [{"side": "node" | "edge", "index": int, "reason": str}],
    "regenerated": bool, "regenerate_pending": bool}` (`regenerated`: the store could not be read and was rebuilt
    empty first; `regenerate_pending`: it needs that but another process holds its files, and nothing was written).
    Raises CrabError when the input is not JSON data or the store cannot be written.
    """

def bq_id(name: str) -> str:
    """Any spelling of a BigQuery name as a lineage export's id: `bq://p/d/t#column`, the project lowercased (a
    domain-scoped `example.com:p` is one project; a `$partition` decorator is dropped). Raises ValueError."""

def bq_path(name: str) -> str:
    """Any spelling of a BigQuery name in the path form `bq://p/d/t/column`, for stores that drop `#fragment`s.
    Raises ValueError."""

def dotted(name: str) -> str:
    """Any spelling of a BigQuery name as the warehouse spells it: `p.d.t.column`. Raises ValueError."""
