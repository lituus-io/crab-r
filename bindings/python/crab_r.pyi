# Author: support@lituus.io
# Copyright: lituus-io, all rights reserved
"""Generative technical metadata for BigQuery (the `crab-describe` command)."""

from typing import Any

__version__: str

class CrabError(Exception):
    """A run or lookup failed (credentials, warehouse, store)."""

def describe(*targets: str, sql: str | None = None, **options: Any) -> dict[str, Any]:
    """Describe projects, datasets (`"p.d"`), tables (`"p.d.t"`) or SQL.

    Options are the command's flags: `target=`, `default_dataset=`,
    `upstream=`, `region=`, `store=`, `publish=`, `budget_usd=`,
    `lineage_location=`, `profile=False`, `apply=True`, ...; an
    OpenAI-compatible model with `openai_url=`, `openai_model=` and
    `openai_key_env=` (the NAME of the variable holding the key, never the
    key), `openai_stream=True`, `price_input_usd_per_mtok=`,
    `price_output_usd_per_mtok=`. Progress goes to the `crab_r` logger. Raises ValueError for invalid options, CrabError
    when the run fails. Returns the report.
    """

def table(store: str, name: str) -> str:
    """A recorded table (`"p.d.t"` or `"bq://p/d/t"`): description, kind, producer, columns, lineage."""

def lookup(store: str, column: str) -> str:
    """A recorded column (`"p.d.t.column"`, `"bq://p/d/t#column"` or `"bq://p/d/t/column"`): description, type,
    provenance."""

def upstream(store: str, column: str, depth: int = 10) -> str:
    """Where a column's values come from; for a table name, the tables it reads."""

def downstream(store: str, column: str, depth: int = 10) -> str:
    """What a column's values and rows feed; for a table name, the tables that read it."""

def due(store: str, n: int = 20) -> str:
    """The tables most overdue for a re-describe."""

def pull(source: str, directory: str) -> str:
    """Fetch (or refresh) a published store into `directory`."""

def ingest_lineage(store: str, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, Any]:
    """Write a static lineage export's `nodes` and `edges`, as the export has them, into `store` (created when absent).

    Lookups then answer for the declared tables with no network, and a later `describe` enriches the same nodes.
    Adds only — a description, type or edge already recorded is kept — and writes in one commit or none, so a
    repeat ingest of the same export writes nothing. Returns `{"committed": bool, "created": int, "updated": int,
    "edges": int, "skipped": {name: count}, "rejected": [{"side": "node" | "edge", "index": int, "reason": str}]}`.
    Raises CrabError when the input is not JSON data or the store cannot be written.
    """

def bq_id(name: str) -> str:
    """Any spelling of a BigQuery name as a lineage export's id: `bq://p/d/t#column`. Raises ValueError."""

def bq_path(name: str) -> str:
    """Any spelling of a BigQuery name in the path form `bq://p/d/t/column`, for stores that drop `#fragment`s.
    Raises ValueError."""

def dotted(name: str) -> str:
    """Any spelling of a BigQuery name as the warehouse spells it: `p.d.t.column`. Raises ValueError."""
