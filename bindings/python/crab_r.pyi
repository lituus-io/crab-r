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
    """A recorded table: description, kind, producer, columns."""

def lookup(store: str, column: str) -> str:
    """A recorded column (`"p.d.t.column"`): description, type, provenance."""

def upstream(store: str, column: str, depth: int = 10) -> str:
    """Where a column's values come from."""

def downstream(store: str, column: str, depth: int = 10) -> str:
    """What a column's values and rows feed."""

def due(store: str, n: int = 20) -> str:
    """The tables most overdue for a re-describe."""

def pull(source: str, directory: str) -> str:
    """Fetch (or refresh) a published store into `directory`."""
