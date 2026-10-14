# crab-r

`pip install uu-crab-r` → `import crab_r` (Python ≥ 3.12, one `abi3` wheel per
platform).

Generative technical metadata for BigQuery. Name a project, a dataset, a
table, or hand over SQL; crab-r reads the catalog, the queries that produce
each table (job history, view definitions, or your SQL), Dataplex lineage and
the values themselves, then writes column and table descriptions that are
checked against the data before they are trusted.

```python
import crab_r

# A dataset, keeping state so an unchanged rerun costs nothing.
report = crab_r.describe("my-project.mart", store="state", budget_usd=1)

# One table and what it reads, one level up.
crab_r.describe("my-project.mart.orders", upstream=1, store="state")

# A query's result, described as a virtual table (checks run over the query).
crab_r.describe(sql="SELECT customer_id, COUNT(*) AS orders FROM orders GROUP BY 1",
                default_dataset="my-project.mart")

# An OpenAI-compatible server instead of Vertex AI. The key is read from the
# environment variable named here, never passed as a value.
crab_r.describe("my-project.mart", openai_url="https://host/v1",
                openai_model="gpt-4o", openai_key_env="OPENAI_API_KEY")

# Lookups read the store, no network.
print(crab_r.lookup("state", "my-project.mart.orders.status"))
print(crab_r.upstream("state", "my-project.mart.orders.status"))
```

## Static lineage and BigQuery names

A deployment program's SQL lineage export (`bq://` ids; `contains`,
`derives_from` and `column_derives_from` edges) can be written into a store
before any describe, so declared tables answer lookups offline:

```python
export = ...  # {"nodes": [...], "edges": [...]}, as the lineage export emits it
done = crab_r.ingest_lineage("state", export["nodes"], export["edges"])
# {"committed": True, "created": 14, "updated": 0, "edges": 20,
#  "skipped": {"writes_to": 1, ...}, "rejected": []}
print(crab_r.upstream("state", "bq://proj/mart/orders_v#amt"))
print(crab_r.downstream("state", "proj.mart.orders"))   # a table: what reads it
```

Ingest only adds: a description, type or edge the store holds is kept, and a
repeat ingest of the same export writes nothing. A later `describe` fills the
same nodes in and keeps what was declared. Entries crab-r does not model
(routines, `writes_to`, `defined_by`, ...) are counted in `skipped`; unusable
ones are listed in `rejected` by position, never repeated.

One BigQuery name has three spellings, and every function here accepts all
of them:

| Spelling | Where it comes from |
|---|---|
| `bq://p/d/t#column` (`bq_id`) | the lineage export's own ids |
| `bq://p/d/t/column` (`bq_path`) | a store that keys by url and canonicalises urls the way graph-r's Python `add()` does, which **drops the `#fragment`** — there a column's `#` id would silently become its table. Key such a store with the path form |
| `p.d.t.column` (`dotted`) | the warehouse's own |

```python
crab_r.bq_path("bq://proj/mart/orders#Status")   # 'bq://proj/mart/orders/status'
crab_r.dotted("bq://proj/mart/orders/status")    # 'proj.mart.orders.status'
```

crab-r's own store keys the exact `sql://bigquery/p/d/t#column` uri with no
url canonicalisation, so columns and tables stay distinct there. Column
names are lowercased (BigQuery treats them case-insensitively); a component
holding anything but letters, digits, `_` and `-` (and `:` in a project) is
refused rather than guessed at.

Options are the `crab-describe` command's flags as keyword arguments
(`lineage_location="us"`, `profile=False`, `apply=True`, ...). Credentials come
from Application Default Credentials; with `openai_url=` the model's key comes
from the variable `openai_key_env=` names (default `OPENAI_API_KEY`). Nothing
is written to the warehouse unless `apply=True`.

License: AGPL-3.0-or-later, or a commercial license from lituus-io.
