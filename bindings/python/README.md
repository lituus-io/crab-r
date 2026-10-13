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

# Lookups read the store, no network.
print(crab_r.lookup("state", "my-project.mart.orders.status"))
print(crab_r.upstream("state", "my-project.mart.orders.status"))
```

Options are the `crab-describe` command's flags as keyword arguments
(`lineage_location="us"`, `profile=False`, `apply=True`, ...). Credentials come
from Application Default Credentials. Nothing is written to the warehouse
unless `apply=True`.

License: AGPL-3.0-or-later, or a commercial license from lituus-io.
