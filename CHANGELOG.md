# Changelog

All notable changes to crab-r are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
[SemVer](https://semver.org/spec/v2.0.0.html).

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
