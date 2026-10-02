// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Incremental runs: a previous report is the state.
//!
//! Every completed table carries two keys in the report:
//!
//! - `prekey`: its shape, data and producer — column paths and types, row
//!   count, logical bytes, the producing statement and SQL with its last
//!   write, the view definition, and the settings that change results.
//!   Metadata-only edits (such as applying descriptions) do not move it.
//! - `key`: `prekey` plus the descriptions of the out-of-scope tables it
//!   reads and the model.
//!
//! On a rerun a table is **reused** — no profile, no model call, no check
//! query — when its key matches, every current description is either the
//! one it had or the one the report produced (so applying a report does not
//! invalidate it), and every in-scope table it reads is reused too. Its
//! columns, verdicts and check evidence are carried over from the report.

use crate::describe::Action;
use crate::generate::Value;
use crab_catalog::TableSpec;
use crab_core::json::Writer;
use graph_r::hash::Hasher;
use std::collections::HashMap;

/// A column as a previous report recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PriorColumn {
    /// Column path (lower-cased).
    pub column: String,
    /// What happened.
    pub action: Action,
    /// The description the table had then.
    pub existing: Option<String>,
    /// The description produced.
    pub description: Option<String>,
    /// Semantic type.
    pub semantic_type: Option<String>,
    /// Unit.
    pub unit: Option<String>,
    /// Confidence.
    pub confidence: Option<String>,
    /// Ids of its checks.
    pub checks: Vec<u32>,
}

/// A table as a previous report recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PriorTable {
    /// Shape, data and producer key.
    pub prekey: u64,
    /// Full key.
    pub key: u64,
    /// Table description the table had then.
    pub existing: Option<String>,
    /// Table description produced.
    pub description: Option<String>,
    /// Grain.
    pub grain: Vec<String>,
    /// Columns.
    pub columns: Vec<PriorColumn>,
    /// Its `checks` array, as JSON (carried into the new report).
    pub checks_json: String,
}

/// The state of a previous run: its completed tables.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Prior {
    tables: HashMap<String, PriorTable>,
}

fn action_of(s: &str) -> Option<Action> {
    Some(match s {
        "keep" => Action::Keep,
        "confirm" => Action::Confirm,
        "flag" => Action::Flag,
        "propagate" => Action::Propagate,
        "generate" => Action::Generate,
        "skip" => Action::Skip,
        _ => return None,
    })
}

fn hex(s: &str) -> Option<u64> {
    u64::from_str_radix(s, 16).ok()
}

/// Serialise a parsed value back to JSON.
fn write_value(w: &mut Writer<'_>, v: &Value) {
    match v {
        Value::Null => {
            w.null();
        }
        Value::Bool(b) => {
            w.bool(*b);
        }
        Value::Num(n) => {
            w.raw(n);
        }
        Value::Str(s) => {
            w.string(s);
        }
        Value::Arr(a) => {
            w.begin_array();
            for x in a {
                write_value(w, x);
            }
            w.end_array();
        }
        Value::Obj(m) => {
            w.begin_object();
            for (k, x) in m {
                w.key(k);
                write_value(w, x);
            }
            w.end_object();
        }
    }
}

impl Prior {
    /// Read a previous report. Tables without keys (pending or incomplete)
    /// and anything unreadable are left out: they are simply described
    /// again.
    #[must_use]
    pub fn from_json(text: &str) -> Self {
        let mut tables = HashMap::new();
        let Some(v) = Value::parse(text) else { return Self { tables } };
        for t in v.arr("tables") {
            let (Some(name), Some(prekey), Some(key)) =
                (t.str("table"), t.str("prekey").and_then(hex), t.str("key").and_then(hex))
            else {
                continue;
            };
            let mut columns = Vec::new();
            let mut ok = true;
            for c in t.arr("columns") {
                let (Some(column), Some(action)) =
                    (c.str("column"), c.str("action").and_then(action_of))
                else {
                    ok = false;
                    break;
                };
                columns.push(PriorColumn {
                    column: column.to_owned(),
                    action,
                    existing: c.str("existing_description").map(str::to_owned),
                    description: c.str("description").map(str::to_owned),
                    semantic_type: c.str("semantic_type").map(str::to_owned),
                    unit: c.str("unit").map(str::to_owned),
                    confidence: c.str("confidence").map(str::to_owned),
                    checks: c
                        .arr("checks")
                        .iter()
                        .filter_map(|x| match x {
                            Value::Num(n) => n.parse().ok(),
                            _ => None,
                        })
                        .collect(),
                });
            }
            if !ok {
                continue;
            }
            let mut checks_json = String::new();
            write_value(
                &mut Writer::new(&mut checks_json),
                t.get("checks").unwrap_or(&Value::Arr(vec![])),
            );
            tables.insert(
                name.to_owned(),
                PriorTable {
                    prekey,
                    key,
                    existing: t.str("existing_description").map(str::to_owned),
                    description: t.str("description").map(str::to_owned),
                    grain: t
                        .arr("grain")
                        .iter()
                        .filter_map(|g| match g {
                            Value::Str(s) => Some(s.clone()),
                            _ => None,
                        })
                        .collect(),
                    columns,
                    checks_json,
                },
            );
        }
        Self { tables }
    }

    /// Tables recorded.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// No tables recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// The record of `table`.
    #[must_use]
    pub fn get(&self, table: &str) -> Option<&PriorTable> {
        self.tables.get(table)
    }
}

fn put(h: &mut Hasher, s: &str) {
    h.update(&(s.len() as u64).to_le_bytes());
    h.update(s.as_bytes());
}

/// What the prekey covers besides the table itself.
#[derive(Clone, Copy, Debug)]
pub struct KeyInputs<'a> {
    /// Producing statement type, SQL and last write.
    pub producer: Option<(&'a str, &'a str, u64)>,
    /// View or materialized-view definition.
    pub view_query: Option<&'a str>,
    /// Settings that change results (audit, overwrite, profile, rounds).
    pub settings: u64,
}

/// Shape, data and producer key of `spec`.
#[must_use]
pub fn prekey(table: &str, spec: &TableSpec, k: KeyInputs<'_>) -> u64 {
    let mut h = Hasher::new();
    put(&mut h, table);
    for c in &spec.columns {
        put(&mut h, &c.path.to_ascii_lowercase());
        put(&mut h, &c.data_type);
    }
    h.update(&spec.row_count.to_le_bytes());
    h.update(&spec.logical_bytes.to_le_bytes());
    if let Some((st, q, last)) = k.producer {
        put(&mut h, st);
        put(&mut h, q);
        h.update(&last.to_le_bytes());
    }
    put(&mut h, k.view_query.unwrap_or(""));
    h.update(&k.settings.to_le_bytes());
    h.digest64()
}

/// Full key: `prekey`, the descriptions of the out-of-scope tables read
/// (`upstream`, any order) and the model.
#[must_use]
pub fn key(prekey: u64, upstream: &[&TableSpec], model: &str) -> u64 {
    let mut ups: Vec<&&TableSpec> = upstream.iter().collect();
    ups.sort_by(|a, b| (&a.dataset, &a.name).cmp(&(&b.dataset, &b.name)));
    let mut h = Hasher::new();
    h.update(&prekey.to_le_bytes());
    put(&mut h, model);
    for t in ups {
        put(&mut h, &t.dataset);
        put(&mut h, &t.name);
        put(&mut h, t.description.as_deref().unwrap_or(""));
        for c in &t.columns {
            put(&mut h, &c.path);
            put(&mut h, c.description.as_deref().unwrap_or(""));
        }
    }
    h.digest64()
}

fn norm(s: Option<&str>) -> Option<&str> {
    s.map(str::trim).filter(|s| !s.is_empty())
}

fn same_text(now: Option<&str>, a: Option<&str>, b: Option<&str>) -> bool {
    let now = norm(now);
    now == norm(a) || (now.is_some() && now == norm(b))
}

/// Whether the table's descriptions now are what the report saw or what it
/// produced (for example after applying it).
#[must_use]
pub fn descriptions_compatible(spec: &TableSpec, p: &PriorTable) -> bool {
    if !same_text(spec.description.as_deref(), p.existing.as_deref(), p.description.as_deref()) {
        return false;
    }
    spec.columns.iter().filter(|c| !c.path.contains('.')).all(|c| {
        let name = c.path.to_ascii_lowercase();
        p.columns.iter().find(|x| x.column == name).is_some_and(|x| {
            same_text(c.description.as_deref(), x.existing.as_deref(), x.description.as_deref())
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crab_catalog::{ColumnSpec, TableKind};

    fn spec(desc: Option<&str>) -> TableSpec {
        TableSpec {
            dataset: "d".into(),
            name: "t".into(),
            kind: TableKind::Table,
            description: None,
            row_count: 10,
            logical_bytes: 100,
            last_modified_ms: 5,
            require_partition_filter: false,
            columns: vec![ColumnSpec {
                path: "A".into(),
                data_type: "INT64".into(),
                ordinal: 1,
                nullable: true,
                partitioning: false,
                hidden: false,
                clustering: None,
                description: desc.map(Into::into),
            }],
        }
    }

    #[test]
    fn keys_move_with_data_and_sql_not_with_descriptions_or_touches() {
        let k =
            KeyInputs { producer: Some(("CTAS", "SELECT 1", 7)), view_query: None, settings: 1 };
        let base = prekey("p.d.t", &spec(None), k);
        let mut s = spec(Some("described"));
        s.last_modified_ms = 99;
        assert_eq!(prekey("p.d.t", &s, k), base, "descriptions and metadata touches");
        s.row_count = 11;
        assert_ne!(prekey("p.d.t", &s, k), base, "new rows");
        let k2 = KeyInputs { producer: Some(("CTAS", "SELECT 2", 7)), ..k };
        assert_ne!(prekey("p.d.t", &spec(None), k2), base, "new SQL");
        let up = spec(Some("x"));
        let up2 = spec(Some("y"));
        assert_ne!(key(base, &[&up], "m"), key(base, &[&up2], "m"), "upstream text");
        assert_ne!(key(base, &[&up], "m"), key(base, &[&up], "m2"), "model");
    }

    #[test]
    fn reads_a_report_and_accepts_applied_descriptions() {
        let json = r#"{"tables":[
          {"table":"p.d.t","prekey":"ff","key":"a1","existing_description":null,"description":"T.",
           "grain":["a"],"columns":[{"column":"a","action":"generate","existing_description":null,
           "description":"An id.","confidence":"high","checks":[1]}],
           "checks":[{"id":1,"claim":"unique","expr":"COUNT(*)-COUNT(DISTINCT a)","result":"0","sampled":false}]},
          {"table":"p.d.pending","prekey":"1","key":null,"columns":[]}]}"#;
        let p = Prior::from_json(json);
        assert_eq!(p.len(), 1);
        let t = p.get("p.d.t").unwrap();
        assert_eq!((t.prekey, t.key, t.grain.as_slice()), (0xff, 0xa1, &["a".to_owned()][..]));
        assert_eq!(t.columns[0].checks, vec![1]);
        assert!(t.checks_json.starts_with(r#"[{"id":1,"claim":"unique""#), "{}", t.checks_json);
        assert!(descriptions_compatible(&spec(None), t), "as seen");
        let mut applied = spec(Some("An id."));
        applied.description = Some("T.".into());
        assert!(descriptions_compatible(&applied, t), "as produced");
        assert!(!descriptions_compatible(&spec(Some("Edited by hand.")), t), "edited");
        assert!(Prior::from_json("not json").is_empty());
    }
}
