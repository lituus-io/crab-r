// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! What a describe run hands over: a JSON report, schema files for
//! infrastructure-as-code, and an etag-guarded apply.

use crate::checks::{CheckSql, Outcome};
use crate::describe::{Action, Report, TableReport};
use crate::splice::splice;
use crab_catalog::CatalogBuilder;
use crab_core::json::{Event, Scanner, Writer};
use crab_exec::{TableMeta, TableMetadata};
use std::collections::BTreeMap;

/// The report as JSON (the raw table resources are not included).
#[must_use]
pub fn to_json(r: &Report) -> String {
    let mut out = String::with_capacity(16 * 1024);
    let mut w = Writer::new(&mut out);
    w.begin_object();
    w.key("levels").begin_array();
    for l in &r.levels {
        w.begin_array();
        for t in l {
            w.string(t);
        }
        w.end_array();
    }
    w.end_array();
    w.key("warnings").begin_array();
    for x in &r.warnings {
        w.string(x);
    }
    w.end_array();
    spend(&mut w, "spend", r.spend);
    w.key("tables").begin_array();
    for t in &r.tables {
        table(&mut w, t);
    }
    w.end_array().end_object();
    out
}

fn spend(w: &mut Writer<'_>, key: &str, s: crab_core::Spend) {
    w.key(key).begin_object();
    w.key("input_tokens")
        .uint(s.input_tokens)
        .key("cached_tokens")
        .uint(s.cached_tokens)
        .key("output_tokens")
        .uint(s.output_tokens);
    w.key("warehouse_bytes").uint(s.warehouse_bytes).key("micro_usd").uint(s.micro_usd);
    w.end_object();
}

fn opt(w: &mut Writer<'_>, key: &str, v: Option<&str>) {
    w.key(key);
    match v {
        Some(s) => w.string(s),
        None => w.null(),
    };
}

fn table(w: &mut Writer<'_>, t: &TableReport) {
    w.begin_object();
    w.key("table")
        .string(&t.table)
        .key("kind")
        .string(&t.kind)
        .key("rows")
        .uint(t.rows)
        .key("bytes")
        .uint(t.bytes)
        .key("last_modified_ms")
        .uint(t.last_modified_ms);
    w.key("pending").bool(t.pending).key("model_calls").uint(u64::from(t.model_calls));
    // Incremental state: a rerun reuses the table while these still match.
    w.key("prekey").string(&format!("{:016x}", t.prekey));
    opt(w, "key", t.key.map(|k| format!("{k:016x}")).as_deref());
    w.key("reused").bool(t.reused);
    opt(w, "existing_description", t.existing.as_deref());
    opt(w, "description", t.description.as_deref());
    w.key("grain").begin_array();
    for g in &t.grain {
        w.string(g);
    }
    w.end_array();
    w.key("filters").begin_array();
    for f in &t.filters {
        w.string(f);
    }
    w.end_array();
    w.key("staged_through").begin_array();
    for x in &t.staged {
        w.string(x);
    }
    w.end_array();
    w.key("producer");
    match &t.producer {
        Some(p) => {
            w.begin_object()
                .key("statement_type")
                .string(&p.statement_type)
                .key("writes")
                .uint(p.writes)
                .key("writers")
                .uint(p.writers);
            w.key("first_ms")
                .uint(p.first_ms)
                .key("last_ms")
                .uint(p.last_ms)
                .key("sql")
                .string(&p.query);
            w.key("other_writers").begin_array();
            for o in &p.others {
                w.begin_object()
                    .key("statement_type")
                    .string(&o.statement_type)
                    .key("writes")
                    .uint(o.writes)
                    .key("writers")
                    .uint(o.writers)
                    .key("first_ms")
                    .uint(o.first_ms)
                    .key("last_ms")
                    .uint(o.last_ms)
                    .key("sql")
                    .string(&o.query)
                    .end_object();
            }
            w.end_array().end_object();
        }
        None => {
            w.null();
        }
    }
    w.key("columns").begin_array();
    for c in &t.columns {
        w.begin_object()
            .key("column")
            .string(&c.column)
            .key("data_type")
            .string(&c.data_type)
            .key("action")
            .string(c.action.as_str());
        opt(w, "existing_description", c.existing.as_deref());
        opt(w, "description", c.description.as_deref());
        opt(w, "semantic_type", c.semantic_type.as_deref());
        opt(w, "unit", c.unit.as_deref());
        opt(w, "confidence", c.confidence.as_deref());
        opt(
            w,
            "transformation",
            c.transform.map(|x| format!("{x:?}").to_ascii_lowercase()).as_deref(),
        );
        opt(w, "expression", c.expression.as_deref());
        w.key("sources").begin_array();
        for s in &c.sources {
            w.string(s);
        }
        w.end_array();
        w.key("hops").begin_array();
        for h in &c.hops {
            w.string(h);
        }
        w.end_array();
        w.key("checks").begin_array();
        for id in &c.checks {
            w.uint(u64::from(*id));
        }
        w.end_array();
        w.key("consumers").begin_array();
        for x in &c.consumers {
            w.string(x);
        }
        w.end_array().end_object();
    }
    w.end_array();
    if let Some(carried) = &t.carried_checks {
        w.key("checks").raw(carried);
    } else {
        checks(w, t);
    }
    w.key("warnings").begin_array();
    for x in &t.warnings {
        w.string(x);
    }
    w.end_array();
    spend(w, "spend", t.spend);
    w.end_object();
}

fn checks(w: &mut Writer<'_>, t: &TableReport) {
    w.key("checks").begin_array();
    for c in &t.checks {
        let r = &c.result;
        w.begin_object()
            .key("id")
            .uint(u64::from(r.check.id))
            .key("round")
            .uint(u64::from(c.round));
        opt(w, "column", r.check.column.as_deref());
        w.key("claim").string(&r.check.claim);
        match &r.check.sql {
            CheckSql::Aggregate(s) => w.key("expr").string(s),
            CheckSql::Query(s) => w.key("sql").string(s),
        };
        match &r.outcome {
            Outcome::Value(v) => w.key("result").string(v),
            Outcome::Rows(rows) => {
                w.key("rows").begin_array();
                for row in rows {
                    w.begin_array();
                    for cell in row {
                        w.string(cell);
                    }
                    w.end_array();
                }
                w.end_array()
            }
            Outcome::Rejected(why) => w.key("rejected").string(why),
            Outcome::Failed(why) => w.key("failed").string(why),
        };
        w.key("sampled")
            .bool(r.sampled)
            .key("tests_existing")
            .bool(r.check.tests_existing)
            .key("disputed")
            .bool(c.disputed);
        opt(w, "expect", r.check.expect.as_deref());
        opt(w, "verdict", c.verdict.as_deref());
        opt(w, "reason", c.reason.as_deref());
        w.end_object();
    }
    w.end_array();
}

/// Column descriptions to write for `t`: generated and propagated ones, and
/// flagged corrections when `correct`.
#[must_use]
pub fn updates(t: &TableReport, correct: bool) -> BTreeMap<String, String> {
    t.columns
        .iter()
        .filter(|c| {
            matches!(c.action, Action::Generate | Action::Propagate)
                || (correct && c.action == Action::Flag)
        })
        .filter_map(|c| c.description.clone().map(|d| (c.column.clone(), d)))
        .collect()
}

/// Overlay `r`'s descriptions onto a catalog before it is built, so the
/// text-to-SQL engine renders them in schema context and links on them:
/// generated and propagated columns, flagged columns' evidence-backed
/// corrections (unless low confidence), and new table descriptions.
/// Confirmed and kept descriptions are already there. Returns the number of
/// descriptions set.
pub fn overlay(r: &Report, b: &mut CatalogBuilder) -> usize {
    let mut n = 0;
    for t in &r.tables {
        let mut parts = t.table.rsplitn(3, '.');
        let (Some(name), Some(ds), Some(project)) = (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        if let (Some(d), None) = (&t.description, &t.existing) {
            n += usize::from(b.set_description(project, ds, name, None, d));
        }
        for c in &t.columns {
            let use_it = match c.action {
                Action::Generate | Action::Propagate => true,
                Action::Flag => {
                    c.confidence.as_deref() != Some("low") && c.description != c.existing
                }
                _ => false,
            };
            if let (true, Some(d)) = (use_it, &c.description) {
                n += usize::from(b.set_description(project, ds, name, Some(&c.column), d));
            }
        }
    }
    n
}

/// The run as a metadata image (see `crab-meta`): every table and column
/// with its description, provenance, confidence, transform, expression and
/// producing SQL, and column lineage in both directions — sources outside
/// the described datasets become external nodes.
pub fn meta_image(r: &Report) -> Result<Vec<u8>, crab_meta::Error> {
    use crab_meta::{ColumnEntry, Confidence, MetaBuilder, Provenance, TableEntry, Transform};
    let mut b = MetaBuilder::new();
    for t in &r.tables {
        let provenance = match (&t.description, &t.existing) {
            (Some(d), Some(e)) if d == e => Provenance::Declared,
            (Some(_), _) => Provenance::Generated,
            _ => Provenance::Unknown,
        };
        let columns = t
            .columns
            .iter()
            .map(|c| ColumnEntry {
                path: c.column.clone(),
                data_type: c.data_type.clone(),
                description: c.description.clone(),
                provenance: match c.action {
                    Action::Keep => Provenance::Declared,
                    Action::Confirm => Provenance::Confirmed,
                    Action::Flag => Provenance::Flagged,
                    Action::Propagate => Provenance::Propagated,
                    Action::Generate => Provenance::Generated,
                    Action::Skip if c.existing.is_some() => Provenance::Declared,
                    Action::Skip => Provenance::Unknown,
                },
                confidence: Confidence::parse(c.confidence.as_deref().unwrap_or("")),
                transform: c.transform.map_or(Transform::Unknown, |x| {
                    Transform::parse(&format!("{x:?}").to_ascii_lowercase())
                }),
                expression: c.expression.clone(),
            })
            .collect();
        b.table(
            TableEntry {
                name: t.table.clone(),
                kind: t.kind.clone(),
                description: t.description.clone(),
                provenance,
                statement_type: t.producer.as_ref().map(|p| p.statement_type.clone()),
                producer_sql: t.producer.as_ref().map(|p| p.query.clone()),
                rows: t.rows,
                last_modified_ms: t.last_modified_ms,
            },
            columns,
        );
        for c in &t.columns {
            // A single source copied as is (by the SQL, or as the lineage
            // service recorded it) is an exact copy.
            let copy = c.sources.len() == 1
                && matches!(
                    c.transform,
                    Some(crab_ir::Transform::Passthrough | crab_ir::Transform::Rename) | None
                );
            for s in &c.sources {
                if let Some((st, sc)) = s.rsplit_once('.') {
                    b.edge((&t.table, &c.column), (st, sc), copy);
                }
            }
            for k in &c.consumers {
                if let Some((kt, kc)) = k.rsplit_once('.') {
                    b.edge((kt, kc), (&t.table, &c.column), false);
                }
            }
        }
    }
    b.build()
}

/// The `fields` array of a schema object, as written.
fn fields_array(schema: &str) -> Option<&str> {
    let mut s = Scanner::new(schema);
    if s.expect().ok()? != Event::ObjectStart {
        return None;
    }
    loop {
        match s.expect().ok()? {
            Event::Key(k) if k.eq_plain("fields") => {
                let before = s.offset();
                let v = s.expect().ok()?;
                let start = before + schema[before..].find('[')?;
                s.skip(v).ok()?;
                return Some(&schema[start..s.offset()]);
            }
            Event::Key(_) => {
                let v = s.expect().ok()?;
                s.skip(v).ok()?;
            }
            _ => return None,
        }
    }
}

/// A BigQuery schema file (the `fields` array, as Terraform's
/// `schema = file(...)` and `bq mk --schema` take it) with the new
/// descriptions spliced into the exact schema that was read.
#[must_use]
pub fn schema_file(t: &TableReport, correct: bool) -> Option<String> {
    let meta = t.meta.as_ref()?;
    let (spliced, _) = splice(meta.schema_json(), &updates(t, correct)).ok()?;
    fields_array(&spliced).map(str::to_owned)
}

/// Existing descriptions of a table resource, by lower-cased path.
fn current_descriptions(meta: &TableMeta) -> BTreeMap<String, String> {
    crate::describe::schema_columns(meta)
        .into_iter()
        .filter_map(|c| {
            c.description.filter(|d| !d.trim().is_empty()).map(|d| (c.path.to_ascii_lowercase(), d))
        })
        .collect()
}

/// Outcome of applying one table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Applied {
    /// Nothing to write.
    Unchanged,
    /// Descriptions written: columns, and whether the table description.
    Written {
        /// Columns written.
        columns: usize,
        /// Table description written.
        table: bool,
    },
    /// The table changed while applying (etag mismatch): re-run describe.
    Conflict,
    /// Failed.
    Failed(String),
}

/// Write `t`'s descriptions to the live table: re-read it, fill only
/// descriptions still empty (flagged corrections only when `correct`), and
/// patch under `If-Match`.
pub async fn apply<W: TableMetadata>(w: &W, t: &TableReport, correct: bool) -> Applied {
    let mut parts = t.table.rsplitn(3, '.');
    let (Some(name), Some(ds), Some(project)) = (parts.next(), parts.next(), parts.next()) else {
        return Applied::Failed("bad table name".into());
    };
    let fresh = match w.table_get(project, ds, name).await {
        Ok(Some(m)) => m,
        Ok(None) => return Applied::Failed("table does not exist".into()),
        Err(e) => return Applied::Failed(e.to_string()),
    };
    let now = current_descriptions(&fresh);
    let flagged: Vec<&str> =
        t.columns.iter().filter(|c| c.action == Action::Flag).map(|c| c.column.as_str()).collect();
    let cols: BTreeMap<String, String> = updates(t, correct)
        .into_iter()
        .filter(|(c, _)| !now.contains_key(c) || (correct && flagged.contains(&c.as_str())))
        .collect();
    let table_desc = t.description.as_deref().filter(|_| fresh.description.is_none());
    if cols.is_empty() && table_desc.is_none() {
        return Applied::Unchanged;
    }
    let (schema, n) = match splice(fresh.schema_json(), &cols) {
        Ok(x) => x,
        Err(e) => return Applied::Failed(e.to_string()),
    };
    let mut body = String::with_capacity(schema.len() + 256);
    body.push_str("{\"schema\":");
    body.push_str(&schema);
    if let Some(d) = table_desc {
        body.push_str(",\"description\":");
        crab_core::json::escape_into(d, &mut body);
    }
    body.push('}');
    match w.table_patch(project, ds, name, objstore_r::Bytes::from(body), &fresh.etag).await {
        Ok(_) => Applied::Written { columns: n, table: table_desc.is_some() },
        Err(crab_exec::Error::PreconditionFailed) => Applied::Conflict,
        Err(e) => Applied::Failed(e.to_string()),
    }
}
