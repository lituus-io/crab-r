// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! `crab_r`: the `crab-describe` command as a Python module.
//!
//! `describe` takes what the command takes — projects, datasets, tables
//! (`"project.dataset.table"`), and SQL text — as arguments, and options as
//! keyword arguments named like the command's flags (`budget_usd=0.5` is
//! `--budget-usd 0.5`; `profile=False` is `--no-profile`). It returns the
//! report as a dict. The lookups read a store, no network. Every call
//! detaches from the interpreter while Rust works.

use crab::describe::app;
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

create_exception!(crab_r, CrabError, pyo3::exceptions::PyException);

/// Flags that switch a default off when given `False`.
const NEGATED: [(&str, &str); 2] = [("profile", "--no-profile"), ("audit", "--no-audit")];

/// The command's arguments for `targets` and `options`.
fn argv(
    targets: &Bound<'_, PyTuple>,
    options: Option<&Bound<'_, PyDict>>,
) -> PyResult<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for t in targets.iter() {
        out.push(t.extract()?);
    }
    let Some(options) = options else { return Ok(out) };
    let mut sql: Option<String> = None;
    for (k, v) in options.iter() {
        let key: String = k.extract()?;
        if v.is_none() {
            continue;
        }
        if key == "sql" {
            sql = Some(v.extract()?);
            continue;
        }
        let flag = format!("--{}", key.replace('_', "-"));
        if let Ok(b) = v.extract::<bool>() {
            match NEGATED.iter().find(|(name, _)| *name == key) {
                Some((_, off)) if !b => out.push((*off).to_owned()),
                Some(_) => {}
                None if b => out.push(flag),
                None => {}
            }
        } else {
            out.push(flag);
            out.push(v.str()?.to_string());
        }
    }
    if let Some(sql) = sql {
        // SQL travels through a private file so any text, however long, is
        // passed verbatim.
        let path = std::env::temp_dir().join(format!(
            "crab-r-{}-{}.sql",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        std::fs::write(&path, sql).map_err(|e| CrabError::new_err(format!("sql: {e}")))?;
        out.push("--sql".into());
        out.push(path.to_string_lossy().into_owned());
    }
    Ok(out)
}

/// Describe BigQuery projects, datasets, tables or SQL; returns the report.
///
/// ```python
/// crab_r.describe("my-project.mart.orders", upstream=1, store="state")
/// crab_r.describe(sql="SELECT ...", default_dataset="my-project.mart")
/// ```
#[pyfunction]
#[pyo3(signature = (*targets, **options))]
fn describe<'py>(
    py: Python<'py>,
    targets: &Bound<'py, PyTuple>,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let args = argv(targets, options)?;
    let sql_file = args.iter().position(|a| a == "--sql").and_then(|i| args.get(i + 1).cloned());
    let opts = app::parse(args).map_err(pyo3::exceptions::PyValueError::new_err)?;
    let (run, log) = py.detach(|| {
        let mut log: Vec<String> = Vec::new();
        let run =
            objstore_r::http::block_on(app::run(&opts, &mut |line| log.push(line.to_owned())));
        (run, log)
    });
    if let Some(f) = sql_file {
        let _ = std::fs::remove_file(f);
    }
    let logger = py.import("logging")?.call_method1("getLogger", ("crab_r",))?;
    for line in &log {
        logger.call_method1("info", (line,))?;
    }
    let report = run
        .map_err(|e| CrabError::new_err(format!("runtime: {e}")))?
        .map_err(CrabError::new_err)?;
    let json = crab::describe::report::to_json(&report);
    py.import("json")?.call_method1("loads", (json,))
}

fn look(py: Python<'_>, cmd: &str, args: Vec<String>) -> PyResult<String> {
    py.detach(|| app::lookup(cmd, &args)).map_err(CrabError::new_err)
}

/// A table recorded in `store`: its description, kind, producer, columns.
#[pyfunction]
fn table(py: Python<'_>, store: String, name: String) -> PyResult<String> {
    look(py, "table", vec![store, name])
}

/// A column recorded in `store` (`"project.dataset.table.column"`): its
/// description, type, provenance, sources and consumers.
#[pyfunction]
fn lookup(py: Python<'_>, store: String, column: String) -> PyResult<String> {
    look(py, "lookup", vec![store, column])
}

/// Where a column's values come from, `depth` hops up.
#[pyfunction]
#[pyo3(signature = (store, column, depth = 10))]
fn upstream(py: Python<'_>, store: String, column: String, depth: u32) -> PyResult<String> {
    look(py, "upstream", vec![store, column, depth.to_string()])
}

/// What a column's values and rows feed, `depth` hops down.
#[pyfunction]
#[pyo3(signature = (store, column, depth = 10))]
fn downstream(py: Python<'_>, store: String, column: String, depth: u32) -> PyResult<String> {
    look(py, "downstream", vec![store, column, depth.to_string()])
}

/// The `n` tables most overdue for a re-describe.
#[pyfunction]
#[pyo3(signature = (store, n = 20))]
fn due(py: Python<'_>, store: String, n: usize) -> PyResult<String> {
    look(py, "due", vec![store, n.to_string()])
}

/// Write a static lineage export's `nodes` and `edges` (lists of dicts, as
/// the export has them) into `store`, created when absent, so lookups answer
/// for the declared tables with no network and a later `describe` enriches
/// the same nodes. Adds only; one commit or none. Returns
/// `{"committed", "created", "updated", "edges", "skipped": {name: n},
/// "rejected": [{"side", "index", "reason"}]}`.
#[pyfunction]
fn ingest_lineage<'py>(
    py: Python<'py>,
    store: String,
    nodes: &Bound<'py, PyAny>,
    edges: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let json = py.import("json")?;
    let text = |v: &Bound<'py, PyAny>, what: &str| -> PyResult<String> {
        json.call_method1("dumps", (v,))
            .and_then(|s| s.extract())
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err(format!("{what}: not JSON data")))
    };
    let (nodes, edges) = (text(nodes, "nodes")?, text(edges, "edges")?);
    let done = py
        .detach(|| crab::describe::ingest::ingest_dir(&store, &nodes, &edges))
        .map_err(CrabError::new_err)?;
    json.call_method1("loads", (done.to_json(),))
}

fn name(s: &str) -> PyResult<crab::base::key::BqName<'_>> {
    crab::base::key::BqName::parse(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

/// A BigQuery name, given in any spelling, as a lineage export's id:
/// `bq://project/dataset/table#column`.
#[pyfunction]
fn bq_id(s: &str) -> PyResult<String> {
    Ok(name(s)?.bq_id())
}

/// A BigQuery name, given in any spelling, in the path form
/// `bq://project/dataset/table/column`: the key for a document store that
/// drops a url's `#fragment`.
#[pyfunction]
fn bq_path(s: &str) -> PyResult<String> {
    Ok(name(s)?.bq_path())
}

/// A BigQuery name, given in any spelling, as the warehouse spells it:
/// `project.dataset.table.column`.
#[pyfunction]
fn dotted(s: &str) -> PyResult<String> {
    Ok(name(s)?.dotted())
}

/// Fetch a published store (`gs://bucket/prefix` or a directory) into
/// `directory`, or bring it up to date with only what changed.
#[pyfunction]
fn pull(py: Python<'_>, source: String, directory: String) -> PyResult<String> {
    py.detach(|| objstore_r::http::block_on(app::pull(&source, &directory)))
        .map_err(|e| CrabError::new_err(format!("runtime: {e}")))?
        .map_err(CrabError::new_err)
}

#[pymodule]
fn crab_r(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(describe, m)?)?;
    m.add_function(wrap_pyfunction!(table, m)?)?;
    m.add_function(wrap_pyfunction!(lookup, m)?)?;
    m.add_function(wrap_pyfunction!(upstream, m)?)?;
    m.add_function(wrap_pyfunction!(downstream, m)?)?;
    m.add_function(wrap_pyfunction!(due, m)?)?;
    m.add_function(wrap_pyfunction!(pull, m)?)?;
    m.add_function(wrap_pyfunction!(ingest_lineage, m)?)?;
    m.add_function(wrap_pyfunction!(bq_id, m)?)?;
    m.add_function(wrap_pyfunction!(bq_path, m)?)?;
    m.add_function(wrap_pyfunction!(dotted, m)?)?;
    m.add("CrabError", m.py().get_type::<CrabError>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
