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
//!
//! All network work runs on one process-wide multi-thread Tokio runtime,
//! created on first use and reused by every call (and its HTTP connections
//! with it). `table_meta_async` and `describe_offline_async` are awaitables
//! on it; cancelling the Python future drops the Rust future, which cancels
//! its requests.
//!
//! Credentials: `access_token=` is used as given, nothing is discovered and
//! the instance metadata server is never asked. Without it, Application
//! Default Credentials are discovered with the metadata server forbidden
//! (`metadata_server=True` allows it).
//!
//! A store that cannot be read is regenerated, never an error: `describe`,
//! `ingest_lineage` and `prune` rebuild it (`"regenerated": True` in what
//! they return), and a lookup answers `"regenerate_needed: nothing
//! recorded ..."`; `regenerate_needed(store)` asks directly. When another
//! process holds the store's files so it can be neither deleted nor renamed
//! aside, the writer reports `"regenerate_pending": True` instead and
//! changes nothing.

use crab::describe::app::{self, Credentials};
use crab::exec::table_meta::{self as tm, MetaClient, MetaOptions, Outcome};
use objstore_r::token::{Adc, MetadataPolicy};
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use std::sync::OnceLock;

create_exception!(crab_r, CrabError, pyo3::exceptions::PyException);

/// Flags that switch a default off when given `False`.
const NEGATED: [(&str, &str); 2] = [("profile", "--no-profile"), ("audit", "--no-audit")];

/// The one runtime every call shares, created on first use.
fn runtime() -> PyResult<&'static tokio::runtime::Runtime> {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    if let Some(rt) = RT.get() {
        return Ok(rt);
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(std::thread::available_parallelism().map_or(2, |n| n.get().clamp(2, 8)))
        .thread_name("crab-r")
        .enable_all()
        .build()
        .map_err(|e| CrabError::new_err(format!("runtime: {e}")))?;
    let rt = RT.get_or_init(|| rt);
    // Awaitables run on this same runtime. A second call finds it set.
    let _ = pyo3_async_runtimes::tokio::init_with_runtime(rt);
    Ok(rt)
}

/// An identifier of the shared runtime: the same for every call in a
/// process (for tests).
#[pyfunction]
fn _runtime_id() -> PyResult<usize> {
    Ok(std::ptr::from_ref(runtime()?) as usize)
}

/// Run the future `make` builds to completion on the shared runtime,
/// detached from the interpreter.
fn block<T: Send, Fut: std::future::Future<Output = T>>(
    py: Python<'_>,
    make: impl FnOnce() -> Fut + Send,
) -> PyResult<T> {
    let rt = runtime()?;
    Ok(py.detach(|| rt.block_on(make())))
}

/// An awaitable of the future `make` builds. The future (which need not be
/// `Send`) is built and run on one of the shared runtime's blocking threads;
/// the awaitable waits for its result. Cancelling the awaitable drops the
/// future where it runs, and with it every request in flight.
fn awaitable<'py, T, Fut>(
    py: Python<'py>,
    make: impl FnOnce() -> Fut + Send + 'static,
    finish: fn(Python<'_>, T) -> PyResult<Py<PyAny>>,
) -> PyResult<Bound<'py, PyAny>>
where
    Fut: std::future::Future<Output = T>,
    T: Send + 'static,
{
    use std::future::Future as _;
    use std::task::Poll;
    let rt = runtime()?;
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<T>();
    let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = rt.handle().clone();
    rt.spawn_blocking(move || {
        let fut = make();
        let out = handle.block_on(async move {
            let mut fut = std::pin::pin!(fut);
            std::future::poll_fn(|cx| {
                // The awaitable is gone (cancelled): stop here, dropping
                // the run.
                if std::pin::Pin::new(&mut cancel_rx).poll(cx).is_ready() {
                    return Poll::Ready(None);
                }
                fut.as_mut().poll(cx).map(Some)
            })
            .await
        });
        if let Some(v) = out {
            let _ = done_tx.send(v);
        }
    });
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        // Dropped with this future when Python cancels it.
        let _cancel_on_drop = cancel_tx;
        let v = done_rx.await.map_err(|_| CrabError::new_err("cancelled"))?;
        Python::attach(|py| finish(py, v))
    })
}

/// The credential options a call takes: `access_token`, then whether
/// discovery may ask the metadata server.
fn credentials(access_token: Option<String>, metadata_server: bool) -> Credentials {
    let policy = if metadata_server { MetadataPolicy::Allow } else { MetadataPolicy::Forbid };
    Credentials::new(access_token, policy)
}

/// Remove `name` from `options`, if given.
fn take<T: for<'a, 'py> FromPyObject<'a, 'py>>(
    options: Option<&Bound<'_, PyDict>>,
    name: &str,
) -> PyResult<Option<T>> {
    let Some(o) = options else { return Ok(None) };
    let Some(v) = o.get_item(name)? else { return Ok(None) };
    o.del_item(name)?;
    if v.is_none() {
        return Ok(None);
    }
    v.extract().map(Some).map_err(Into::into)
}

/// The command's arguments for `targets` and `options`; the SQL file to
/// delete afterwards, when `sql=` was given.
fn argv(
    targets: &Bound<'_, PyTuple>,
    options: Option<&Bound<'_, PyDict>>,
) -> PyResult<(Vec<String>, Option<std::path::PathBuf>)> {
    let mut out: Vec<String> = Vec::new();
    for t in targets.iter() {
        out.push(t.extract()?);
    }
    let Some(options) = options else { return Ok((out, None)) };
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
    let mut file = None;
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
        file = Some(path);
    }
    Ok((out, file))
}

fn report_dict<'py>(
    py: Python<'py>,
    run: Result<crab::describe::describe::Report, String>,
    log: &[String],
) -> PyResult<Bound<'py, PyAny>> {
    let logger = py.import("logging")?.call_method1("getLogger", ("crab_r",))?;
    for line in log {
        logger.call_method1("info", (line,))?;
    }
    let report = run.map_err(CrabError::new_err)?;
    let json = crab::describe::report::to_json(&report);
    py.import("json")?.call_method1("loads", (json,))
}

/// Describe BigQuery projects, datasets, tables or SQL; returns the report.
///
/// ```python
/// crab_r.describe("my-project.mart.orders", upstream=1, store="state", access_token=tok)
/// crab_r.describe(sql="SELECT ...", default_dataset="my-project.mart")
/// ```
#[pyfunction]
#[pyo3(signature = (*targets, **options))]
fn describe<'py>(
    py: Python<'py>,
    targets: &Bound<'py, PyTuple>,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let token: Option<String> = take(options, "access_token")?;
    let metadata: bool = take(options, "metadata_server")?.unwrap_or(false);
    let (args, sql_file) = argv(targets, options)?;
    let parsed = app::parse(args);
    let opts = parsed.map_err(pyo3::exceptions::PyValueError::new_err)?;
    let opts = opts.with_credentials(credentials(token, metadata));
    let rt = runtime()?;
    let (run, log) = py.detach(|| {
        let mut log: Vec<String> = Vec::new();
        let run = rt.block_on(app::run(&opts, &mut |line| log.push(line.to_owned())));
        (run, log)
    });
    if let Some(f) = sql_file {
        let _ = std::fs::remove_file(f);
    }
    report_dict(py, run, &log)
}

fn look(
    py: Python<'_>,
    cmd: &str,
    args: Vec<String>,
    budget_tokens: Option<u32>,
) -> PyResult<String> {
    let budget = budget_tokens.map(graph_r::TokenBudget);
    py.detach(|| app::lookup_within(cmd, &args, budget)).map_err(CrabError::new_err)
}

/// A table recorded in `store`: its description, kind, producer, columns.
/// With `budget_tokens`, the answer fits that many tokens (whole lines,
/// then a `truncated:` line).
#[pyfunction]
#[pyo3(signature = (store, name, budget_tokens = None))]
fn table(
    py: Python<'_>,
    store: String,
    name: String,
    budget_tokens: Option<u32>,
) -> PyResult<String> {
    look(py, "table", vec![store, name], budget_tokens)
}

/// A column recorded in `store` (`"project.dataset.table.column"`): its
/// description, type, provenance, sources and consumers.
#[pyfunction]
#[pyo3(signature = (store, column, budget_tokens = None))]
fn lookup(
    py: Python<'_>,
    store: String,
    column: String,
    budget_tokens: Option<u32>,
) -> PyResult<String> {
    look(py, "lookup", vec![store, column], budget_tokens)
}

/// Where a column's values come from, `depth` hops up.
#[pyfunction]
#[pyo3(signature = (store, column, depth = 10, budget_tokens = None))]
fn upstream(
    py: Python<'_>,
    store: String,
    column: String,
    depth: u32,
    budget_tokens: Option<u32>,
) -> PyResult<String> {
    look(py, "upstream", vec![store, column, depth.to_string()], budget_tokens)
}

/// What a column's values and rows feed, `depth` hops down.
#[pyfunction]
#[pyo3(signature = (store, column, depth = 10, budget_tokens = None))]
fn downstream(
    py: Python<'_>,
    store: String,
    column: String,
    depth: u32,
    budget_tokens: Option<u32>,
) -> PyResult<String> {
    look(py, "downstream", vec![store, column, depth.to_string()], budget_tokens)
}

/// The `n` tables most overdue for a re-describe.
#[pyfunction]
#[pyo3(signature = (store, n = 20, budget_tokens = None))]
fn due(py: Python<'_>, store: String, n: usize, budget_tokens: Option<u32>) -> PyResult<String> {
    look(py, "due", vec![store, n.to_string()], budget_tokens)
}

/// The store's tables and columns best matching `text` (names and
/// descriptions), ranked, at most `n`, within `budget_tokens`.
#[pyfunction]
#[pyo3(signature = (store, text, n = 20, budget_tokens = None))]
fn search(
    py: Python<'_>,
    store: String,
    text: String,
    n: usize,
    budget_tokens: Option<u32>,
) -> PyResult<String> {
    look(py, "search", vec![store, text, n.to_string()], budget_tokens)
}

/// Write a static lineage export's `nodes` and `edges` (lists of dicts, as
/// the export has them) into `store`, created when absent, so lookups answer
/// for the declared tables with no network and a later `describe` enriches
/// the same nodes. Adds only; one commit or none. Returns
/// `{"committed", "created", "updated", "edges", "skipped": {name: n},
/// "rejected": [{"side", "index", "reason"}], "regenerated",
/// "regenerate_pending"}`.
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
    if done.regenerated {
        regenerated_warning(py, &store)?;
    }
    json.call_method1("loads", (done.to_json(),))
}

/// The one line a regeneration logs, on the `crab_r` logger.
fn regenerated_warning(py: Python<'_>, store: &str) -> PyResult<()> {
    py.import("logging")?.call_method1("getLogger", ("crab_r",))?.call_method1(
        "warning",
        (format!("store {store} could not be read; regenerated empty"),),
    )?;
    Ok(())
}

/// Remove from `store` the tables no `describe` or `ingest_lineage` has
/// written or touched in `max_age_days` days, with their columns: one
/// transaction and one compaction, nothing written when nothing is that old.
/// Pinned nodes stay. Returns `{"tables", "columns", "regenerated",
/// "regenerate_pending"}`.
#[pyfunction]
fn prune<'py>(py: Python<'py>, store: String, max_age_days: u32) -> PyResult<Bound<'py, PyDict>> {
    use crab::describe::lifecycle::Regen;
    let (p, regen) = py
        .detach(|| crab::describe::lifecycle::prune(&store, max_age_days))
        .map_err(|e| CrabError::new_err(format!("{store}: {e}")))?;
    if regen == Regen::Done {
        regenerated_warning(py, &store)?;
    }
    let out = PyDict::new(py);
    out.set_item("tables", p.tables)?;
    out.set_item("columns", p.columns)?;
    out.set_item("regenerated", regen == Regen::Done)?;
    out.set_item("regenerate_pending", regen == Regen::Pending)?;
    Ok(out)
}

/// `store` exists but cannot be read: lookups answer nothing recorded, and
/// the next `describe`, `ingest_lineage` or `prune` regenerates it.
#[pyfunction]
fn regenerate_needed(py: Python<'_>, store: String) -> bool {
    py.detach(|| crab::describe::lifecycle::regenerate_needed(&store))
}

fn name(s: &str) -> PyResult<crab::base::key::BqName<'_>> {
    crab::base::key::BqName::parse(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

/// A BigQuery name, given in any spelling, as a lineage export's id:
/// `bq://project/dataset/table#column` (the project lowercased).
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
#[pyo3(signature = (source, directory, access_token = None, metadata_server = false))]
fn pull(
    py: Python<'_>,
    source: String,
    directory: String,
    access_token: Option<String>,
    metadata_server: bool,
) -> PyResult<String> {
    let creds = credentials(access_token, metadata_server);
    block(py, || app::pull(&source, &directory, &creds))?.map_err(CrabError::new_err)
}

/// The one `INFORMATION_SCHEMA.JOBS` projection, as SQL: what the
/// `ops.observed_lineage` and `ops.runs` views are generated from.
#[pyfunction]
#[pyo3(signature = (jobs_project, region, lookback_days = 180))]
fn jobs_projection_sql(jobs_project: &str, region: &str, lookback_days: u32) -> PyResult<String> {
    crab::describe::producers::jobs_projection_sql(jobs_project, region, lookback_days).ok_or_else(
        || pyo3::exceptions::PyValueError::new_err("a project or region that is not a plain name"),
    )
}

/// What `table_meta` needs, owned, so its future can outlive the call.
struct MetaCall {
    tables: Vec<String>,
    omit: Vec<String>,
    include_view_sql: bool,
    adc: Result<Adc, objstore_r::Error>,
}

fn meta_call(
    tables: Vec<String>,
    include_view_sql: bool,
    omit_columns: Option<Vec<String>>,
    access_token: Option<String>,
    metadata_server: bool,
) -> MetaCall {
    let adc = match access_token {
        Some(t) => Adc::from_access_token(t),
        None => Adc::discover_with(if metadata_server {
            MetadataPolicy::Allow
        } else {
            MetadataPolicy::Forbid
        }),
    };
    MetaCall { tables, omit: omit_columns.unwrap_or_default(), include_view_sql, adc }
}

/// The batch, as one JSON array; never fails.
async fn run_meta(call: MetaCall) -> String {
    let names: Vec<&str> = call.tables.iter().map(String::as_str).collect();
    let omit: Vec<&str> = call.omit.iter().map(String::as_str).collect();
    let opts = MetaOptions {
        include_view_sql: call.include_view_sql,
        omit_columns: &omit,
        ..MetaOptions::default()
    };
    let answers = match call.adc {
        Err(e) => tm::all_failed(&names, Outcome::of_error(&e), "no usable credential"),
        Ok(adc) => {
            let guard = crab::exec::guard::MetadataGuard::default();
            match MetaClient::new(adc, guard, None, &opts.limits) {
                Ok(c) => c.table_meta(&names, &opts).await,
                Err(e) => tm::all_failed(&names, Outcome::Unreachable, &e.to_string()),
            }
        }
    };
    tm::answers_json(&answers)
}

fn loads<'py>(py: Python<'py>, json: &str) -> PyResult<Bound<'py, PyAny>> {
    py.import("json")?.call_method1("loads", (json,))
}

/// Exact metadata of `tables` (`"project.dataset.table"`), read at once:
/// one dict per table, in order, `{"table", "available", "reason", ...}`.
/// `reason` is `ok` (with `type`, `location`, `description`, `labels`,
/// partitioning, `clustering`, `schema` as BigQuery's own JSON with
/// `omit_columns` removed, `columns`, `omitted`, `truncated_descriptions`,
/// and `view_sql` when asked), or `no_access`, `not_found`,
/// `no_credentials`, `expired`, `unreachable`, `invalid_name` (with a
/// `message`). Never raises for a table; nothing is retried on a 4xx.
#[pyfunction]
#[pyo3(signature = (tables, include_view_sql = false, omit_columns = None, access_token = None, metadata_server = false))]
fn table_meta<'py>(
    py: Python<'py>,
    tables: Vec<String>,
    include_view_sql: bool,
    omit_columns: Option<Vec<String>>,
    access_token: Option<String>,
    metadata_server: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let call = meta_call(tables, include_view_sql, omit_columns, access_token, metadata_server);
    let json = block(py, move || run_meta(call))?;
    loads(py, &json)
}

/// [`table_meta`] as an awaitable on the shared runtime; cancelling it
/// cancels the requests.
#[pyfunction]
#[pyo3(signature = (tables, include_view_sql = false, omit_columns = None, access_token = None, metadata_server = false))]
fn table_meta_async<'py>(
    py: Python<'py>,
    tables: Vec<String>,
    include_view_sql: bool,
    omit_columns: Option<Vec<String>>,
    access_token: Option<String>,
    metadata_server: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let call = meta_call(tables, include_view_sql, omit_columns, access_token, metadata_server);
    awaitable(py, move || run_meta(call), |py, json: String| loads(py, &json).map(Bound::unbind))
}

/// The options and catalog of a drafting run.
fn offline_call(
    store: Option<String>,
    catalog_metadata: &Bound<'_, PyAny>,
    options: Option<&Bound<'_, PyDict>>,
) -> PyResult<(app::Options, String)> {
    let py = catalog_metadata.py();
    let catalog: String = if let Ok(s) = catalog_metadata.extract::<String>() {
        s
    } else {
        py.import("json")?.call_method1("dumps", (catalog_metadata,))?.extract().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err("catalog_metadata: not JSON data")
        })?
    };
    let supplied = crab::describe::offline::parse_catalog(&catalog, app::MAX_OFFLINE_TABLES)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    let names: Vec<String> = supplied.into_iter().map(|(t, _)| t).collect();
    let targets = PyTuple::new(py, &names)?;
    let (mut args, _) = argv(&targets, options)?;
    if let Some(s) = store {
        args.push("--store".into());
        args.push(s);
    }
    let opts = app::parse(args).map_err(pyo3::exceptions::PyValueError::new_err)?;
    Ok((opts, catalog))
}

/// Draft descriptions for the tables of `catalog_metadata` (what
/// `table_meta` returned, as a list or its JSON), with no Google client:
/// an OpenAI-compatible model (`openai_url=`, `openai_model=`,
/// `openai_key_env=`, prices) reached through a client that may contact
/// only the model's host. `store` is reused (unchanged tables are not
/// drafted again; its lineage and descriptions are inherited) and the
/// drafts are recorded in it. Writes nothing anywhere else. Returns the
/// report.
#[pyfunction]
#[pyo3(signature = (store, catalog_metadata, **options))]
fn describe_offline<'py>(
    py: Python<'py>,
    store: Option<String>,
    catalog_metadata: &Bound<'py, PyAny>,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let (opts, catalog) = offline_call(store, catalog_metadata, options)?;
    let rt = runtime()?;
    let (run, log) = py.detach(|| {
        let mut log: Vec<String> = Vec::new();
        let run = rt.block_on(app::run_offline(&opts, &catalog, &mut |l| log.push(l.to_owned())));
        (run, log)
    });
    report_dict(py, run, &log)
}

/// [`describe_offline`] as an awaitable on the shared runtime; cancelling
/// it cancels the model's requests.
#[pyfunction]
#[pyo3(signature = (store, catalog_metadata, **options))]
fn describe_offline_async<'py>(
    py: Python<'py>,
    store: Option<String>,
    catalog_metadata: &Bound<'py, PyAny>,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let (opts, catalog) = offline_call(store, catalog_metadata, options)?;
    awaitable(
        py,
        move || async move {
            let mut log: Vec<String> = Vec::new();
            let run = app::run_offline(&opts, &catalog, &mut |l| log.push(l.to_owned())).await;
            (run, log)
        },
        |py, (run, log)| report_dict(py, run, &log).map(Bound::unbind),
    )
}

#[pymodule]
fn crab_r(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(describe, m)?)?;
    m.add_function(wrap_pyfunction!(describe_offline, m)?)?;
    m.add_function(wrap_pyfunction!(describe_offline_async, m)?)?;
    m.add_function(wrap_pyfunction!(table_meta, m)?)?;
    m.add_function(wrap_pyfunction!(table_meta_async, m)?)?;
    m.add_function(wrap_pyfunction!(jobs_projection_sql, m)?)?;
    m.add_function(wrap_pyfunction!(table, m)?)?;
    m.add_function(wrap_pyfunction!(lookup, m)?)?;
    m.add_function(wrap_pyfunction!(upstream, m)?)?;
    m.add_function(wrap_pyfunction!(downstream, m)?)?;
    m.add_function(wrap_pyfunction!(due, m)?)?;
    m.add_function(wrap_pyfunction!(search, m)?)?;
    m.add_function(wrap_pyfunction!(pull, m)?)?;
    m.add_function(wrap_pyfunction!(ingest_lineage, m)?)?;
    m.add_function(wrap_pyfunction!(prune, m)?)?;
    m.add_function(wrap_pyfunction!(regenerate_needed, m)?)?;
    m.add_function(wrap_pyfunction!(bq_id, m)?)?;
    m.add_function(wrap_pyfunction!(bq_path, m)?)?;
    m.add_function(wrap_pyfunction!(dotted, m)?)?;
    m.add_function(wrap_pyfunction!(_runtime_id, m)?)?;
    m.add("CrabError", m.py().get_type::<CrabError>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
