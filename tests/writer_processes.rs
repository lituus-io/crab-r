// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Describe saves, lineage ingests and readers on one store, as real
// processes. The parent test re-executes this test binary with `--exact
// child` and a role in the environment; `child` is a no-op without one, so a
// normal run passes it trivially. Children start together on a "go" file so
// their commits genuinely race, and compactions are forced into the race.

#![cfg(feature = "cli")]

use crab_r::describe::describe::{Action, ColumnReport, Report, TableReport};
use crab_r::describe::ingest::ingest;
use crab_r::describe::state;
use crab_r::lineage::Column;
use graph_r::{Config, Error, Store};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const ROLE: &str = "CRAB_R_WP_ROLE";
const ARGS: &str = "CRAB_R_WP_ARGS";
const NODES: &str = include_str!("data/static_lineage/nodes.json");
const EDGES: &str = include_str!("data/static_lineage/edges.json");
/// The export declares `proj.mart`; saves land there too, so a save and an
/// ingest replace the same containment set.
const DATASETS: [&str; 3] = ["mart", "d1", "d2"];

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("crab-wp-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn cfg(compact_every: usize) -> Config {
    Config { compact_after_ops: compact_every.max(1), ..Config::default() }
}

fn table_name(writer: usize, k: usize) -> String {
    format!("proj.{}.w{writer}_t{k}", DATASETS[(writer + k) % DATASETS.len()])
}

fn report(name: &str, key: u64) -> Report {
    let column = |c: &str| ColumnReport {
        column: c.into(),
        data_type: "INT64".into(),
        action: Action::Generate,
        existing: None,
        description: Some(format!("{name}.{c}")),
        semantic_type: None,
        unit: None,
        transform: None,
        expression: None,
        sources: vec![Column::new("proj.mart.orders", "id")],
        hops: vec![],
        confidence: Some("high".into()),
        checks: vec![],
        consumers: vec![],
    };
    Report {
        tables: vec![TableReport {
            table: name.into(),
            kind: "TABLE".into(),
            rows: 1,
            bytes: 1,
            last_modified_ms: 0,
            producer: None,
            existing: None,
            description: Some(format!("{name}, described.")),
            grain: vec![],
            filters: vec![],
            staged: vec![],
            conditions: vec![],
            columns: vec![column("a"), column("b")],
            checks: vec![],
            model_calls: 1,
            warnings: vec![],
            pending: false,
            spend: Default::default(),
            meta: None,
            prekey: key,
            key: Some(key),
            reused: false,
            carried_checks: None,
        }],
        ..Report::default()
    }
}

fn spawn(role: &str, args: &[String]) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child", "--nocapture", "--test-threads=1"])
        .env(ROLE, role)
        .env(ARGS, args.join("\n"))
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap()
}

fn wait_for(path: &Path) {
    let start = Instant::now();
    while !path.exists() {
        assert!(start.elapsed() < Duration::from_secs(120), "never released: {path:?}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn ready_then_go(d: &Path, id: &str) {
    std::fs::write(d.join(format!("ready.{id}")), b"").unwrap();
    wait_for(&d.join("go"));
}

fn go_when_ready(d: &Path, ids: &[String]) {
    for id in ids {
        wait_for(&d.join(format!("ready.{id}")));
    }
    std::fs::write(d.join("go"), b"").unwrap();
}

fn succeeded(c: &mut Child, what: &str) {
    let status = c.wait().unwrap();
    assert!(status.success(), "{what} failed: {status}");
}

/// Tables a writer recorded as saved (each line written after its save
/// returned, so durable).
fn saved(d: &Path, id: usize) -> Vec<String> {
    std::fs::read_to_string(d.join(format!("saved.{id}")))
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

/// Every table `load` offers for reuse is a complete record: both columns,
/// with the descriptions its save wrote.
fn assert_complete(store: &Store, tables: &[String]) {
    let prior = state::load(store);
    for t in tables {
        let rec = prior.get(t).unwrap_or_else(|| panic!("{t} was saved but is not recorded"));
        let mut cols: Vec<_> =
            rec.columns.iter().map(|c| (c.column.as_str(), c.description.as_deref())).collect();
        cols.sort_unstable();
        let (a, b) = (format!("{t}.a"), format!("{t}.b"));
        assert_eq!(cols, vec![("a", Some(a.as_str())), ("b", Some(b.as_str()))], "{t}");
    }
}

#[test]
fn child() {
    let Ok(role) = std::env::var(ROLE) else { return };
    let args: Vec<String> = std::env::var(ARGS).unwrap().lines().map(str::to_owned).collect();
    let d = PathBuf::from(&args[0]);
    match role.as_str() {
        // Save `m` tables, one save per table, noting each once it returned.
        "saves" => {
            let (id, m, every): (usize, usize, usize) =
                (args[1].parse().unwrap(), args[2].parse().unwrap(), args[3].parse().unwrap());
            let store = Store::open_or_create(&d, cfg(every)).unwrap();
            ready_then_go(&d, &format!("w{id}"));
            let mut log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(d.join(format!("saved.{id}")))
                .unwrap();
            for k in 0..m {
                let name = table_name(id, k);
                state::save(&store, &report(&name, (id * 10_000 + k) as u64 + 1), k as u64)
                    .unwrap();
                writeln!(log, "{name}").unwrap();
            }
        }
        // Ingest the export `m` times.
        "ingests" => {
            let (id, m, every): (String, usize, usize) =
                (args[1].clone(), args[2].parse().unwrap(), args[3].parse().unwrap());
            let store = Store::open_or_create(&d, cfg(every)).unwrap();
            ready_then_go(&d, &id);
            for k in 0..m {
                ingest(&store, NODES, EDGES, k as u64).unwrap();
            }
        }
        // Reload and read until told to stop; any error but a compaction
        // caught mid-rename (`Busy`, retried by design) is a failure.
        "reader" => {
            ready_then_go(&d, "reader");
            let store = loop {
                match Store::open_read_only(&d) {
                    Ok(s) => break s,
                    Err(Error::Busy { .. }) => std::thread::yield_now(),
                    Err(e) => panic!("open: {e}"),
                }
            };
            let (mut reads, mut busy) = (0u64, 0u64);
            while !d.join("stop").exists() {
                match store.reload_if_stale() {
                    Ok(_) => {
                        std::hint::black_box(state::load(&store));
                        reads += 1;
                    }
                    Err(Error::Busy { .. }) => busy += 1,
                    Err(e) => panic!("reader: {e}"),
                }
            }
            std::fs::write(d.join("reads"), format!("{reads} {busy}")).unwrap();
        }
        other => panic!("unknown role {other}"),
    }
}

#[test]
fn processes_saving_and_ingesting_through_compactions_lose_nothing() {
    const WRITERS: usize = 4;
    const M: usize = 18;
    const EVERY: usize = 24;
    let d = dir("race");
    drop(Store::create(&d, cfg(EVERY)).unwrap());
    let a = |v: [String; 3]| -> Vec<String> {
        std::iter::once(d.display().to_string()).chain(v).collect()
    };
    let mut kids: Vec<(String, Child)> = Vec::new();
    for id in 0..WRITERS {
        kids.push((format!("w{id}"), spawn("saves", &a([id, M, EVERY].map(|x| x.to_string())))));
    }
    for id in ["i0", "i1"] {
        let args = a([id.to_owned(), "6".to_owned(), EVERY.to_string()]);
        kids.push((id.to_owned(), spawn("ingests", &args)));
    }
    let mut reader = spawn("reader", &[d.display().to_string()]);
    let mut ids: Vec<String> = kids.iter().map(|(i, _)| i.clone()).collect();
    ids.push("reader".into());
    go_when_ready(&d, &ids);
    for (id, c) in &mut kids {
        succeeded(c, id);
    }
    std::fs::write(d.join("stop"), b"").unwrap();
    succeeded(&mut reader, "reader");
    let reads = std::fs::read_to_string(d.join("reads")).unwrap();
    assert!(reads.split(' ').next().unwrap().parse::<u64>().unwrap() > 0, "the reader read");

    let store = Store::open(&d).unwrap();
    assert!(store.generation() > 2, "compactions landed during the race");
    let mut want: Vec<String> =
        (0..WRITERS).flat_map(|i| (0..M).map(move |k| table_name(i, k))).collect();
    want.sort();
    let mut got: Vec<String> = (0..WRITERS).flat_map(|i| saved(&d, i)).collect();
    got.sort();
    assert_eq!(got, want, "every save returned");
    assert_complete(&store, &want);
    assert_eq!(state::load(&store).len(), want.len(), "nothing but the saved tables is recorded");
    // The export's own tables are still declared beside the saved ones, and
    // a repeat ingest finds nothing missing.
    assert!(!ingest(&store, NODES, EDGES, 99).unwrap().committed, "an ingest was clobbered");
    // And the saves survived the ingests replacing `proj.mart`'s set.
    assert_complete(&store, &want);
}

#[test]
fn killed_writers_cost_the_others_nothing() {
    const SURVIVORS: usize = 2;
    const KILLED: usize = 2;
    const M: usize = 30;
    const EVERY: usize = 24;
    let d = dir("kill");
    drop(Store::create(&d, cfg(EVERY)).unwrap());
    let args = |id: usize, m: usize| -> Vec<String> {
        vec![d.display().to_string(), id.to_string(), m.to_string(), EVERY.to_string()]
    };
    // The doomed writers never finish on their own.
    let mut doomed: Vec<Child> =
        (SURVIVORS..SURVIVORS + KILLED).map(|id| spawn("saves", &args(id, 1_000_000))).collect();
    let mut survivors: Vec<Child> = (0..SURVIVORS).map(|id| spawn("saves", &args(id, M))).collect();
    let ids: Vec<String> = (0..SURVIVORS + KILLED).map(|i| format!("w{i}")).collect();
    go_when_ready(&d, &ids);
    // Kill each doomed writer once it has saved something, mid-run.
    for (n, c) in doomed.iter_mut().enumerate() {
        let id = SURVIVORS + n;
        let start = Instant::now();
        while saved(&d, id).len() < 3 + 2 * n {
            assert!(start.elapsed() < Duration::from_secs(60), "w{id} never saved");
            std::thread::sleep(Duration::from_millis(1));
        }
        c.kill().unwrap();
        c.wait().unwrap();
    }
    for (id, c) in survivors.iter_mut().enumerate() {
        succeeded(c, &format!("w{id}"));
    }

    // A writable open and one more save go through: no lock is left held and
    // whatever a killed writer left half-appended is repaired.
    let store = Store::open(&d).unwrap();
    state::save(&store, &report("proj.d1.after_t", 7), 1).unwrap();
    let mut all: Vec<String> = (0..SURVIVORS + KILLED).flat_map(|i| saved(&d, i)).collect();
    all.push("proj.d1.after_t".into());
    assert!(all.len() > SURVIVORS * M, "the doomed writers saved before they died");
    assert_complete(&store, &all);
    // Anything else recorded (a killed writer's last save, which landed
    // before its note did) is a complete record too.
    let prior = state::load(&store);
    let mut extra = Vec::new();
    for id in SURVIVORS..SURVIVORS + KILLED {
        for k in 0..1_000 {
            let t = table_name(id, k);
            if prior.get(&t).is_some() && !all.contains(&t) {
                extra.push(t);
            }
        }
    }
    assert!(extra.len() <= KILLED, "at most one unnoted save per killed writer: {extra:?}");
    assert_complete(&store, &extra);
}
