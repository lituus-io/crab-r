// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// A damaged store regenerates instead of erroring: the writable open (what
// describe, ingest and prune use) rebuilds it empty, lookups answer
// "nothing recorded" with the `regenerate_needed` flag, a locked or busy
// store is never touched, and the delete never leaves the store's own data
// files — no symbolic link is followed, nothing else in the directory goes.

#![cfg(feature = "cli")]

use crab_r::describe::app;
use crab_r::describe::describe::{Action, ColumnReport, Report, TableReport};
use crab_r::describe::ingest::ingest_dir;
use crab_r::describe::lifecycle::{
    open_for_lookup, open_or_regenerate, open_or_regenerate_cfg, regenerate_needed, OpenError,
    REGENERATE_NEEDED,
};
use crab_r::describe::state;
use graph_r::store::{BASE_FILE, WAL_FILE};
use graph_r::{Config, Store};
use std::path::{Path, PathBuf};
use std::time::Duration;

const NODES: &str = include_str!("data/static_lineage/nodes.json");
const EDGES: &str = include_str!("data/static_lineage/edges.json");

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("crab-regen-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn s(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

fn report() -> Report {
    Report {
        tables: vec![TableReport {
            table: "proj.mart.orders_t".into(),
            kind: "TABLE".into(),
            rows: 10,
            bytes: 100,
            last_modified_ms: 0,
            producer: None,
            existing: None,
            description: Some("Orders.".into()),
            grain: vec![],
            filters: vec![],
            staged: vec![],
            conditions: vec![],
            columns: vec![ColumnReport {
                column: "id".into(),
                data_type: "INT64".into(),
                action: Action::Generate,
                existing: None,
                description: Some("Order id.".into()),
                semantic_type: None,
                unit: None,
                transform: None,
                expression: None,
                sources: vec![],
                hops: vec![],
                confidence: None,
                checks: vec![],
                consumers: vec![],
            }],
            checks: vec![],
            model_calls: 1,
            warnings: vec![],
            pending: false,
            spend: Default::default(),
            meta: None,
            prekey: 7,
            key: Some(9),
            reused: false,
            carried_checks: None,
        }],
        ..Report::default()
    }
}

/// A store holding one described table, plus a file and a directory of the
/// user's beside it.
fn populated(name: &str) -> PathBuf {
    let d = dir(name);
    let store = Store::open_or_create(&d, Config::default()).unwrap();
    state::save(&store, &report(), 1_000).unwrap();
    store.compact().unwrap();
    state::save(&store, &report(), 2_000).unwrap(); // a log beyond the base
    std::fs::write(d.join("notes.txt"), b"mine").unwrap();
    std::fs::create_dir_all(d.join("keep")).unwrap();
    std::fs::write(d.join("keep").join(BASE_FILE), b"not the store's").unwrap();
    d
}

fn bytes(p: &Path) -> Vec<u8> {
    std::fs::read(p).unwrap()
}

/// Ways a data file is damaged.
fn damage(file: &str, how: &str, d: &Path) {
    let path = d.join(file);
    let mut b = bytes(&path);
    match how {
        "garbage" => b = b"this is not a graph-r file at all, not even close".repeat(3),
        "truncated" => b.truncate(10),
        // Inside the checksummed header.
        "bit-flipped" => b[12] ^= 0x10,
        // The version word that follows the 4-byte magic.
        "wrong-version" => b[4..6].copy_from_slice(&999u16.to_le_bytes()),
        "empty" => b.clear(),
        _ => unreachable!(),
    }
    std::fs::write(&path, b).unwrap();
}

const DAMAGE: [&str; 5] = ["garbage", "truncated", "bit-flipped", "wrong-version", "empty"];

/// The damage that applies to `file`: an empty log is graph-r's own "no log
/// yet" (a committer rewrites it), not damage.
fn damages(file: &str) -> impl Iterator<Item = &'static str> + '_ {
    DAMAGE.into_iter().filter(move |h| !(file == WAL_FILE && *h == "empty"))
}

/// The user's files survived.
fn untouched_beside(d: &Path) {
    assert_eq!(bytes(&d.join("notes.txt")), b"mine");
    assert_eq!(bytes(&d.join("keep").join(BASE_FILE)), b"not the store's");
}

#[test]
fn a_damaged_store_is_refused_by_graph_r_as_damage() {
    // The premise: each damage is a typed refusal, never a panic or a store
    // that opens with data missing.
    for file in [BASE_FILE, WAL_FILE] {
        for how in damages(file) {
            let d = populated(&format!("premise-{how}-{file}"));
            damage(file, how, &d);
            let e = Store::open_read_only(&d).unwrap_err();
            assert!(
                matches!(
                    e,
                    graph_r::Error::Format { .. }
                        | graph_r::Error::Corrupt { .. }
                        | graph_r::Error::Version { .. }
                ),
                "{file} {how}: {e:?}"
            );
            if how == "wrong-version" && file == WAL_FILE {
                assert!(matches!(e, graph_r::Error::Version { found: 999, .. }), "{e:?}");
            }
        }
    }
}

#[test]
fn the_describe_open_regenerates_a_damaged_store_and_it_is_usable() {
    for file in [BASE_FILE, WAL_FILE] {
        for how in damages(file) {
            let d = populated(&format!("open-{how}-{file}"));
            std::fs::write(d.join(".graph.base.1.2.tmp"), b"leftover").unwrap();
            std::fs::write(d.join(".graph.basement.1.tmp"), b"not ours").unwrap();
            damage(file, how, &d);
            let (store, regenerated) = open_or_regenerate(&d).unwrap();
            assert!(regenerated, "{file} {how}");
            assert!(state::load(&store).is_empty(), "regenerated empty");
            state::save(&store, &report(), 3_000).unwrap();
            assert_eq!(state::load(&store).len(), 1, "{file} {how}: usable");
            drop(store);
            assert!(!regenerate_needed(&d));
            let (again, regenerated) = open_or_regenerate(&d).unwrap();
            assert!(!regenerated, "a healthy store is opened as it is");
            assert_eq!(state::load(&again).len(), 1);
            untouched_beside(&d);
            assert!(!d.join(".graph.base.1.2.tmp").exists(), "graph-r's temporary went");
            assert_eq!(bytes(&d.join(".graph.basement.1.tmp")), b"not ours");
        }
    }
}

#[test]
fn ingest_regenerates_a_damaged_store() {
    for how in DAMAGE {
        let d = populated(&format!("ingest-{how}"));
        damage(BASE_FILE, how, &d);
        let done = ingest_dir(&s(&d), NODES, EDGES).unwrap();
        assert!(done.regenerated && done.committed, "{how}");
        assert!(done.to_json().ends_with(r#""regenerated":true}"#), "{}", done.to_json());
        let again = ingest_dir(&s(&d), NODES, EDGES).unwrap();
        assert!(!again.regenerated && !again.committed);
        untouched_beside(&d);
    }
}

#[test]
fn lookups_on_a_damaged_store_answer_nothing_recorded_flagged() {
    for file in [BASE_FILE, WAL_FILE] {
        for how in damages(file) {
            let d = populated(&format!("lookup-{how}-{file}"));
            damage(file, how, &d);
            let before = (bytes(&d.join(BASE_FILE)), bytes(&d.join(WAL_FILE)));
            assert!(regenerate_needed(&d), "{file} {how}");
            assert!(open_for_lookup(&d).unwrap().is_none());
            for (cmd, name) in [
                ("table", "proj.mart.orders_t"),
                ("lookup", "proj.mart.orders_t.id"),
                ("upstream", "proj.mart.orders_t.id"),
                ("downstream", "proj.mart.orders_t.id"),
                ("due", "5"),
            ] {
                let out = app::lookup(cmd, &[s(&d), name.to_owned()]).unwrap();
                assert!(
                    out.starts_with(&format!("{REGENERATE_NEEDED}: nothing recorded")),
                    "{out}"
                );
                assert_eq!(out.lines().count(), 1, "{out}");
            }
            // A reader deletes nothing.
            assert_eq!((bytes(&d.join(BASE_FILE)), bytes(&d.join(WAL_FILE))), before);
        }
    }
    // A healthy store answers as before, and a missing one is still an error.
    let d = populated("lookup-healthy");
    let out = app::lookup("table", &[s(&d), "proj.mart.orders_t".into()]).unwrap();
    assert!(out.starts_with("bigquery/proj/mart/orders_t") || out.contains("orders_t"), "{out}");
    assert!(!out.contains(REGENERATE_NEEDED));
    assert!(!regenerate_needed(&d));
    assert!(app::lookup("table", &[s(&d.join("absent")), "p.d.t".into()]).is_err());
    assert!(!regenerate_needed(d.join("absent")));
}

/// A store whose base is one generation behind its log: what a reader sees
/// mid-compaction, and damage when it stays so with the commit lock free.
fn log_ahead_of_base(name: &str) -> PathBuf {
    let d = populated(name);
    let old_base = bytes(&d.join(BASE_FILE));
    Store::open(&d).unwrap().compact().unwrap();
    std::fs::write(d.join(BASE_FILE), old_base).unwrap();
    d
}

#[test]
fn a_locked_or_busy_store_is_never_deleted() {
    // Healthy, the lock held elsewhere: opened as it is, data intact.
    let d = populated("locked-healthy");
    let holder = Store::open(&d).unwrap();
    let lock = holder.commit_lock().unwrap();
    let (store, regenerated) = open_or_regenerate(&d).unwrap();
    assert!(!regenerated);
    assert_eq!(state::load(&store).len(), 1, "the original data");
    drop((store, lock));

    // Damaged, the lock held: a regeneration waits for it, then refuses
    // with `Locked`, deleting nothing.
    let d = populated("locked-damaged");
    let holder = Store::open(&d).unwrap();
    let lock = holder.commit_lock().unwrap();
    damage(BASE_FILE, "garbage", &d);
    let before = (bytes(&d.join(BASE_FILE)), bytes(&d.join(WAL_FILE)));
    let cfg = Config { lock_wait: Duration::from_millis(50), ..Config::default() };
    let e = open_or_regenerate_cfg(&d, cfg).unwrap_err();
    assert!(matches!(e, OpenError::Store(graph_r::Error::Locked)), "{e:?}");
    assert_eq!((bytes(&d.join(BASE_FILE)), bytes(&d.join(WAL_FILE))), before);
    drop(lock);
    assert!(open_or_regenerate_cfg(&d, cfg).unwrap().1, "regenerated once the lock is free");

    // A log ahead of its base while the lock is held is a compaction in
    // flight: `Busy`, nothing deleted, for the writer and the reader alike.
    let d = log_ahead_of_base("busy");
    // The lock as a compactor holds it (no store opens here to take it).
    let held =
        std::fs::OpenOptions::new().write(true).open(d.join(graph_r::store::LOCK_FILE)).unwrap();
    held.lock().unwrap();
    let before = (bytes(&d.join(BASE_FILE)), bytes(&d.join(WAL_FILE)));
    let e = open_or_regenerate_cfg(&d, cfg).unwrap_err();
    assert!(matches!(e, OpenError::Store(graph_r::Error::Busy { .. })), "{e:?}");
    assert!(matches!(open_for_lookup(&d), Err(graph_r::Error::Busy { .. })));
    assert!(!regenerate_needed(&d));
    assert_eq!((bytes(&d.join(BASE_FILE)), bytes(&d.join(WAL_FILE))), before);
    // Lock free and still so: damage, flagged for readers, regenerated by a
    // writer.
    held.unlock().unwrap();
    assert!(regenerate_needed(&d));
    assert!(open_or_regenerate(&d).unwrap().1);
    untouched_beside(&d);
}

#[cfg(unix)]
#[test]
fn links_are_refused_and_their_targets_untouched() {
    // A store directory that is a link to a damaged store.
    let real = populated("link-target");
    damage(BASE_FILE, "garbage", &real);
    let before = bytes(&real.join(BASE_FILE));
    let link = std::env::temp_dir().join(format!("crab-regen-link-{}", std::process::id()));
    let _ = std::fs::remove_file(&link);
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let e = open_or_regenerate(&link).unwrap_err();
    assert!(matches!(&e, OpenError::Symlink(p) if *p == link), "{e:?}");
    assert_eq!(bytes(&real.join(BASE_FILE)), before);
    assert!(real.join(WAL_FILE).exists());
    let e = ingest_dir(&s(&link), NODES, EDGES).unwrap_err();
    assert!(e.contains("symbolic link"), "{e}");
    assert_eq!(bytes(&real.join(BASE_FILE)), before);
    std::fs::remove_file(&link).unwrap();

    // A `graph.base` that is a link to a file elsewhere.
    let d = populated("link-base");
    let outside = dir("link-outside").join("precious");
    std::fs::write(&outside, b"precious, and no store").unwrap();
    std::fs::remove_file(d.join(BASE_FILE)).unwrap();
    std::os::unix::fs::symlink(&outside, d.join(BASE_FILE)).unwrap();
    let e = open_or_regenerate(&d).unwrap_err();
    assert!(matches!(&e, OpenError::Symlink(p) if p.ends_with(BASE_FILE)), "{e:?}");
    assert_eq!(bytes(&outside), b"precious, and no store");
    assert!(std::fs::symlink_metadata(d.join(BASE_FILE)).unwrap().file_type().is_symlink());
    assert!(d.join(WAL_FILE).exists(), "nothing deleted");
    untouched_beside(&d);

    // A planted link among graph-r's temporaries is refused as well.
    let d = populated("link-tmp");
    damage(BASE_FILE, "garbage", &d);
    std::os::unix::fs::symlink(&outside, d.join(".graph.wal.1.1.tmp")).unwrap();
    assert!(matches!(open_or_regenerate(&d).unwrap_err(), OpenError::Symlink(_)));
    assert_eq!(bytes(&outside), b"precious, and no store");
}

#[test]
fn a_store_file_that_is_a_directory_is_refused() {
    let d = populated("not-a-file");
    std::fs::remove_file(d.join(WAL_FILE)).unwrap();
    std::fs::create_dir(d.join(WAL_FILE)).unwrap();
    std::fs::write(d.join(WAL_FILE).join("inside"), b"x").unwrap();
    let e = open_or_regenerate(&d).unwrap_err();
    assert!(matches!(&e, OpenError::NotAFile(p) if p.ends_with(WAL_FILE)), "{e:?}");
    assert_eq!(bytes(&d.join(WAL_FILE).join("inside")), b"x");
}

#[test]
fn the_report_carries_regenerated() {
    let mut r = Report::default();
    assert!(crab_r::describe::report::to_json(&r).contains(r#""regenerated":false"#));
    r.regenerated = true;
    assert!(crab_r::describe::report::to_json(&r).contains(r#""regenerated":true"#));
}

#[test]
fn a_pull_into_a_damaged_replica_regenerates_it_by_a_full_pull() {
    let src = populated("pull-src");
    let bucket = dir("pull-bucket");
    let store = Store::open(&src).unwrap();
    let objects = objstore_r::LocalDir::open(&bucket).unwrap();
    objstore_r::http::block_on(async {
        objstore_r::mirror::Mirror::new(&objects, "").unwrap().publish(&store).await.unwrap()
    })
    .unwrap();
    let replica = dir("pull-replica");
    let out = objstore_r::http::block_on(app::pull(&s(&bucket), &s(&replica))).unwrap().unwrap();
    assert!(out.starts_with("pulled generation"), "{out}");
    std::fs::write(replica.join("notes.txt"), b"mine").unwrap();
    damage(BASE_FILE, "garbage", &replica);
    assert!(regenerate_needed(&replica));
    let out = objstore_r::http::block_on(app::pull(&s(&bucket), &s(&replica))).unwrap().unwrap();
    assert!(out.starts_with("regenerated: pulled generation"), "{out}");
    assert!(!regenerate_needed(&replica));
    let pulled = Store::open_read_only(&replica).unwrap();
    assert_eq!(state::load(&pulled).len(), 1, "the published data");
    assert_eq!(bytes(&replica.join("notes.txt")), b"mine");
}
