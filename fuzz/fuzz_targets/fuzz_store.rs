// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Arbitrary bytes as a describe store's `graph.base` and `graph.wal`: every
// lookup and the regenerating writable open must answer or refuse with a
// typed error, never panic. A regenerated store must then be usable.

#![no_main]

use crab_r::describe::{app, lifecycle, state};
use graph_r::store::{BASE_FILE, WAL_FILE};
use libfuzzer_sys::fuzz_target;
use std::path::PathBuf;
use std::sync::OnceLock;

fn dir() -> &'static PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!("crab-fuzz-store-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    })
}

fuzz_target!(|data: &[u8]| {
    let d = dir();
    // The first two bytes split the rest into the base and the log; a split
    // past the end leaves no log at all.
    let (split, rest) = match data {
        [a, b, rest @ ..] => (usize::from(u16::from_le_bytes([*a, *b])), rest),
        _ => return,
    };
    let (base, wal) = rest.split_at(split.min(rest.len()));
    std::fs::write(d.join(BASE_FILE), base).unwrap();
    if split > rest.len() {
        let _ = std::fs::remove_file(d.join(WAL_FILE));
    } else {
        std::fs::write(d.join(WAL_FILE), wal).unwrap();
    }
    let name = d.to_string_lossy().into_owned();
    let needed = lifecycle::regenerate_needed(d);
    for (cmd, arg) in [
        ("table", "p.d.t"),
        ("lookup", "p.d.t.c"),
        ("upstream", "p.d.t.c"),
        ("downstream", "p.d.t.c"),
        ("due", "3"),
    ] {
        if let Ok(out) = app::lookup(cmd, &[name.clone(), arg.to_owned()]) {
            assert_eq!(out.starts_with(lifecycle::REGENERATE_NEEDED), needed, "{cmd}");
        }
    }
    if let Some(store) = lifecycle::open_or_regenerate(d).ok().and_then(lifecycle::Opened::store) {
        let _ = state::load(&store);
        assert!(!lifecycle::regenerate_needed(d), "an opened store reads");
    }
});
