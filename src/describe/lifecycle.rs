// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! A describe store's lifecycle: it regenerates when damaged and is bounded
//! by age.
//!
//! The store is a cache of what describe runs and lineage ingests found, so
//! a damaged one is rebuilt, never reported as an error to work around:
//!
//! - **Writers** ([`open_or_regenerate`], the one writable open of `describe`,
//!   `ingest` and [`prune`]): a store graph-r refuses as damaged — a format
//!   version this build does not read, a failed check, a malformed file, a
//!   base or log that cannot be read — has its own data files deleted and is
//!   created afresh; the run then records what it describes. A store that is
//!   only locked or busy is never touched.
//! - **Readers** ([`open_for_lookup`]): a damaged store answers as one with
//!   nothing recorded, and [`regenerate_needed`] says so.
//!
//! The delete is bounded: only `graph.base`, `graph.wal` and graph-r's
//! temporaries of those two, by name, in `dir` itself; never a symbolic link
//! (refused, nothing deleted) and never anything else in the directory. The
//! lock files `graph.lock` and `graph.wait` stay: they hold no data, and
//! another process may be waiting on them, so replacing them would let two
//! writers hold "the" lock at once.

use crate::describe::state::{self, Pruned};
use graph_r::store::{BASE_FILE, LOCK_FILE, WAL_FILE};
use graph_r::{Config, Store};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// The first word of a lookup's answer when the store cannot be read: nothing
/// is recorded, and the next describe regenerates it.
pub const REGENERATE_NEEDED: &str = "regenerate_needed";

/// The lock queue graph-r keeps beside the commit lock.
const WAIT_FILE: &str = "graph.wait";

/// Why a store could not be opened, or regenerated.
#[derive(Debug)]
#[non_exhaustive]
pub enum OpenError {
    /// graph-r refused, and not because the store is damaged: another writer
    /// holds the commit lock, a compaction is in flight, an I/O failure.
    Store(graph_r::Error),
    /// The store's directory, or one of its own files, is a symbolic link: a
    /// damaged store there is not regenerated, and nothing is deleted.
    Symlink(PathBuf),
    /// One of the store's own files is not a regular file (a directory, a
    /// device): not regenerated, nothing deleted.
    NotAFile(PathBuf),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(e) => e.fmt(f),
            Self::Symlink(p) => write!(
                f,
                "{} is a symbolic link: a damaged store is not regenerated through one",
                p.display()
            ),
            Self::NotAFile(p) => write!(
                f,
                "{} is not a regular file: a damaged store is not regenerated over it",
                p.display()
            ),
        }
    }
}

impl std::error::Error for OpenError {}

impl From<graph_r::Error> for OpenError {
    fn from(e: graph_r::Error) -> Self {
        Self::Store(e)
    }
}

impl From<std::io::Error> for OpenError {
    fn from(e: std::io::Error) -> Self {
        Self::Store(e.into())
    }
}

/// `e`, from opening the store in `dir`, says the store is damaged: rebuild
/// it rather than report it.
fn damaged(dir: &Path, e: &graph_r::Error) -> bool {
    use graph_r::Error as E;
    match e {
        E::Version { .. } | E::Corrupt { .. } | E::Format { .. } => true,
        // A failure reading the store's own files, not one writing (a full
        // disk while a writable open repairs a torn tail is no damage).
        E::Io { .. } => unreadable(dir),
        _ => false,
    }
}

/// A store file in `dir` exists and cannot be read to its end as a regular
/// file (a directory or a link in its place counts: a regeneration then
/// refuses it by name).
fn unreadable(dir: &Path) -> bool {
    [BASE_FILE, WAL_FILE].iter().any(|name| {
        let path = dir.join(name);
        fs::symlink_metadata(&path).is_ok_and(|md| {
            !md.file_type().is_file()
                || fs::File::open(&path)
                    .and_then(|mut f| std::io::copy(&mut f, &mut std::io::sink()))
                    .is_err()
        })
    })
}

/// graph-r's temporaries of the data files: `.graph.base.<pid>.<n>.tmp`.
/// Matched exactly as graph-r spells them.
fn is_own_temporary(name: &str) -> bool {
    let Some(stem) = name.strip_prefix('.').and_then(|n| n.strip_suffix(".tmp")) else {
        return false;
    };
    [BASE_FILE, WAL_FILE]
        .iter()
        .any(|f| stem.strip_prefix(f).is_some_and(|r| r.is_empty() || r.starts_with('.')))
}

/// `path` is absent or a regular file; a symbolic link or anything else is
/// refused.
fn check_entry(path: PathBuf) -> Result<(), OpenError> {
    match fs::symlink_metadata(&path) {
        Ok(md) if md.file_type().is_symlink() => Err(OpenError::Symlink(path)),
        Ok(md) if !md.file_type().is_file() => Err(OpenError::NotAFile(path)),
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// The files a regeneration deletes (those that exist): the data files, then
/// their temporaries. Refused, before anything is deleted, when `dir` or any
/// of the store's own entries is a symbolic link or not a regular file.
fn own_files(dir: &Path) -> Result<Vec<PathBuf>, OpenError> {
    let md = fs::symlink_metadata(dir)?;
    if md.file_type().is_symlink() {
        return Err(OpenError::Symlink(dir.to_path_buf()));
    }
    if !md.is_dir() {
        return Err(OpenError::NotAFile(dir.to_path_buf()));
    }
    for name in [LOCK_FILE, WAIT_FILE] {
        check_entry(dir.join(name))?;
    }
    let mut out = Vec::with_capacity(2);
    for name in [BASE_FILE, WAL_FILE] {
        let path = dir.join(name);
        check_entry(path.clone())?;
        out.push(path);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_name().to_str().is_some_and(is_own_temporary) {
            check_entry(entry.path())?;
            out.push(entry.path());
        }
    }
    Ok(out)
}

/// Refuse `dir` when it, or one of the store's own files in it, is a
/// symbolic link or not what it should be: the check a regeneration makes
/// before it replaces anything.
pub fn refuse_links(dir: impl AsRef<Path>) -> Result<(), OpenError> {
    own_files(dir.as_ref()).map(|_| ())
}

/// A reader's `Busy` (a log ahead of its base) or a missing base.
fn is_busy_or_absent(e: &graph_r::Error) -> bool {
    match e {
        graph_r::Error::Busy { .. } => true,
        graph_r::Error::Io { source } => source.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

/// The commit lock of the store in `dir`, waiting up to `wait`: no writer
/// commits, compacts or creates the store while it is held. Released when
/// the file is dropped.
fn lock(dir: &Path, wait: Duration) -> Result<fs::File, OpenError> {
    check_entry(dir.join(LOCK_FILE))?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(LOCK_FILE))?;
    let start = Instant::now();
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(fs::TryLockError::WouldBlock) if start.elapsed() < wait => {
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(fs::TryLockError::WouldBlock) => return Err(graph_r::Error::Locked.into()),
            Err(fs::TryLockError::Error(e)) => return Err(e.into()),
        }
    }
}

/// Open the store in `dir` read-write, created when absent and regenerated
/// when damaged; `true` beside it when it was regenerated (empty, the old
/// data gone). The one writable open of a describe store.
pub fn open_or_regenerate(dir: impl AsRef<Path>) -> Result<(Store, bool), OpenError> {
    open_or_regenerate_cfg(dir, Config::default())
}

/// [`open_or_regenerate`] with an explicit configuration; its `lock_wait`
/// also bounds the wait for the commit lock a regeneration takes.
pub fn open_or_regenerate_cfg(
    dir: impl AsRef<Path>,
    cfg: Config,
) -> Result<(Store, bool), OpenError> {
    let dir = dir.as_ref();
    match Store::open_or_create(dir, cfg) {
        Ok(s) => return Ok((s, false)),
        Err(e) if !damaged(dir, &e) => return Err(e.into()),
        Err(_) => {}
    }
    {
        // Refused before the lock file is touched: a link is never followed.
        own_files(dir)?;
        let _held = lock(dir, cfg.lock_wait)?;
        // Judged again under the lock: another process may have regenerated
        // it meanwhile. No compaction can be in flight now, so a log ahead
        // of its base (`Busy` to a reader) is damage too, and a log with no
        // base is the leftover of a creation that never finished.
        match Store::open_read_only(dir) {
            Ok(_) => {}
            Err(e) if damaged(dir, &e) || is_busy_or_absent(&e) => {
                for path in own_files(dir)? {
                    match fs::remove_file(&path) {
                        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
                        _ => {}
                    }
                }
                graph_r::fsio::sync_dir(dir);
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok((Store::open_or_create(dir, cfg)?, true))
}

/// Open the store in `dir` for lookups: `None` when it is damaged (answer
/// "nothing recorded"; the next describe regenerates it). A missing store,
/// or one busy or locked, is still an error.
pub fn open_for_lookup(dir: impl AsRef<Path>) -> Result<Option<Store>, graph_r::Error> {
    let dir = dir.as_ref();
    match Store::open_read_only(dir) {
        Ok(s) => Ok(Some(s)),
        Err(e) if damaged(dir, &e) => Ok(None),
        // A log ahead of its base is a compaction in flight, unless it
        // stays so while the commit lock is free: then it is damage. The
        // lock is taken only if its file exists (a reader creates nothing).
        Err(graph_r::Error::Busy { message }) => {
            let held = fs::File::open(dir.join(LOCK_FILE))
                .ok()
                .filter(|f| check_entry(dir.join(LOCK_FILE)).is_ok() && f.try_lock().is_ok());
            match (held, Store::open_read_only(dir)) {
                (_, Ok(s)) => Ok(Some(s)),
                (_, Err(e)) if damaged(dir, &e) => Ok(None),
                (Some(_), Err(graph_r::Error::Busy { .. })) => Ok(None),
                (None, Err(graph_r::Error::Busy { .. })) => Err(graph_r::Error::Busy { message }),
                (_, Err(e)) => Err(e),
            }
        }
        Err(e) => Err(e),
    }
}

/// The store in `dir` exists but cannot be read: lookups answer nothing, and
/// the next describe, ingest or prune regenerates it.
#[must_use]
pub fn regenerate_needed(dir: impl AsRef<Path>) -> bool {
    matches!(open_for_lookup(dir), Ok(None))
}

/// A lookup's whole answer from a store that cannot be read.
#[must_use]
pub fn nothing_recorded(dir: &str) -> String {
    format!(
        "{REGENERATE_NEEDED}: nothing recorded; {dir} cannot be read, and the next describe regenerates it\n"
    )
}

/// Milliseconds in a day.
const DAY_MS: u64 = 86_400_000;

/// Remove from the store in `dir` the tables no describe or ingest has
/// written or touched in `max_age_days`, with their columns (see
/// [`state::prune`]): one transaction and one compaction, or no write at all
/// when nothing is that old. Opened through [`open_or_regenerate`]; `true`
/// beside the count when the store was regenerated instead (then nothing was
/// left to prune).
pub fn prune(dir: impl AsRef<Path>, max_age_days: u32) -> Result<(Pruned, bool), OpenError> {
    prune_at(dir, max_age_days, graph_r::fsio::now_ms())
}

/// [`prune`] as of `now_ms`.
pub fn prune_at(
    dir: impl AsRef<Path>,
    max_age_days: u32,
    now_ms: u64,
) -> Result<(Pruned, bool), OpenError> {
    let (store, regenerated) = open_or_regenerate(dir)?;
    let cutoff = now_ms.saturating_sub(u64::from(max_age_days) * DAY_MS);
    Ok((state::prune(&store, cutoff)?, regenerated))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_graph_r_temporaries_of_the_data_files_are_own() {
        for (name, own) in [
            (".graph.base.123.0.tmp", true),
            (".graph.wal.9.18.tmp", true),
            (".graph.base.tmp", true),
            ("graph.base.1.tmp", false),
            (".graph.basement.1.tmp", false),
            (".graph.lock.1.tmp", false),
            (".graph.base.1.tmp.keep", false),
            ("notes.tmp", false),
            (".tmp", false),
        ] {
            assert_eq!(is_own_temporary(name), own, "{name}");
        }
    }
}
