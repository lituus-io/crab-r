// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Verified-answer cache.
//!
//! A question answered with high confidence is remembered as its canonical
//! SQL (not its rows). Asking it again, on the same day and the same catalog
//! generation, skips the model entirely: the SQL is re-run, and because the
//! emission is canonical and byte-identical BigQuery serves it from its free
//! 24-hour result cache while the tables are unchanged — fresh rows when
//! they changed. Zero tokens, one fast warehouse round trip, and the same
//! answer every time (consistency).
//!
//! The store is the host's: [`AnswerCache`] is a GAT trait so a remote cache
//! (shared by replicas) needs no boxing; [`MemoryCache`] is a bounded
//! in-process one, [`NoCache`] disables caching.

use std::collections::{HashMap, VecDeque};
use std::future::{ready, Future, Ready};
use std::sync::{Mutex, PoisonError};

/// A remembered answer.
#[derive(Clone, Debug, PartialEq)]
pub struct CachedAnswer {
    /// Canonical SQL.
    pub sql: String,
    /// Meaning-level fingerprint.
    pub fingerprint: u128,
    /// Confidence when it was answered.
    pub confidence: f32,
    /// Byte cap it ran under.
    pub max_bytes_billed: u64,
}

/// Where verified answers are kept.
pub trait AnswerCache {
    /// Lookup future.
    type Get<'a>: Future<Output = Option<CachedAnswer>> + 'a
    where
        Self: 'a;
    /// Store future.
    type Put<'a>: Future<Output = ()> + 'a
    where
        Self: 'a;

    /// Look `key` up.
    fn get(&self, key: u128) -> Self::Get<'_>;

    /// Remember `value` under `key`.
    fn put(&self, key: u128, value: CachedAnswer) -> Self::Put<'_>;
}

/// No caching.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCache;

impl AnswerCache for NoCache {
    type Get<'a> = Ready<Option<CachedAnswer>>;
    type Put<'a> = Ready<()>;

    fn get(&self, _: u128) -> Self::Get<'_> {
        ready(None)
    }

    fn put(&self, _: u128, _: CachedAnswer) -> Self::Put<'_> {
        ready(())
    }
}

/// A bounded in-process cache; the oldest entry is evicted when full.
#[derive(Debug)]
pub struct MemoryCache {
    inner: Mutex<Inner>,
    capacity: usize,
}

#[derive(Debug, Default)]
struct Inner {
    map: HashMap<u128, CachedAnswer>,
    order: VecDeque<u128>,
}

impl MemoryCache {
    /// A cache holding at most `capacity` answers.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self { inner: Mutex::new(Inner::default()), capacity: capacity.max(1) }
    }

    /// Answers held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner).map.len()
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl AnswerCache for MemoryCache {
    type Get<'a> = Ready<Option<CachedAnswer>>;
    type Put<'a> = Ready<()>;

    fn get(&self, key: u128) -> Self::Get<'_> {
        ready(self.inner.lock().unwrap_or_else(PoisonError::into_inner).map.get(&key).cloned())
    }

    fn put(&self, key: u128, value: CachedAnswer) -> Self::Put<'_> {
        let mut g = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if g.map.insert(key, value).is_none() {
            g.order.push_back(key);
            while g.order.len() > self.capacity {
                if let Some(old) = g.order.pop_front() {
                    g.map.remove(&old);
                }
            }
        }
        ready(())
    }
}

/// The cache key of a question: catalog generation, the session's "today"
/// (relative dates resolve to literals) and the question with case and
/// spacing normalised (punctuation kept: `> 100` and `< 100` differ).
#[must_use]
pub fn key(generation: u64, today_days: i32, question: &str) -> u128 {
    let mut buf = Vec::with_capacity(question.len() + 16);
    buf.extend_from_slice(&generation.to_le_bytes());
    buf.extend_from_slice(&today_days.to_le_bytes());
    let q = question.trim().trim_end_matches(['?', '.', '!', ' ']);
    let mut space = false;
    for c in q.chars() {
        if c.is_whitespace() {
            space = true;
            continue;
        }
        if space {
            buf.push(b' ');
            space = false;
        }
        for l in c.to_lowercase() {
            let mut b = [0; 4];
            buf.extend_from_slice(l.encode_utf8(&mut b).as_bytes());
        }
    }
    graph_r::hash::xxh3_128(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now<F: Future>(f: F) -> F::Output {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let mut f = std::pin::pin!(f);
        match f.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(v) => v,
            std::task::Poll::Pending => unreachable!(),
        }
    }

    #[test]
    fn keys_ignore_case_spacing_and_end_punctuation_only() {
        let k = |q| key(1, 100, q);
        assert_eq!(k("How many  orders?"), k("how many orders"));
        assert_ne!(k("orders > 100"), k("orders < 100"));
        assert_ne!(key(1, 100, "x"), key(2, 100, "x"), "new catalog generation");
        assert_ne!(key(1, 100, "x"), key(1, 101, "x"), "new day");
    }

    #[test]
    fn memory_cache_is_bounded() {
        let c = MemoryCache::new(2);
        let a = |s: &str| CachedAnswer {
            sql: s.into(),
            fingerprint: 0,
            confidence: 1.0,
            max_bytes_billed: 1,
        };
        now(c.put(1, a("one")));
        now(c.put(2, a("two")));
        now(c.put(3, a("three")));
        assert_eq!(c.len(), 2);
        assert_eq!(now(c.get(1)), None, "oldest evicted");
        assert_eq!(now(c.get(3)).unwrap().sql, "three");
    }
}
