// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! [`Pool`]: a typed index arena.
//!
//! Tree-shaped per-request data (the IR, candidate sets, diagnostics) is
//! stored as `u32` handles into one contiguous `Vec<T>` per node type instead
//! of heap references. That gives, with no `unsafe`:
//!
//! - **contiguity**: siblings and children sit next to each other in memory,
//!   which is what tree walks in the binder and canonicaliser want;
//! - **no per-node allocation**: [`Pool::clear`] keeps capacity, so a warm
//!   session allocates nothing per question;
//! - **4-byte links**: an [`Idx`] is half a pointer, a [`Span`] is one pointer.
//!
//! Handles are typed (`Idx<Expr>` cannot index a `Pool<Rel>`) but not branded
//! to one pool instance; a handle from a cleared pool is caught by the bounds
//! check on access in debug and returns `None` from [`Pool::get`].

use std::fmt;
use std::marker::PhantomData;

/// A handle to one element of a [`Pool<T>`].
pub struct Idx<T> {
    raw: u32,
    _ty: PhantomData<fn() -> T>,
}

impl<T> Idx<T> {
    /// The raw position.
    #[must_use]
    pub fn raw(self) -> u32 {
        self.raw
    }

    const fn new(raw: u32) -> Self {
        Self { raw, _ty: PhantomData }
    }
}

// Manual impls: derives would wrongly require `T: Clone` etc.
impl<T> Clone for Idx<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Idx<T> {}
impl<T> PartialEq for Idx<T> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}
impl<T> Eq for Idx<T> {}
impl<T> std::hash::Hash for Idx<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.raw.hash(state);
    }
}
impl<T> PartialOrd for Idx<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for Idx<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.raw.cmp(&other.raw)
    }
}
impl<T> fmt::Debug for Idx<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.raw)
    }
}

/// A contiguous run of elements in a [`Pool<T>`] (children lists, argument
/// lists). The empty span is valid in every pool.
pub struct Span<T> {
    start: u32,
    len: u32,
    _ty: PhantomData<fn() -> T>,
}

impl<T> Span<T> {
    /// The empty span.
    pub const EMPTY: Self = Self { start: 0, len: 0, _ty: PhantomData };

    /// Number of elements.
    #[must_use]
    pub fn len(self) -> usize {
        self.len as usize
    }

    /// True when the span holds nothing.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    /// Handles of every element, in order.
    pub fn iter(self) -> impl ExactSizeIterator<Item = Idx<T>> + Clone {
        (self.start..self.start + self.len).map(Idx::new)
    }
}

impl<T> Clone for Span<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Span<T> {}
impl<T> PartialEq for Span<T> {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.len == other.len
    }
}
impl<T> Eq for Span<T> {}
impl<T> fmt::Debug for Span<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}..{}", self.start, self.start + self.len)
    }
}

/// A typed index arena. See the module docs.
pub struct Pool<T> {
    items: Vec<T>,
    cap: u32,
}

impl<T> Default for Pool<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for Pool<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool").field("len", &self.items.len()).field("cap", &self.cap).finish()
    }
}

impl<T> Pool<T> {
    /// An empty pool whose element count may reach `u32::MAX`.
    #[must_use]
    pub fn new() -> Self {
        Self { items: Vec::new(), cap: u32::MAX }
    }

    /// An empty pool with room for `n` elements and a hard element cap of
    /// `limit`. Pushing past the cap is an error, never unbounded growth.
    #[must_use]
    pub fn with_limit(n: usize, limit: u32) -> Self {
        Self { items: Vec::with_capacity(n.min(limit as usize)), cap: limit }
    }

    /// Element count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Drop every element but keep the allocation for reuse.
    pub fn clear(&mut self) {
        self.items.clear();
    }

    /// Drop every element at or after position `len` (a backtracking
    /// parser's restore point), keeping capacity.
    pub fn truncate(&mut self, len: usize) {
        self.items.truncate(len);
    }

    /// Store `value`; returns its handle.
    pub fn push(&mut self, value: T) -> crate::Result<Idx<T>> {
        let at = self.items.len();
        if at >= self.cap as usize {
            return Err(crate::Error::Limit("pool element cap"));
        }
        self.items.push(value);
        Ok(Idx::new(at as u32))
    }

    /// Store every element of `values` contiguously; returns their span.
    /// On a cap overflow nothing past the cap is kept and the pool is rolled
    /// back to its previous length.
    pub fn extend(&mut self, values: impl IntoIterator<Item = T>) -> crate::Result<Span<T>> {
        let start = self.items.len();
        for v in values {
            if self.items.len() >= self.cap as usize {
                self.items.truncate(start);
                return Err(crate::Error::Limit("pool element cap"));
            }
            self.items.push(v);
        }
        let len = self.items.len() - start;
        Ok(Span { start: start as u32, len: len as u32, _ty: PhantomData })
    }

    /// The element behind `idx`, or `None` for a stale handle.
    #[must_use]
    pub fn get(&self, idx: Idx<T>) -> Option<&T> {
        self.items.get(idx.raw as usize)
    }

    /// Mutable access to the element behind `idx`.
    pub fn get_mut(&mut self, idx: Idx<T>) -> Option<&mut T> {
        self.items.get_mut(idx.raw as usize)
    }

    /// The elements of `span`, or an empty slice for a stale span.
    #[must_use]
    pub fn slice(&self, span: Span<T>) -> &[T] {
        let start = span.start as usize;
        self.items.get(start..start + span.len as usize).unwrap_or(&[])
    }

    /// Every element with its handle, in insertion order.
    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (Idx<T>, &T)> {
        self.items.iter().enumerate().map(|(i, v)| (Idx::new(i as u32), v))
    }
}

impl<T> std::ops::Index<Idx<T>> for Pool<T> {
    type Output = T;
    fn index(&self, idx: Idx<T>) -> &T {
        &self.items[idx.raw as usize]
    }
}

impl<T> std::ops::IndexMut<Idx<T>> for Pool<T> {
    fn index_mut(&mut self, idx: Idx<T>) -> &mut T {
        &mut self.items[idx.raw as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_index() {
        let mut p = Pool::new();
        let a = p.push("a").unwrap();
        let b = p.push("b").unwrap();
        assert_eq!(p[a], "a");
        assert_eq!(p[b], "b");
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn extend_is_contiguous() {
        let mut p = Pool::new();
        p.push(0).unwrap();
        let s = p.extend([1, 2, 3]).unwrap();
        assert_eq!(p.slice(s), &[1, 2, 3]);
        assert_eq!(s.iter().map(|i| p[i]).collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn empty_span_is_valid_everywhere() {
        let p: Pool<u8> = Pool::new();
        assert!(p.slice(Span::EMPTY).is_empty());
        assert_eq!(Span::<u8>::EMPTY.iter().count(), 0);
    }

    #[test]
    fn clear_keeps_capacity_and_invalidates_handles() {
        let mut p = Pool::new();
        let a = p.push(7u64).unwrap();
        let cap = p.items.capacity();
        p.clear();
        assert!(p.get(a).is_none());
        assert_eq!(p.items.capacity(), cap);
    }

    #[test]
    fn cap_is_enforced_and_extend_rolls_back() {
        let mut p = Pool::with_limit(4, 3);
        p.push(1).unwrap();
        assert!(p.extend([2, 3, 4]).is_err());
        assert_eq!(p.len(), 1, "a failed extend must not leave a partial run");
        p.extend([2, 3]).unwrap();
        assert!(p.push(4).is_err());
    }

    #[test]
    fn truncate_restores_a_mark() {
        let mut p = Pool::new();
        p.extend([1, 2]).unwrap();
        let mark = p.len();
        p.extend([3, 4]).unwrap();
        p.truncate(mark);
        assert_eq!(p.len(), 2);
        assert_eq!(p.push(9).unwrap().raw(), 2, "handles continue from the mark");
    }

    #[test]
    fn stale_span_is_empty_not_a_panic() {
        let mut p = Pool::new();
        let s = p.extend([1, 2]).unwrap();
        p.clear();
        assert!(p.slice(s).is_empty());
    }
}
