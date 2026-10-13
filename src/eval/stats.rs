// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Small, exact statistics: percentiles and the paired bootstrap.

use crate::base::prng::Xoshiro256;

/// Nearest-rank percentile of an **ascending-sorted** slice (`q` in 0..=1).
/// Empty input gives 0.
#[must_use]
pub fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (q.clamp(0.0, 1.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// Mean of a slice (0 for empty).
#[must_use]
pub fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        0.0
    } else {
        xs.iter().sum::<f64>() / xs.len() as f64
    }
}

/// A paired comparison of two runs over the same items.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PairedDelta {
    /// Mean of `b - a`.
    pub delta: f64,
    /// 2.5th percentile of the bootstrap distribution of the mean delta.
    pub lo: f64,
    /// 97.5th percentile.
    pub hi: f64,
    /// Items compared.
    pub n: usize,
}

impl PairedDelta {
    /// True when the 95 % interval excludes zero in the improving direction.
    #[must_use]
    pub fn significant_gain(&self) -> bool {
        self.lo > 0.0
    }

    /// True when the 95 % interval excludes zero in the worsening direction.
    #[must_use]
    pub fn significant_loss(&self) -> bool {
        self.hi < 0.0
    }
}

/// Paired bootstrap of `mean(b) - mean(a)` where `a[i]` and `b[i]` score the
/// same item (e.g. 1.0 = correct). Deterministic for a given `seed`.
/// Resampling draws item indices, so per-item difficulty cancels out — the
/// reason a paired test detects gains an unpaired one would miss.
#[must_use]
pub fn paired_bootstrap(a: &[f64], b: &[f64], iters: usize, seed: u64) -> PairedDelta {
    let n = a.len().min(b.len());
    if n == 0 {
        return PairedDelta { delta: 0.0, lo: 0.0, hi: 0.0, n: 0 };
    }
    let diffs: Vec<f64> = a[..n].iter().zip(&b[..n]).map(|(x, y)| y - x).collect();
    let delta = mean(&diffs);
    let iters = iters.max(1);
    let mut rng = Xoshiro256::new(seed);
    let mut means: Vec<f64> = Vec::with_capacity(iters);
    for _ in 0..iters {
        let mut sum = 0.0;
        for _ in 0..n {
            sum += diffs[rng.below(n as u64) as usize];
        }
        means.push(sum / n as f64);
    }
    means.sort_by(f64::total_cmp);
    let at = |q: f64| means[((q * iters as f64) as usize).min(iters - 1)];
    PairedDelta { delta, lo: at(0.025), hi: at(0.975), n }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_nearest_rank() {
        let v: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&v, 0.5), 50);
        assert_eq!(percentile(&v, 0.95), 95);
        assert_eq!(percentile(&v, 0.99), 99);
        assert_eq!(percentile(&v, 1.0), 100);
        assert_eq!(percentile(&v, 0.0), 1);
        assert_eq!(percentile(&[], 0.5), 0);
    }

    #[test]
    fn identical_runs_have_zero_delta() {
        let a = vec![1.0, 0.0, 1.0, 1.0];
        let d = paired_bootstrap(&a, &a, 1000, 1);
        assert_eq!((d.delta, d.lo, d.hi), (0.0, 0.0, 0.0));
        assert!(!d.significant_gain() && !d.significant_loss());
    }

    #[test]
    fn a_clear_gain_is_significant() {
        // b fixes 30 of 200 items and breaks none.
        let a: Vec<f64> = (0..200).map(|i| if i < 100 { 1.0 } else { 0.0 }).collect();
        let b: Vec<f64> = (0..200).map(|i| if i < 130 { 1.0 } else { 0.0 }).collect();
        let d = paired_bootstrap(&a, &b, 2000, 7);
        assert!((d.delta - 0.15).abs() < 1e-12);
        assert!(d.significant_gain(), "{d:?}");
        assert!(d.lo <= d.delta && d.delta <= d.hi);
    }

    #[test]
    fn noise_is_not_significant() {
        // b fixes 3 and breaks 3.
        let a: Vec<f64> = (0..200).map(|i| if i < 100 { 1.0 } else { 0.0 }).collect();
        let mut b = a.clone();
        for i in 0..3 {
            b[i] = 0.0;
            b[150 + i] = 1.0;
        }
        let d = paired_bootstrap(&a, &b, 2000, 7);
        assert!(!d.significant_gain() && !d.significant_loss(), "{d:?}");
    }

    #[test]
    fn deterministic_for_a_seed() {
        let a = vec![0.0, 1.0, 0.0, 1.0, 1.0];
        let b = vec![1.0, 1.0, 0.0, 1.0, 0.0];
        assert_eq!(paired_bootstrap(&a, &b, 500, 3), paired_bootstrap(&a, &b, 500, 3));
    }
}
