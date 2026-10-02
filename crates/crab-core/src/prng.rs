// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Deterministic pseudo-random generators: [`SplitMix64`] for seeding and
//! cheap streams, [`Xoshiro256`] (xoshiro256**) for bulk generation such as
//! synthetic micro-databases. Not cryptographic. Same seed, same sequence, on
//! every platform — reproducibility is the point.

/// SplitMix64 (Steele, Lea, Flood 2014).
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Seeded generator.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// xoshiro256** (Blackman, Vigna 2018).
#[derive(Clone, Debug)]
pub struct Xoshiro256 {
    s: [u64; 4],
}

impl Xoshiro256 {
    /// Seeded generator; the seed is expanded through [`SplitMix64`] so any
    /// seed (including 0) gives a valid, non-zero state.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        let mut sm = SplitMix64::new(seed);
        Self { s: [sm.next_u64(), sm.next_u64(), sm.next_u64(), sm.next_u64()] }
    }

    /// Next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    /// Uniform integer in `0..n` (Lemire's multiply-shift with rejection;
    /// unbiased). `n == 0` returns 0.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        loop {
            let m = u128::from(self.next_u64()) * u128::from(n);
            let low = m as u64;
            if low >= n.wrapping_neg() % n {
                return (m >> 64) as u64;
            }
        }
    }

    /// Uniform float in `[0, 1)` with 53 bits of precision.
    pub fn unit_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// `true` with probability `p` (clamped to `[0, 1]`).
    pub fn chance(&mut self, p: f64) -> bool {
        self.unit_f64() < p.clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix_reference_vector() {
        // First outputs for seed 1234567 from the reference implementation.
        let mut g = SplitMix64::new(1_234_567);
        assert_eq!(g.next_u64(), 6_457_827_717_110_365_317);
        assert_eq!(g.next_u64(), 3_203_168_211_198_807_973);
    }

    #[test]
    fn same_seed_same_sequence() {
        let mut a = Xoshiro256::new(42);
        let mut b = Xoshiro256::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn zero_seed_is_usable() {
        let mut g = Xoshiro256::new(0);
        assert_ne!(g.next_u64(), 0);
    }

    #[test]
    fn below_stays_in_range_and_covers_it() {
        let mut g = Xoshiro256::new(7);
        let mut seen = [false; 10];
        for _ in 0..1000 {
            let v = g.below(10);
            assert!(v < 10);
            seen[v as usize] = true;
        }
        assert!(seen.iter().all(|&s| s));
        assert_eq!(g.below(0), 0);
    }

    #[test]
    fn unit_is_half_open() {
        let mut g = Xoshiro256::new(9);
        for _ in 0..10_000 {
            let x = g.unit_f64();
            assert!((0.0..1.0).contains(&x));
        }
    }
}
