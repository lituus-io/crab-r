// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Per-request spending limits. Every state transition in the engine charges
//! a [`Spend`] against a [`Budget`]; once any dimension is exhausted the
//! engine stops exploring and decides with what it has. Money is tracked in
//! micro-dollars so accounting is exact integer arithmetic.

/// What one step cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Spend {
    /// Uncached input tokens.
    pub input_tokens: u64,
    /// Input tokens served from a provider cache.
    pub cached_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
    /// Warehouse bytes billed.
    pub warehouse_bytes: u64,
    /// Micro-dollars (1e-6 USD).
    pub micro_usd: u64,
}

impl Spend {
    /// Component-wise saturating sum.
    #[must_use]
    pub fn plus(self, o: Self) -> Self {
        Self {
            input_tokens: self.input_tokens.saturating_add(o.input_tokens),
            cached_tokens: self.cached_tokens.saturating_add(o.cached_tokens),
            output_tokens: self.output_tokens.saturating_add(o.output_tokens),
            warehouse_bytes: self.warehouse_bytes.saturating_add(o.warehouse_bytes),
            micro_usd: self.micro_usd.saturating_add(o.micro_usd),
        }
    }
}

/// Limits for one request, and what has been spent so far.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    /// Maximum total tokens (input + cached + output).
    pub max_tokens: u64,
    /// Maximum warehouse bytes billed.
    pub max_warehouse_bytes: u64,
    /// Maximum micro-dollars.
    pub max_micro_usd: u64,
    /// Maximum wall time in milliseconds.
    pub max_wall_ms: u64,
    spent: Spend,
}

/// Which limit a charge would break.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exhausted {
    /// Token limit.
    Tokens,
    /// Warehouse-bytes limit.
    WarehouseBytes,
    /// Money limit.
    Money,
    /// Wall-time limit.
    WallTime,
}

impl Default for Budget {
    /// Defaults for the routed tiers: 60 k tokens, 10 GiB billed, $0.10, 20 s.
    fn default() -> Self {
        Self::new(60_000, 10 << 30, 100_000, 20_000)
    }
}

impl Budget {
    /// A fresh budget.
    #[must_use]
    pub fn new(
        max_tokens: u64,
        max_warehouse_bytes: u64,
        max_micro_usd: u64,
        max_wall_ms: u64,
    ) -> Self {
        Self {
            max_tokens,
            max_warehouse_bytes,
            max_micro_usd,
            max_wall_ms,
            spent: Spend::default(),
        }
    }

    /// Everything charged so far.
    #[must_use]
    pub fn spent(&self) -> Spend {
        self.spent
    }

    /// Would charging `s` stay within every limit? Does not charge.
    pub fn check(&self, s: Spend) -> Result<(), Exhausted> {
        let after = self.spent.plus(s);
        let tokens = after
            .input_tokens
            .saturating_add(after.cached_tokens)
            .saturating_add(after.output_tokens);
        if tokens > self.max_tokens {
            return Err(Exhausted::Tokens);
        }
        if after.warehouse_bytes > self.max_warehouse_bytes {
            return Err(Exhausted::WarehouseBytes);
        }
        if after.micro_usd > self.max_micro_usd {
            return Err(Exhausted::Money);
        }
        Ok(())
    }

    /// Record `s` unconditionally (spend that already happened) and report
    /// whether a limit is now exceeded.
    pub fn charge(&mut self, s: Spend) -> Result<(), Exhausted> {
        let verdict = self.check(s);
        self.spent = self.spent.plus(s);
        verdict
    }

    /// Wall-time check against a start instant.
    pub fn check_wall(&self, elapsed_ms: u64) -> Result<(), Exhausted> {
        if elapsed_ms > self.max_wall_ms {
            Err(Exhausted::WallTime)
        } else {
            Ok(())
        }
    }

    /// Warehouse bytes still available, for `maximumBytesBilled`.
    #[must_use]
    pub fn remaining_warehouse_bytes(&self) -> u64 {
        self.max_warehouse_bytes.saturating_sub(self.spent.warehouse_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(n: u64) -> Spend {
        Spend { input_tokens: n, ..Spend::default() }
    }

    #[test]
    fn check_does_not_charge() {
        let b = Budget::new(100, 0, 0, 0);
        assert!(b.check(tokens(50)).is_ok());
        assert_eq!(b.spent(), Spend::default());
    }

    #[test]
    fn charge_accumulates_and_reports_overrun() {
        let mut b = Budget::new(100, 10, 10, 10);
        assert!(b.charge(tokens(60)).is_ok());
        assert_eq!(b.charge(tokens(60)), Err(Exhausted::Tokens));
        assert_eq!(b.spent().input_tokens, 120, "spend that happened is always recorded");
    }

    #[test]
    fn every_dimension_is_limited() {
        let b = Budget::new(u64::MAX, 5, 5, 5);
        assert_eq!(
            b.check(Spend { warehouse_bytes: 6, ..Spend::default() }),
            Err(Exhausted::WarehouseBytes)
        );
        assert_eq!(b.check(Spend { micro_usd: 6, ..Spend::default() }), Err(Exhausted::Money));
        assert_eq!(b.check_wall(6), Err(Exhausted::WallTime));
    }

    #[test]
    fn cached_and_output_tokens_count_toward_the_limit() {
        let b = Budget::new(10, 0, 0, 0);
        assert!(b.check(Spend { cached_tokens: 6, output_tokens: 5, ..Spend::default() }).is_err());
    }

    #[test]
    fn remaining_bytes_saturates() {
        let mut b = Budget::new(0, 100, u64::MAX, 0);
        let _ = b.charge(Spend { warehouse_bytes: 150, ..Spend::default() });
        assert_eq!(b.remaining_warehouse_bytes(), 0);
    }

    #[test]
    fn plus_saturates() {
        let a = Spend { micro_usd: u64::MAX, ..Spend::default() };
        assert_eq!(a.plus(a).micro_usd, u64::MAX);
    }
}
