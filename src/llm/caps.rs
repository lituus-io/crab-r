// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Capability and price descriptors: everything the engine needs to choose a
//! strategy for a model without knowing who made it.

use crate::base::Spend;

/// How output can be constrained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConstraintSupport {
    /// Accepts a Lark context-free grammar (e.g. llguidance servers). The
    /// engine then decodes SQL under the grammar instead of validating after.
    pub lark: bool,
    /// Accepts a GBNF grammar (llama.cpp-style servers).
    pub gbnf: bool,
    /// Accepts a JSON schema for structured output.
    pub json_schema: bool,
}

/// Prompt-prefix caching.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheSupport {
    /// No caching: every input token is billed in full.
    None,
    /// The provider caches repeated prefixes automatically; byte-stable
    /// prompt ordering is all it takes.
    Implicit,
    /// Prefixes must be registered explicitly (and may also be cached
    /// implicitly).
    Explicit,
}

/// Prices in micro-dollars per million tokens (exact integer accounting).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Price {
    /// Uncached input.
    pub input: u64,
    /// Input served from a prefix cache.
    pub cached_input: u64,
    /// Output (including any reasoning tokens the provider bills as output).
    pub output: u64,
}

impl Price {
    /// Micro-dollars for the given token counts, rounded up.
    #[must_use]
    pub fn cost(&self, input: u64, cached: u64, output: u64) -> u64 {
        // Each product fits in u128; only the sum can overflow.
        let m = (u128::from(input) * u128::from(self.input))
            .saturating_add(u128::from(cached) * u128::from(self.cached_input))
            .saturating_add(u128::from(output) * u128::from(self.output));
        u64::try_from(m.div_ceil(1_000_000)).unwrap_or(u64::MAX)
    }
}

/// What a model supports and costs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Caps {
    /// Stable identifier (recorded in replay keys and reports).
    pub id: String,
    /// Output constraints.
    pub constraints: ConstraintSupport,
    /// Most candidates one request can return (1 = no multi-sampling; the
    /// engine then issues parallel requests).
    pub max_candidates: u8,
    /// Most candidates one *streamed* request can return. Some servers
    /// multi-sample only without streaming; a backend then serves `n` above
    /// this from one non-streamed request (still one prefill).
    pub max_stream_candidates: u8,
    /// Prefix caching.
    pub cache: CacheSupport,
    /// Context window, tokens.
    pub context_tokens: u32,
    /// Largest output per candidate, tokens.
    pub max_output_tokens: u32,
    /// Streaming deltas (enables stream-abort).
    pub streaming: bool,
    /// Prices.
    pub price: Price,
}

impl Caps {
    /// What a call with this usage cost, as a budget [`Spend`].
    #[must_use]
    pub fn spend(&self, input: u64, cached: u64, output: u64) -> Spend {
        Spend {
            input_tokens: input,
            cached_tokens: cached,
            output_tokens: output,
            warehouse_bytes: 0,
            micro_usd: self.price.cost(input, cached, output),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_is_exact_and_rounds_up() {
        let p = Price { input: 300_000, cached_input: 75_000, output: 2_500_000 };
        // 1M input = $0.30, 1M cached = $0.075, 1M output = $2.50.
        assert_eq!(p.cost(1_000_000, 0, 0), 300_000);
        assert_eq!(p.cost(0, 1_000_000, 0), 75_000);
        assert_eq!(p.cost(0, 0, 1_000_000), 2_500_000);
        assert_eq!(p.cost(1, 0, 0), 1, "fractions of a micro-dollar round up");
        assert_eq!(p.cost(0, 0, 0), 0);
    }

    #[test]
    fn cost_never_overflows() {
        let p = Price { input: u64::MAX, cached_input: u64::MAX, output: u64::MAX };
        assert_eq!(p.cost(u64::MAX, u64::MAX, u64::MAX), u64::MAX);
    }
}
