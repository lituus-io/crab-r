// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Per-item records and the joint [`Scorecard`].
//!
//! Scoring rules:
//! - On an **answerable** item, `Correct` is success; `Wrong` is a false
//!   success; abstaining, clarifying or refusing is a (cheap) miss.
//! - On an **unanswerable** item, abstaining, clarifying or refusing is
//!   success, and **any answer is a false success** — the engine confidently
//!   returned a number that cannot be right.
//! - `Error` (crash, timeout, budget exhaustion without a decision) counts
//!   against accuracy but is reported separately: it is a reliability bug,
//!   not a model mistake.

use crate::stats::{self, PairedDelta};
use crab_core::Spend;
use std::collections::HashMap;

/// What the engine did with one item.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Verdict {
    /// Answered; the result matches verified gold.
    Correct,
    /// Answered; the result does not match (or the item was unanswerable).
    Wrong,
    /// Declined to answer.
    Abstained,
    /// Asked the user a clarifying question.
    Clarified,
    /// Refused on policy or read-only grounds.
    Refused,
    /// Failed without a decision.
    Error,
}

impl Verdict {
    /// Did the engine return an answer?
    #[must_use]
    pub fn answered(self) -> bool {
        matches!(self, Self::Correct | Self::Wrong)
    }

    /// A deliberate non-answer.
    #[must_use]
    pub fn declined(self) -> bool {
        matches!(self, Self::Abstained | Self::Clarified | Self::Refused)
    }
}

/// One scored item.
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    /// Item id.
    pub item_id: String,
    /// Whether the item has a correct SQL answer.
    pub answerable: bool,
    /// Outcome.
    pub verdict: Verdict,
    /// Engine's calibrated confidence for its answer (0..=1).
    pub confidence: f32,
    /// Everything spent on the item.
    pub spend: Spend,
    /// End-to-end latency, milliseconds.
    pub latency_ms: u64,
}

impl Record {
    /// Success under the scoring rules.
    #[must_use]
    pub fn success(&self) -> bool {
        if self.answerable {
            self.verdict == Verdict::Correct
        } else {
            self.verdict.declined()
        }
    }

    /// Answered, and wrong (includes any answer to an unanswerable item).
    #[must_use]
    pub fn false_success(&self) -> bool {
        self.verdict == Verdict::Wrong || (!self.answerable && self.verdict == Verdict::Correct)
    }
}

/// The joint report.
#[derive(Clone, Debug, PartialEq)]
pub struct Scorecard {
    /// Items scored.
    pub n: usize,
    /// Items with a correct answer.
    pub answerable: usize,
    /// Success rate over all items (answerable: correct; unanswerable: declined).
    pub success_rate: f64,
    /// Correct answers ÷ answerable items (classic execution accuracy).
    pub accuracy: f64,
    /// False successes ÷ all items.
    pub false_success_rate: f64,
    /// Answered ÷ all items.
    pub coverage: f64,
    /// Wrong ÷ answered (0 when nothing answered).
    pub selective_risk: f64,
    /// Area under the risk–coverage curve over answered items ordered by
    /// confidence (lower is better; perfect calibration minimises it).
    pub aurc: f64,
    /// Declines that were on unanswerable items ÷ all declines.
    pub decline_precision: f64,
    /// Unanswerable items declined ÷ unanswerable items.
    pub decline_recall: f64,
    /// Items that errored.
    pub errors: usize,
    /// Total spend.
    pub spend: Spend,
    /// Cached share of input tokens.
    pub cached_share: f64,
    /// Median cost per item, micro-dollars.
    pub cost_p50: u64,
    /// Latency percentiles, milliseconds.
    pub latency_p50: u64,
    /// 95th percentile latency.
    pub latency_p95: u64,
    /// 99th percentile latency.
    pub latency_p99: u64,
}

fn ratio(a: usize, b: usize) -> f64 {
    if b == 0 {
        0.0
    } else {
        a as f64 / b as f64
    }
}

impl Scorecard {
    /// Score a run.
    #[must_use]
    pub fn from_records(records: &[Record]) -> Self {
        let n = records.len();
        let answerable = records.iter().filter(|r| r.answerable).count();
        let unanswerable = n - answerable;
        let success = records.iter().filter(|r| r.success()).count();
        let correct = records.iter().filter(|r| r.answerable && r.verdict == Verdict::Correct).count();
        let false_success = records.iter().filter(|r| r.false_success()).count();
        let answered = records.iter().filter(|r| r.verdict.answered()).count();
        let declined = records.iter().filter(|r| r.verdict.declined()).count();
        let declined_unanswerable = records.iter().filter(|r| !r.answerable && r.verdict.declined()).count();
        let errors = records.iter().filter(|r| r.verdict == Verdict::Error).count();

        let spend = records.iter().fold(Spend::default(), |acc, r| acc.plus(r.spend));
        let input_all = spend.input_tokens + spend.cached_tokens;
        let mut costs: Vec<u64> = records.iter().map(|r| r.spend.micro_usd).collect();
        costs.sort_unstable();
        let mut lat: Vec<u64> = records.iter().map(|r| r.latency_ms).collect();
        lat.sort_unstable();

        Self {
            n,
            answerable,
            success_rate: ratio(success, n),
            accuracy: ratio(correct, answerable),
            false_success_rate: ratio(false_success, n),
            coverage: ratio(answered, n),
            selective_risk: ratio(false_success, answered),
            aurc: aurc(records),
            decline_precision: ratio(declined_unanswerable, declined),
            decline_recall: ratio(declined_unanswerable, unanswerable),
            errors,
            spend,
            cached_share: if input_all == 0 { 0.0 } else { spend.cached_tokens as f64 / input_all as f64 },
            cost_p50: stats::percentile(&costs, 0.5),
            latency_p50: stats::percentile(&lat, 0.5),
            latency_p95: stats::percentile(&lat, 0.95),
            latency_p99: stats::percentile(&lat, 0.99),
        }
    }
}

/// AURC over answered items: sort by confidence descending (ties broken by
/// id for determinism), then average the running error rate.
fn aurc(records: &[Record]) -> f64 {
    let mut answered: Vec<&Record> = records.iter().filter(|r| r.verdict.answered()).collect();
    if answered.is_empty() {
        return 0.0;
    }
    answered.sort_by(|a, b| b.confidence.total_cmp(&a.confidence).then_with(|| a.item_id.cmp(&b.item_id)));
    let mut wrong = 0usize;
    let mut area = 0.0;
    for (k, r) in answered.iter().enumerate() {
        if r.false_success() {
            wrong += 1;
        }
        area += wrong as f64 / (k + 1) as f64;
    }
    area / answered.len() as f64
}

/// pass^k: fraction of items that succeed in **every** one of k runs
/// (matched by item id; items missing from any run count as failures).
#[must_use]
pub fn pass_k(runs: &[&[Record]]) -> f64 {
    let Some(first) = runs.first() else { return 0.0 };
    if first.is_empty() {
        return 0.0;
    }
    let maps: Vec<HashMap<&str, bool>> =
        runs.iter().map(|run| run.iter().map(|r| (r.item_id.as_str(), r.success())).collect()).collect();
    let all = first.iter().filter(|r| maps.iter().all(|m| m.get(r.item_id.as_str()).copied().unwrap_or(false))).count();
    ratio(all, first.len())
}

/// Paired bootstrap of success between a baseline and a candidate run,
/// aligned by item id (items present in only one run are dropped).
#[must_use]
pub fn compare(baseline: &[Record], candidate: &[Record], iters: usize, seed: u64) -> PairedDelta {
    let cand: HashMap<&str, &Record> = candidate.iter().map(|r| (r.item_id.as_str(), r)).collect();
    let mut a = Vec::with_capacity(baseline.len());
    let mut b = Vec::with_capacity(baseline.len());
    for r in baseline {
        if let Some(c) = cand.get(r.item_id.as_str()) {
            a.push(f64::from(u8::from(r.success())));
            b.push(f64::from(u8::from(c.success())));
        }
    }
    stats::paired_bootstrap(&a, &b, iters, seed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &str, answerable: bool, verdict: Verdict, confidence: f32, usd: u64, ms: u64) -> Record {
        Record {
            item_id: id.into(),
            answerable,
            verdict,
            confidence,
            spend: Spend { micro_usd: usd, input_tokens: 100, cached_tokens: 300, ..Spend::default() },
            latency_ms: ms,
        }
    }

    #[test]
    fn scoring_rules() {
        let rs = [
            rec("a", true, Verdict::Correct, 0.9, 10, 100),
            rec("b", true, Verdict::Wrong, 0.4, 10, 200),
            rec("c", true, Verdict::Abstained, 0.1, 5, 300),
            rec("d", false, Verdict::Clarified, 0.2, 5, 400),
            rec("e", false, Verdict::Correct, 0.8, 10, 500), // answered the unanswerable
        ];
        let s = Scorecard::from_records(&rs);
        assert_eq!((s.n, s.answerable), (5, 3));
        assert!((s.accuracy - 1.0 / 3.0).abs() < 1e-12);
        assert!((s.success_rate - 2.0 / 5.0).abs() < 1e-12, "a and d succeed");
        assert!((s.false_success_rate - 2.0 / 5.0).abs() < 1e-12, "b and e");
        assert!((s.coverage - 3.0 / 5.0).abs() < 1e-12);
        assert!((s.selective_risk - 2.0 / 3.0).abs() < 1e-12);
        assert!((s.decline_precision - 0.5).abs() < 1e-12, "c declined an answerable item");
        assert!((s.decline_recall - 0.5).abs() < 1e-12);
        assert!((s.cached_share - 0.75).abs() < 1e-12);
        assert_eq!((s.cost_p50, s.latency_p50, s.latency_p99), (10, 300, 500));
    }

    #[test]
    fn aurc_rewards_calibration() {
        // Same answers; only the confidence ordering differs.
        let good = [rec("1", true, Verdict::Correct, 0.9, 0, 0), rec("2", true, Verdict::Wrong, 0.1, 0, 0)];
        let bad = [rec("1", true, Verdict::Correct, 0.1, 0, 0), rec("2", true, Verdict::Wrong, 0.9, 0, 0)];
        assert!(Scorecard::from_records(&good).aurc < Scorecard::from_records(&bad).aurc);
    }

    #[test]
    fn pass_k_requires_every_run() {
        let r1 = [rec("x", true, Verdict::Correct, 1.0, 0, 0), rec("y", true, Verdict::Correct, 1.0, 0, 0)];
        let r2 = [rec("x", true, Verdict::Correct, 1.0, 0, 0), rec("y", true, Verdict::Wrong, 1.0, 0, 0)];
        assert!((pass_k(&[&r1, &r2]) - 0.5).abs() < 1e-12);
        assert!((pass_k(&[&r1]) - 1.0).abs() < 1e-12);
        assert!(pass_k(&[]).abs() < 1e-12);
    }

    #[test]
    fn compare_aligns_by_id() {
        let base = [rec("x", true, Verdict::Wrong, 0.0, 0, 0), rec("y", true, Verdict::Correct, 0.0, 0, 0)];
        let cand = [rec("y", true, Verdict::Correct, 0.0, 0, 0), rec("x", true, Verdict::Correct, 0.0, 0, 0)];
        let d = compare(&base, &cand, 200, 1);
        assert_eq!(d.n, 2);
        assert!((d.delta - 0.5).abs() < 1e-12);
    }

    #[test]
    fn empty_run_is_all_zero() {
        let s = Scorecard::from_records(&[]);
        assert_eq!(s.n, 0);
        assert!(s.accuracy.abs() < 1e-12 && s.aurc.abs() < 1e-12);
    }
}
