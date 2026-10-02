// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Candidate pool and execution voting.
//!
//! Samples are deduplicated by meaning-level fingerprint (two spellings of
//! one query are one candidate with two votes), only the most-voted distinct
//! candidates are executed, and executed candidates are grouped by result:
//! different SQL returning the same rows agree. A declined sample
//! (`CANNOT_ANSWER`) is a vote against answering.

use crab_exec::QueryResult;

/// One distinct candidate.
#[derive(Debug)]
pub(crate) struct Cand {
    /// Canonical SQL (what runs).
    pub(crate) canonical: String,
    /// The model's SQL, first spelling seen.
    pub(crate) model_sql: String,
    /// Meaning-level fingerprint.
    pub(crate) fp: u128,
    /// Catalog tables it reads (for the byte-cap floor).
    pub(crate) tables: u64,
    /// Samples that produced it.
    pub(crate) votes: u32,
    /// Execution state.
    pub(crate) state: State,
}

/// Where a candidate is in execution.
#[derive(Debug)]
pub(crate) enum State {
    /// Not executed (yet).
    Pending,
    /// Executed: result and its grouping key.
    Done { result: Box<QueryResult>, key: u64, bytes: u64 },
    /// Rejected by the warehouse or the guardrails.
    Failed(String),
}

/// A group of candidates returning the same result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Group {
    /// Result key.
    pub(crate) key: u64,
    /// Summed votes.
    pub(crate) votes: u32,
    /// Member candidates, most votes first.
    pub(crate) members: Vec<usize>,
}

/// All samples of one question.
#[derive(Debug, Default)]
pub(crate) struct Pool {
    /// Distinct candidates, in first-seen order.
    pub(crate) cands: Vec<Cand>,
    /// Samples that declined, and the first reason given.
    pub(crate) declines: u32,
    pub(crate) decline_reason: Option<String>,
    /// Samples drawn.
    pub(crate) drawn: u32,
}

impl Pool {
    /// Add a valid sample (a vote for its fingerprint).
    pub(crate) fn vote(&mut self, canonical: String, model_sql: &str, fp: u128, tables: u64) {
        if let Some(c) = self.cands.iter_mut().find(|c| c.fp == fp) {
            c.votes += 1;
            return;
        }
        self.cands.push(Cand {
            canonical,
            model_sql: model_sql.to_owned(),
            fp,
            tables,
            votes: 1,
            state: State::Pending,
        });
    }

    /// Add a declining sample.
    pub(crate) fn decline(&mut self, reason: &str) {
        self.declines += 1;
        if self.decline_reason.is_none() {
            self.decline_reason = Some(reason.to_owned());
        }
    }

    /// Candidates not failed.
    pub(crate) fn viable(&self) -> usize {
        self.cands.iter().filter(|c| !matches!(c.state, State::Failed(_))).count()
    }

    /// Any candidate executed successfully.
    pub(crate) fn has_result(&self) -> bool {
        self.cands.iter().any(|c| matches!(c.state, State::Done { .. }))
    }

    /// Pending candidates to execute now, most votes first (first seen on
    /// ties), so that at most `max` candidates are ever executed.
    pub(crate) fn to_execute(&self, max: usize) -> Vec<usize> {
        let tried = self.cands.iter().filter(|c| !matches!(c.state, State::Pending)).count();
        let mut idx: Vec<usize> = (0..self.cands.len())
            .filter(|&i| matches!(self.cands[i].state, State::Pending))
            .collect();
        idx.sort_by(|&a, &b| self.cands[b].votes.cmp(&self.cands[a].votes).then(a.cmp(&b)));
        idx.truncate(max.saturating_sub(tried));
        idx
    }

    /// Result groups, most votes first (ties: the group seen first).
    pub(crate) fn groups(&self) -> Vec<Group> {
        let mut groups: Vec<Group> = Vec::new();
        for (i, c) in self.cands.iter().enumerate() {
            let State::Done { key, .. } = c.state else { continue };
            match groups.iter_mut().find(|g| g.key == key) {
                Some(g) => {
                    g.votes += c.votes;
                    g.members.push(i);
                }
                None => groups.push(Group { key, votes: c.votes, members: vec![i] }),
            }
        }
        for g in &mut groups {
            g.members
                .sort_by(|&a, &b| self.cands[b].votes.cmp(&self.cands[a].votes).then(a.cmp(&b)));
        }
        // Stable sort keeps first-seen order on ties.
        groups.sort_by_key(|g| std::cmp::Reverse(g.votes));
        groups
    }

    /// Votes that count toward confidence: every non-failed sample plus
    /// declines (a decline disagrees with every answer).
    pub(crate) fn total_votes(&self) -> u32 {
        self.cands
            .iter()
            .filter(|c| !matches!(c.state, State::Failed(_)))
            .map(|c| c.votes)
            .sum::<u32>()
            + self.declines
    }

    /// Warehouse/guardrail failures, for repair.
    pub(crate) fn failures(&self) -> Vec<String> {
        self.cands
            .iter()
            .filter_map(|c| match &c.state {
                State::Failed(m) => Some(m.clone()),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crab_core::batch::Batch;

    fn done(key: u64) -> State {
        State::Done {
            result: Box::new(QueryResult {
                batch: Batch::new(&[], 0),
                total_rows: 0,
                truncated: false,
                bytes_processed: 0,
                bytes_billed: 0,
                cache_hit: false,
                wall_ms: 0,
            }),
            key,
            bytes: 0,
        }
    }

    #[test]
    fn votes_dedup_by_fingerprint_and_group_by_result() {
        let mut p = Pool::default();
        p.vote("A".into(), "a1", 1, 1);
        p.vote("B".into(), "b", 2, 1);
        p.vote("A".into(), "a2", 1, 1);
        p.vote("C".into(), "c", 3, 1);
        p.decline("no");
        assert_eq!(p.cands.len(), 3);
        assert_eq!(p.cands[0].model_sql, "a1", "first spelling kept");
        assert_eq!(p.to_execute(2), vec![0, 1], "most votes first, then first seen");
        p.cands[0].state = done(7);
        p.cands[1].state = done(9);
        assert_eq!(p.to_execute(2), Vec::<usize>::new(), "never more than max executions");
        assert_eq!(p.to_execute(3), vec![2]);
        p.cands[2].state = done(9); // different SQL, same rows: agrees with B
        let g = p.groups();
        assert_eq!(g.len(), 2);
        assert_eq!((g[0].key, g[0].votes), (7, 2), "ties keep the first-seen group");
        assert_eq!((g[1].key, g[1].votes, g[1].members.clone()), (9, 2, vec![1, 2]));
        assert_eq!(p.total_votes(), 5, "declines count against every answer");
        p.cands[2].state = State::Failed("bad".into());
        assert_eq!((p.total_votes(), p.viable(), p.failures()), (4, 2, vec!["bad".to_owned()]));
    }
}
