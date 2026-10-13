// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! # `lineage`
//!
//! Column-level lineage for describing a warehouse.
//!
//! - [`Graph`]: child → parent column edges (each marked exact-copy or not)
//!   and unreachable regions. It answers the two questions metadata
//!   discovery needs:
//!   - [`Graph::table_levels`]: tables in **topological levels**, sources
//!     first, so each table is described after everything it reads from;
//!   - [`Graph::nearest_documented`]: breadth-first search upstream to the
//!     shallowest documented ancestors of an undocumented column, with the
//!     path and whether every hop is an exact copy (then a description
//!     can be inherited verbatim).
//! - [`LineageSource`]: where edges come from. The SQL that produces each
//!   table (parsed and bound by `sql` / `ir`) gives exact in-warehouse
//!   lineage without any service; [`dataplex::Dataplex`] (feature
//!   `dataplex`) adds cross-project lineage recorded by Google's Data
//!   Lineage API. Part 2 (usage context) plugs query logs in the same way.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc, clippy::doc_markdown)]

#[cfg(feature = "dataplex")]
pub mod dataplex;

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::Future;

/// A physical column: `project.dataset.table` and the column path, both
/// lower-cased (`BigQuery` column names are case-insensitive; table names
/// are compared as written).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Column {
    /// `project.dataset.table`.
    pub table: String,
    /// Column path (`amount`, `addr.city`), lower-cased.
    pub column: String,
}

impl Column {
    /// A column of `table`.
    #[must_use]
    pub fn new(table: impl Into<String>, column: &str) -> Self {
        Self { table: table.into(), column: column.to_ascii_lowercase() }
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.table, self.column)
    }
}

/// Errors of lineage sources.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Transport failure.
    Transport(objstore_r::Error),
    /// The service answered with an error.
    Api {
        /// HTTP status.
        status: u16,
        /// Message.
        message: String,
    },
    /// The response could not be decoded.
    Decode(crate::base::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "transport: {e}"),
            Self::Api { status, message } => write!(f, "lineage api {status}: {message}"),
            Self::Decode(e) => write!(f, "decode: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<objstore_r::Error> for Error {
    fn from(e: objstore_r::Error) -> Self {
        Self::Transport(e)
    }
}

impl From<crate::base::Error> for Error {
    fn from(e: crate::base::Error) -> Self {
        Self::Decode(e)
    }
}

/// Result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A documented ancestor reached from an undocumented column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inherited {
    /// The documented ancestor.
    pub from: Column,
    /// Its description.
    pub description: String,
    /// The path, start first, ancestor last.
    pub path: Vec<Column>,
    /// Every hop on the path is an exact copy.
    pub exact_copy_chain: bool,
}

/// A column-level lineage graph.
#[derive(Clone, Debug, Default)]
pub struct Graph {
    /// Child → parents (parent, exact copy).
    parents: HashMap<Column, Vec<(Column, bool)>>,
    /// Regions the lineage service could not reach (the graph may be
    /// incomplete there).
    pub unreachable: BTreeSet<String>,
}

impl Graph {
    /// An empty graph.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `child ← parent`; an edge seen again keeps the strongest
    /// exact-copy evidence.
    pub fn add(&mut self, child: Column, parent: Column, exact: bool) {
        if child == parent {
            return;
        }
        let ps = self.parents.entry(child).or_default();
        match ps.iter_mut().find(|(p, _)| *p == parent) {
            Some(e) => e.1 |= exact,
            None => ps.push((parent, exact)),
        }
    }

    /// Merge another graph (e.g. SQL lineage + Dataplex).
    pub fn merge(&mut self, other: Graph) {
        for (child, ps) in other.parents {
            for (p, exact) in ps {
                self.add(child.clone(), p, exact);
            }
        }
        self.unreachable.extend(other.unreachable);
    }

    /// Direct parents of `c`.
    #[must_use]
    pub fn parents(&self, c: &Column) -> &[(Column, bool)] {
        self.parents.get(c).map_or(&[], Vec::as_slice)
    }

    /// Every table the graph mentions, as child or parent.
    #[must_use]
    pub fn tables(&self) -> BTreeSet<&str> {
        self.parents
            .iter()
            .flat_map(|(c, ps)| std::iter::once(c).chain(ps.iter().map(|(p, _)| p)))
            .map(|c| c.table.as_str())
            .collect()
    }

    /// Every edge: (child, parent, exact copy).
    pub fn edges(&self) -> impl Iterator<Item = (&Column, &Column, bool)> + '_ {
        self.parents.iter().flat_map(|(c, ps)| ps.iter().map(move |(p, e)| (c, p, *e)))
    }

    /// Edges in the graph.
    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.parents.values().map(Vec::len).sum()
    }

    /// `tables` in topological order of the table-level lineage among them:
    /// sources first. Tables in a cycle (or not connected) keep their input
    /// order after everything that can be ordered before them.
    #[must_use]
    fn table_order(&self, tables: &[String]) -> Vec<String> {
        let index: HashMap<&str, usize> =
            tables.iter().enumerate().map(|(i, t)| (t.as_str(), i)).collect();
        // upstream[i] = tables among `tables` that table i reads.
        let mut upstream: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); tables.len()];
        for (child, ps) in &self.parents {
            let Some(&c) = index.get(child.table.as_str()) else { continue };
            for (p, _) in ps {
                if let Some(&u) = index.get(p.table.as_str()) {
                    if u != c {
                        upstream[c].insert(u);
                    }
                }
            }
        }
        let mut pending: Vec<usize> = upstream.iter().map(BTreeSet::len).collect();
        let mut downstream: Vec<Vec<usize>> = vec![Vec::new(); tables.len()];
        for (c, ups) in upstream.iter().enumerate() {
            for &u in ups {
                downstream[u].push(c);
            }
        }
        // Kahn's algorithm, always taking the lowest input position ready.
        let mut ready: BTreeSet<usize> = (0..tables.len()).filter(|&i| pending[i] == 0).collect();
        let mut out = Vec::with_capacity(tables.len());
        let mut done = vec![false; tables.len()];
        while let Some(&i) = ready.iter().next() {
            ready.remove(&i);
            done[i] = true;
            out.push(tables[i].clone());
            for &d in &downstream[i] {
                pending[d] -= 1;
                if pending[d] == 0 {
                    ready.insert(d);
                }
            }
        }
        out.extend((0..tables.len()).filter(|&i| !done[i]).map(|i| tables[i].clone()));
        out
    }

    /// Tables grouped by depth in the lineage among `tables`: every table in
    /// a level reads only from earlier levels (so a level can be described
    /// concurrently). Cycles form a last level.
    #[must_use]
    pub fn table_levels(&self, tables: &[String]) -> Vec<Vec<String>> {
        let order = self.table_order(tables);
        let pos: HashMap<&str, usize> =
            order.iter().enumerate().map(|(i, t)| (t.as_str(), i)).collect();
        // Table-level parents in one pass over the edges: O(T + E).
        let mut parents_of: HashMap<&str, Vec<usize>> = HashMap::new();
        for (child, ps) in &self.parents {
            let v = parents_of.entry(child.table.as_str()).or_default();
            v.extend(ps.iter().filter_map(|(p, _)| pos.get(p.table.as_str()).copied()));
        }
        let mut level: Vec<usize> = vec![0; order.len()];
        for (i, t) in order.iter().enumerate() {
            let l = parents_of
                .get(t.as_str())
                .into_iter()
                .flatten()
                .filter(|&&j| j < i)
                .map(|&j| level[j] + 1)
                .max()
                .unwrap_or(0);
            level[i] = l;
        }
        let mut out: Vec<Vec<String>> = Vec::new();
        for (i, t) in order.into_iter().enumerate() {
            let l = level[i];
            if out.len() <= l {
                out.resize_with(l + 1, Vec::new);
            }
            out[l].push(t);
        }
        // Stable output: the same DAG always gives the same report order.
        for l in &mut out {
            l.sort_unstable();
        }
        out
    }

    /// The shallowest documented ancestors of `start` (all of them at that
    /// depth, so a join of two documented parents yields both). Where a
    /// column is an exact copy of a parent, only its exact-copy parents are
    /// followed. `describe` returns a column's description, if it has one.
    #[must_use]
    pub fn nearest_documented(
        &self,
        start: &Column,
        mut describe: impl FnMut(&Column) -> Option<String>,
    ) -> Vec<Inherited> {
        let mut prev: HashMap<Column, (Column, bool)> = HashMap::new();
        let mut seen: HashSet<Column> = HashSet::from([start.clone()]);
        let mut frontier: VecDeque<Column> = VecDeque::from([start.clone()]);
        while !frontier.is_empty() {
            let mut next = VecDeque::new();
            let mut hits: Vec<(Column, String)> = Vec::new();
            for node in frontier.drain(..) {
                // A column that is an exact copy of a parent takes its value
                // from that parent alone: its other parents are conditions
                // (the filter or join columns lineage services report as
                // dependencies), not sources of its meaning.
                let ps = self.parents(&node);
                let copied = ps.iter().any(|(_, exact)| *exact);
                for (parent, exact) in ps.iter().filter(|(_, e)| *e || !copied) {
                    if !seen.insert(parent.clone()) {
                        continue;
                    }
                    prev.insert(parent.clone(), (node.clone(), *exact));
                    match describe(parent) {
                        Some(d) => hits.push((parent.clone(), d)),
                        None => next.push_back(parent.clone()),
                    }
                }
            }
            if !hits.is_empty() {
                hits.sort_by(|a, b| a.0.cmp(&b.0));
                return hits
                    .into_iter()
                    .map(|(from, description)| {
                        let mut path = vec![from.clone()];
                        let mut exact_copy_chain = true;
                        let mut cur = from.clone();
                        while let Some((child, exact)) = prev.get(&cur) {
                            exact_copy_chain &= *exact;
                            path.push(child.clone());
                            cur = child.clone();
                        }
                        path.reverse();
                        Inherited { from, description, path, exact_copy_chain }
                    })
                    .collect();
            }
            frontier = next;
        }
        Vec::new()
    }
}

/// A source of upstream column lineage.
pub trait LineageSource {
    /// Future of [`search`](LineageSource::search).
    type Search<'a>: Future<Output = Result<Graph>> + 'a
    where
        Self: 'a;

    /// The lineage of `roots` in `direction`, up to `max_depth` hops, as
    /// child ← parent edges either way.
    fn search<'a>(
        &'a self,
        roots: &'a [Column],
        max_depth: u32,
        direction: Direction,
    ) -> Self::Search<'a>;

    /// Where `roots` come from.
    fn upstream<'a>(&'a self, roots: &'a [Column], max_depth: u32) -> Self::Search<'a> {
        self.search(roots, max_depth, Direction::Out)
    }

    /// What is computed from `roots` (impact).
    fn downstream<'a>(&'a self, roots: &'a [Column], max_depth: u32) -> Self::Search<'a> {
        self.search(roots, max_depth, Direction::In)
    }
}

/// Which way to follow lineage: [`Direction::Out`] towards sources
/// (upstream), [`Direction::In`] towards dependants (downstream impact) —
/// graph-r's walk directions, so a search and a store walk say the same.
pub use graph_r::Direction;

/// No external lineage (SQL-derived lineage only).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoLineage;

impl LineageSource for NoLineage {
    type Search<'a> = std::future::Ready<Result<Graph>>;

    fn search<'a>(&'a self, _: &'a [Column], _: u32, _: Direction) -> Self::Search<'a> {
        std::future::ready(Ok(Graph::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(t: &str, col: &str) -> Column {
        Column::new(t, col)
    }

    #[test]
    fn nearest_documented_ancestors_and_exact_chains() {
        let mut g = Graph::new();
        // mart.rev ← stg.amount (derived) ← raw.amount (documented, exact)
        g.add(c("p.mart.m", "rev"), c("p.stg.s", "amount"), false);
        g.add(c("p.stg.s", "amount"), c("p.raw.r", "amt"), true);
        // mart.cust ← stg.cust (exact) ← raw.customer_id (exact, documented)
        g.add(c("p.mart.m", "cust"), c("p.stg.s", "cust"), true);
        g.add(c("p.stg.s", "cust"), c("p.raw.r", "customer_id"), true);
        // a join of two documented parents at the same depth
        g.add(c("p.mart.m", "rate_x"), c("p.raw.fx", "rate"), false);
        g.add(c("p.mart.m", "rate_x"), c("p.raw.r", "amt"), false);
        let docs = |x: &Column| match (x.table.as_str(), x.column.as_str()) {
            ("p.raw.r", "amt") => Some("Order amount in USD".to_owned()),
            ("p.raw.r", "customer_id") => Some("Customer key".to_owned()),
            ("p.raw.fx", "rate") => Some("FX rate".to_owned()),
            _ => None,
        };
        let cust = g.nearest_documented(&c("p.mart.m", "cust"), docs);
        assert_eq!(cust.len(), 1);
        assert!(cust[0].exact_copy_chain);
        assert_eq!(
            cust[0].path,
            vec![c("p.mart.m", "cust"), c("p.stg.s", "cust"), c("p.raw.r", "customer_id")]
        );
        let rev = g.nearest_documented(&c("p.mart.m", "rev"), docs);
        assert_eq!(
            (rev[0].description.as_str(), rev[0].exact_copy_chain),
            ("Order amount in USD", false)
        );
        assert_eq!(
            g.nearest_documented(&c("p.mart.m", "rate_x"), docs).len(),
            2,
            "both documented parents"
        );
        assert!(g.nearest_documented(&c("p.mart.m", "nothing"), docs).is_empty());
    }

    #[test]
    fn tables_in_topological_levels() {
        let mut g = Graph::new();
        g.add(c("p.d.view", "a"), c("p.d.mart", "a"), true);
        g.add(c("p.d.mart", "a"), c("p.d.stg", "a"), false);
        g.add(c("p.d.stg", "a"), c("p.d.raw", "a"), true);
        g.add(c("p.d.mart", "b"), c("p.d.raw2", "b"), true);
        g.add(c("p.d.stg", "a"), c("p.other.x", "a"), true); // outside the set
        let tables: Vec<String> =
            ["p.d.view", "p.d.mart", "p.d.raw2", "p.d.stg", "p.d.raw"].map(String::from).to_vec();
        assert_eq!(
            g.table_order(&tables),
            vec!["p.d.raw2", "p.d.raw", "p.d.stg", "p.d.mart", "p.d.view"]
        );
        let levels = g.table_levels(&tables);
        assert_eq!(
            levels,
            vec![
                vec!["p.d.raw".to_owned(), "p.d.raw2".to_owned()],
                vec!["p.d.stg".to_owned()],
                vec!["p.d.mart".to_owned()],
                vec!["p.d.view".to_owned()]
            ]
        );
        // A cycle does not lose tables.
        g.add(c("p.d.raw", "a"), c("p.d.view", "a"), false);
        let order = g.table_order(&tables);
        assert_eq!(order.len(), 5);
        assert_eq!(order[0], "p.d.raw2");
    }

    #[test]
    fn merging_keeps_the_strongest_evidence() {
        let mut a = Graph::new();
        a.add(c("t", "x"), c("s", "x"), false);
        let mut b = Graph::new();
        b.add(c("t", "x"), c("s", "x"), true);
        b.unreachable.insert("europe-west1".into());
        a.merge(b);
        assert_eq!(a.parents(&c("t", "x")), &[(c("s", "x"), true)]);
        assert_eq!(a.edge_count(), 1);
        assert!(a.unreachable.contains("europe-west1"));
    }
}
