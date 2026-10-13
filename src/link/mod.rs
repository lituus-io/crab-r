// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! # `link`
//!
//! Which tables does a question need? For catalogs whose schema does not fit
//! the prompt budget, the engine sends only the linked tables — fewer input
//! tokens, less distraction — plus how they join.
//!
//! - [`SchemaIndex::score`]: BM25 over one "card" per table (table name ×3,
//!   dataset, description, column names, column descriptions), with light
//!   stemming (`orders` ~ `order`) and identifiers split on `_`, `.` and
//!   case. Value groundings from `values` add evidence the words cannot
//!   ("Brazil" names no table, but `users.country` holds `'Brasil'`).
//! - An inferred **join graph** within each dataset: `x_id` columns meet
//!   the `id` (or the same-named key) of table `x`/`xs`/`xes`/`x→ies`
//!   (`order_items.order_id = orders.order_id`,
//!   `order_items.product_id = products.id`).
//! - [`SchemaIndex::link`]: the best tables above a relative threshold, the
//!   grounded tables, then **join-path completion** — bridge tables on the
//!   shortest join paths are added so the query can actually connect them.
//!
//! Built once per catalog; a question is linked in microseconds.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::module_name_repetitions
)]

use crate::catalog::{Catalog, ColId, TableId};
use link_r::index::sparse::{idf, term_weight, DEFAULT_B, DEFAULT_K1};
use std::collections::{HashMap, VecDeque};

/// A join between two tables' columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Edge {
    /// Referencing table and column (`order_items.order_id`).
    pub from: (TableId, ColId),
    /// Referenced table and column (`orders.order_id`).
    pub to: (TableId, ColId),
}

/// Linking settings.
#[derive(Clone, Copy, Debug)]
pub struct LinkOpts {
    /// Most tables chosen by score (before join completion).
    pub max_tables: usize,
    /// Tables scoring below this share of the best score are left out.
    pub relative: f32,
    /// Longest join path bridged, hops.
    pub max_hops: usize,
}

impl Default for LinkOpts {
    fn default() -> Self {
        Self { max_tables: 6, relative: 0.3, max_hops: 3 }
    }
}

/// Field weights of a table card.
const W_TABLE: f32 = 3.0;
const W_DATASET: f32 = 1.0;
const W_DESC: f32 = 1.0;
const W_COLUMN: f32 = 1.0;
const W_COL_DESC: f32 = 0.5;

/// The schema index. See the crate docs.
#[derive(Debug, Default)]
pub struct SchemaIndex {
    /// Term → (table ordinal, BM25 term weight without IDF — length
    /// normalisation precomputed, so a query is one multiply-add per
    /// posting).
    postings: HashMap<String, Vec<(u32, f32)>>,
    /// Table ordinal → catalog id, and back.
    ids: Vec<TableId>,
    ordinal: HashMap<TableId, u32>,
    /// Join edges and per-table adjacency (edge indices).
    edges: Vec<Edge>,
    adj: Vec<Vec<u32>>,
    /// Per table ordinal: the representative of its family (itself for a
    /// singleton). A family is a dataset's tables whose names differ only
    /// in digits and whose columns are identical (`gsod1929`…`gsod2025`,
    /// date-sharded exports); its representative is the last by name.
    family: Vec<u32>,
    /// Family sizes by representative ordinal.
    family_size: HashMap<u32, u32>,
}

/// Lower-case terms of an identifier or text: graph-r's tokens (runs of
/// alphanumerics), further split at lower→upper case changes, stemmed;
/// stopwords dropped.
pub fn terms(text: &str, mut f: impl FnMut(String)) {
    let mut emit = |part: &str| {
        let t = stem(&graph_r::text::normalize(part));
        if !t.is_empty() && !crate::values::is_stop(&t) {
            f(t);
        }
    };
    for token in graph_r::text::tokens(text) {
        let mut start = 0;
        let mut prev_lower = false;
        for (i, c) in token.char_indices() {
            if c.is_uppercase() && prev_lower {
                emit(&token[start..i]);
                start = i;
            }
            prev_lower = c.is_lowercase() || c.is_ascii_digit();
        }
        emit(&token[start..]);
    }
}

/// A light English stemmer: plural and common suffix forms fold together.
fn stem(w: &str) -> String {
    let n = w.len();
    if n > 4 && w.ends_with("ies") {
        return format!("{}y", &w[..n - 3]);
    }
    if n > 4
        && (w.ends_with("sses") || w.ends_with("xes") || w.ends_with("ches") || w.ends_with("shes"))
    {
        return w[..n - 2].to_owned();
    }
    if n > 3 && w.ends_with('s') && !w.ends_with("ss") && !w.ends_with("us") && !w.ends_with("is") {
        return w[..n - 1].to_owned();
    }
    w.to_owned()
}

/// Singular forms a table name may take as a key prefix (`orders` →
/// `order`, `categories` → `category`, `addresses` → `address`).
fn singular(table: &str) -> String {
    stem(&table.to_lowercase())
}

impl SchemaIndex {
    /// Index every table of `cat`.
    #[must_use]
    pub fn build(cat: &Catalog) -> Self {
        let mut idx = Self::default();
        let mut per_table: Vec<HashMap<String, f32>> = Vec::new();
        for t in cat.tables() {
            let ord = idx.ids.len() as u32;
            idx.ids.push(t.id);
            idx.ordinal.insert(t.id, ord);
            let mut tf: HashMap<String, f32> = HashMap::new();
            let mut add =
                |text: &str, w: f32| terms(text, |term| *tf.entry(term).or_insert(0.0) += w);
            add(t.name, W_TABLE);
            add(t.dataset_name, W_DATASET);
            add(t.description, W_DESC);
            for c in cat.columns(t.id) {
                add(c.path, W_COLUMN);
                add(c.description, W_COL_DESC);
            }
            per_table.push(tf);
        }
        let lens: Vec<f32> = per_table.iter().map(|tf| tf.values().sum()).collect();
        let avg = if lens.is_empty() { 1.0 } else { lens.iter().sum::<f32>() / lens.len() as f32 };
        for (ord, tf) in per_table.into_iter().enumerate() {
            for (term, w) in tf {
                let weight = term_weight(w, lens[ord], avg, DEFAULT_K1, DEFAULT_B);
                idx.postings.entry(term).or_default().push((ord as u32, weight));
            }
        }
        idx.build_joins(cat);
        idx.build_families(cat);
        idx
    }

    fn build_families(&mut self, cat: &Catalog) {
        // Key: dataset, name with digit runs replaced, column list.
        let mut groups: HashMap<(u32, String, Vec<String>), Vec<u32>> = HashMap::new();
        for (ord, &id) in self.ids.iter().enumerate() {
            let Some(t) = cat.table(id) else { continue };
            let mut shape = String::with_capacity(t.name.len());
            let mut in_digits = false;
            for c in t.name.chars() {
                if c.is_ascii_digit() {
                    if !in_digits {
                        shape.push('#');
                    }
                    in_digits = true;
                } else {
                    shape.push(c);
                    in_digits = false;
                }
            }
            let cols: Vec<String> = cat.columns(id).map(|c| c.path.to_lowercase()).collect();
            groups.entry((t.dataset, shape, cols)).or_default().push(ord as u32);
        }
        self.family = (0..self.ids.len() as u32).collect();
        for (_, members) in groups {
            if members.len() < 2 {
                continue;
            }
            let rep = *members
                .iter()
                .max_by_key(|&&o| cat.table(self.ids[o as usize]).map(|t| t.name))
                .unwrap_or(&members[0]);
            for &m in &members {
                self.family[m as usize] = rep;
            }
            self.family_size.insert(rep, members.len() as u32);
        }
    }

    /// The representative of `t`'s table family and the family size (1 for
    /// a table without siblings).
    #[must_use]
    pub fn family(&self, t: TableId) -> (TableId, u32) {
        match self.ordinal.get(&t) {
            Some(&o) => {
                let rep = self.family[o as usize];
                (self.ids[rep as usize], self.family_size.get(&rep).copied().unwrap_or(1))
            }
            None => (t, 1),
        }
    }

    /// Every table, one per family (its representative), in catalog order.
    pub fn representatives(&self) -> impl Iterator<Item = TableId> + '_ {
        self.family.iter().enumerate().filter(|&(o, &r)| o as u32 == r).map(|(o, _)| self.ids[o])
    }

    /// Infer join edges within each dataset from key naming.
    fn build_joins(&mut self, cat: &Catalog) {
        self.adj = vec![Vec::new(); self.ids.len()];
        // (dataset, singular table name) → table; each table's `id` column.
        let mut by_name: HashMap<(u32, String), TableId> = HashMap::new();
        let mut id_col: HashMap<TableId, ColId> = HashMap::new();
        let mut cols_by_name: HashMap<(TableId, String), ColId> = HashMap::new();
        for t in cat.tables() {
            by_name.insert((t.dataset, singular(t.name)), t.id);
            for c in cat.columns(t.id).filter(|c| c.depth == 0) {
                let name = c.path.to_lowercase();
                if name == "id" {
                    id_col.insert(t.id, c.id);
                }
                cols_by_name.insert((t.id, name), c.id);
            }
        }
        for t in cat.tables() {
            for c in cat.columns(t.id).filter(|c| c.depth == 0) {
                let name = c.path.to_lowercase();
                let Some(prefix) = name.strip_suffix("_id") else { continue };
                // `inventory_item_id` → table `inventory_items`; also match on
                // the last word (`buyer_user_id` → `users`).
                let candidates =
                    [prefix.to_owned(), prefix.rsplit('_').next().unwrap_or(prefix).to_owned()];
                for cand in candidates {
                    let Some(&target) = by_name.get(&(t.dataset, stem(&cand))) else { continue };
                    if target == t.id {
                        // `orders.order_id` is the table's own key.
                        continue;
                    }
                    let to =
                        cols_by_name.get(&(target, name.clone())).or_else(|| id_col.get(&target));
                    if let Some(&to_col) = to {
                        self.add_edge(Edge { from: (t.id, c.id), to: (target, to_col) });
                        break;
                    }
                }
            }
        }
    }

    fn add_edge(&mut self, e: Edge) {
        if self.edges.contains(&e) {
            return;
        }
        let i = self.edges.len() as u32;
        self.edges.push(e);
        for t in [e.from.0, e.to.0] {
            if let Some(&o) = self.ordinal.get(&t) {
                self.adj[o as usize].push(i);
            }
        }
    }

    /// Tables indexed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Every inferred join edge.
    #[must_use]
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    /// Join edges between tables of `set`.
    pub fn edges_among<'a>(&'a self, set: &'a [TableId]) -> impl Iterator<Item = &'a Edge> + 'a {
        self.edges.iter().filter(move |e| set.contains(&e.from.0) && set.contains(&e.to.0))
    }

    /// Tables scoring at least `relative` of the best, best first (only
    /// those are sorted).
    fn score_above(&self, question: &str, relative: f32) -> Vec<(TableId, f32)> {
        let acc = self.accumulate(question);
        let top = acc.iter().copied().fold(0.0f32, f32::max);
        if top <= 0.0 {
            return Vec::new();
        }
        let floor = top * relative;
        let mut out: Vec<(TableId, f32)> = acc
            .iter()
            .enumerate()
            .filter(|(_, &s)| s >= floor && s > 0.0)
            .map(|(o, &s)| (self.ids[o], s))
            .collect();
        out.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        out
    }

    /// BM25 score of every table matching `question`, best first (ties by
    /// catalog order).
    #[must_use]
    pub fn score(&self, question: &str) -> Vec<(TableId, f32)> {
        let acc = self.accumulate(question);
        let mut out: Vec<(TableId, f32)> = acc
            .iter()
            .enumerate()
            .filter(|(_, &s)| s > 0.0)
            .map(|(o, &s)| (self.ids[o], s))
            .collect();
        out.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        out
    }

    /// Per-table BM25 scores of `question`, by table ordinal.
    fn accumulate(&self, question: &str) -> Vec<f32> {
        let n = self.ids.len() as f32;
        let mut seen: Vec<String> = Vec::new();
        terms(question, |t| {
            if !seen.contains(&t) {
                seen.push(t);
            }
        });
        let mut acc: Vec<f32> = vec![0.0; self.ids.len()];
        for term in &seen {
            let Some(post) = self.postings.get(term) else { continue };
            let df = post.len() as f32;
            let idf = idf(n, df);
            for &(ord, w) in post {
                acc[ord as usize] += idf * w;
            }
        }
        acc
    }

    /// Link `question`: tables whose word score reaches `relative` of the
    /// best, plus `boosts` (e.g. value grounding) — each boost is a
    /// fraction of the best word score (1.0 = as strong as the best match)
    /// and qualifies its table on its own — ranked, capped, then bridged by
    /// the tables on the shortest join paths between them.
    #[must_use]
    pub fn link(&self, question: &str, boosts: &[(TableId, f32)], opts: LinkOpts) -> Vec<TableId> {
        let mut scored = self.score_above(question, opts.relative);
        let top = scored.first().map_or(1.0, |s| s.1.max(f32::EPSILON));
        for &(t, b) in boosts {
            let add = b * top;
            match scored.iter_mut().find(|(x, _)| *x == t) {
                Some(s) => s.1 += add,
                None => scored.push((t, add)),
            }
        }
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        let mut chosen: Vec<TableId> =
            scored.iter().take(opts.max_tables).map(|(t, _)| *t).collect();
        self.complete(&mut chosen, opts.max_hops);
        chosen
    }

    /// Add bridge tables so every chosen table connects to the first one
    /// (when a join path of at most `max_hops` exists).
    pub fn complete(&self, chosen: &mut Vec<TableId>, max_hops: usize) {
        let Some(&root) = chosen.first() else { return };
        let targets: Vec<TableId> = chosen[1..].to_vec();
        for t in targets {
            if let Some(path) = self.path(root, t, max_hops, chosen) {
                for p in path {
                    if !chosen.contains(&p) {
                        chosen.push(p);
                    }
                }
            }
        }
    }

    /// Shortest join path `from` → `to` (interior tables only), preferring
    /// paths through tables already in `have` (BFS; ties to the lower id).
    fn path(
        &self,
        from: TableId,
        to: TableId,
        max_hops: usize,
        have: &[TableId],
    ) -> Option<Vec<TableId>> {
        let (&a, &b) = (self.ordinal.get(&from)?, self.ordinal.get(&to)?);
        if a == b {
            return Some(Vec::new());
        }
        let mut prev: Vec<u32> = vec![u32::MAX; self.ids.len()];
        let mut dist: Vec<u32> = vec![u32::MAX; self.ids.len()];
        let mut q = VecDeque::new();
        dist[a as usize] = 0;
        q.push_back(a);
        while let Some(x) = q.pop_front() {
            if x == b || dist[x as usize] as usize >= max_hops {
                continue;
            }
            // Neighbours already chosen first: bridges are shared.
            let mut next: Vec<u32> = self.adj[x as usize]
                .iter()
                .map(|&e| {
                    let e = self.edges[e as usize];
                    let other = if e.from.0 == self.ids[x as usize] { e.to.0 } else { e.from.0 };
                    self.ordinal[&other]
                })
                .collect();
            next.sort_by_key(|&o| (!have.contains(&self.ids[o as usize]), o));
            for y in next {
                if dist[y as usize] == u32::MAX {
                    dist[y as usize] = dist[x as usize] + 1;
                    prev[y as usize] = x;
                    q.push_back(y);
                }
            }
        }
        if dist[b as usize] == u32::MAX {
            return None;
        }
        let mut out = Vec::new();
        let mut x = prev[b as usize];
        while x != a {
            out.push(self.ids[x as usize]);
            x = prev[x as usize];
        }
        out.reverse();
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogBuilder, ColumnSpec, TableKind, TableSpec};

    fn col(p: &str) -> ColumnSpec {
        ColumnSpec {
            path: p.into(),
            data_type: "STRING".into(),
            ordinal: 1,
            nullable: true,
            partitioning: false,
            hidden: false,
            clustering: None,
            description: None,
        }
    }

    fn table(ds: &str, name: &str, desc: Option<&str>, cols: &[&str]) -> TableSpec {
        TableSpec {
            dataset: ds.into(),
            name: name.into(),
            kind: TableKind::Table,
            description: desc.map(Into::into),
            row_count: 10,
            logical_bytes: 0,
            last_modified_ms: 0,
            require_partition_filter: false,
            columns: cols.iter().map(|c| col(c)).collect(),
        }
    }

    pub(crate) fn shop() -> Catalog {
        let mut b = CatalogBuilder::new("p");
        b.add_table(table(
            "shop",
            "orders",
            Some("One row per order"),
            &["order_id", "user_id", "status", "created_at"],
        ));
        b.add_table(table(
            "shop",
            "order_items",
            None,
            &["id", "order_id", "product_id", "inventory_item_id", "sale_price"],
        ));
        b.add_table(table("shop", "products", None, &["id", "brand", "category", "retail_price"]));
        b.add_table(table("shop", "inventory_items", None, &["id", "product_id", "cost"]));
        b.add_table(table(
            "shop",
            "users",
            Some("Customers"),
            &["id", "country", "traffic_source"],
        ));
        b.add_table(table("shop", "distribution_centers", None, &["id", "name"]));
        b.add_table(table(
            "bikes",
            "trips",
            Some("Bike share trips"),
            &["trip_id", "station_id", "duration_minutes", "subscriber_type"],
        ));
        b.add_table(table("bikes", "stations", None, &["station_id", "name", "status"]));
        Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap()
    }

    fn tid(c: &Catalog, ds: &str, t: &str) -> TableId {
        c.find_table(ds, t).unwrap()
    }

    fn names(c: &Catalog, ts: &[TableId]) -> Vec<String> {
        ts.iter().map(|&t| c.table(t).unwrap().name.to_owned()).collect()
    }

    #[test]
    fn digit_sharded_tables_collapse_into_families() {
        let mut b = CatalogBuilder::new("p");
        for y in [1929, 1930, 2024] {
            b.add_table(table("noaa", &format!("gsod{y}"), None, &["stn", "temp"]));
        }
        b.add_table(table("noaa", "stations", None, &["usaf", "name"]));
        b.add_table(table("noaa", "gsod_notes2024", None, &["x"]));
        let c = Catalog::from_bytes(objstore_r::Bytes::from(b.build(1, 0).unwrap())).unwrap();
        let idx = SchemaIndex::build(&c);
        let (rep, n) = idx.family(tid(&c, "noaa", "gsod1930"));
        assert_eq!((c.table(rep).unwrap().name, n), ("gsod2024", 3));
        assert_eq!(idx.family(tid(&c, "noaa", "stations")).1, 1);
        let reps: Vec<&str> = idx.representatives().map(|t| c.table(t).unwrap().name).collect();
        assert_eq!(
            reps,
            vec!["gsod2024", "gsod_notes2024", "stations"],
            "catalog order, one per family"
        );
    }

    #[test]
    fn terms_split_identifiers_and_stem() {
        let mut v = Vec::new();
        terms("orderItems.sale_price of the Categories", |t| v.push(t));
        assert_eq!(v, vec!["order", "item", "sale", "price", "category"]);
        assert_eq!(stem("status"), "status");
        assert_eq!(stem("addresses"), "address");
    }

    #[test]
    fn infers_joins_from_key_names() {
        let c = shop();
        let idx = SchemaIndex::build(&c);
        let e: Vec<String> = idx
            .edges()
            .iter()
            .map(|e| {
                format!(
                    "{}.{}={}.{}",
                    c.table(e.from.0).unwrap().name,
                    c.column(e.from.1).unwrap().path,
                    c.table(e.to.0).unwrap().name,
                    c.column(e.to.1).unwrap().path
                )
            })
            .collect();
        for want in [
            "orders.user_id=users.id",
            "order_items.order_id=orders.order_id",
            "order_items.product_id=products.id",
            "order_items.inventory_item_id=inventory_items.id",
            "inventory_items.product_id=products.id",
            "trips.station_id=stations.station_id",
        ] {
            assert!(e.contains(&want.to_owned()), "missing {want} in {e:?}");
        }
        assert!(
            !e.iter().any(|x| x.starts_with("orders.order_id")),
            "a table's own key is not a join: {e:?}"
        );
        assert!(
            !e.iter().any(|x| x.contains("trips") && x.contains("orders")),
            "no cross-dataset joins"
        );
    }

    #[test]
    fn links_relevant_tables_and_bridges_join_paths() {
        let c = shop();
        let idx = SchemaIndex::build(&c);
        let got = idx.link("Total sale price per product brand", &[], LinkOpts::default());
        assert_eq!(names(&c, &got[..2]), vec!["products", "order_items"], "{:?}", names(&c, &got));
        // Users ↔ products need the orders → order_items bridge.
        let got = idx.link(
            "How many brands did customers from Brazil buy?",
            &[(tid(&c, "shop", "users"), 1.0)],
            LinkOpts { max_tables: 2, ..LinkOpts::default() },
        );
        let n = names(&c, &got);
        for t in ["users", "products", "orders", "order_items"] {
            assert!(n.contains(&t.to_owned()), "{n:?}");
        }
        assert!(!n.contains(&"trips".to_owned()));
        let bikes = idx.link("average trip duration by subscriber type", &[], LinkOpts::default());
        assert_eq!(names(&c, &bikes)[0], "trips");
        assert!(idx.link("zzz qqq", &[], LinkOpts::default()).is_empty());
    }
}
