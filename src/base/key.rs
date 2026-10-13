// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! [`EntityKey`]: the durable identity of a warehouse entity (dataset, table,
//! column or nested field path). It is `graph_r::UrlKey::of` over a canonical
//! URI, so the same key addresses the entity in the catalog image, in link-r
//! cards and in the graph-r knowledge graph.
//!
//! Canonical URI shape (BigQuery):
//!
//! ```text
//! sql://bigquery/{project}/{dataset}                 dataset
//! sql://bigquery/{project}/{dataset}/{table}         table
//! sql://bigquery/{project}/{dataset}/{table}#{path}  column / nested field ("a.b.c")
//! ```
//!
//! BigQuery project, dataset and table names are case-sensitive and stay as
//! given; column and field names are case-insensitive and are lowercased.

use std::fmt::Write as _;

/// The URI scheme and engine prefix every BigQuery key starts with.
pub const BIGQUERY_PREFIX: &str = "sql://bigquery/";

/// Durable identity of a warehouse entity: xxh3 of its canonical URI.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntityKey(pub u64);

impl EntityKey {
    /// Key an already-canonical URI.
    #[must_use]
    pub fn of_uri(uri: &str) -> Self {
        Self(graph_r::UrlKey::of(uri).raw())
    }

    /// The raw 64-bit value.
    #[must_use]
    pub fn raw(self) -> u64 {
        self.0
    }
}

/// Builds canonical BigQuery URIs into a reusable buffer, so keying many
/// entities allocates once.
#[derive(Debug, Default)]
pub struct UriBuf {
    buf: String,
}

impl UriBuf {
    /// A buffer with room for typical URIs.
    #[must_use]
    pub fn new() -> Self {
        Self { buf: String::with_capacity(128) }
    }

    /// Canonical dataset URI.
    pub fn dataset(&mut self, project: &str, dataset: &str) -> &str {
        self.buf.clear();
        let _ = write!(self.buf, "{BIGQUERY_PREFIX}{project}/{dataset}");
        &self.buf
    }

    /// Canonical table URI.
    pub fn table(&mut self, project: &str, dataset: &str, table: &str) -> &str {
        self.buf.clear();
        let _ = write!(self.buf, "{BIGQUERY_PREFIX}{project}/{dataset}/{table}");
        &self.buf
    }

    /// Canonical column or nested-field URI; `path` is dot-separated and is
    /// lowercased because BigQuery column names are case-insensitive.
    pub fn column(&mut self, project: &str, dataset: &str, table: &str, path: &str) -> &str {
        self.buf.clear();
        let _ = write!(self.buf, "{BIGQUERY_PREFIX}{project}/{dataset}/{table}#");
        self.buf.extend(path.chars().flat_map(char::to_lowercase));
        &self.buf
    }
}

/// Split `project.dataset.table` into its parts. Projects may contain dots
/// (`google.com:proj`); datasets and tables never do, so the split is from
/// the right.
#[must_use]
pub fn split_fqn(fqn: &str) -> Option<(&str, &str, &str)> {
    let (rest, table) = fqn.rsplit_once('.')?;
    let (project, dataset) = rest.rsplit_once('.')?;
    (!project.is_empty() && !dataset.is_empty() && !table.is_empty())
        .then_some((project, dataset, table))
}

/// Key of a dataset.
#[must_use]
pub fn dataset_key(project: &str, dataset: &str) -> EntityKey {
    EntityKey::of_uri(UriBuf::new().dataset(project, dataset))
}

/// Key of a table.
#[must_use]
pub fn table_key(project: &str, dataset: &str, table: &str) -> EntityKey {
    EntityKey::of_uri(UriBuf::new().table(project, dataset, table))
}

/// Key of a column or nested field.
#[must_use]
pub fn column_key(project: &str, dataset: &str, table: &str, path: &str) -> EntityKey {
    EntityKey::of_uri(UriBuf::new().column(project, dataset, table, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_matches_graph_r_url_key() {
        let uri = "sql://bigquery/p/d/t";
        assert_eq!(EntityKey::of_uri(uri).raw(), graph_r::UrlKey::of(uri).raw());
    }

    #[test]
    fn uri_shapes() {
        let mut b = UriBuf::new();
        assert_eq!(b.dataset("p", "d"), "sql://bigquery/p/d");
        assert_eq!(b.table("p", "d", "Orders"), "sql://bigquery/p/d/Orders");
        assert_eq!(b.column("p", "d", "t", "Addr.City"), "sql://bigquery/p/d/t#addr.city");
    }

    #[test]
    fn table_names_are_case_sensitive() {
        assert_ne!(table_key("p", "d", "Orders"), table_key("p", "d", "orders"));
    }

    #[test]
    fn column_names_are_case_insensitive() {
        assert_eq!(column_key("p", "d", "t", "Status"), column_key("p", "d", "t", "status"));
    }

    #[test]
    fn entity_levels_are_distinct() {
        let ds = dataset_key("p", "d");
        let tb = table_key("p", "d", "t");
        let col = column_key("p", "d", "t", "c");
        assert_ne!(ds, tb);
        assert_ne!(tb, col);
        assert_ne!(ds, col);
    }

    #[test]
    fn buffer_reuse_does_not_leak_previous_uri() {
        let mut b = UriBuf::new();
        let _ = b.table("project-with-a-long-name", "dataset", "table");
        assert_eq!(b.dataset("p", "d"), "sql://bigquery/p/d");
    }

    #[test]
    fn fqns_split_from_the_right() {
        assert_eq!(split_fqn("p.d.t"), Some(("p", "d", "t")));
        assert_eq!(split_fqn("google.com:p.d.t"), Some(("google.com:p", "d", "t")));
        assert_eq!(split_fqn("d.t"), None);
        assert_eq!(split_fqn("p..t"), None);
    }
}
