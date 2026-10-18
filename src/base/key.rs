// Author: spicyzhug@gmail.com
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
//! Identity follows one spec, shared with pulumi-rs-yaml's
//! `sql_lineage::ids` (the lineage exports' ids) so ids from either side
//! join: the **project is lowercased** (GCP project ids are lowercase; a
//! domain-scoped `example.com:proj` is one project), dataset and table names
//! are case-sensitive and stay as given (a `$partition` decorator is not
//! part of a table's name), and column and field names are case-insensitive
//! and lowercased.

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

    /// `prefix` then the project, lowercased.
    fn start(&mut self, project: &str) {
        self.buf.clear();
        self.buf.push_str(BIGQUERY_PREFIX);
        push_project(&mut self.buf, project);
    }

    /// Canonical dataset URI.
    pub fn dataset(&mut self, project: &str, dataset: &str) -> &str {
        self.start(project);
        let _ = write!(self.buf, "/{dataset}");
        &self.buf
    }

    /// Canonical table URI.
    pub fn table(&mut self, project: &str, dataset: &str, table: &str) -> &str {
        self.start(project);
        let _ = write!(self.buf, "/{dataset}/{table}");
        &self.buf
    }

    /// Canonical column or nested-field URI; `path` is dot-separated and is
    /// lowercased because BigQuery column names are case-insensitive.
    pub fn column(&mut self, project: &str, dataset: &str, table: &str, path: &str) -> &str {
        self.start(project);
        let _ = write!(self.buf, "/{dataset}/{table}#");
        self.buf.extend(path.chars().flat_map(char::to_lowercase));
        &self.buf
    }
}

/// Append `project` in its canonical spelling: lowercased.
fn push_project(out: &mut String, project: &str) {
    if project.chars().any(char::is_uppercase) {
        out.extend(project.chars().flat_map(char::to_lowercase));
    } else {
        out.push_str(project);
    }
}

/// `project` in its canonical spelling (lowercased); borrowed when it
/// already is.
#[must_use]
pub fn canonical_project(project: &str) -> std::borrow::Cow<'_, str> {
    if project.chars().any(char::is_uppercase) {
        std::borrow::Cow::Owned(project.chars().flat_map(char::to_lowercase).collect())
    } else {
        std::borrow::Cow::Borrowed(project)
    }
}

/// Split a dotted reference into its project and the rest, keeping a
/// domain-scoped project (`example.com:proj.d.t` → `example.com:proj`,
/// `d.t`) whole: the same rule as pulumi-rs-yaml's `split_project`.
#[must_use]
pub fn split_project(reference: &str) -> Option<(&str, &str)> {
    if let Some(colon) = reference.find(':') {
        if reference[..colon].contains('.') {
            let dot = reference[colon..].find('.')? + colon;
            return Some((&reference[..dot], &reference[dot + 1..]));
        }
    }
    reference.split_once('.')
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

/// The `bq://` scheme of a BigQuery name in its slash spelling.
pub const BQ_SCHEME: &str = "bq://";
/// Longest name component accepted, bytes (BigQuery's own limits are far
/// lower; this only bounds what a malformed input can make us hold).
pub const MAX_COMPONENT: usize = 1024;

/// How deep a [`BqName`] goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// `p`
    Project,
    /// `p.d`
    Dataset,
    /// `p.d.t`
    Table,
    /// `p.d.t.c`
    Column,
}

/// Why a name was refused. Never carries the input (it may be anything).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NameError {
    /// Nothing, an empty component, or more components than a column has.
    Malformed,
    /// A component holds a character no BigQuery name of that kind can
    /// (a dot — so the two spellings could disagree on the split — a slash,
    /// `#`, a quote or backtick, whitespace, a control character, ...).
    BadComponent,
    /// A component longer than [`MAX_COMPONENT`].
    TooLong,
}

impl std::fmt::Display for NameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Malformed => {
                "not a BigQuery name: bq://project[/dataset[/table[#column]]] or project[.dataset[.table[.column]]]"
            }
            Self::BadComponent => {
                "a name component holds a character no BigQuery name can (letters, digits, _ and -; : in a project)"
            }
            Self::TooLong => "a name component is too long",
        })
    }
}

impl std::error::Error for NameError {}

/// A BigQuery project, dataset, table or column name, borrowed from any of
/// its three spellings, which all parse to the same parts and so address the
/// same store node:
///
/// - `bq://project/dataset/table#column`: a static lineage export's ids.
/// - `bq://project/dataset/table/column`: the path form, for document stores
///   that canonicalise urls through link-r, which drops a `#fragment` (there
///   a column's `#` id would collapse onto its table).
/// - `project.dataset.table.column`: the warehouse's own.
///
/// Every component is refused unless it is letters, digits, `_` and `-`,
/// and nothing that could close a quoted identifier or a path segment. A
/// project may be domain-scoped (`example.com:p`): dots before its one `:`
/// only, so the dotted spelling still has exactly one split. A nested
/// `a.b` field path is refused rather than guessed.
///
/// Identity follows the shared spec (see the module docs): two names are
/// equal when they differ only in the case of the project (or a column),
/// every spelling emits the project lowercased, and a table's `$partition`
/// decorator is dropped on parse.
#[derive(Clone, Copy, Debug)]
pub struct BqName<'a> {
    /// Project.
    pub project: &'a str,
    /// Dataset, from [`Level::Dataset`] down.
    pub dataset: Option<&'a str>,
    /// Table, from [`Level::Table`] down.
    pub table: Option<&'a str>,
    /// Column, at [`Level::Column`].
    pub column: Option<&'a str>,
}

impl PartialEq for BqName<'_> {
    fn eq(&self, other: &Self) -> bool {
        let lower = |s: &str| s.chars().flat_map(char::to_lowercase).collect::<String>();
        let column = |a: Option<&str>, b: Option<&str>| match (a, b) {
            (Some(a), Some(b)) => lower(a) == lower(b),
            (a, b) => a == b,
        };
        lower(self.project) == lower(other.project)
            && self.dataset == other.dataset
            && self.table == other.table
            && column(self.column, other.column)
    }
}

impl Eq for BqName<'_> {}

fn plain(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-'
}

fn component(s: &str, project: bool) -> Result<&str, NameError> {
    if s.is_empty() {
        return Err(NameError::Malformed);
    }
    if s.len() > MAX_COMPONENT {
        return Err(NameError::TooLong);
    }
    let ok = if project {
        // `domain:project`: a dotted domain, one colon, a plain project.
        match s.split_once(':') {
            Some((domain, p)) => {
                !domain.is_empty()
                    && !p.is_empty()
                    && domain.split('.').all(|l| !l.is_empty() && l.chars().all(plain))
                    && p.chars().all(plain)
            }
            None => s.chars().all(plain),
        }
    } else {
        s.chars().all(plain)
    };
    if ok {
        Ok(s)
    } else {
        Err(NameError::BadComponent)
    }
}

/// A table name without its `$partition` decorator (`t$20240101` → `t`).
fn undecorated(t: &str) -> &str {
    t.split_once('$').map_or(t, |(name, _)| name)
}

impl<'a> BqName<'a> {
    /// Parse either spelling (`bq://` decides which).
    pub fn parse(s: &'a str) -> Result<Self, NameError> {
        match s.strip_prefix(BQ_SCHEME) {
            Some(rest) => Self::parse_slashed(rest),
            None => Self::parse_dotted(s),
        }
    }

    /// `project/dataset/table#column` or `project/dataset/table/column`,
    /// after the scheme. A column is named one way or the other, never both.
    fn parse_slashed(rest: &'a str) -> Result<Self, NameError> {
        let (path, fragment) = match rest.split_once('#') {
            Some((p, c)) => (p, Some(c)),
            None => (rest, None),
        };
        let mut parts = path.split('/');
        let parts = [parts.next(), parts.next(), parts.next(), parts.next(), parts.next()];
        let column = match (parts[3], fragment) {
            _ if parts[4].is_some() => return Err(NameError::Malformed),
            (Some(_), Some(_)) => return Err(NameError::Malformed),
            (c, None) | (None, c) => c,
        };
        Self::from_parts(parts[0].unwrap_or(""), parts[1], parts[2], column)
    }

    /// `project.dataset.table.column`: one to four components; a
    /// domain-scoped project keeps its dots ([`split_project`]).
    fn parse_dotted(s: &'a str) -> Result<Self, NameError> {
        if let Some(colon) = s.find(':').filter(|&c| s[..c].contains('.')) {
            if !s[colon..].contains('.') {
                // A domain-scoped project alone.
                return Self::from_parts(s, None, None, None);
            }
        }
        if let Some((project, rest)) = split_project(s).filter(|(p, _)| p.contains(':')) {
            let mut parts = rest.split('.');
            let parts = [parts.next(), parts.next(), parts.next(), parts.next()];
            if parts[3].is_some() {
                return Err(NameError::Malformed);
            }
            return Self::from_parts(project, parts[0], parts[1], parts[2]);
        }
        let mut parts = s.split('.');
        let parts = [parts.next(), parts.next(), parts.next(), parts.next(), parts.next()];
        if parts[4].is_some() {
            return Err(NameError::Malformed);
        }
        Self::from_parts(parts[0].unwrap_or(""), parts[1], parts[2], parts[3])
    }

    fn from_parts(
        project: &'a str,
        dataset: Option<&'a str>,
        table: Option<&'a str>,
        column: Option<&'a str>,
    ) -> Result<Self, NameError> {
        // A column needs its table, a table its dataset.
        if (column.is_some() && table.is_none()) || (table.is_some() && dataset.is_none()) {
            return Err(NameError::Malformed);
        }
        Ok(Self {
            project: component(project, true)?,
            dataset: dataset.map(|d| component(d, false)).transpose()?,
            table: table.map(|t| component(undecorated(t), false)).transpose()?,
            column: column.map(|c| component(c, false)).transpose()?,
        })
    }

    /// How deep the name goes.
    #[must_use]
    pub fn level(&self) -> Level {
        match (self.dataset, self.table, self.column) {
            (_, _, Some(_)) => Level::Column,
            (_, Some(_), None) => Level::Table,
            (Some(_), None, None) => Level::Dataset,
            (None, None, None) => Level::Project,
        }
    }

    /// The `bq://project/dataset/table#column` spelling (column lowercased:
    /// BigQuery column names are case-insensitive).
    #[must_use]
    pub fn bq_id(&self) -> String {
        let mut out = String::with_capacity(BQ_SCHEME.len() + 64);
        out.push_str(BQ_SCHEME);
        push_project(&mut out, self.project);
        for (sep, part) in [('/', self.dataset), ('/', self.table)] {
            if let Some(p) = part {
                out.push(sep);
                out.push_str(p);
            }
        }
        if let Some(c) = self.column {
            out.push('#');
            out.extend(c.chars().flat_map(char::to_lowercase));
        }
        out
    }

    /// The `bq://project/dataset/table/column` path form (column
    /// lowercased): what a store that drops url fragments must key on. A
    /// project, dataset or table spells as in [`BqName::bq_id`].
    #[must_use]
    pub fn bq_path(&self) -> String {
        let mut out = self.table_of().unwrap_or(*self).bq_id();
        if let Some(c) = self.column {
            out.push('/');
            out.extend(c.chars().flat_map(char::to_lowercase));
        }
        out
    }

    /// The `project.dataset.table.column` spelling (column lowercased).
    #[must_use]
    pub fn dotted(&self) -> String {
        let mut out = String::with_capacity(64);
        push_project(&mut out, self.project);
        for part in [self.dataset, self.table].into_iter().flatten() {
            out.push('.');
            out.push_str(part);
        }
        if let Some(c) = self.column {
            out.push('.');
            out.extend(c.chars().flat_map(char::to_lowercase));
        }
        out
    }

    /// The canonical store URI (a dataset, table or column; a project has
    /// none — the store's root holds every project's datasets).
    pub fn uri<'b>(&self, buf: &'b mut UriBuf) -> Option<&'b str> {
        match (self.dataset, self.table, self.column) {
            (Some(d), Some(t), Some(c)) => Some(buf.column(self.project, d, t, c)),
            (Some(d), Some(t), None) => Some(buf.table(self.project, d, t)),
            (Some(d), None, None) => Some(buf.dataset(self.project, d)),
            _ => None,
        }
    }

    /// The table this name is or belongs to.
    #[must_use]
    pub fn table_of(&self) -> Option<Self> {
        self.table.map(|_| Self { column: None, ..*self })
    }

    /// The dataset this name is or belongs to.
    #[must_use]
    pub fn dataset_of(&self) -> Option<Self> {
        self.dataset.map(|_| Self { table: None, column: None, ..*self })
    }
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

    #[test]
    fn both_spellings_name_the_same_thing() {
        for (slashed, dotted, level) in [
            ("bq://p", "p", Level::Project),
            ("bq://p/d", "p.d", Level::Dataset),
            ("bq://my-proj/mart/orders_v", "my-proj.mart.orders_v", Level::Table),
            ("bq://my-proj/mart/orders_v#id", "my-proj.mart.orders_v.id", Level::Column),
            ("bq://dom:p/d/t#c", "dom:p.d.t.c", Level::Column),
        ] {
            let a = BqName::parse(slashed).unwrap();
            let b = BqName::parse(dotted).unwrap();
            let path = a.bq_path();
            assert_eq!((a, a), (b, BqName::parse(&path).unwrap()));
            assert_eq!(a.level(), level);
            assert_eq!((a.bq_id(), a.dotted()), (slashed.to_owned(), dotted.to_owned()));
            assert_eq!(a.bq_path(), slashed.replace('#', "/"));
        }
        let n = BqName::parse("bq://p/d/T/Status").unwrap();
        assert_eq!(
            (n.bq_id(), n.bq_path()),
            ("bq://p/d/T#status".into(), "bq://p/d/T/status".into())
        );
        let n = BqName::parse("bq://p/d/T#Status").unwrap();
        assert_eq!((n.bq_id(), n.dotted()), ("bq://p/d/T#status".into(), "p.d.T.status".into()));
        let mut b = UriBuf::new();
        assert_eq!(n.uri(&mut b), Some("sql://bigquery/p/d/T#status"));
        assert_eq!(
            BqName::parse("p.d.T.status").unwrap().uri(&mut b),
            Some("sql://bigquery/p/d/T#status")
        );
        assert_eq!(BqName::parse("bq://p").unwrap().uri(&mut b), None);
        assert_eq!(n.table_of().unwrap().dotted(), "p.d.T");
        assert_eq!(n.dataset_of().unwrap().bq_id(), "bq://p/d");
    }

    #[test]
    fn projects_are_canonical_lowercase_and_may_be_domain_scoped() {
        let mut b = UriBuf::new();
        assert_eq!(b.table("My-Proj", "DS", "T"), "sql://bigquery/my-proj/DS/T");
        assert_eq!(table_key("My-Proj", "d", "t"), table_key("my-proj", "d", "t"));
        assert_ne!(table_key("p", "D", "t"), table_key("p", "d", "t"), "datasets keep case");
        let n = BqName::parse("Example.com:Proj.DS.T$20240101.Col").unwrap();
        assert_eq!((n.project, n.dataset, n.table), ("Example.com:Proj", Some("DS"), Some("T")));
        assert_eq!(n.bq_id(), "bq://example.com:proj/DS/T#col");
        assert_eq!(n.dotted(), "example.com:proj.DS.T.col");
        assert_eq!(n, BqName::parse("bq://example.com:proj/DS/T#COL").unwrap());
        assert_ne!(n, BqName::parse("bq://example.com:proj/ds/T#col").unwrap());
        assert_eq!(n.uri(&mut b), Some("sql://bigquery/example.com:proj/DS/T#col"));
        assert_eq!(split_project("example.com:p.d.t"), Some(("example.com:p", "d.t")));
        assert_eq!(split_project("p.d.t"), Some(("p", "d.t")));
        let p = BqName::parse("a.b:c").unwrap();
        assert_eq!(
            (p.level(), p.dotted(), p.bq_id()),
            (Level::Project, "a.b:c".into(), "bq://a.b:c".into())
        );
        assert_eq!(canonical_project("P"), "p");
        assert!(matches!(canonical_project("p"), std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn ambiguous_or_malformed_names_are_refused() {
        use NameError::{BadComponent, Malformed, TooLong};
        for (s, e) in [
            ("", Malformed),
            ("bq://", Malformed),
            ("p..t", Malformed),
            ("p.d.t.c.x", Malformed),
            ("bq://p/d/t/c/x", Malformed),
            ("bq://p/d/t/c#c", Malformed),
            ("bq://p/d/t/#c", Malformed),
            ("bq://p/d/t/", Malformed),
            ("bq://p//t", Malformed),
            ("bq://p/d#c", Malformed),
            ("bq://p#c", Malformed),
            ("bq://p/d/t#", Malformed),
            ("bq://p/d/t#a.b", BadComponent),
            ("bq://example..com:p/d/t", BadComponent),
            ("bq://example.com:/d/t", BadComponent),
            ("bq://:p/d/t", BadComponent),
            ("bq://a:b:c/d/t", BadComponent),
            ("example.com:p.d.t.c.x", Malformed),
            ("bq://p/d/t#a#b", BadComponent),
            ("p.d.t`; DROP TABLE x; --", BadComponent),
            ("bq://p/d/t'", BadComponent),
            ("bq://p/d/t c", BadComponent),
            ("bq://p/../t", BadComponent),
            ("bq://p/d/..", BadComponent),
            ("bq://p/d:x/t", BadComponent),
            ("bq://p/d/t\n", BadComponent),
            ("p.d.t\u{0}", BadComponent),
        ] {
            assert_eq!(BqName::parse(s), Err(e), "{s:?}");
        }
        assert_eq!(BqName::parse(&format!("p.d.{}", "t".repeat(MAX_COMPONENT + 1))), Err(TooLong));
        assert!(!NameError::BadComponent.to_string().contains('`'));
    }
}
