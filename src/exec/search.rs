// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! `search_catalog`: BigQuery tables and views found by keyword in the
//! Dataplex Universal Catalog, with the caller's own credential.
//!
//! One request: `POST {origin}/v1/projects/{project}/locations/global:searchEntries`
//! with `query`, `pageSize` and `orderBy=relevance` and no body. `project`
//! is where the search is attributed (the caller needs
//! `dataplex.projects.search` there); what comes back spans whatever the
//! caller may see, and nothing else, because the catalog applies the
//! caller's permissions.
//!
//! The text the caller gives is sent with `system=bigquery (type=table OR
//! type=view)` after it, so a search answers with tables and views and never
//! with another system's entries.
//!
//! One page is read, and it is always asked for at the catalog's largest size
//! ([`PAGE_SIZE`]), whatever the caller's limit. The catalog ranks every
//! match first and applies the caller's permissions to that page afterwards:
//! where a caller may see a small share of a large catalog, a page of twenty
//! is twenty entries they may not see, and the answer is empty although
//! entries they may see matched further down. Measured against a catalog of
//! some 150,000 entries, a keyword with eleven visible matches answered none
//! of them at twenty a page and all eleven at a hundred or more, in the same
//! time. Of that page at most [`SearchOptions::limit`] entries are read into
//! the answer; the rest are stepped over without being kept, the whole
//! response is refused past [`MAX_RESPONSE_BYTES`], and `truncated` says
//! whether more matched.
//!
//! Each entry is reduced to a fixed set of members (`name`, `project`,
//! `type`, `system`, `location`, `display_name`, `description`, `fqn`,
//! `resource`, `updated`, `entry`); aspects and anything else in the
//! catalog's answer are dropped. A description is cut at
//! [`SearchOptions::max_description`] characters.
//!
//! A failure is one [`SearchError`] with a typed [`Failure`]. Its message is
//! fixed text or a transport's own (which never holds a credential): never
//! the catalog's response body.

use crate::base::json::{Event, Scanner, Writer};
use objstore_r::transport::{Method, Request, Retry, Transport};
use std::fmt;

/// Why a search failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    /// The caller may not search in the project (403).
    NoAccess,
    /// The project does not exist, or is not visible to the caller (404).
    NotFound,
    /// No credential was supplied or discovered.
    NoCredentials,
    /// The credential was refused as expired or invalid (401).
    Expired,
    /// The catalog could not be reached, or answered with a server error, a
    /// throttle, or something unreadable.
    Unreachable,
    /// The request policy refused the request; nothing was sent.
    RefusedByGuard,
    /// The text, the project or the limit is not usable, or the catalog
    /// found the query invalid (400).
    InvalidQuery,
    /// The catalog did not answer within the timeout.
    Timeout,
}

impl Failure {
    /// The failure's wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoAccess => "no_access",
            Self::NotFound => "not_found",
            Self::NoCredentials => "no_credentials",
            Self::Expired => "expired",
            Self::Unreachable => "unreachable",
            Self::RefusedByGuard => "refused_by_guard",
            Self::InvalidQuery => "invalid_query",
            Self::Timeout => "timeout",
        }
    }

    /// The failure of an HTTP status that is not a success.
    #[must_use]
    pub fn of_status(status: u16) -> Self {
        match status {
            401 => Self::Expired,
            403 => Self::NoAccess,
            404 => Self::NotFound,
            400 | 405..=428 | 430..=499 => Self::InvalidQuery,
            _ => Self::Unreachable,
        }
    }

    /// The failure of a transport error.
    #[must_use]
    pub fn of_transport(e: &objstore_r::Error) -> Self {
        match e {
            objstore_r::Error::Policy(_) => Self::RefusedByGuard,
            objstore_r::Error::NoCredentials { .. } => Self::NoCredentials,
            objstore_r::Error::Auth { .. } => Self::Expired,
            objstore_r::Error::Unavailable { message, .. } if message.contains("timed out") => {
                Self::Timeout
            }
            _ => Self::Unreachable,
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A failed search: what kind of failure, and a message that never holds
/// the credential or a response body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchError {
    /// What kind.
    pub failure: Failure,
    /// What happened.
    pub message: String,
}

impl SearchError {
    fn new(failure: Failure, message: impl Into<String>) -> Self {
        Self { failure, message: message.into() }
    }
}

impl fmt::Display for SearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.failure, self.message)
    }
}

impl std::error::Error for SearchError {}

/// Default entries returned.
pub const DEFAULT_LIMIT: u32 = 20;
/// Most entries one search returns.
pub const MAX_LIMIT: u32 = 100;
/// Longest search text, bytes.
pub const MAX_QUERY_BYTES: usize = 256;
/// Default timeout, milliseconds.
pub const DEFAULT_TIMEOUT_MS: u32 = 10_000;
/// Default longest description kept, characters.
pub const DEFAULT_MAX_DESCRIPTION: usize = 300;
/// Entries asked for in the one page read: the most the catalog gives. See
/// the module docs for why this is not the caller's limit.
pub const PAGE_SIZE: u32 = 1_000;
/// Largest search response read, bytes.
pub const MAX_RESPONSE_BYTES: usize = 4 << 20;

/// The catalog API origin.
pub const CATALOG_ORIGIN: &str = "https://dataplex.googleapis.com";

/// What every search is narrowed to.
const SCOPE: &str = "system=bigquery (type=table OR type=view)";

/// How to run one search.
#[derive(Clone, Copy, Debug)]
pub struct SearchOptions<'a> {
    /// Entries returned, at most (1 to [`MAX_LIMIT`]).
    pub limit: u32,
    /// Time waited, at most, milliseconds.
    pub timeout_ms: u32,
    /// Longest description kept, characters.
    pub max_description: usize,
    /// The API origin (default [`CATALOG_ORIGIN`]; the guard decides whether
    /// another is reachable).
    pub endpoint: Option<&'a str>,
}

impl Default for SearchOptions<'_> {
    fn default() -> Self {
        Self {
            limit: DEFAULT_LIMIT,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            max_description: DEFAULT_MAX_DESCRIPTION,
            endpoint: None,
        }
    }
}

/// What a search found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    /// The entries, as one JSON array of objects (see the module docs).
    pub json: String,
    /// Entries in `json`.
    pub count: usize,
    /// More entries matched than were returned.
    pub truncated: bool,
}

/// `project` is a plain project id: lowercase letters, digits and hyphens.
fn valid_project(project: &str) -> bool {
    (1..=63).contains(&project.len())
        && project.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !project.starts_with('-')
        && !project.ends_with('-')
}

/// The query sent for `text`: the text, then [`SCOPE`]. A text that uses
/// `OR` is grouped, so the scope applies to all of it.
fn scoped(text: &str) -> String {
    let text = text.trim();
    if text.contains(" OR ") || text.contains('|') {
        format!("({text}) {SCOPE}")
    } else {
        format!("{text} {SCOPE}")
    }
}

/// `s` percent-encoded for a query value (RFC 3986 unreserved kept).
fn encode_into(s: &str, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[usize::from(b >> 4)] as char);
            out.push(HEX[usize::from(b & 15)] as char);
        }
    }
}

/// The URL of one search, or why the arguments cannot be searched with.
pub fn request_url(
    origin: &str,
    project: &str,
    text: &str,
    limit: u32,
) -> Result<String, SearchError> {
    let invalid = |m: &'static str| SearchError::new(Failure::InvalidQuery, m);
    if !valid_project(project) {
        return Err(invalid("project: not a project id"));
    }
    if text.trim().is_empty() {
        return Err(invalid("query: empty"));
    }
    if text.len() > MAX_QUERY_BYTES {
        return Err(invalid("query: longer than 256 bytes"));
    }
    if text.chars().any(char::is_control) {
        return Err(invalid("query: a control character"));
    }
    if limit == 0 || limit > MAX_LIMIT {
        return Err(invalid("limit: from 1 to 100"));
    }
    let origin = origin.trim_end_matches('/');
    let mut url = format!("{origin}/v1/projects/{project}/locations/global:searchEntries?query=");
    encode_into(&scoped(text), &mut url);
    url.push_str("&pageSize=");
    url.push_str(&PAGE_SIZE.to_string());
    url.push_str("&orderBy=relevance");
    Ok(url)
}

/// `s` cut to `max` characters.
fn cut(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

/// The next value when it is a string; anything else is skipped.
fn string(s: &mut Scanner<'_>, buf: &mut String) -> crate::base::Result<Option<String>> {
    match s.expect()? {
        Event::Str(v) => Ok(Some(v.decode(buf)?.to_owned())),
        other => {
            s.skip(other)?;
            Ok(None)
        }
    }
}

/// What is kept of one catalog entry.
#[derive(Debug, Default)]
struct Entry {
    resource_name: Option<String>,
    kind: Option<String>,
    fqn: Option<String>,
    updated: Option<String>,
    resource: Option<String>,
    system: Option<String>,
    display_name: Option<String>,
    description: Option<String>,
    location: Option<String>,
}

impl Entry {
    /// The entry as a JSON object.
    fn to_json(&self, max_description: usize) -> String {
        let mut out = String::with_capacity(256);
        let mut w = Writer::new(&mut out);
        w.begin_object();
        // `bigquery:project.dataset.table`: the name a table is read by.
        let dotted = self
            .fqn
            .as_deref()
            .and_then(|f| f.strip_prefix("bigquery:"))
            .filter(|n| n.split('.').count() == 3 && n.split('.').all(|p| !p.is_empty()));
        if let Some(name) = dotted {
            w.key("name").string(name);
            if let Some(project) = name.split('.').next() {
                w.key("project").string(project);
            }
        }
        let mut put = |key: &str, value: Option<&str>| {
            if let Some(v) = value.filter(|v| !v.is_empty()) {
                w.key(key).string(v);
            }
        };
        // `projects/…/entryTypes/bigquery-table`: its last segment.
        put("type", self.kind.as_deref().and_then(|t| t.rsplit('/').next()));
        put("system", self.system.as_deref());
        put("location", self.location.as_deref());
        put("display_name", self.display_name.as_deref());
        put("description", self.description.as_deref().map(|d| cut(d, max_description)));
        put("fqn", self.fqn.as_deref());
        put("resource", self.resource.as_deref());
        put("updated", self.updated.as_deref());
        put("entry", self.resource_name.as_deref());
        w.end_object();
        out
    }
}

/// The members kept of an `entrySource` object (its `{` already read).
fn source(s: &mut Scanner<'_>, e: &mut Entry, buf: &mut String) -> crate::base::Result<()> {
    loop {
        match s.expect()? {
            Event::ObjectEnd => return Ok(()),
            Event::Key(k) => match k.as_plain().unwrap_or("") {
                "resource" => e.resource = string(s, buf)?,
                "system" => e.system = string(s, buf)?,
                "displayName" => e.display_name = string(s, buf)?,
                "description" => e.description = string(s, buf)?,
                "location" => e.location = string(s, buf)?,
                _ => s.skip_value()?,
            },
            _ => return Err(crate::base::Error::json("expected key")),
        }
    }
}

/// One `dataplexEntry` object (its `{` already read).
fn entry(s: &mut Scanner<'_>, buf: &mut String) -> crate::base::Result<Entry> {
    let mut e = Entry::default();
    loop {
        match s.expect()? {
            Event::ObjectEnd => return Ok(e),
            Event::Key(k) => match k.as_plain().unwrap_or("") {
                "name" => e.resource_name = string(s, buf)?,
                "entryType" => e.kind = string(s, buf)?,
                "fullyQualifiedName" => e.fqn = string(s, buf)?,
                "updateTime" => e.updated = string(s, buf)?,
                "entrySource" => match s.expect()? {
                    Event::ObjectStart => source(s, &mut e, buf)?,
                    other => s.skip(other)?,
                },
                _ => s.skip_value()?,
            },
            _ => return Err(crate::base::Error::json("expected key")),
        }
    }
}

/// One element of `results` (its `{` already read): its entry, if it has one.
fn result(s: &mut Scanner<'_>, buf: &mut String) -> crate::base::Result<Option<Entry>> {
    let mut found = None;
    loop {
        match s.expect()? {
            Event::ObjectEnd => return Ok(found),
            Event::Key(k) if k.eq_plain("dataplexEntry") => match s.expect()? {
                Event::ObjectStart => found = Some(entry(s, buf)?),
                other => s.skip(other)?,
            },
            Event::Key(_) => s.skip_value()?,
            _ => return Err(crate::base::Error::json("expected key")),
        }
    }
}

/// What a `searchEntries` response body holds: at most `limit` entries, each
/// description cut at `max_description` characters.
pub fn entries(raw: &[u8], limit: usize, max_description: usize) -> crate::base::Result<Found> {
    let src = std::str::from_utf8(raw).map_err(|_| crate::base::Error::json("not UTF-8"))?;
    let mut s = Scanner::new(src);
    if s.expect()? != Event::ObjectStart {
        return Err(crate::base::Error::json("not an object"));
    }
    let mut json = String::from("[");
    let mut count = 0usize;
    let mut truncated = false;
    let mut buf = String::new();
    loop {
        match s.expect()? {
            Event::ObjectEnd => break,
            Event::Key(k) if k.eq_plain("results") => match s.expect()? {
                Event::ArrayStart => loop {
                    match s.expect()? {
                        Event::ArrayEnd => break,
                        Event::ObjectStart => {
                            if count == limit {
                                // Past the limit an element is stepped over,
                                // not read: nothing of it is kept in memory.
                                s.skip(Event::ObjectStart)?;
                                truncated = true;
                                continue;
                            }
                            let Some(e) = result(&mut s, &mut buf)? else { continue };
                            if count > 0 {
                                json.push(',');
                            }
                            json.push_str(&e.to_json(max_description));
                            count += 1;
                        }
                        other => s.skip(other)?,
                    }
                },
                other => s.skip(other)?,
            },
            Event::Key(k) if k.eq_plain("nextPageToken") => {
                truncated |= string(&mut s, &mut buf)?.is_some_and(|t| !t.is_empty());
            }
            Event::Key(_) => s.skip_value()?,
            _ => return Err(crate::base::Error::json("expected key")),
        }
    }
    if s.next_event()?.is_some() {
        return Err(crate::base::Error::json("text after the response"));
    }
    json.push(']');
    Ok(Found { json, count, truncated })
}

/// Search the catalog at `origin` for `text` through `transport`, in
/// `project`. See the module docs.
pub async fn search_catalog<T: Transport>(
    transport: &T,
    origin: &str,
    project: &str,
    text: &str,
    opts: &SearchOptions<'_>,
) -> Result<Found, SearchError> {
    let url = request_url(origin, project, text, opts.limit)?;
    // A search changes nothing, so one repeat of a transient failure is safe.
    // The method takes no body, and a POST that does not state its length is
    // refused (411) before the catalog sees it.
    let req = Request {
        method: Method::Post,
        retry: Retry::Transient,
        headers: &[("content-length", "0")],
        ..Request::get(&url)
    };
    let resp = transport
        .send(req)
        .await
        .map_err(|e| SearchError::new(Failure::of_transport(&e), e.to_string()))?;
    if !resp.is_success() {
        return Err(SearchError::new(
            Failure::of_status(resp.status),
            format!("the catalog answered {}", resp.status),
        ));
    }
    if resp.body.len() > MAX_RESPONSE_BYTES {
        return Err(SearchError::new(Failure::Unreachable, "search response too large"));
    }
    entries(&resp.body, opts.limit as usize, opts.max_description)
        .map_err(|_| SearchError::new(Failure::Unreachable, "not a search response"))
}

/// Search with the credential `tokens` (a discovery's result, so a missing
/// credential is a typed failure) under `guard` (feature `http`).
///
/// The arguments and the guard are checked first; no credential is touched
/// and nothing is sent when either refuses.
#[cfg(feature = "http")]
pub async fn search<T: objstore_r::token::TokenProvider>(
    tokens: objstore_r::Result<T>,
    guard: crate::exec::guard::MetadataGuard<'_>,
    project: &str,
    text: &str,
    opts: &SearchOptions<'_>,
) -> Result<Found, SearchError> {
    use objstore_r::policy::{RequestPolicy, Url};
    let origin = opts.endpoint.unwrap_or(CATALOG_ORIGIN);
    let url = request_url(origin, project, text, opts.limit)?;
    if opts.timeout_ms == 0 {
        return Err(SearchError::new(Failure::InvalidQuery, "timeout_ms: must be positive"));
    }
    let parsed = Url::parse(&url)
        .map_err(|_| SearchError::new(Failure::RefusedByGuard, "refused: not a URL"))?;
    guard
        .check(Method::Post, &parsed)
        .map_err(|e| SearchError::new(Failure::RefusedByGuard, format!("refused: {e}")))?;
    let tokens = tokens.map_err(|e| SearchError::new(Failure::of_transport(&e), e.to_string()))?;
    let cfg = objstore_r::http::HttpConfig {
        max_retries: 1,
        timeout: std::time::Duration::from_millis(u64::from(opts.timeout_ms)),
        max_response_bytes: MAX_RESPONSE_BYTES as u64,
        ..objstore_r::http::HttpConfig::default()
    };
    let client = objstore_r::http::Client::with_policy(tokens, guard, cfg)
        .map_err(|e| SearchError::new(Failure::Unreachable, e.to_string()))?;
    search_catalog(&client, origin, project, text, opts).await
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = r#"{
      "results": [
        {"linkedResource": "x", "snippets": {"dataplexEntry": {"name": "ignored"}},
         "dataplexEntry": {
           "name": "projects/p1/locations/us/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/p1/datasets/mart/tables/orders",
           "entryType": "projects/655216118709/locations/global/entryTypes/bigquery-table",
           "fullyQualifiedName": "bigquery:p1.mart.orders",
           "updateTime": "2026-01-02T03:04:05Z",
           "aspects": {"a.b.schema": {"data": {"fields": [{"name": "secret_column"}]}}},
           "entrySource": {"resource": "//bigquery.googleapis.com/projects/p1/datasets/mart/tables/orders",
             "system": "BIGQUERY", "displayName": "orders", "description": "Every order, one row each.",
             "labels": {"team": "sales"}, "location": "us"}}},
        {"dataplexEntry": {"entryType": "projects/1/locations/global/entryTypes/bigquery-view",
           "fullyQualifiedName": "bigquery:p2.mart.v_orders", "entrySource": {"system": "BIGQUERY"}}},
        {"dataplexEntry": {"fullyQualifiedName": "custom:thing", "entrySource": {"description": null}}},
        {"unexpected": true},
        "not an object"
      ],
      "totalSize": 9,
      "nextPageToken": "",
      "unreachable": ["projects/x/locations/y"]
    }"#;

    #[test]
    fn an_entry_keeps_only_the_listed_members() {
        let f = entries(BODY.as_bytes(), 10, 300).unwrap();
        assert_eq!((f.count, f.truncated), (3, false));
        Scanner::new(&f.json).skip_value().unwrap();
        for want in [
            r#""name":"p1.mart.orders","project":"p1","type":"bigquery-table","system":"BIGQUERY","location":"us","display_name":"orders","description":"Every order, one row each.""#,
            r#""fqn":"bigquery:p1.mart.orders""#,
            r#""updated":"2026-01-02T03:04:05Z""#,
            r#"{"name":"p2.mart.v_orders","project":"p2","type":"bigquery-view","system":"BIGQUERY","fqn":"bigquery:p2.mart.v_orders"}"#,
            r#"{"fqn":"custom:thing"}"#,
        ] {
            assert!(f.json.contains(want), "{want} in {}", f.json);
        }
        for dropped in ["secret_column", "aspects", "labels", "sales", "snippets", "ignored"] {
            assert!(!f.json.contains(dropped), "{dropped} in {}", f.json);
        }
    }

    #[test]
    fn the_limit_and_a_next_page_both_mean_truncated() {
        let f = entries(BODY.as_bytes(), 2, 300).unwrap();
        assert_eq!((f.count, f.truncated), (2, true));
        let more = r#"{"results": [], "nextPageToken": "t"}"#;
        assert!(entries(more.as_bytes(), 5, 300).unwrap().truncated);
        let none = entries(b"{}", 5, 300).unwrap();
        assert_eq!((none.json.as_str(), none.count, none.truncated), ("[]", 0, false));
    }

    #[test]
    fn a_description_is_cut_on_a_character() {
        let body = format!(
            r#"{{"results":[{{"dataplexEntry":{{"entrySource":{{"description":"{}"}}}}}}]}}"#,
            "é".repeat(50)
        );
        let f = entries(body.as_bytes(), 1, 7).unwrap();
        assert!(f.json.contains(&format!(r#""description":"{}""#, "é".repeat(7))), "{}", f.json);
    }

    #[test]
    fn what_is_not_a_response_is_an_error() {
        for raw in ["", "[]", "{", r#"{"results": [}"#, "{} {}", "\u{0}"] {
            assert!(entries(raw.as_bytes(), 5, 300).is_err(), "{raw:?}");
        }
        assert!(entries(&[0xff, 0xfe], 5, 300).is_err());
    }

    #[test]
    fn the_url_carries_the_scoped_text_and_nothing_unencoded() {
        let u = request_url("https://h/", "my-proj", "monthly active users", 10).unwrap();
        assert_eq!(
            u,
            "https://h/v1/projects/my-proj/locations/global:searchEntries?query=monthly%20active%20users%20system%3Dbigquery%20%28type%3Dtable%20OR%20type%3Dview%29&pageSize=1000&orderBy=relevance"
        );
        // An `OR` of the caller's is grouped, so the scope still binds.
        let u = request_url("https://h", "p", "orders OR invoices", 1).unwrap();
        assert!(u.contains("query=%28orders%20OR%20invoices%29%20system%3Dbigquery"), "{u}");
        // Nothing of the text can add a parameter or a path.
        let u = request_url("https://h", "p", "a&pageSize=1000#/../x?y=é", 1).unwrap();
        let (path, query) = u.split_once('?').unwrap();
        assert_eq!(path, "https://h/v1/projects/p/locations/global:searchEntries");
        assert_eq!(query.matches('&').count(), 2, "{query}");
        assert!(!query.contains('#') && !query.contains('/') && !query.contains('?'), "{query}");
    }

    #[test]
    fn unusable_arguments_are_refused_before_anything_is_built() {
        let bad = |project: &str, text: &str, limit: u32| {
            request_url("https://h", project, text, limit).unwrap_err().failure
        };
        for project in ["", "My-Proj", "p/q", "p:searchEntries", "p?x", "-p", "p-", "p.q", "é"] {
            assert_eq!(bad(project, "orders", 5), Failure::InvalidQuery, "{project:?}");
        }
        assert_eq!(bad(&"p".repeat(64), "orders", 5), Failure::InvalidQuery);
        for text in ["", "   ", "a\nb", "a\u{0}", "a\u{7f}"] {
            assert_eq!(bad("p", text, 5), Failure::InvalidQuery, "{text:?}");
        }
        assert_eq!(bad("p", &"x".repeat(MAX_QUERY_BYTES + 1), 5), Failure::InvalidQuery);
        assert_eq!(bad("p", "orders", 0), Failure::InvalidQuery);
        assert_eq!(bad("p", "orders", MAX_LIMIT + 1), Failure::InvalidQuery);
        assert!(request_url("https://h", "p", &"x".repeat(MAX_QUERY_BYTES), MAX_LIMIT).is_ok());
    }

    #[test]
    fn statuses_are_typed() {
        for (status, want) in [
            (400, Failure::InvalidQuery),
            (401, Failure::Expired),
            (403, Failure::NoAccess),
            (404, Failure::NotFound),
            (429, Failure::Unreachable),
            (500, Failure::Unreachable),
            (503, Failure::Unreachable),
        ] {
            assert_eq!(Failure::of_status(status), want, "{status}");
        }
    }
}
