// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Google Data Lineage (Dataplex) column-level lineage over any objstore-r
//! [`Transport`] (feature `dataplex`).
//!
//! One `searchLineageStreaming` call per batch of 20 root columns (the
//! service's limit), up to 8 batches at once, walks the
//! upstream graph server-side, across projects and regions; the links are
//! rebuilt into a [`Graph`]. Each link's `dependencyInfo` says whether the
//! target is an `EXACT_COPY` of the source (a description may be copied
//! verbatim along it) or depends on it otherwise (`OTHER`: computed from it,
//! or filtered or joined on it). A second call filtered to `EXACT_COPY` is
//! made only when asked for and the service sent no dependency information.
//!
//! A column path is sent as its segments (`field: ["addr", "city"]` is the
//! nested field `addr.city`; several columns are several entities).
//!
//! The method is in Preview. Responses are parsed defensively: a streamed
//! response arrives over REST as a JSON array of chunks (a single object is
//! accepted too); links without a column (`field`) are table-level and
//! skipped; unknown members are ignored; `unreachable` regions are reported
//! so callers can say where the graph may be incomplete.

use crate::{Column, Direction, Error, Graph, LineageSource, Result};
use crab_core::json::{Event, Scanner, Writer};
use objstore_r::transport::{Request, Transport};
use objstore_r::Bytes;
use std::future::Future;

/// Roots per request.
/// Root entities per request: the service rejects more than 20.
const BATCH: usize = 20;
/// Requests in flight at once.
const IN_FLIGHT: usize = 8;

/// Settings.
#[derive(Clone, Debug)]
pub struct DataplexConfig {
    /// Project billed for the search.
    pub project: String,
    /// Location in the request path (`us`, `eu`, a region).
    pub location: String,
    /// Regions to search (empty: `location` only).
    pub locations: Vec<String>,
    /// Mark exact-copy edges even when the service returns no dependency
    /// information (one more call per batch, only then).
    pub exact_copy: bool,
    /// Override the API origin (tests).
    pub endpoint: Option<String>,
}

/// The Dataplex lineage client.
#[derive(Debug)]
pub struct Dataplex<T: Transport> {
    transport: T,
    parent: String,
    url: String,
    locations: Vec<String>,
    exact_copy: bool,
}

impl<T: Transport> Dataplex<T> {
    /// A client reached through `transport`.
    #[must_use]
    pub fn new(transport: T, cfg: DataplexConfig) -> Self {
        let parent = format!("projects/{}/locations/{}", cfg.project, cfg.location);
        let origin = cfg
            .endpoint
            .as_deref()
            .unwrap_or("https://datalineage.googleapis.com")
            .trim_end_matches('/');
        let url = format!("{origin}/v1/{parent}:searchLineageStreaming");
        let locations = if cfg.locations.is_empty() { vec![cfg.location] } else { cfg.locations };
        Self { transport, parent, url, locations, exact_copy: cfg.exact_copy }
    }

    /// The transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    fn body(&self, roots: &[Column], max_depth: u32, exact_only: bool, dir: Direction) -> Bytes {
        let mut out = String::with_capacity(256 + roots.len() * 96);
        let mut w = Writer::new(&mut out);
        w.begin_object().key("parent").string(&self.parent);
        w.key("locations").begin_array();
        for l in &self.locations {
            w.string(l);
        }
        w.end_array();
        w.key("rootCriteria")
            .begin_object()
            .key("entities")
            .begin_object()
            .key("entities")
            .begin_array();
        for r in roots {
            w.begin_object().key("fullyQualifiedName").string(&format!("bigquery:{}", r.table));
            w.key("field").begin_array();
            for seg in r.column.split('.') {
                w.string(seg);
            }
            w.end_array().end_object();
        }
        w.end_array().end_object().end_object();
        w.key("direction").string(match dir {
            Direction::Upstream => "UPSTREAM",
            Direction::Downstream => "DOWNSTREAM",
        });
        w.key("limits")
            .begin_object()
            .key("maxDepth")
            .uint(u64::from(max_depth))
            .key("maxResults")
            .uint(5_000)
            .end_object();
        if exact_only {
            w.key("filters")
                .begin_object()
                .key("dependencyTypes")
                .begin_array()
                .string("EXACT_COPY")
                .end_array()
                .end_object();
        }
        w.end_object();
        Bytes::from(out)
    }

    async fn request(
        &self,
        roots: &[Column],
        max_depth: u32,
        exact_only: bool,
        dir: Direction,
        g: &mut Graph,
    ) -> Result<bool> {
        let resp = self
            .transport
            .send(Request::post_json(&self.url, self.body(roots, max_depth, exact_only, dir)))
            .await?;
        if !resp.is_success() {
            return Err(Error::Api {
                status: resp.status,
                message: String::from_utf8_lossy(&resp.body)
                    .chars()
                    .filter(|c| !c.is_control())
                    .take(300)
                    .collect(),
            });
        }
        parse_links(&resp.body, g, exact_only)
    }
}

impl<T: Transport> LineageSource for Dataplex<T> {
    type Search<'a>
        = impl Future<Output = Result<Graph>> + 'a
    where
        Self: 'a;

    fn search<'a>(
        &'a self,
        roots: &'a [Column],
        max_depth: u32,
        dir: Direction,
    ) -> Self::Search<'a> {
        async move {
            let mut g = Graph::new();
            let batches: Vec<&[Column]> = roots.chunks(BATCH).collect();
            for wave in batches.chunks(IN_FLIGHT) {
                let parts = crab_core::join::join_all(wave.iter().map(|batch| async move {
                    let mut part = Graph::new();
                    let typed = self.request(batch, max_depth, false, dir, &mut part).await?;
                    if self.exact_copy && !typed {
                        self.request(batch, max_depth, true, dir, &mut part).await?;
                    }
                    Ok::<Graph, Error>(part)
                }))
                .await;
                for part in parts {
                    g.merge(part?);
                }
            }
            Ok(g)
        }
    }
}

/// Decode a `searchLineageStreaming` response into a graph (every edge
/// marked `exact` when the request was filtered to exact copies; otherwise
/// by each link's `dependencyInfo`).
pub fn parse_response(body: &[u8], exact: bool) -> Result<Graph> {
    let mut g = Graph::new();
    parse_links(body, &mut g, exact)?;
    Ok(g)
}

/// Add the links of a response (array of chunks, or one object) to `g`.
/// Returns whether any link carried dependency information.
pub(crate) fn parse_links(body: &[u8], g: &mut Graph, exact: bool) -> Result<bool> {
    let mut typed = false;
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(typed);
    }
    let mut s = Scanner::from_bytes(body)?;
    match s.expect()? {
        Event::ArrayStart => loop {
            match s.expect()? {
                Event::ArrayEnd => return Ok(typed),
                Event::ObjectStart => chunk(&mut s, g, exact, &mut typed)?,
                other => s.skip(other)?,
            }
        },
        Event::ObjectStart => chunk(&mut s, g, exact, &mut typed).map(|()| typed),
        _ => {
            Err(crab_core::Error::Json { at: 0, msg: "lineage response is not an object or array" }
                .into())
        }
    }
}

fn skip_value(s: &mut Scanner<'_>) -> Result<()> {
    let v = s.expect()?;
    s.skip(v)?;
    Ok(())
}

/// One chunk: `{"links": [...], "unreachable": [...]}` (other members ignored).
fn chunk(s: &mut Scanner<'_>, g: &mut Graph, exact: bool, typed: &mut bool) -> Result<()> {
    let mut buf = String::new();
    loop {
        match s.expect()? {
            Event::ObjectEnd => return Ok(()),
            Event::Key(k) if k.eq_plain("links") => {
                let first = s.expect()?;
                if first != Event::ArrayStart {
                    s.skip(first)?;
                    continue;
                }
                loop {
                    match s.expect()? {
                        Event::ArrayEnd => break,
                        Event::ObjectStart => {
                            let (mut src, mut dst, mut copy) = (None, None, exact);
                            loop {
                                match s.expect()? {
                                    Event::ObjectEnd => break,
                                    Event::Key(k) if k.eq_plain("source") => {
                                        src = entity(s, &mut buf)?;
                                    }
                                    Event::Key(k) if k.eq_plain("target") => {
                                        dst = entity(s, &mut buf)?;
                                    }
                                    Event::Key(k) if k.eq_plain("dependencyInfo") => {
                                        if let Some(c) = dependency(s)? {
                                            *typed = true;
                                            copy |= c;
                                        }
                                    }
                                    Event::Key(_) => skip_value(s)?,
                                    _ => {
                                        return Err(crab_core::Error::Json {
                                            at: s.offset(),
                                            msg: "expected key",
                                        }
                                        .into())
                                    }
                                }
                            }
                            // Column-level links only: the target is derived from the source.
                            if let (Some(parent), Some(child)) = (src, dst) {
                                g.add(child, parent, copy);
                            }
                        }
                        other => s.skip(other)?,
                    }
                }
            }
            Event::Key(k) if k.eq_plain("unreachable") => {
                let first = s.expect()?;
                if first != Event::ArrayStart {
                    s.skip(first)?;
                    continue;
                }
                loop {
                    match s.expect()? {
                        Event::ArrayEnd => break,
                        Event::Str(v) => {
                            g.unreachable.insert(v.decode(&mut buf)?.to_owned());
                        }
                        other => s.skip(other)?,
                    }
                }
            }
            Event::Key(_) => skip_value(s)?,
            _ => return Err(crab_core::Error::Json { at: s.offset(), msg: "expected key" }.into()),
        }
    }
}

/// `[{"dependencyType": "EXACT_COPY"}, {"dependencyType": "OTHER"}]` →
/// whether any is an exact copy; `None` when no type is given.
fn dependency(s: &mut Scanner<'_>) -> Result<Option<bool>> {
    let first = s.expect()?;
    if first != Event::ArrayStart {
        s.skip(first)?;
        return Ok(None);
    }
    let mut out = None;
    loop {
        match s.expect()? {
            Event::ArrayEnd => return Ok(out),
            Event::ObjectStart => loop {
                match s.expect()? {
                    Event::ObjectEnd => break,
                    Event::Key(k) if k.eq_plain("dependencyType") => match s.expect()? {
                        Event::Str(v) => {
                            let exact = v.eq_plain("EXACT_COPY");
                            out = Some(out.unwrap_or(false) | exact);
                        }
                        other => s.skip(other)?,
                    },
                    Event::Key(_) => skip_value(s)?,
                    _ => {
                        return Err(
                            crab_core::Error::Json { at: s.offset(), msg: "expected key" }.into()
                        )
                    }
                }
            },
            other => s.skip(other)?,
        }
    }
}

/// `{"fullyQualifiedName": "bigquery:p.d.t", "field": ["col", ...]}` → the
/// column (segments of a nested path joined by `.`); `None` for
/// non-BigQuery or table-level entities.
fn entity(s: &mut Scanner<'_>, buf: &mut String) -> Result<Option<Column>> {
    let first = s.expect()?;
    if first != Event::ObjectStart {
        s.skip(first)?;
        return Ok(None);
    }
    let (mut fqn, mut field) = (None::<String>, None::<String>);
    loop {
        match s.expect()? {
            Event::ObjectEnd => break,
            Event::Key(k) if k.eq_plain("fullyQualifiedName") => {
                if let Event::Str(v) = s.expect()? {
                    fqn = Some(v.decode(buf)?.to_owned());
                }
            }
            Event::Key(k) if k.eq_plain("field") => match s.expect()? {
                Event::ArrayStart => loop {
                    match s.expect()? {
                        Event::ArrayEnd => break,
                        Event::Str(v) => {
                            // Segments of one (nested) field path.
                            let seg = v.decode(buf)?;
                            match &mut field {
                                Some(f) => {
                                    f.push('.');
                                    f.push_str(seg);
                                }
                                None => field = Some(seg.to_owned()),
                            }
                        }
                        other => s.skip(other)?,
                    }
                },
                Event::Str(v) => field = Some(v.decode(buf)?.to_owned()),
                other => s.skip(other)?,
            },
            Event::Key(_) => skip_value(s)?,
            _ => return Err(crab_core::Error::Json { at: s.offset(), msg: "expected key" }.into()),
        }
    }
    let Some(table) = fqn.as_deref().and_then(|f| f.strip_prefix("bigquery:")) else {
        return Ok(None);
    };
    Ok(field.map(|f| Column::new(table.trim_matches('`'), &f)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_chunked_links_skipping_table_level_and_foreign_entities() {
        let body = br#"[
          {"links": [
            {"source": {"fullyQualifiedName": "bigquery:p.raw.orders", "field": ["Amount"]},
             "target": {"fullyQualifiedName": "bigquery:p.stg.orders", "field": ["amount_usd"]},
             "dependencyInfo": [{"dependencyType": "OTHER"}]},
            {"source": {"fullyQualifiedName": "bigquery:p.raw.orders"},
             "target": {"fullyQualifiedName": "bigquery:p.stg.orders"}}
          ]},
          {"links": [
            {"source": {"fullyQualifiedName": "spark:hdfs://x", "field": ["a"]},
             "target": {"fullyQualifiedName": "bigquery:p.stg.orders", "field": ["a"]}}
          ], "unreachable": ["europe-west1"], "nextPageToken": ""}
        ]"#;
        let mut g = Graph::new();
        parse_links(body, &mut g, false).unwrap();
        assert_eq!(g.edge_count(), 1, "table-level and non-BigQuery links skipped");
        assert_eq!(
            g.parents(&Column::new("p.stg.orders", "amount_usd")),
            &[(Column::new("p.raw.orders", "amount"), false)]
        );
        assert!(g.unreachable.contains("europe-west1"));
        let mut e = Graph::new();
        parse_links(br#"{"links":[{"source":{"fullyQualifiedName":"bigquery:p.a.b","field":["x"]},"target":{"fullyQualifiedName":"bigquery:p.a.c","field":["x"]}}]}"#, &mut e, true).unwrap();
        assert_eq!(e.parents(&Column::new("p.a.c", "x")), &[(Column::new("p.a.b", "x"), true)]);
        assert!(parse_links(b"   ", &mut e, false).is_ok());
        assert!(parse_links(b"42", &mut e, false).is_err());
    }

    use objstore_r::transport::{BodyStream, Response};
    use std::cell::RefCell;
    use std::future::{ready, Ready};

    #[derive(Default)]
    struct Mock {
        sent: RefCell<Vec<(String, String)>>,
    }

    struct NoBody;
    impl BodyStream for NoBody {
        type Next<'a> = Ready<objstore_r::Result<Option<Bytes>>>;
        fn next_chunk(&mut self) -> Self::Next<'_> {
            ready(Ok(None))
        }
    }

    impl Transport for Mock {
        type Send<'a> = Ready<objstore_r::Result<Response>>;
        type Body<'a> = NoBody;
        type Open<'a> = Ready<objstore_r::Result<(u16, NoBody)>>;
        fn send<'a>(&'a self, req: Request<'a>) -> Self::Send<'a> {
            let body = String::from_utf8(req.body.to_vec()).unwrap();
            let exact = body.contains("EXACT_COPY");
            self.sent.borrow_mut().push((req.url.to_owned(), body));
            let links = if exact {
                r#"[{"links":[{"source":{"fullyQualifiedName":"bigquery:p.raw.orders","field":["amount"]},"target":{"fullyQualifiedName":"bigquery:p.stg.orders","field":["amount"]}}]}]"#
            } else {
                r#"[{"links":[{"source":{"fullyQualifiedName":"bigquery:p.raw.orders","field":["amount"]},"target":{"fullyQualifiedName":"bigquery:p.stg.orders","field":["amount"]}},{"source":{"fullyQualifiedName":"bigquery:p.raw.fx","field":["rate"]},"target":{"fullyQualifiedName":"bigquery:p.stg.orders","field":["amount_cad"]}}]}]"#
            };
            ready(Ok(Response { status: 200, body: Bytes::from(links) }))
        }
        fn open<'a>(&'a self, _: Request<'a>) -> Self::Open<'a> {
            unreachable!()
        }
    }

    fn block_on<F: Future>(f: F) -> F::Output {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let mut f = std::pin::pin!(f);
        loop {
            if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    #[test]
    fn upstream_batches_roots_and_marks_exact_copies() {
        let d = Dataplex::new(
            Mock::default(),
            DataplexConfig {
                project: "p".into(),
                location: "us".into(),
                locations: vec!["us".into(), "northamerica-northeast1".into()],
                exact_copy: true,
                endpoint: Some("https://lineage.test".into()),
            },
        );
        let roots: Vec<Column> =
            (0..30).map(|i| Column::new("p.stg.orders", &format!("C{i}"))).collect();
        let g = block_on(d.upstream(&roots, 10)).unwrap();
        let sent = d.transport().sent.borrow();
        assert_eq!(sent.len(), 4, "2 batches (20 + 10) × (all edges + exact-copy pass)");
        assert_eq!(
            sent[0].0,
            "https://lineage.test/v1/projects/p/locations/us:searchLineageStreaming"
        );
        assert!(sent[0].1.starts_with(r#"{"parent":"projects/p/locations/us","locations":["us","northamerica-northeast1"],"rootCriteria":{"entities":{"entities":[{"fullyQualifiedName":"bigquery:p.stg.orders","field":["c0"]}"#), "{}", sent[0].1);
        assert!(sent[0]
            .1
            .ends_with(r#""direction":"UPSTREAM","limits":{"maxDepth":10,"maxResults":5000}}"#));
        assert!(sent[1].1.contains(r#""filters":{"dependencyTypes":["EXACT_COPY"]}"#));
        assert!(sent.iter().all(|(_, b)| b.matches("fullyQualifiedName").count() <= 20));
        assert_eq!(
            g.parents(&Column::new("p.stg.orders", "amount")),
            &[(Column::new("p.raw.orders", "amount"), true)]
        );
        assert_eq!(
            g.parents(&Column::new("p.stg.orders", "amount_cad")),
            &[(Column::new("p.raw.fx", "rate"), false)]
        );
    }

    /// As the service answered for a CTAS with `WHERE status != 'Cancelled'`
    /// (2026-09-30): the filter column is an `OTHER` dependency of every
    /// output column; the copied columns are `EXACT_COPY`.
    const LIVE: &[u8] = br#"[{"links":[
      {"source":{"fullyQualifiedName":"bigquery:p.mock.raw_orders","field":["returned_at"]},"target":{"fullyQualifiedName":"bigquery:p.mock.stg_orders","field":["returned_at"]},"dependencyInfo":[{"dependencyType":"EXACT_COPY"}],"depth":1,"location":"us"},
      {"source":{"fullyQualifiedName":"bigquery:p.mock.raw_orders","field":["status"]},"target":{"fullyQualifiedName":"bigquery:p.mock.stg_orders","field":["returned_at"]},"dependencyInfo":[{"dependencyType":"OTHER"}],"depth":1,"location":"us"},
      {"source":{"fullyQualifiedName":"bigquery:p.mock.raw_orders","field":["status"]},"target":{"fullyQualifiedName":"bigquery:p.mock.stg_orders","field":["order_status"]},"dependencyInfo":[{"dependencyType":"EXACT_COPY"},{"dependencyType":"OTHER"}],"depth":1,"location":"us"}]},
     {"links":[
      {"source":{"fullyQualifiedName":"bigquery:bigquery-public-data.thelook_ecommerce.orders","field":["returned_at"]},"target":{"fullyQualifiedName":"bigquery:p.mock.raw_orders","field":["returned_at"]},"dependencyInfo":[{"dependencyType":"EXACT_COPY"}],"depth":2,"location":"us"},
      {"source":{"fullyQualifiedName":"bigquery:p.mock.raw_orders","field":["addr","city"]},"target":{"fullyQualifiedName":"bigquery:p.mock.stg_orders","field":["ship","city"]},"dependencyInfo":[{"dependencyType":"EXACT_COPY"}],"depth":1,"location":"us"}]}]"#;

    #[test]
    fn dependency_types_mark_copies_and_filters_are_not_sources() {
        let mut g = Graph::new();
        assert!(parse_links(LIVE, &mut g, false).unwrap(), "typed response");
        let col = |t: &str, c: &str| Column::new(t, c);
        assert_eq!(
            g.parents(&col("p.mock.stg_orders", "returned_at")),
            &[
                (col("p.mock.raw_orders", "returned_at"), true),
                (col("p.mock.raw_orders", "status"), false)
            ]
        );
        assert_eq!(
            g.parents(&col("p.mock.stg_orders", "order_status")),
            &[(col("p.mock.raw_orders", "status"), true)]
        );
        assert_eq!(
            g.parents(&col("p.mock.stg_orders", "ship.city")),
            &[(col("p.mock.raw_orders", "addr.city"), true)],
            "nested paths arrive as segments"
        );
        // The documented filter column must not compete with the copy.
        let docs = |c: &Column| match (c.table.as_str(), c.column.as_str()) {
            ("p.mock.raw_orders", "status") => Some("Order status.".to_owned()),
            ("bigquery-public-data.thelook_ecommerce.orders", "returned_at") => {
                Some("When the order was returned.".to_owned())
            }
            _ => None,
        };
        let hits = g.nearest_documented(&col("p.mock.stg_orders", "returned_at"), docs);
        assert_eq!(hits.len(), 1);
        assert_eq!(
            (hits[0].description.as_str(), hits[0].exact_copy_chain),
            ("When the order was returned.", true)
        );
    }

    #[test]
    fn typed_responses_need_no_exact_copy_pass_and_nested_roots_are_segmented() {
        struct Typed(RefCell<Vec<String>>);
        impl Transport for Typed {
            type Send<'a> = Ready<objstore_r::Result<Response>>;
            type Body<'a> = NoBody;
            type Open<'a> = Ready<objstore_r::Result<(u16, NoBody)>>;
            fn send<'a>(&'a self, req: Request<'a>) -> Self::Send<'a> {
                self.0.borrow_mut().push(String::from_utf8(req.body.to_vec()).unwrap());
                ready(Ok(Response { status: 200, body: Bytes::from_static(LIVE) }))
            }
            fn open<'a>(&'a self, _: Request<'a>) -> Self::Open<'a> {
                unreachable!()
            }
        }
        let d = Dataplex::new(
            Typed(RefCell::new(vec![])),
            DataplexConfig {
                project: "p".into(),
                location: "us".into(),
                locations: vec![],
                exact_copy: true,
                endpoint: Some("https://lineage.test".into()),
            },
        );
        let roots = [Column::new("p.mock.stg_orders", "ship.city")];
        let g = block_on(d.upstream(&roots, 10)).unwrap();
        let sent = d.transport().0.borrow();
        assert_eq!(sent.len(), 1, "dependency types came back: no second pass");
        assert!(sent[0].contains(r#""field":["ship","city"]"#), "{}", sent[0]);
        assert_eq!(g.edge_count(), 5);
    }
}
