# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
"""The 0.5.3 surface: search_catalog and search_catalog_async. Unusable
arguments are ValueErrors before any credential is looked for; entries are
dicts in a CatalogEntries list with an explicit `truncated`; every failure
is a SearchError with a `.reason`; the token goes in the authorization
header and nowhere else, and never appears in an error; the metadata server
is never asked. A local stub plays the catalog (testing builds), a decoy
the instance metadata server."""

import asyncio
import http.server
import json
import socket
import socketserver
import threading
import urllib.parse

import pytest

import crab_r

TOKEN = "ya29.python-search-test-token"
SCOPE = "system=bigquery (type=table OR type=view)"

ENTRIES = [
    {"dataplexEntry": {
        "name": "projects/p1/locations/us/entryGroups/@bigquery/entries/x",
        "entryType": "projects/1/locations/global/entryTypes/bigquery-table",
        "fullyQualifiedName": "bigquery:p1.mart.orders",
        "updateTime": "2026-01-02T03:04:05Z",
        "aspects": {"schema": {"data": {"fields": [{"name": "ASPECT-MARKER"}]}}},
        "entrySource": {"system": "BIGQUERY", "displayName": "orders", "location": "us",
                        "description": "d" * 400, "labels": {"team": "LABEL-MARKER"}}}},
    {"dataplexEntry": {
        "entryType": "projects/1/locations/global/entryTypes/bigquery-view",
        "fullyQualifiedName": "bigquery:p2.mart.v_orders",
        "entrySource": {"system": "BIGQUERY"}}},
]


class _Catalog:
    """A searchEntries stand-in: a marker word in the query picks the answer;
    records every request."""

    def __init__(self):
        self.requests = []
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _send(self, status, body):
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                outer.requests.append(("GET", self.path, b"", dict(self.headers)))
                self._send(404, {"error": {"code": 404, "message": "no"}})

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("content-length", 0)))
                outer.requests.append(("POST", self.path, body, dict(self.headers)))
                url = urllib.parse.urlsplit(self.path)
                params = urllib.parse.parse_qs(url.query)
                text = params.get("query", [""])[0]
                auth = self.headers.get("authorization", "")
                for word, status in (("s400", 400), ("s401", 401), ("s403", 403), ("s404", 404), ("s500", 500)):
                    if text.startswith(word):
                        return self._send(status, {"error": {"code": status, "message": "BODY-MARKER"}})
                if text.startswith("echo"):
                    return self._send(403, {"error": {"code": 403, "message": f"denied for {auth}"}})
                size = int(params.get("pageSize", ["10"])[0])
                self._send(200, {"results": ENTRIES[:size],
                                 "nextPageToken": "more" if size < len(ENTRIES) else ""})

        class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True

        self.server = S(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"


needs_testing_build = pytest.mark.skipif(not hasattr(crab_r, "_set_test_origin"),
                                         reason="needs a testing build (maturin develop --features testing)")


@pytest.fixture
def catalog():
    s = _Catalog()
    crab_r._set_test_origin(s.url)
    yield s
    crab_r._set_test_origin(None)
    s.server.shutdown()


@pytest.fixture
def decoy(monkeypatch, tmp_path):
    """The instance metadata server's stand-in, counting connections; no
    gcloud credentials anywhere."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    s.listen(8)
    s.setblocking(False)
    monkeypatch.setenv("GCE_METADATA_HOST", f"127.0.0.1:{s.getsockname()[1]}")
    monkeypatch.setenv("CLOUDSDK_CONFIG", str(tmp_path / "no-gcloud"))
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    def hits():
        n = 0
        while True:
            try:
                s.accept()
                n += 1
            except BlockingIOError:
                return n
    yield hits
    s.close()


def _reason(*a, **kw):
    with pytest.raises(crab_r.SearchError) as e:
        crab_r.search_catalog(*a, **kw)
    return e.value


def test_surface():
    assert callable(crab_r.search_catalog) and callable(crab_r.search_catalog_async)
    assert issubclass(crab_r.SearchError, crab_r.CrabError)
    assert issubclass(crab_r.CatalogEntries, list)


def test_unusable_arguments_are_value_errors_before_any_credential(decoy):
    bad = [
        {"query": ""}, {"query": "   "}, {"query": "a\nb"}, {"query": "x" * 257},
        {"project": ""}, {"project": "Not-Lower"}, {"project": "p/../q"}, {"project": "p:searchEntries"},
        {"limit": 0}, {"limit": 101}, {"timeout_ms": 0},
    ]
    for kw in bad:
        with pytest.raises(ValueError):
            crab_r.search_catalog(**{"query": "orders", "project": "p", **kw})
    assert decoy() == 0, "the metadata server was asked"


def test_no_credential_is_typed_and_the_metadata_server_is_not_asked(decoy):
    assert _reason("orders", "p").reason == "no_credentials"
    assert decoy() == 0, "the metadata server was asked"


@needs_testing_build
def test_entries_are_dicts_with_only_the_listed_members(catalog):
    found = crab_r.search_catalog("monthly orders", "home-proj", access_token=TOKEN)
    assert isinstance(found, crab_r.CatalogEntries) and isinstance(found, list)
    assert not found.truncated and len(found) == 2
    first = found[0]
    assert first["name"] == "p1.mart.orders" and first["project"] == "p1"
    assert first["type"] == "bigquery-table" and first["system"] == "BIGQUERY" and first["location"] == "us"
    assert first["description"] == "d" * 300, "cut at 300 characters"
    assert found[1] == {"name": "p2.mart.v_orders", "project": "p2", "type": "bigquery-view",
                        "system": "BIGQUERY", "fqn": "bigquery:p2.mart.v_orders"}
    text = json.dumps(found)  # plain data
    assert "ASPECT-MARKER" not in text and "LABEL-MARKER" not in text

    (method, path, body, headers) = catalog.requests[0]
    url = urllib.parse.urlsplit(path)
    assert (method, url.path, body) == ("POST", "/v1/projects/home-proj/locations/global:searchEntries", b"")
    assert urllib.parse.parse_qs(url.query) == {
        "query": [f"monthly orders {SCOPE}"], "pageSize": ["20"], "orderBy": ["relevance"]}
    lowered = {k.lower(): v for k, v in headers.items()}
    assert lowered["authorization"] == f"Bearer {TOKEN}" and lowered["content-length"] == "0"
    assert TOKEN not in path


@needs_testing_build
def test_the_limit_is_the_page_and_truncation_is_visible(catalog):
    one = crab_r.search_catalog("orders", "p", limit=1, access_token=TOKEN)
    assert len(one) == 1 and one.truncated
    assert "pageSize=1&" in catalog.requests[0][1]
    assert len(catalog.requests) == 1, "one page, one request"


@needs_testing_build
def test_a_text_cannot_add_a_parameter(catalog):
    crab_r.search_catalog("a&pageSize=1000&scope=organizations/1", "p", limit=2, access_token=TOKEN)
    params = urllib.parse.parse_qs(urllib.parse.urlsplit(catalog.requests[0][1]).query)
    assert sorted(params) == ["orderBy", "pageSize", "query"] and params["pageSize"] == ["2"]
    assert params["query"] == [f"a&pageSize=1000&scope=organizations/1 {SCOPE}"]


@needs_testing_build
def test_every_failure_is_typed_and_holds_neither_token_nor_body(catalog):
    for word, reason in (("s400", "invalid_query"), ("s401", "expired"), ("s403", "no_access"),
                         ("s404", "not_found"), ("s500", "unreachable")):
        e = _reason(f"{word} x", "p", access_token=TOKEN)
        assert e.reason == reason, (word, e)
        assert "BODY-MARKER" not in str(e) and TOKEN not in str(e)
    e = _reason("echo x", "p", access_token=TOKEN)
    assert e.reason == "no_access" and TOKEN not in str(e)


@needs_testing_build
def test_the_awaitable_answers_the_same(catalog):
    async def go():
        return await crab_r.search_catalog_async("orders", "p", access_token=TOKEN)
    found = asyncio.run(go())
    assert isinstance(found, crab_r.CatalogEntries) and [e["name"] for e in found] == \
        ["p1.mart.orders", "p2.mart.v_orders"]

    async def fail():
        await crab_r.search_catalog_async("s403 x", "p", access_token=TOKEN)
    with pytest.raises(crab_r.SearchError) as e:
        asyncio.run(fail())
    assert e.value.reason == "no_access"
