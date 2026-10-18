# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
"""The 0.5.1 surface: query and query_async. The guard refuses before any
credential or byte; rows are dicts of plain scalars in a QueryRows list with
an explicit `truncated`; every failure is a QueryError with a `.reason`; the
GIL is released and the shared runtime reused; cancelling the awaitable
hangs up the request; the metadata server is never asked; the token never
appears in an error. A local stub plays BigQuery (testing builds), a decoy
the instance metadata server."""

import asyncio
import http.server
import json
import select
import socket
import socketserver
import sys
import threading
import time

import pytest

import crab_r

TOKEN = "ya29.python-query-test-token"
OK_SQL = "SELECT * FROM `p.ops.resource_health`"
FORBIDDEN = [
    "SELECT * FROM p.mart.orders",
    "SELECT * FROM ops.resource_health, secret.t",
    "SELECT 1; SELECT * FROM p.d.t",
    "DELETE FROM ops.resource_health WHERE TRUE",
    "SELECT * FROM EXTERNAL_QUERY('c', 'SELECT 1')",
]

SCHEMA = {"fields": [
    {"name": "resource", "type": "STRING"}, {"name": "failures", "type": "INTEGER"},
    {"name": "ratio", "type": "FLOAT"}, {"name": "healthy", "type": "BOOLEAN"},
    {"name": "last_run", "type": "TIMESTAMP"}, {"name": "note", "type": "STRING"},
]}


def _row(i):
    return {"f": [{"v": f"res-{i}"}, {"v": str(i)}, {"v": "0.25"}, {"v": "false"},
                  {"v": "1700000000123456"}, {"v": None}]}


class _BigQuery:
    """A jobs.query stand-in: a marker literal in the SQL picks the answer;
    records every request; `slow` waits for a hang-up."""

    def __init__(self, n_rows=3, delay=0.0):
        self.n_rows, self.delay = n_rows, delay
        self.requests, self.aborted = [], 0
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
                outer.requests.append(("GET", self.path, None, self.headers.get("authorization")))
                self._send(404, {"error": {"code": 404, "message": "no such job"}})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))))
                auth = self.headers.get("authorization", "")
                outer.requests.append(("POST", self.path, body, auth))
                sql = body["query"]
                if "'slow'" in sql:
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        r, _, _ = select.select([self.connection], [], [], 0.05)
                        if r and not self.connection.recv(1, socket.MSG_PEEK):
                            outer.aborted += 1
                            return
                if outer.delay:
                    time.sleep(outer.delay)
                if "'echo'" in sql:
                    return self._send(403, {"error": {"code": 403, "message": f"denied for {auth}",
                                                      "errors": [{"reason": "accessDenied"}]}})
                if "'e404'" in sql:
                    return self._send(404, {"error": {"code": 404, "message": "Not found: Table p:ops.x",
                                                      "errors": [{"reason": "notFound"}]}})
                if "'ebytes'" in sql:
                    return self._send(400, {"error": {"code": 400, "message": "too many bytes",
                                                      "errors": [{"reason": "bytesBilledLimitExceeded"}]}})
                want = min(outer.n_rows, int(body.get("maxResults", outer.n_rows)))
                self._send(200, {"jobComplete": True, "schema": SCHEMA, "rows": [_row(i) for i in range(want)],
                                 "totalRows": str(outer.n_rows), "totalBytesProcessed": "2048",
                                 "totalBytesBilled": "10485760", "cacheHit": False})

        class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True

        self.server = S(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def queries(self):
        return [r for r in self.requests if r[0] == "POST"]


needs_testing_build = pytest.mark.skipif(not hasattr(crab_r, "_set_test_origin"),
                                   reason="needs a testing build (maturin develop --features testing)")


@pytest.fixture
def bq():
    stubs = []

    def make(**kw):
        s = _BigQuery(**kw)
        crab_r._set_test_origin(s.url)
        stubs.append(s)
        return s
    yield make
    crab_r._set_test_origin(None)
    for s in stubs:
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


def _reason(**kw):
    with pytest.raises(crab_r.QueryError) as e:
        crab_r.query(**kw)
    return e.value


def test_surface():
    assert callable(crab_r.query) and callable(crab_r.query_async)
    assert issubclass(crab_r.QueryError, crab_r.CrabError)
    assert issubclass(crab_r.QueryRows, list)


def test_the_guard_refuses_before_any_credential_or_byte(decoy):
    for sql in FORBIDDEN:
        e = _reason(sql=sql, project="p", access_token=TOKEN)
        assert e.reason == "refused_by_guard", (sql, e)
        # No credential at all: still the guard's answer, nothing discovered.
        assert _reason(sql=sql, project="p").reason == "refused_by_guard"
    assert _reason(sql=OK_SQL, project="p").reason == "no_credentials"
    assert decoy() == 0, "the metadata server was asked"
    # A project-scoped graph dataset refuses the same dataset elsewhere.
    e = _reason(sql="SELECT * FROM other.ops.t", project="p", access_token=TOKEN, graph_datasets=["graph-p.ops"])
    assert e.reason == "refused_by_guard"


def test_bad_arguments_are_value_errors():
    for kw in ({"project": ""}, {"max_rows": 0}, {"timeout_ms": 0}, {"graph_datasets": [""]}):
        with pytest.raises(ValueError):
            crab_r.query(OK_SQL, **{"project": "p", **kw})


@needs_testing_build
def test_rows_are_dicts_of_plain_scalars(bq):
    stub = bq()
    rows = crab_r.query(OK_SQL, "p", access_token=TOKEN)
    assert isinstance(rows, crab_r.QueryRows) and isinstance(rows, list)
    assert rows[1] == {"resource": "res-1", "failures": 1, "ratio": 0.25, "healthy": False,
                       "last_run": "2023-11-14T22:13:20.123456Z", "note": None}
    assert type(rows[1]["failures"]) is int and type(rows[1]["ratio"]) is float
    assert (rows.truncated, rows.total_rows, rows.bytes_processed, rows.bytes_billed, rows.cache_hit) == \
        (False, 3, 2048, 10485760, False)
    assert json.dumps(rows)  # plain data
    (_, path, body, auth) = stub.requests[0]
    assert path == "/bigquery/v2/projects/p/queries" and auth == f"Bearer {TOKEN}"
    assert body["maximumBytesBilled"] == str(1 << 30) and "location" not in body
    crab_r.query(OK_SQL, "p", location="EU", access_token=TOKEN, max_bytes_billed=5 << 20)
    assert stub.requests[1][2]["location"] == "EU" and stub.requests[1][2]["maximumBytesBilled"] == str(5 << 20)


@needs_testing_build
def test_row_cap_and_budget_truncate_visibly(bq):
    stub = bq(n_rows=50)
    capped = crab_r.query(OK_SQL, "p", access_token=TOKEN, max_rows=4)
    assert len(capped) == 4 and capped.truncated and capped.total_rows == 50
    # Counted, not timed: one request, asking for exactly the cap.
    assert len(stub.requests) == 1 and stub.requests[0][2]["maxResults"] == 4
    full = crab_r.query(OK_SQL, "p", access_token=TOKEN, max_rows=50)
    assert len(full) == 50 and not full.truncated
    cut = crab_r.query(OK_SQL, "p", access_token=TOKEN, max_rows=50, budget_tokens=100)
    assert 0 < len(cut) < 50 and cut.truncated and list(cut) == list(full)[:len(cut)]
    spent = sum((len(json.dumps(r, separators=(",", ":"))) + 3) // 4 for r in cut)
    assert spent <= 100, spent


@needs_testing_build
def test_a_forbidden_table_never_reaches_bigquery(bq):
    stub = bq()
    for sql in FORBIDDEN:
        assert _reason(sql=sql, project="p", access_token=TOKEN).reason == "refused_by_guard"
    assert stub.requests == []
    crab_r.query(OK_SQL, "p", access_token=TOKEN)
    assert len(stub.requests) == 1


@needs_testing_build
def test_failures_are_typed_and_never_hold_the_token(bq, decoy):
    bq()
    for marker, reason in (("echo", "no_access"), ("e404", "not_found"), ("ebytes", "bytes_limit")):
        e = _reason(sql=f"SELECT '{marker}' FROM ops.t", project="p", access_token=TOKEN)
        assert e.reason == reason
        for shown in (str(e), repr(e), repr(e.args)):
            assert TOKEN not in shown, shown
    assert decoy() == 0


_SLOW_STUB = """
import http.server, json, sys, time
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_POST(self):
        self.rfile.read(int(self.headers.get("content-length", 0)))
        time.sleep(0.5)
        body = json.dumps({"jobComplete": True, "schema": {"fields": [{"name": "n", "type": "INTEGER"}]},
                           "rows": [{"f": [{"v": "1"}]}], "totalRows": "1"}).encode()
        self.send_response(200)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
s = http.server.HTTPServer(("127.0.0.1", 0), H)
print(s.server_address[1], flush=True)
s.serve_forever()
"""


@needs_testing_build
def test_the_interpreter_runs_while_rust_waits():
    # The stub is another process: were the GIL held, this test would fail
    # on its tick count, not deadlock against an in-process server.
    import subprocess
    proc = subprocess.Popen([sys.executable, "-c", _SLOW_STUB], stdout=subprocess.PIPE, text=True)
    try:
        crab_r._set_test_origin(f"http://127.0.0.1:{proc.stdout.readline().strip()}")
        done = threading.Event()
        ticks = 0

        def call():
            crab_r.query(OK_SQL, "p", access_token=TOKEN)
            done.set()

        t = threading.Thread(target=call)
        t.start()
        while not done.is_set():
            ticks += 1
        t.join()
        assert ticks > 100_000, f"the GIL was held: {ticks} ticks"
    finally:
        crab_r._set_test_origin(None)
        proc.kill()
        proc.wait()


@needs_testing_build
def test_one_runtime_and_awaitables(bq):
    stub = bq()
    first = crab_r._runtime_id()
    crab_r.query(OK_SQL, "p", access_token=TOKEN)

    async def go():
        return await asyncio.gather(*(crab_r.query_async(OK_SQL, "p", access_token=TOKEN) for _ in range(8)))

    got = asyncio.run(go())
    assert [len(r) for r in got] == [3] * 8 and all(isinstance(r, crab_r.QueryRows) for r in got)
    assert crab_r._runtime_id() == first
    assert len(stub.requests) == 9

    async def refused():
        await crab_r.query_async("SELECT * FROM p.mart.orders", "p", access_token=TOKEN)

    with pytest.raises(crab_r.QueryError) as e:
        asyncio.run(refused())
    assert e.value.reason == "refused_by_guard" and len(stub.requests) == 9


@needs_testing_build
def test_cancelling_the_awaitable_hangs_up_the_request(bq):
    stub = bq()

    async def go():
        task = asyncio.ensure_future(crab_r.query_async("SELECT 'slow' FROM ops.t", "p", access_token=TOKEN))
        while not stub.requests:
            await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    start = time.monotonic()
    asyncio.run(go())
    deadline = time.monotonic() + 3
    while stub.aborted == 0 and time.monotonic() < deadline:
        time.sleep(0.02)
    assert stub.aborted == 1, "the request was not dropped"
    assert time.monotonic() - start < 4
