# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
"""The 0.5 surface: table_meta, describe_offline, their awaitables, the shared
runtime, token budgets, and the credential rules. No real Google service is
contacted: a local stand-in plays the model gateway, and a decoy plays the
instance metadata server."""

import asyncio
import http.server
import json
import pathlib
import select
import socket
import socketserver
import sys
import threading
import time

import pytest

import crab_r

_EXPORT = pathlib.Path(__file__).resolve().parents[3] / "tests" / "data" / "static_lineage"

CATALOG = [
    {"table": "lab.stg.orders", "available": True, "reason": "ok", "type": "VIEW",
     "schema": {"fields": [{"name": "order_id", "type": "INTEGER"}, {"name": "order_date", "type": "DATE"}]},
     "view_sql": "SELECT order_id, DATE(ts) AS order_date FROM lab.raw.orders"},
]
DRAFT = json.dumps({"table_description": "One row per order.", "grain": ["order_id"], "table_checks": [],
                    "columns": [{"column": "order_id", "description": "Order key.", "checks": []},
                                {"column": "order_date", "description": "Order date.", "checks": []}]})
JUDGE = json.dumps({"verdicts": [], "columns": [
    {"column": "order_id", "description": "Order key.", "confidence": "medium"},
    {"column": "order_date", "description": "Order date.", "confidence": "medium"}],
    "table_description": "One row per order.", "more_checks": []})


class _Gateway:
    """A chat-completions stand-in: each answer after `delay` seconds; records
    whether a client hung up before its answer."""

    def __init__(self, delay=0.0):
        self.delay, self.hits, self.aborted = delay, 0, 0
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                n = int(self.headers.get("content-length", 0))
                self.rfile.read(n)
                outer.hits += 1
                deadline = time.monotonic() + outer.delay
                while time.monotonic() < deadline:
                    r, _, _ = select.select([self.connection], [], [], 0.05)
                    if r and not self.connection.recv(1, socket.MSG_PEEK):
                        outer.aborted += 1
                        return
                reply = DRAFT if outer.hits % 2 == 1 else JUDGE
                body = json.dumps({"choices": [{"index": 0, "message": {"role": "assistant", "content": reply},
                                                "finish_reason": "stop"}],
                                   "usage": {"prompt_tokens": 10, "completion_tokens": 5}}).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True

        self.server = S(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/v1"


def _model(gw, monkeypatch):
    monkeypatch.setenv("CRAB_PY_TEST_KEY", "sk-test")
    return dict(openai_url=gw.url, openai_model="m", openai_key_env="CRAB_PY_TEST_KEY",
                price_input_usd_per_mtok=1, price_output_usd_per_mtok=1)


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


def test_surface():
    for name in ("table_meta", "table_meta_async", "describe_offline", "describe_offline_async",
                 "jobs_projection_sql", "search"):
        assert callable(getattr(crab_r, name)), name


def test_a_batch_never_raises_and_every_table_is_typed(decoy):
    got = crab_r.table_meta(["p.d.t", "q.e.u"])
    assert [a["reason"] for a in got] == ["no_credentials", "no_credentials"]
    assert all(a["available"] is False and a["table"] for a in got)
    bad = crab_r.table_meta(["not a name", "p.d"], access_token="ya29.token")
    assert [a["reason"] for a in bad] == ["invalid_name", "invalid_name"]
    assert "ya29" not in json.dumps(bad)
    assert decoy() == 0, "the metadata server was asked"


def test_the_awaitable_answers_the_same(decoy):
    async def go():
        return await crab_r.table_meta_async(["bad name", "p.d.t"])

    got = asyncio.run(go())
    assert [a["reason"] for a in got] == ["invalid_name", "no_credentials"]
    assert decoy() == 0


def test_one_runtime_for_every_call():
    first = crab_r._runtime_id()
    crab_r.table_meta(["x"], access_token="t")
    async def go():
        return await asyncio.gather(*(crab_r.table_meta_async(["x"], access_token="t") for _ in range(8)))

    assert len(asyncio.run(go())) == 8
    assert crab_r._runtime_id() == first


def test_jobs_projection_is_the_view_text():
    sql = crab_r.jobs_projection_sql("my-proj", "us", 30)
    assert "`my-proj`.`region-us`.INFORMATION_SCHEMA.JOBS" in sql and "job_key" in sql
    with pytest.raises(ValueError):
        crab_r.jobs_projection_sql("p`; DROP", "us")


def test_lookups_fit_a_token_budget(tmp_path):
    store = str(tmp_path / "s")
    nodes = json.loads((_EXPORT / "nodes.json").read_text())
    edges = json.loads((_EXPORT / "edges.json").read_text())
    crab_r.ingest_lineage(store, nodes, edges)
    full = crab_r.upstream(store, "proj.mart.orders_t.doubled")
    cut = crab_r.upstream(store, "proj.mart.orders_t.doubled", budget_tokens=8)
    assert len(cut) < len(full) and cut.splitlines()[-1].startswith("truncated")
    assert "orders" in crab_r.search(store, "orders", budget_tokens=500)


def test_describe_offline_drafts_through_the_model_only(tmp_path, monkeypatch, decoy):
    gw = _Gateway()
    store = str(tmp_path / "state")
    report = crab_r.describe_offline(store, CATALOG, **_model(gw, monkeypatch))
    cols = {c["column"]: c for t in report["tables"] for c in t["columns"]}
    assert cols["order_id"]["description"] == "Order key."
    assert gw.hits >= 1 and decoy() == 0
    # Recorded in the store: lookups answer from it.
    assert "Order key." in crab_r.lookup(store, "lab.stg.orders.order_id")
    with pytest.raises(crab_r.CrabError, match="Google host"):
        crab_r.describe_offline(None, CATALOG, **{**_model(gw, monkeypatch),
                                                   "openai_url": "https://aiplatform.googleapis.com/v1"})


def test_cancelling_the_awaitable_cancels_the_request(monkeypatch):
    gw = _Gateway(delay=5.0)

    async def go():
        task = asyncio.ensure_future(crab_r.describe_offline_async(None, CATALOG, **_model(gw, monkeypatch)))
        while gw.hits == 0:
            await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    start = time.monotonic()
    asyncio.run(go())
    deadline = time.monotonic() + 3
    while gw.aborted == 0 and time.monotonic() < deadline:
        time.sleep(0.02)
    assert gw.aborted == 1, "the request was not dropped"
    assert time.monotonic() - start < 4


def test_the_interpreter_runs_while_rust_works(monkeypatch):
    gw = _Gateway(delay=0.5)
    done = threading.Event()
    ticks = 0

    def call():
        crab_r.describe_offline(None, CATALOG, **_model(gw, monkeypatch))
        done.set()

    t = threading.Thread(target=call)
    t.start()
    while not done.is_set():
        ticks += 1
    t.join()
    assert ticks > 100_000, f"the GIL was held: {ticks} ticks"


def test_memory_is_flat_over_ten_thousand_calls(tmp_path):
    resource = pytest.importorskip("resource")
    store = str(tmp_path / "s")
    crab_r.ingest_lineage(store, json.loads((_EXPORT / "nodes.json").read_text()),
                          json.loads((_EXPORT / "edges.json").read_text()))

    def rss():
        r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return r if sys.platform == "darwin" else r * 1024

    for _ in range(1_000):
        crab_r.table_meta(["bad name"], access_token="t")
        crab_r.lookup(store, "proj.mart.orders_t.doubled")
    before = rss()
    for _ in range(10_000):
        crab_r.table_meta(["bad name"], access_token="t")
        crab_r.lookup(store, "proj.mart.orders_t.doubled")
    assert rss() - before < 16 << 20, f"grew {(rss() - before) >> 20} MiB"


def test_batch_latency_p95():
    names = [f"bad name {i}" for i in range(20)]
    times = []
    for _ in range(200):
        t = time.perf_counter()
        crab_r.table_meta(names, access_token="t")
        times.append(time.perf_counter() - t)
    times.sort()
    assert times[int(len(times) * 0.95)] < 0.005, times[-5:]
