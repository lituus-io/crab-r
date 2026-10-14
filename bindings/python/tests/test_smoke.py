# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
"""Offline checks of the module surface (no credentials, no network)."""

import json
import pathlib

import pytest

import crab_r

_EXPORT = pathlib.Path(__file__).resolve().parents[3] / "tests" / "data" / "static_lineage"


def test_version_and_surface():
    assert crab_r.__version__
    for name in ("describe", "table", "lookup", "upstream", "downstream", "due", "pull"):
        assert callable(getattr(crab_r, name))


def test_options_are_validated_before_any_call():
    with pytest.raises(ValueError, match="name a project"):
        crab_r.describe()
    with pytest.raises(ValueError, match="unknown argument"):
        crab_r.describe("p.d", no_such_option=1)
    with pytest.raises(ValueError, match="--upstream"):
        crab_r.describe("p.d.t", upstream="many")


def test_lookups_report_missing_stores(tmp_path):
    with pytest.raises(crab_r.CrabError):
        crab_r.lookup(str(tmp_path / "absent"), "p.d.t.c")
    with pytest.raises(crab_r.CrabError):
        crab_r.pull(str(tmp_path / "nothing"), str(tmp_path / "replica"))


def test_openai_options_are_checked_before_any_call():
    with pytest.raises(ValueError, match="goes with --openai-url"):
        crab_r.describe("p.d", openai_model="gpt-4o")
    with pytest.raises(ValueError, match="--model configures Vertex AI"):
        crab_r.describe("p.d", openai_url="https://h/v1", openai_model="gpt-4o", model="g")
    with pytest.raises(ValueError, match="--budget-usd cannot be enforced"):
        crab_r.describe("p.d", openai_url="https://h/v1", openai_model="local")
    with pytest.raises(ValueError, match="credentials in the URL"):
        crab_r.describe("p.d", openai_url="https://u:p@h/v1", openai_model="gpt-4o")


def test_a_missing_key_variable_is_named_before_any_network(monkeypatch):
    monkeypatch.delenv("CRAB_SMOKE_NO_KEY", raising=False)
    with pytest.raises(crab_r.CrabError, match="CRAB_SMOKE_NO_KEY is not set"):
        crab_r.describe(
            "p.d",
            openai_url="https://h/v1",
            openai_model="gpt-4o",
            openai_key_env="CRAB_SMOKE_NO_KEY",
            openai_stream=True,
            price_input_usd_per_mtok=1,
            price_output_usd_per_mtok=2,
        )


def test_the_lineage_and_name_functions_are_exposed():
    for name in ("ingest_lineage", "bq_id", "bq_path", "dotted"):
        assert callable(getattr(crab_r, name))


def test_every_spelling_of_a_name_is_one_name():
    for spelled in ("bq://p/d/t#Col", "bq://p/d/t/col", "p.d.t.COL"):
        assert crab_r.bq_id(spelled) == "bq://p/d/t#col"
        assert crab_r.bq_path(spelled) == "bq://p/d/t/col"
        assert crab_r.dotted(spelled) == "p.d.t.col"
    assert crab_r.bq_path("bq://p/d/t") == "bq://p/d/t"
    for bad in ("", "bq://p/d/t#a.b", "p.d.t`; DROP TABLE x", "bq://p/d/t/c#c"):
        with pytest.raises(ValueError) as e:
            crab_r.bq_id(bad)
        assert "DROP" not in str(e.value)


def _export():
    return (json.loads((_EXPORT / "nodes.json").read_text()),
            json.loads((_EXPORT / "edges.json").read_text()))


def test_a_lineage_export_ingests_once_and_answers_offline(tmp_path):
    store = str(tmp_path / "state")
    nodes, edges = _export()
    done = crab_r.ingest_lineage(store, nodes, edges)
    assert done["committed"] is True
    assert (done["created"], done["updated"], done["edges"]) == (14, 0, 20)
    assert done["skipped"] == {"defined_by": 4, "derives_from:bq-routine": 1, "routine": 1, "writes_to": 1}
    assert done["rejected"] == []
    again = crab_r.ingest_lineage(store, nodes, edges)
    assert again["committed"] is False
    assert "other/raw/events#amount" in crab_r.upstream(store, "bq://proj/mart/orders_v#amt")
    assert "proj/mart/orders_v" in crab_r.downstream(store, crab_r.dotted("bq://other/raw/events"))
    assert crab_r.lookup(store, "bq://proj/mart/orders_t/doubled") == crab_r.lookup(
        store, "proj.mart.orders_t.doubled")
    assert "[static]" in crab_r.table(store, "bq://proj/mart/orders_v")


def test_unusable_entries_are_refused_by_position(tmp_path):
    done = crab_r.ingest_lineage(
        str(tmp_path / "state"),
        [{"id": "bq://p/d/t`; DROP TABLE x", "kind": "table"}, {"id": "bq://p/d/ok", "kind": "view"}, 3],
        [{"relationship": "derives_from", "source_id": "bq://p/d/ok"}],
    )
    assert [(r["side"], r["index"]) for r in done["rejected"]] == [("node", 0), ("node", 2), ("edge", 0)]
    assert "DROP" not in json.dumps(done)
    assert done["created"] == 2
    with pytest.raises(crab_r.CrabError):
        crab_r.ingest_lineage(str(tmp_path / "x"), {"not": "a list"}, [])
    with pytest.raises(TypeError):
        crab_r.ingest_lineage(str(tmp_path / "y"), [object()], [])
