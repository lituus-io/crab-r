# Author: support@lituus.io
# Copyright: lituus-io, all rights reserved
"""Offline checks of the module surface (no credentials, no network)."""

import pytest

import crab_r


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
