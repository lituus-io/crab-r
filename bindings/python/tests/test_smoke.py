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
