#!/usr/bin/env python3
# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
"""Enforce the crab-r dependency policy.

Every workspace member may name only graph-r, link-r, objstore-r and other
crab-* crates under [dependencies] / [build-dependencies] (including
target-specific tables). Dev tooling is allowed only under [dev-dependencies]
and in the separate fuzz crate. Exits non-zero listing every violation.
"""

import pathlib
import sys
import tomllib

ALLOWED = {"graph-r", "link-r", "objstore-r"}
# tokio: test runtime only, required because reqwest (inside objstore-r) needs
# a tokio reactor. The library itself is executor-agnostic.
DEV_ALLOWED = ALLOWED | {"proptest", "criterion", "libfuzzer-sys", "tokio"}
ROOT = pathlib.Path(__file__).resolve().parent.parent


def dep_tables(manifest):
    """Yield (table_name, deps) for every dependency table in a manifest."""
    for name in ("dependencies", "build-dependencies", "dev-dependencies"):
        if name in manifest:
            yield name, manifest[name]
    for target, body in manifest.get("target", {}).items():
        for name in ("dependencies", "build-dependencies", "dev-dependencies"):
            if name in body:
                yield f"target.{target}.{name}", body[name]


def allowed(table, dep):
    if dep.startswith("crab-"):
        return True
    return dep in (DEV_ALLOWED if table.endswith("dev-dependencies") else ALLOWED)


def main():
    root = tomllib.loads((ROOT / "Cargo.toml").read_text())
    violations = []
    for dep in root.get("workspace", {}).get("dependencies", {}):
        if not (dep.startswith("crab-") or dep in DEV_ALLOWED):
            violations.append(f"Cargo.toml [workspace.dependencies]: {dep}")
    for member in root["workspace"]["members"]:
        path = ROOT / member / "Cargo.toml"
        manifest = tomllib.loads(path.read_text())
        for table, deps in dep_tables(manifest):
            for dep in deps:
                if not allowed(table, dep):
                    violations.append(f"{path.relative_to(ROOT)} [{table}]: {dep}")
    if violations:
        print("dependency policy violations:", *violations, sep="\n  ")
        return 1
    print("dependency policy: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
