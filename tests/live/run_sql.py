#!/usr/bin/env python3
# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
"""Run a `-- job`-delimited SQL file against BigQuery, one query job per
block, with Application Default Credentials (via gcloud).

    run_sql.py FILE PROJECT DATASET [LOCATION]

Creates the dataset if absent. `${DS}` in the SQL is `PROJECT.DATASET`.
Every job is capped at 2 GiB billed.
"""

import json
import re
import subprocess
import sys
import urllib.error
import urllib.request


def token():
    return subprocess.check_output(
        ["gcloud", "auth", "application-default", "print-access-token"], stderr=subprocess.DEVNULL
    ).decode().strip()


def call(method, url, body=None):
    req = urllib.request.Request(
        url,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": "Bearer " + token(), "Content-Type": "application/json"},
    )
    try:
        return json.load(urllib.request.urlopen(req))
    except urllib.error.HTTPError as e:
        return {"error": json.loads(e.read() or b"{}").get("error", {"message": str(e)})}


def main():
    path, project, dataset = sys.argv[1:4]
    location = sys.argv[4] if len(sys.argv) > 4 else "US"
    api = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}"
    ds = call("POST", f"{api}/datasets", {
        "datasetReference": {"projectId": project, "datasetId": dataset},
        "location": location,
        "description": "crab-r live-test mock warehouse; safe to delete.",
        "labels": {"purpose": "crab-r-live-tests"},
    })
    if "error" in ds and ds["error"].get("code") != 409:
        sys.exit(f"dataset: {ds['error'].get('message')}")
    sql = open(path).read().replace("${DS}", f"{project}.{dataset}")
    blocks = [b.strip() for b in re.split(r"(?m)^-- job$", sql)[1:] if b.strip()]
    for i, block in enumerate(blocks, 1):
        r = call("POST", f"{api}/queries", {
            "query": block, "useLegacySql": False, "location": location,
            "timeoutMs": 120000, "maximumBytesBilled": str(2 << 30),
        })
        if "error" in r:
            sys.exit(f"job {i}: {r['error'].get('message')}")
        first = block.splitlines()[0][:70]
        print(f"job {i}/{len(blocks)} ok: {first}  ({r.get('totalBytesProcessed', '0')} bytes)")


if __name__ == "__main__":
    main()
