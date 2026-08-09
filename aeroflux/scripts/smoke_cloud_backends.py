"""Smoke test for the cloud storage backends — the gate before sync_cloud.py
ever touches a caller.

Round-trips DynamoDBStateRepository and S3LakeStore against the REAL table/
bucket (empty, disposable prefix/item — cleaned up at the end) using exactly
the same factories (`state_backend_from_env`, `lake_backend_from_env`) the
app and sync step use. If credentials, permissions, or region are wrong,
this fails here, loudly, not three steps later inside a live sync cycle.

Usage:
    AWS_PROFILE=aeroflux-local STATE_BACKEND=dynamodb LAKE_BACKEND=s3 \\
    S3_BUCKET=aeroflux-lake-411750981882-us-east-1-an AWS_REGION=us-east-1 \\
    python scripts/smoke_cloud_backends.py

Nothing here is a caller of the app's real data path — this only proves the
backends work, using a `smoke-test/` prefix and a throwaway flight_key so it
can never collide with (or get mistaken for) real gold/predictions data.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl

from aeroflux_ml import state_backend_from_env, lake_backend_from_env

FLIGHT_KEY = "SMOKE-TEST-0001"


def _require(var: str) -> str:
    val = os.getenv(var)
    if not val:
        print(f"FAIL: ${var} is not set — see the module docstring for required env vars.")
        sys.exit(1)
    return val


def smoke_dynamodb() -> None:
    print("=" * 70)
    print("DynamoDB — StateRepository (state + prediction, disjoint attrs)")
    print("=" * 70)
    table_name = os.getenv("DYNAMODB_TABLE", "aeroflux-current-state")
    region = os.getenv("AWS_REGION", "us-east-1")

    repo = state_backend_from_env()
    print(f"backend: {type(repo).__name__}  table={table_name}  region={region}")

    print(f"\n1) upsert_flight_state({FLIGHT_KEY}) ...")
    repo.upsert_flight_state({
        "flight_instance_id": FLIGHT_KEY,
        "callsign": "SMK001",
        "origin": "KDFW",
        "destination": "KATL",
        "flight_status": "ACTIVE",
    })
    print("   done.")

    print(f"\n2) upsert_prediction({FLIGHT_KEY}) — same item, disjoint attrs ...")
    repo.upsert_prediction({
        "flight_key": FLIGHT_KEY,
        "delay_probability": 0.42,
        "predicted_delayed": 0,
        "model_version": "smoke-test",
        "scored_at": "2026-08-08T00:00:00Z",
    })
    print("   done.")

    # Raw low-level get_item (not the resource API) so the wire-level type
    # tag (N vs S) is visible — this is the literal proof expires_at is a
    # DynamoDB Number, not a string TTL would silently ignore.
    import boto3
    raw = boto3.client("dynamodb", region_name=region).get_item(
        TableName=table_name, Key={"flight_key": {"S": FLIGHT_KEY}})
    item = raw.get("Item")
    if not item:
        print("\nFAIL: get_item found no item after upsert.")
        sys.exit(1)

    print("\n3) raw item (low-level client, shows wire types):")
    for k in sorted(item):
        print(f"   {k:20s} {item[k]}")

    expires = item.get("expires_at", {}).get("N")
    updated = item.get("updated_at", {}).get("S")
    state_present = all(k in item for k in ("origin", "destination", "flight_status"))
    pred_present = all(k in item for k in ("delay_probability", "model_version"))

    print("\n4) checks:")
    ok = True
    if expires and expires.isdigit() and 9 <= len(expires) <= 11:
        print(f"   PASS expires_at is a Number, {len(expires)} digits: {expires}")
    else:
        print(f"   FAIL expires_at is not a sane epoch-seconds Number: {item.get('expires_at')}")
        ok = False
    if updated:
        print(f"   PASS updated_at present (ISO string): {updated}")
    else:
        print("   FAIL updated_at missing")
        ok = False
    if state_present and pred_present:
        print("   PASS state attrs AND prediction attrs both present on the "
              "SAME item — disjoint upserts did not clobber each other")
    else:
        print(f"   FAIL missing group — state_present={state_present} pred_present={pred_present}")
        ok = False

    print("\n5) cleanup: deleting smoke-test item (best-effort) ...")
    try:
        boto3.client("dynamodb", region_name=region).delete_item(
            TableName=table_name, Key={"flight_key": {"S": FLIGHT_KEY}})
        print("   done.")
    except Exception as e:
        print(f"   WARN cleanup failed (not fatal — the item just outlives the test "
              f"until expires_at TTLs it out): {e}")

    if not ok:
        sys.exit(1)


def smoke_s3() -> None:
    print()
    print("=" * 70)
    print("S3 — LakeStore write/read/list round-trip")
    print("=" * 70)
    bucket = _require("S3_BUCKET")
    region = os.getenv("AWS_REGION", "us-east-1")
    key = "smoke-test/round_trip.parquet"

    lake = lake_backend_from_env()
    print(f"backend: {type(lake).__name__}  bucket={bucket}  region={region}")

    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    print(f"\n1) write_parquet(key={key!r}) ...")
    written = lake.write_parquet(df, key)
    print(f"   wrote: {written}")

    print("\n2) read_parquet(...) round-trip ...")
    back = lake.read_parquet(key)
    ok = back.equals(df)
    print(f"   {'PASS' if ok else 'FAIL'} read-back {'matches' if ok else 'DOES NOT MATCH'} what was written")
    if not ok:
        print(f"   wrote: {df}\n   read:  {back}")

    print("\n3) list('smoke-test') ...")
    listed = lake.list("smoke-test")
    print(f"   {listed}")
    if key in listed:
        print("   PASS written key appears in list()")
    else:
        print("   FAIL written key missing from list()")
        ok = False

    print("\n4) cleanup: deleting smoke-test object (best-effort) ...")
    import boto3
    try:
        boto3.client("s3", region_name=region).delete_object(Bucket=bucket, Key=key)
        print("   done.")
    except Exception as e:
        print(f"   WARN cleanup failed (not fatal — one small object left under "
              f"smoke-test/): {e}")

    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    print(f"AWS_PROFILE={os.getenv('AWS_PROFILE', '(unset — using env creds / instance role)')}")
    print(f"STATE_BACKEND={os.getenv('STATE_BACKEND', 'postgres')}  "
          f"LAKE_BACKEND={os.getenv('LAKE_BACKEND', 'local')}\n")
    if os.getenv("STATE_BACKEND") != "dynamodb":
        print("FAIL: set STATE_BACKEND=dynamodb to run this smoke test."); sys.exit(1)
    if os.getenv("LAKE_BACKEND") != "s3":
        print("FAIL: set LAKE_BACKEND=s3 to run this smoke test."); sys.exit(1)

    smoke_dynamodb()
    smoke_s3()

    print("\n" + "=" * 70)
    print("ALL SMOKE TESTS PASSED")
    print("=" * 70)
