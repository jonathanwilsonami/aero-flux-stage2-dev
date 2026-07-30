#!/usr/bin/env python3
"""Resolution report -- proves how much of the identity chain closes on real
SWIM data.

Input: a JSONL of fused flight instances (produce it with
    run_parser.py <source> ... --fuse --sink jsonl --sink-path flights.jsonl)
each line having at least a "callsign".

It answers: for real SWIM flights, how many callsigns parse, resolve to a known
airline, already carry a tail (GA), or would need an ADS-B lookup -- and it
demonstrates the passenger-facing direction (flight number -> SWIM callsign).

    python resolve_report.py flights.jsonl
    python resolve_report.py flights.jsonl --live 5   # try live ADS-B lookups
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter

from aeroflux_parser import (
    parse_callsign, callsign_to_flight_number, resolve_flight_number,
)


def load_callsigns(path: str) -> list[str]:
    out = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            cs = rec.get("callsign")
            if cs:
                out.append(cs)
    return out


def report(callsigns: list[str], live: int) -> None:
    total = len(callsigns)
    unique = sorted(set(callsigns))
    buckets = Counter()
    airline_needs_adsb: list[str] = []
    ga_has_tail: list[str] = []
    unresolved: list[str] = []
    flightno_ok = 0

    for cs in unique:
        p = parse_callsign(cs)
        if p.is_registration:
            buckets["ga_tail_from_callsign"] += 1
            ga_has_tail.append(cs)
        elif p.resolved:
            buckets["airline_resolved"] += 1
            airline_needs_adsb.append(cs)
            if callsign_to_flight_number(cs):
                flightno_ok += 1
        elif p.flight_number:
            buckets["split_but_unknown_airline"] += 1
            unresolved.append(cs)
        else:
            buckets["unparseable"] += 1
            unresolved.append(cs)

    print("=" * 64)
    print(f"{total} flight instance(s); {len(unique)} unique callsign(s)")
    print("-" * 64)
    for label in ("airline_resolved", "ga_tail_from_callsign",
                  "split_but_unknown_airline", "unparseable"):
        n = buckets.get(label, 0)
        pct = 100 * n / len(unique) if unique else 0
        print(f"  {label:28} {n:5}  ({pct:4.1f}%)")
    print("-" * 64)
    print("What the identity chain looks like on this data:")
    print(f"  * {buckets.get('airline_resolved', 0)} airline flights -> callsign parses to a")
    print(f"    known carrier; passenger flight number reconstructed for {flightno_ok} of them.")
    print(f"    Tail number for these must come from ADS-B (not in SWIM).")
    print(f"  * {buckets.get('ga_tail_from_callsign', 0)} GA flights -> tail number IS the callsign, already resolved.")
    print("=" * 64)

    print("\nSample airline resolutions (SWIM callsign -> passenger flight no.):")
    for cs in airline_needs_adsb[:8]:
        p = parse_callsign(cs)
        fn = callsign_to_flight_number(cs) or "?"
        print(f"  {cs:10} -> {fn:8} ({p.airline_name})")

    if ga_has_tail:
        print("\nGA flights whose tail is already known (callsign == registration):")
        for cs in ga_has_tail[:8]:
            print(f"  {cs}")

    if unresolved:
        print(f"\n{len(unresolved)} unresolved callsign(s) (sample):", ", ".join(unresolved[:12]))

    # Passenger-facing direction: flight number -> SWIM callsign to search for.
    print("\nForward direction (what a user would type -> SWIM callsign):")
    for user in ("AA2033", "WN5103", "DL100", "UA1690"):
        r = resolve_flight_number(user)
        cand = r.callsign_candidates[0] if r.callsign_candidates else "?"
        print(f"  {user:8} -> {cand:10} ({r.airline_name or 'unknown'})")

    if live:
        _try_live(airline_needs_adsb[:live])


def _try_live(callsigns: list[str]) -> None:
    from aeroflux_parser import AdsbClient
    print(f"\nLive ADS-B tail resolution (airplanes.live), {len(callsigns)} callsign(s):")
    client = AdsbClient()
    for cs in callsigns:
        frame = client.resolve_tail(cs)
        if frame:
            print(f"  {cs:10} -> tail {frame.registration}, hex {frame.hex}, type {frame.aircraft_type}")
        else:
            print(f"  {cs:10} -> not currently visible on the network (coverage/not airborne)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("jsonl", help="fused flight-instance JSONL (from run_parser --sink jsonl)")
    ap.add_argument("--live", type=int, default=0,
                    help="attempt N live ADS-B tail lookups (needs network)")
    args = ap.parse_args()

    callsigns = load_callsigns(args.jsonl)
    if not callsigns:
        sys.exit("No callsigns found in the input.")
    report(callsigns, args.live)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
