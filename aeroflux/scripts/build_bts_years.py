#!/usr/bin/env python3

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import polars as pl


START_YEAR = 2015
END_YEAR = 2025  # Inclusive: 2015–2025 is 11 years

OUTPUT_ROOT = Path("./bts_out")
CACHE_DIR = Path("data/bts")
WEATHER_CACHE_DIR = Path("data/weather")
STATION_BRIDGE = Path("data/reference/airport_to_station_2019.csv")

COMBINED_OUTPUT = OUTPUT_ROOT / "bts_2015_2025.parquet"


def build_year(year: int) -> None:
    """Build one year and wait for it to finish before continuing."""
    yearly_output = OUTPUT_ROOT / f"bts_{year}"

    command = [
        sys.executable,
        "scripts/build_bts_gold.py",
        "--months",
        f"{year}-01:{year}-12",
        "--out",
        str(yearly_output),
        "--cache",
        str(CACHE_DIR),
        "--weather-cache",
        str(WEATHER_CACHE_DIR),
        "--station-bridge",
        str(STATION_BRIDGE),
    ]

    print(f"\n{'=' * 70}")
    print(f"Building {year}")
    print(f"Output: {yearly_output}")
    print(f"{'=' * 70}")

    # subprocess.run is blocking: the next year will not start until this
    # command finishes successfully.
    subprocess.run(command, check=True)

    print(f"Finished {year}")


def find_yearly_parquet_files() -> list[Path]:
    """Find all generated yearly Parquet files."""
    parquet_files: list[Path] = []

    for year in range(START_YEAR, END_YEAR + 1):
        yearly_directory = OUTPUT_ROOT / f"bts_{year}"
        yearly_files = sorted(yearly_directory.rglob("*.parquet"))

        if not yearly_files:
            raise FileNotFoundError(
                f"No Parquet files found for {year} under "
                f"{yearly_directory.resolve()}"
            )

        parquet_files.extend(yearly_files)

    return parquet_files


def combine_parquet_files() -> None:
    """Combine all yearly Parquet outputs into one file with Polars."""
    parquet_files = find_yearly_parquet_files()

    print(f"\nCombining {len(parquet_files)} Parquet files")
    print(f"Combined output: {COMBINED_OUTPUT}")

    yearly_scans = [
        pl.scan_parquet(file, missing_columns="insert")
        for file in parquet_files
    ]

    combined = pl.concat(
        yearly_scans,
        how="diagonal_relaxed",
    )

    # sink_parquet evaluates the lazy query in streaming batches rather than
    # loading the entire combined dataset into memory at once.
    combined.sink_parquet(
        COMBINED_OUTPUT,
        compression="zstd",
        mkdir=True,
    )

    print(f"Created: {COMBINED_OUTPUT.resolve()}")


def main() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    for year in range(START_YEAR, END_YEAR + 1):
        build_year(year)

    combine_parquet_files()

    print("\nAll yearly builds and the final combination completed successfully.")


if __name__ == "__main__":
    main()