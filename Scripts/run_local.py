#!/usr/bin/env python3
"""
scripts/run_local.py

Local CLI runner for the JML pipeline.

WHY THIS EXISTS:
    The pipeline logic lives in run_pipeline() — pure Python with no Azure
    SDK dependency. This script wires it to the command line and handles the
    one thing the Azure Functions runtime normally does automatically: loading
    local.settings.json into the environment before the pipeline reads it.

USAGE:
    python scripts/run_local.py
    python scripts/run_local.py --csv Data/sample_joiners.csv --clean
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import sys
from pathlib import Path

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Functions.Joiner_http import run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s %(name)s — %(message)s",
)
# Azure SDK logs at INFO level are noisy — keep them quiet unless something breaks
logging.getLogger("azure").setLevel(logging.WARNING)


def load_local_settings(settings_path: str = "local.settings.json") -> None:
    """
    Inject local.settings.json values into os.environ before the pipeline runs.

    Azure Functions loads this file automatically at runtime. This function
    replicates that behaviour for plain Python execution so connection strings
    and config are available to the pipeline.

    Only sets keys not already in the environment — real env vars always win.
    Does nothing if the file is missing, so this is safe to call unconditionally.
    """
    path = Path(settings_path)
    if not path.exists():
        logging.getLogger(__name__).warning(
            f"local.settings.json not found at {path.resolve()}"
        )
        return
    with open(path, encoding="utf-8") as f:
        settings = json.load(f)
    values = settings.get("Values", {})
    for key, value in values.items():
        if key not in os.environ:
            os.environ[key] = str(value)


def check_validation_engine() -> bool:
    """
    Ping the validation engine before the pipeline runs and warn if unreachable.

    A missing validation engine does not abort the run — records will still be
    processed up to the validation step, then held. This gives the operator a
    clear heads-up rather than letting records fail silently mid-run.

    Returns True if the engine is reachable, False if not.
    """
    url = os.environ.get("JML_VALIDATION_ENGINE_URL", "")
    if not url:
        print("⚠  JML_VALIDATION_ENGINE_URL not set in local.settings.json")
        return False

    try:
        # We only need to know the host is listening — a POST with an empty
        # body is enough. The response code does not matter here.
        requests.post(url, json={}, timeout=3)
        return True
    except requests.ConnectionError:
        print()
        print("⚠  Validation engine is not running.")
        print(f"   Expected at: {url}")
        print()
        print("   Start it in a separate terminal with:")
        print("   cd Validation_engine && func start")
        print()
        print("   Pipeline will continue but all records will fail")
        print("   validation and be held until the engine is running.")
        print()
        return False
    except Exception:
        # Any other error — engine may still work, do not block the run
        return True


def clean_reports(output_dir: str) -> None:
    """
    Remove all JSON files from the reports directory before a fresh run.

    Called when --clean is passed. Prevents old reports from a previous run
    mixing with the current run's output in the per-record listing.
    """
    output_path = Path(output_dir)
    if not output_path.exists():
        return
    removed = 0
    for f in output_path.glob("*.json"):
        f.unlink()
        removed += 1
    if removed:
        print(f"  Cleaned {removed} report(s) from {output_dir}/")
        print()


def main() -> None:
    load_local_settings()

    parser = argparse.ArgumentParser(
        description="Run the JML pipeline locally."
    )
    parser.add_argument(
        "--csv",
        default="Data/sample_joiners.csv",
        help="Path to the HR CSV file",
    )
    parser.add_argument(
        "--lookup",
        default="Config/canonical_lookup.json",
        help="Path to canonical_lookup.json",
    )
    parser.add_argument(
        "--output",
        default="reports",
        help="Directory to write audit reports into",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clear existing reports before running",
    )
    args = parser.parse_args()

    print()
    print("=" * 60)
    print("  JML Engine — Local Run")
    print("=" * 60)
    print(f"  CSV:     {args.csv}")
    print(f"  Lookup:  {args.lookup}")
    print(f"  Reports: {args.output}")
    print("=" * 60)
    print()

    if args.clean:
        clean_reports(args.output)

    # Warn if the validation engine is unreachable before any records are processed
    check_validation_engine()

    result = run_pipeline(
        csv_path=      args.csv,
        lookup_path=   args.lookup,
        output_dir=    args.output,
        correlation_id="local-run",
    )

    print()
    print("=" * 60)
    print("  Run Complete")
    print("=" * 60)
    print(f"  Total processed : {result.total}")
    print(f"  Succeeded       : {result.succeeded}")
    print(f"  Held            : {result.held}")

    if result.errors:
        print(f"  Audit errors    : {len(result.errors)}")
        for err in result.errors:
            print(f"    ✗ {err}")

    print()
    print(f"  Reports written to: {args.output}/")
    print("=" * 60)
    print()

    output_path = Path(args.output)
    if output_path.exists():
        reports = sorted(output_path.glob("*.json"))
        if reports:
            print("  Written reports:")
            for r in reports:
                # Run summary files are shown separately below — skip them here
                if r.name.startswith("_run_summary"):
                    continue
                with r.open() as f:
                    data = json.load(f)
                status = "✓" if not data.get("hold_reasons") else "⚠"
                print(f"    {status} {r.name}")
                if data.get("hold_reasons"):
                    for reason in data["hold_reasons"]:
                        print(f"        → {reason}")

            # Run summary is shown once at the bottom, separate from per-record files
            summaries = sorted(output_path.glob("_run_summary_*.json"))
            if summaries:
                print()
                latest = summaries[-1]
                with latest.open() as f:
                    summary = json.load(f)
                print("  Run Summary:")
                print(f"  Total processed : {result.total}")
                print(f"  Succeeded       : {result.succeeded}")
                print(f"  Held            : {result.held}")
                print(f"  Failed          : {result.failed}")
                print()

    # Exit with code 1 if any records were held — makes the script CI-friendly
    # so a pipeline run with held records fails a build step rather than passing silently
    if result.held > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()