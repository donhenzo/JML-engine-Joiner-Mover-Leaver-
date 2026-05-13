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
    # CSV mode (existing, unchanged)
    python scripts/run_local.py --csv Data/sample_joiners.csv --clean

    # API mode — single employee by employee number, UPN, or BambooHR ID
    python scripts/run_local.py --source api --id Acc003

    # API mode — multiple employees
    python scripts/run_local.py --source api --id Acc003,Acc004,Acc005,Acc006,Acc007,Acc008

    # API mode — delta poll (process all changes since last checkpoint)
    python scripts/run_local.py --source api --mode delta
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
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


def _format_time_ago(timestamp_str: str) -> str:
    """
    Convert an ISO timestamp to a human-readable 'X hours ago' string.
    Shows the operator when the last poll ran in terms they can understand.
    """
    try:
        ts = datetime.fromisoformat(timestamp_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - ts
        hours = delta.total_seconds() / 3600

        if hours < 1:
            minutes = int(delta.total_seconds() / 60)
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        elif hours < 24:
            h = int(hours)
            return f"{h} hour{'s' if h != 1 else ''} ago"
        else:
            days = int(hours / 24)
            return f"{days} day{'s' if days != 1 else ''} ago"
    except (ValueError, TypeError):
        return "unknown"


def run_api_mode(args) -> int:
    """
    Run the pipeline in API mode — fetch from BambooHR, derive actions,
    and process through the pipeline.

    Returns exit code: 0 for clean run, 1 if any records were held/failed.
    """
    from Provisioning.graph_client import build_graph_client, JmlGraphClient
    from Ingestion.hr_api.pipeline_adapter import PipelineContext
    from Ingestion.hr_api.ingestion_coordinator import run_single, run_delta
    from Ingestion.hr_api.system_state import (
        get_system_state_table_client,
        get_poll_checkpoint,
    )

    conn_str = os.environ.get("AzureWebJobsStorage", "")
    if not conn_str:
        print("✗  AzureWebJobsStorage not set — cannot connect to Azure Table Storage.")
        return 1

    # Build Graph client once for the entire run
    try:
        graph_service, credential = build_graph_client()
        graph_client = JmlGraphClient(graph_service, credential)
    except Exception as e:
        print(f"✗  Failed to build Graph client: {e}")
        return 1

    # Build pipeline context — shared across all records
    ctx = PipelineContext(
        graph_client=graph_client,
        connection_string=conn_str,
        output_dir=args.output,
        correlation_id="api-run",
    )

    if args.mode == "delta":
        # Delta poll mode — process changes since last checkpoint
        state_client = get_system_state_table_client(conn_str)
        checkpoint = get_poll_checkpoint(state_client)
        time_ago = _format_time_ago(checkpoint.last_successful_poll)

        print(f"  Mode:    Delta poll")
        print(f"  Since:   {checkpoint.last_successful_poll}")
        print(f"           ({time_ago})")
        if checkpoint.last_run_status:
            print(f"  Last:    {checkpoint.last_run_status} ({checkpoint.records_processed} records)")
        print("=" * 60)
        print()

        result = run_delta(ctx, state_client)

        print()
        print("=" * 60)
        print("  Delta Poll Complete")
        print("=" * 60)
        print(f"  Fetched from HR  : {result.total_fetched}")
        print(f"  Joiners          : {result.joiner_count}")
        print(f"  Movers           : {result.mover_count}")
        print(f"  Skipped          : {result.skipped_count}")
        print(f"  Failed           : {result.failed_count}")

        if result.errors:
            print()
            for err in result.errors:
                print(f"    ✗ {err}")

        print("=" * 60)
        print()

        return 1 if result.failed_count > 0 else 0

    else:
        # Single/batch mode — process specific employee IDs
        if not args.id:
            print("✗  --id is required in single mode. Provide employee number(s), UPN(s), or BambooHR ID(s).")
            print("   Example: --id Acc003,Acc004,Acc005")
            return 1

        identifiers = [x.strip() for x in args.id.split(",") if x.strip()]

        print(f"  Mode:    Single/batch")
        print(f"  IDs:     {', '.join(identifiers)}")
        print(f"  Count:   {len(identifiers)}")
        print("=" * 60)
        print()

        succeeded = 0
        held = 0
        failed = 0

        for identifier in identifiers:
            print(f"  ▸ Processing: {identifier}")
            result = run_single(identifier, ctx)

            if result is None:
                print(f"    ✗ Fetch failed or skipped")
                failed += 1
            elif not result.get("_pipeline_success", False):
                emp_id = result.get("employee_id", "")
                upn = result.get("upn", "")
                action = result.get("action", "")
                print(f"    ⚠ {emp_id} ({upn}) — {action} — held or rejected")
                failed += 1
            else:
                action = result.get("action", "")
                emp_id = result.get("employee_id", "")
                upn = result.get("upn", "")
                print(f"    ✓ {emp_id} ({upn}) — {action}")
                succeeded += 1

            print()

        total = succeeded + held + failed

        print("=" * 60)
        print("  Batch Complete")
        print("=" * 60)
        print(f"  Total processed : {total}")
        print(f"  Succeeded       : {succeeded}")
        print(f"  Failed/Skipped  : {failed}")
        print("=" * 60)
        print()

        return 1 if failed > 0 else 0


def run_csv_mode(args) -> int:
    """
    Run the pipeline in CSV mode — the original path, unchanged.

    Returns exit code: 0 for clean run, 1 if any records were held.
    """
    print(f"  Source:  CSV")
    print(f"  CSV:     {args.csv}")
    print(f"  Lookup:  {args.lookup}")
    print(f"  Reports: {args.output}")
    print("=" * 60)
    print()

    result = run_pipeline(
        csv_path=args.csv,
        lookup_path=args.lookup,
        output_dir=args.output,
        correlation_id="local-run",
    )

    print()
    print("=" * 60)
    print("  Run Complete")
    print("=" * 60)
    print(f"  Total processed : {result.total}")
    print(f"  Succeeded       : {result.succeeded}")
    print(f"  Held            : {result.held}")
    print(f"  Failed          : {result.failed}")

    if result.errors:
        print(f"  Audit errors    : {len(result.errors)}")
        for err in result.errors:
            print(f"    ✗ {err}")

    print()
    print(f"  Reports written to: {args.output}/")
    print("=" * 60)
    print()

    return 1 if result.held > 0 else 0


def print_reports(output_dir: str) -> None:
    """Print a summary of all audit reports written during the run."""
    output_path = Path(output_dir)
    if not output_path.exists():
        return

    reports = sorted(output_path.glob("*.json"))
    if not reports:
        return

    print("  Written reports:")
    for r in reports:
        if r.name.startswith("_run_summary"):
            continue
        try:
            with r.open() as f:
                data = json.load(f)
            status = "✓" if not data.get("hold_reasons") else "⚠"
            print(f"    {status} {r.name}")
            if data.get("hold_reasons"):
                for reason in data["hold_reasons"]:
                    print(f"        → {reason}")
        except (json.JSONDecodeError, KeyError):
            print(f"    ? {r.name} (could not parse)")

    summaries = sorted(output_path.glob("_run_summary_*.json"))
    if summaries:
        print()
        latest = summaries[-1]
        try:
            with latest.open() as f:
                summary = json.load(f)
            print(f"  Run summary: {latest.name}")
        except (json.JSONDecodeError, KeyError):
            pass

    print()


def main() -> None:
    load_local_settings()

    parser = argparse.ArgumentParser(
        description="Run the JML pipeline locally.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  CSV mode (default):
    python scripts/run_local.py --csv Data/sample_joiners.csv --clean

  API mode — single employee:
    python scripts/run_local.py --source api --id Acc003

  API mode — batch:
    python scripts/run_local.py --source api --id Acc003,Acc004,Acc005

  API mode — delta poll:
    python scripts/run_local.py --source api --mode delta
        """,
    )
    parser.add_argument(
        "--source",
        choices=["csv", "api"],
        default="csv",
        help="Input source: 'csv' for HR CSV file, 'api' for BambooHR API",
    )
    parser.add_argument(
        "--mode",
        choices=["single", "delta"],
        default="single",
        help="API mode: 'single' for specific IDs, 'delta' for changes since last poll",
    )
    parser.add_argument(
        "--id",
        default="",
        help="Comma-separated employee numbers, UPNs, or BambooHR IDs (API single mode)",
    )
    parser.add_argument(
        "--csv",
        default="Data/sample_joiners.csv",
        help="Path to the HR CSV file (CSV mode)",
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

    if args.clean:
        clean_reports(args.output)

    # Warn if the validation engine is unreachable
    check_validation_engine()

    if args.source == "api":
        exit_code = run_api_mode(args)
    else:
        exit_code = run_csv_mode(args)

    # Print report listing for both modes
    print_reports(args.output)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()