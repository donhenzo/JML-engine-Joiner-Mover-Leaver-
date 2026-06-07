#!/usr/bin/env python3
"""
scripts/run_local.py

Local CLI runner for the JML pipeline.

USAGE:
    # Joiner CSV mode
    python scripts/run_local.py --csv Data/sample_joiners.csv --clean

    # Mover CSV mode
    python scripts/run_local.py --source mover --csv Data/sample_movers.csv --clean

    # API mode — single employee
    python scripts/run_local.py --source api --id Acc003

    # API mode — delta poll
    python scripts/run_local.py --source api --mode delta
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Functions.Joiner_http import run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s %(name)s — %(message)s",
)
logging.getLogger("azure").setLevel(logging.WARNING)


def load_local_settings(settings_path: str = "local.settings.json") -> None:
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
    url = os.environ.get("JML_VALIDATION_ENGINE_URL", "")
    if not url:
        print("⚠  JML_VALIDATION_ENGINE_URL not set in local.settings.json")
        return False
    try:
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
        return True


def clean_reports(output_dir: str) -> None:
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


def run_mover_csv_mode(args) -> int:
    """
    Run the Mover pipeline against a CSV file.

    Parses the CSV, normalises each record, and routes MOVER action
    records to run_mover_pipeline(). JOINER and LEAVER records in the
    same file are skipped with a warning — use the Joiner CSV path for those.

    Returns exit code: 0 for clean run, 1 if any records failed or were held.
    """
    from Ingestion.csv_parser import parse_csv
    from Ingestion.schema import IdentityPayload, EmploymentType, JmlAction
    from Normalization.lookup_loader import load_lookup_table
    from Normalization.normalizer import Normalizer
    from Provisioning.graph_client import build_graph_client, JmlGraphClient
    from Functions.Event_store.event_store import generate_event_id
    from Functions.mover_http import run_mover_pipeline
    from azure.data.tables import TableServiceClient

    print(f"  Source:  Mover CSV")
    print(f"  CSV:     {args.csv}")
    print(f"  Lookup:  {args.lookup}")
    print(f"  Reports: {args.output}")
    print("=" * 60)
    print()

    conn_str = os.environ.get("JML_STORAGE_CONNECTION_STRING", "") or \
               os.environ.get("AzureWebJobsStorage", "")

    if not conn_str:
        print("✗  Storage connection string not set.")
        print("   Set JML_STORAGE_CONNECTION_STRING in local.settings.json")
        return 1

    # Build clients once for the entire run
    try:
        graph_service, credential = build_graph_client()
        graph_client = JmlGraphClient(graph_service, credential)
    except Exception as e:
        print(f"✗  Failed to build Graph client: {e}")
        return 1

    table_client = TableServiceClient.from_connection_string(conn_str)

    lookup     = load_lookup_table(args.lookup)
    normalizer = Normalizer(lookup)

    csv_content  = Path(args.csv).read_text(encoding="utf-8-sig")
    parse_result = parse_csv(csv_content)

    if parse_result.rejected_rows:
        print(f"  ⚠  {len(parse_result.rejected_rows)} CSV row(s) rejected at parse:")
        for row in parse_result.rejected_rows:
            print(f"     → {row.get('EmployeeId', 'unknown')}: "
                  f"{row.get('rejection_reason', 'parse error')}")
        print()

    total     = 0
    succeeded = 0
    held      = 0
    failed    = 0

    for raw_row in parse_result.valid_rows:

        # Construct IdentityPayload
        try:
            payload = IdentityPayload(
                employee_id     = raw_row.employee_id,
                upn             = raw_row.upn,
                display_name    = raw_row.display_name,
                department      = raw_row.department_raw,
                job_title       = raw_row.job_title_raw,
                manager_id      = raw_row.manager_id,
                start_date      = raw_row.start_date,
                employment_type = EmploymentType(raw_row.employment_type_raw),
                location        = raw_row.location,
                action          = JmlAction(raw_row.action_raw),
                retain_roles    = raw_row.retain_roles,
                retain_list     = raw_row.retain_list,
            )
        except ValueError as e:
            print(f"  ✗  {raw_row.employee_id} — payload construction failed: {e}")
            total  += 1
            failed += 1
            continue

        # Skip non-Mover records
        if payload.action != JmlAction.MOVER:
            print(f"  ⚠  {payload.employee_id} — action={payload.action.value} "
                  f"skipped (use Joiner CSV path for non-Mover records)")
            continue

        # Normalise
        norm_result = normalizer.normalize(payload)
        if not norm_result.passed:
            print(f"  ✗  {payload.employee_id} ({payload.upn}) — "
                  f"normalisation failed: {norm_result.failures}")
            total  += 1
            held   += 1
            continue

        normalised_payload = norm_result.payload

        # Generate deterministic event ID
        event_id = generate_event_id(
            normalised_payload.employee_id,
            normalised_payload.action.value,
            normalised_payload.start_date.isoformat(),
        )

        print(f"  ▸ Processing: {normalised_payload.employee_id} "
              f"({normalised_payload.upn})")
        print(f"    Move: {normalised_payload.department} / "
              f"{normalised_payload.job_title}")
        print(f"    EventId: {event_id}")

        # Run the Mover pipeline
        try:
            result = run_mover_pipeline(
                payload      = normalised_payload,
                event_id     = event_id,
                table_client = table_client,
                graph_client = graph_client,
            )

            status = result.get("final_status", "UNKNOWN")
            summary = result.get("summary", "")

            if status == "MOVE_SUCCESS":
                print(f"    ✓ {status}")
                succeeded += 1
            elif status in ("HOLD_FOR_REVIEW", "QUEUED_CONCURRENT"):
                print(f"    ⚠ {status} — {summary}")
                held += 1
            else:
                print(f"    ✗ {status} — {summary}")
                failed += 1

        except Exception as e:
            print(f"    ✗ Pipeline error: {e}")
            failed += 1

        total += 1
        print()

    print("=" * 60)
    print("  Mover Run Complete")
    print("=" * 60)
    print(f"  Total processed : {total}")
    print(f"  Succeeded       : {succeeded}")
    print(f"  Held            : {held}")
    print(f"  Failed          : {failed}")
    print("=" * 60)
    print()

    return 1 if (held > 0 or failed > 0) else 0


def run_api_mode(args) -> int:
    from Provisioning.graph_client import build_graph_client, JmlGraphClient
    from Ingestion.hr_api.bamboohr.pipeline_adapter import PipelineContext
    from Ingestion.hr_api.bamboohr.ingestion_coordinator import run_single, run_delta
    from Ingestion.hr_api.system_state import (
        get_system_state_table_client,
        get_poll_checkpoint,
    )

    conn_str = os.environ.get("AzureWebJobsStorage", "")
    if not conn_str:
        print("✗  AzureWebJobsStorage not set — cannot connect to Azure Table Storage.")
        return 1

    try:
        graph_service, credential = build_graph_client()
        graph_client = JmlGraphClient(graph_service, credential)
    except Exception as e:
        print(f"✗  Failed to build Graph client: {e}")
        return 1

    ctx = PipelineContext(
        graph_client     = graph_client,
        connection_string= conn_str,
        output_dir       = args.output,
        correlation_id   = "api-run",
    )

    if args.mode == "delta":
        state_client = get_system_state_table_client(conn_str)
        checkpoint   = get_poll_checkpoint(state_client)
        time_ago     = _format_time_ago(checkpoint.last_successful_poll)

        print(f"  Mode:    Delta poll")
        print(f"  Since:   {checkpoint.last_successful_poll}")
        print(f"           ({time_ago})")
        if checkpoint.last_run_status:
            print(f"  Last:    {checkpoint.last_run_status} "
                  f"({checkpoint.records_processed} records)")
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
        if not args.id:
            print("✗  --id is required in single mode.")
            return 1

        identifiers = [x.strip() for x in args.id.split(",") if x.strip()]

        print(f"  Mode:    Single/batch")
        print(f"  IDs:     {', '.join(identifiers)}")
        print("=" * 60)
        print()

        succeeded = failed = 0

        for identifier in identifiers:
            print(f"  ▸ Processing: {identifier}")
            result = run_single(identifier, ctx)

            if result is None:
                print(f"    ✗ Fetch failed or skipped")
                failed += 1
            elif not result.get("_pipeline_success", False):
                print(f"    ⚠ {result.get('employee_id', '')} — held or rejected")
                failed += 1
            else:
                print(f"    ✓ {result.get('employee_id', '')} — "
                      f"{result.get('action', '')}")
                succeeded += 1
            print()

        print("=" * 60)
        print(f"  Total: {succeeded + failed} | "
              f"Succeeded: {succeeded} | Failed: {failed}")
        print("=" * 60)
        print()
        return 1 if failed > 0 else 0


def run_csv_mode(args) -> int:
    print(f"  Source:  Joiner CSV")
    print(f"  CSV:     {args.csv}")
    print(f"  Lookup:  {args.lookup}")
    print(f"  Reports: {args.output}")
    print("=" * 60)
    print()

    result = run_pipeline(
        csv_path       = args.csv,
        lookup_path    = args.lookup,
        output_dir     = args.output,
        correlation_id = "local-run",
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
        for err in result.errors:
            print(f"    ✗ {err}")

    print()
    print(f"  Reports written to: {args.output}/")
    print("=" * 60)
    print()

    return 1 if result.held > 0 else 0


def print_reports(output_dir: str) -> None:
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
    print()


def main() -> None:
    load_local_settings()

    parser = argparse.ArgumentParser(
        description="Run the JML pipeline locally.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Joiner CSV:
    python scripts/run_local.py --csv Data/sample_joiners.csv --clean

  Mover CSV:
    python scripts/run_local.py --source mover --csv Data/sample_movers.csv --clean

  API single:
    python scripts/run_local.py --source api --id Acc003

  API delta:
    python scripts/run_local.py --source api --mode delta
        """,
    )
    parser.add_argument(
        "--source",
        choices=["csv", "mover", "api"],
        default="csv",
        help="Input source: 'csv' for Joiner CSV, 'mover' for Mover CSV, 'api' for BambooHR",
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
        help="Comma-separated employee IDs (API single mode)",
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

    if args.clean:
        clean_reports(args.output)

    check_validation_engine()

    if args.source == "api":
        exit_code = run_api_mode(args)
    elif args.source == "mover":
        exit_code = run_mover_csv_mode(args)
    else:
        exit_code = run_csv_mode(args)

    if args.source != "mover":
        print_reports(args.output)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()