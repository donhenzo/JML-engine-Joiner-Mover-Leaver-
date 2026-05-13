"""
Action Deriver

Responsibility: determine what lifecycle event a BambooHR record represents
before it enters the JML pipeline.

This module answers one question: given a record from the HR system,
is this a Joiner, a Mover, or nothing we need to act on?

The decision is based on whether the identity already exists in Entra ID
and whether their attributes have changed.

Decision logic:
    1. Look up the UPN in Entra ID via Graph API
    2. If user does NOT exist → Joiner
    3. If user EXISTS:
        a. Compare department and job title from HR against Entra
        b. If either changed → Mover
        c. If nothing changed → Skip (no action needed)
    4. Leaver detection will be added when termination status is available

Separation of concerns:
    bamboohr_client.py   → fetches raw data from BambooHR
    bamboohr_mapper.py   → translates field names
    action_deriver.py    → determines Joiner / Mover / Skip (this file)
    ingestion_coordinator.py → wires everything together
"""

import logging

logger = logging.getLogger(__name__)


# Return values — plain strings, not enums, because this runs before
# the record enters the pipeline where JmlAction enums are constructed.
ACTION_JOINER = "Joiner"
ACTION_MOVER = "Mover"
ACTION_SKIP = "Skip"


def derive_action(mapped_record: dict, graph_client) -> str:
    """
    Determine the lifecycle action for a mapped HR record.

    Args:
        mapped_record: dict from bamboohr_mapper.map_to_raw_identity()
                       Must contain: upn, department, job_title, employee_id
        graph_client:  JmlGraphClient instance for Entra ID lookups

    Returns:
        "Joiner" — identity does not exist in Entra, needs provisioning
        "Mover"  — identity exists but department or job title changed
        "Skip"   — identity exists and nothing meaningful changed
    """
    upn = mapped_record.get("upn", "")
    employee_id = mapped_record.get("employee_id", "")

    if not upn:
        # No UPN means we cannot check Entra. This record will fail
        # normalisation anyway — let the pipeline handle it.
        logger.warning(
            "No UPN for employee %s — cannot derive action, defaulting to Joiner",
            employee_id
        )
        return ACTION_JOINER

    # Check if this identity already exists in Entra ID
    try:
        existing_user = graph_client.get_user(upn)
    except Exception as e:
        error_msg = str(e).lower()
        if "not found" in error_msg or "404" in error_msg:
            # User does not exist — this is a new hire
            logger.info(
                "User %s not found in Entra — action: Joiner (employee: %s)",
                upn, employee_id
            )
            return ACTION_JOINER

        # Graph API error — cannot determine state. Log and default to Skip
        # so we do not accidentally re-provision or trigger a Mover on bad data.
        logger.error(
            "Graph API error checking %s — cannot derive action, skipping: %s",
            upn, e
        )
        return ACTION_SKIP

    # User exists — check if department or job title changed.
    # These are the two fields that drive entitlement recalculation in Mover.
    entra_department = existing_user.get("department") or ""
    entra_job_title = existing_user.get("job_title") or ""
    hr_department = mapped_record.get("department", "")
    hr_job_title = mapped_record.get("job_title", "")

    dept_changed = _normalise_for_comparison(hr_department) != _normalise_for_comparison(entra_department)
    title_changed = _normalise_for_comparison(hr_job_title) != _normalise_for_comparison(entra_job_title)

    if dept_changed or title_changed:
        logger.info(
            "User %s exists — attributes changed — action: Mover (employee: %s, "
            "dept: '%s' → '%s', title: '%s' → '%s')",
            upn, employee_id,
            entra_department, hr_department,
            entra_job_title, hr_job_title
        )
        return ACTION_MOVER

    # User exists and nothing meaningful changed
    logger.debug(
        "User %s exists — no changes detected — action: Skip (employee: %s)",
        upn, employee_id
    )
    return ACTION_SKIP


def _normalise_for_comparison(value: str) -> str:
    """
    Minimal normalisation for comparing HR values against Entra values.
    Strips whitespace and lowercases so 'Sales' matches 'sales' and
    ' Sales ' matches 'Sales'.

    This is NOT the canonical normalisation — that happens in the pipeline.
    This is just enough to avoid false Mover triggers from case differences.
    """
    return value.strip().lower()