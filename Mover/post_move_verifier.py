"""
Post-move verification for Mover events.

After Steps 6 and 7 have executed (access removed, access added), this
module re-fetches the user's actual Entra ID group membership and confirms
it matches the expected post-move state.

Expected state is:
    unchanged ∪ retain_set ∪ groups_to_add

Any discrepancy between expected and actual is recorded in the result.
The orchestrator uses the result to set the final event status:
    MOVE_SUCCESS  — actual state matches expected state exactly
    MOVE_PARTIAL  — discrepancies found

This module also calls the PowerShell validation engine in PostProvision
mode via validation_gate.py. That check runs the full 27-rule governance
evaluation against the real Entra object — the same check the Joiner runs
after provisioning.

Graph API eventual consistency:
    Group membership writes can take seconds to propagate. A configurable
    delay is applied before fetching. The orchestrator passes delay_seconds
    in so tests can set it to zero without mocking time.
"""

from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from Provisioning.graph_client import JmlGraphClient, GraphClientError
from Validation.validation_gate import post_provision_validate, ValidationResult

logger = logging.getLogger(__name__)

# Default delay before re-fetching group membership after writes.
# Accounts for Graph API eventual consistency.
DEFAULT_CONSISTENCY_DELAY_SECONDS = 10



# Data models

class PostMoveStatus(str, Enum):
    """
    The outcome of post-move verification.

    MOVE_SUCCESS  — actual Entra state matches expected state exactly.
                    Governance validation passed.
    MOVE_PARTIAL  — membership discrepancies found, or governance
                    validation returned failures.
    VERIFICATION_ERROR — Graph API call to fetch actual state failed.
                         Cannot determine whether the move succeeded.
                         Orchestrator marks the event MOVE_FAILED.
    """
    MOVE_SUCCESS       = "MOVE_SUCCESS"
    MOVE_PARTIAL       = "MOVE_PARTIAL"
    VERIFICATION_ERROR = "VERIFICATION_ERROR"


@dataclass
class MembershipDiscrepancy:
    """
    A single discrepancy between expected and actual group membership.

    Fields:
        group_id:  Entra group object ID
        kind:      "MISSING" — expected but not found in actual memberOf
                   "UNEXPECTED" — found in actual memberOf but not expected
    """
    group_id: str
    kind:     str    # "MISSING" | "UNEXPECTED"


@dataclass
class PostMoveVerificationResult:
    """
    The complete result of post-move verification.

    Fields:
        status:           MOVE_SUCCESS, MOVE_PARTIAL, or VERIFICATION_ERROR.
        expected_groups:  The group set the user should hold after the move.
                          unchanged ∪ retain_set ∪ groups_to_add.
        actual_groups:    The group set fetched from Entra ID after the move.
                          Empty if the fetch failed.
        discrepancies:    List of MembershipDiscrepancy — groups that are
                          missing or unexpected relative to expected state.
                          Empty on MOVE_SUCCESS.
        governance_result: The ValidationResult from the PowerShell engine.
                           Always present unless the membership fetch failed
                           before the governance call could be made.
        error:            Error message if status is VERIFICATION_ERROR.
    """
    status:            PostMoveStatus
    expected_groups:   frozenset[str]
    actual_groups:     frozenset[str]
    discrepancies:     list[MembershipDiscrepancy]
    governance_result: Optional[ValidationResult]
    error:             str = ""



# Graph fetch

def _fetch_actual_groups(
    graph_client: JmlGraphClient,
    user_id:      str,
) -> frozenset[str]:
    """
    Fetch the user's current group membership from Entra ID.

    Returns a frozenset of group object IDs.
    Raises GraphClientError on failure — caller handles it.

    Uses the SDK memberOf endpoint. Returns only security groups
    the user is a direct member of.
    """
    try:
        members = graph_client._run(
            graph_client._client.users.by_user_id(user_id).member_of.get()
        )

        if not members or not members.value:
            return frozenset()

        return frozenset(
            obj.id
            for obj in members.value
            if obj.id is not None
        )

    except Exception as e:
        status_code = None
        if hasattr(e, "status_code"):
            status_code = e.status_code
        from Provisioning.graph_client import GraphClientError as GCE
        raise GCE(
            f"Failed to fetch memberOf for user {user_id}: {e}",
            status_code=status_code,
        )



# Discrepancy calculation
def _calculate_discrepancies(
    expected:         frozenset[str],
    actual:           frozenset[str],
    unmanaged_groups: frozenset[str] = frozenset(),
) -> list[MembershipDiscrepancy]:
    """
    Compare expected and actual group sets and return discrepancies. 
             expected is the set of groups the user should have after the move.
             actual is the set of groups the user actually has after the move.
             unmanaged_groups is the set of groups intentionally outside 
             the engine's scope — their presence in actual is expected and not a discrepancy.


    MISSING    — in expected but not in actual. A group the move should
                 have added or retained was not found in the tenant.
    UNEXPECTED — in actual but not in expected, and not in unmanaged_groups.
                 Unmanaged groups are excluded — they are intentionally
                 outside the engine's scope and their presence is expected.
                 They are recorded separately in the audit record.
    """
    discrepancies: list[MembershipDiscrepancy] = []

    for group_id in expected - actual:
        discrepancies.append(MembershipDiscrepancy(
            group_id = group_id,
            kind     = "MISSING",
        ))

    for group_id in (actual - expected) - unmanaged_groups:
        discrepancies.append(MembershipDiscrepancy(
            group_id = group_id,
            kind     = "UNEXPECTED",
        ))

    return discrepancies



# Main verifier
def verify_post_move_state(
    graph_client:     JmlGraphClient,
    user_id:          str,
    employee_id:      str,
    unchanged:        frozenset[str],
    retain_set:       frozenset[str],
    groups_to_add:    frozenset[str],
    unmanaged_groups: frozenset[str] = frozenset(),
    delay_seconds:    int = DEFAULT_CONSISTENCY_DELAY_SECONDS,
) -> PostMoveVerificationResult:
    """
    Verify the user's Entra ID state after a Mover event completes.

    Runs two checks in sequence:
        1. Membership check — re-fetches actual memberOf and compares
           against expected state (unchanged ∪ retain_set ∪ groups_to_add).
        2. Governance check — calls the PowerShell validation engine in
           PostProvision mode against the real Entra object.

    Both checks always run unless the membership fetch itself fails,
    in which case the governance check is skipped and status is
    VERIFICATION_ERROR.

    Args:
        graph_client:  Authenticated JmlGraphClient instance.
        user_id:       Entra object ID of the moved user.
        employee_id:   HR source identifier — used for log context only.
        unchanged:     Groups the user held that are still valid post-move.
                       From MoverDelta.unchanged.
        retain_set:    Groups that survived retention evaluation.
                       From RetentionResult.retain_set.
        groups_to_add: Groups added by the new role mapping.
                       From MoverDelta.groups_to_add.
        delay_seconds: Seconds to wait before fetching actual state.
                       Accounts for Graph eventual consistency.
                       Pass 0 in tests.

    Returns:
        PostMoveVerificationResult with status, discrepancies, and
        governance result.

    Side effects:
        Sleeps for delay_seconds before the Graph fetch.
        One Graph API call to fetch memberOf.
        One HTTP call to the PowerShell validation engine.
    """
    expected_groups: frozenset[str] = unchanged | retain_set | groups_to_add

    # Wait for Graph eventual consistency before fetching
    if delay_seconds > 0:
        logger.info(
            "Post-move verification — waiting %ds for Graph consistency — "
            "employee=%s, user_id=%s",
            delay_seconds, employee_id, user_id,
        )
        time.sleep(delay_seconds)

    # Step 1 — fetch actual group membership
    try:
        actual_groups = _fetch_actual_groups(
            graph_client = graph_client,
            user_id      = user_id,
        )
    except GraphClientError as e:
        logger.error(
            "Post-move verification — memberOf fetch failed — "
            "employee=%s, user_id=%s, error=%s",
            employee_id, user_id, str(e),
        )
        return PostMoveVerificationResult(
            status            = PostMoveStatus.VERIFICATION_ERROR,
            expected_groups   = expected_groups,
            actual_groups     = frozenset(),
            discrepancies     = [],
            governance_result = None,
            error             = str(e),
        )

    # Step 2 — calculate discrepancies
    discrepancies = _calculate_discrepancies(
        expected = expected_groups,
        actual   = actual_groups,
        unmanaged_groups = unmanaged_groups,
    )

    if discrepancies:
        logger.warning(
            "Post-move membership discrepancies found — employee=%s, "
            "missing=%d, unexpected=%d",
            employee_id,
            sum(1 for d in discrepancies if d.kind == "MISSING"),
            sum(1 for d in discrepancies if d.kind == "UNEXPECTED"),
        )
    else:
        logger.info(
            "Post-move membership matches expected state — employee=%s",
            employee_id,
        )

    # Step 3 — governance validation against real Entra object
    logger.info(
        "Post-move governance validation — employee=%s, user_id=%s",
        employee_id, user_id,
    )

    governance_result = post_provision_validate(
        entra_object_id = user_id,
        employee_id     = employee_id,
    )

    if not governance_result.passed:
        logger.warning(
            "Post-move governance validation failed — employee=%s, "
            "failures=%s",
            employee_id,
            governance_result.failure_summary(),
        )

    # Resolve final status
    has_discrepancies      = len(discrepancies) > 0
    governance_failed      = not governance_result.passed

    if has_discrepancies or governance_failed:
        status = PostMoveStatus.MOVE_PARTIAL
    else:
        status = PostMoveStatus.MOVE_SUCCESS

    logger.info(
        "Post-move verification complete — employee=%s, status=%s",
        employee_id, status.value,
    )

    return PostMoveVerificationResult(
        status            = status,
        expected_groups   = expected_groups,
        actual_groups     = actual_groups,
        discrepancies     = discrepancies,
        governance_result = governance_result,
    )