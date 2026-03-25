# provisioning/provisioner.py
#
# Joiner provisioning execution layer.
#
# WHY THIS EXISTS:
#   The mapping resolver decides what entitlements a new joiner gets.
#   The Graph client knows how to call the API.
#   This file owns the sequence and it orchestrates those two pieces into
#   a single, audited, retry-safe provisioning run for one identity.
#
# SEQUENCE:
#   1. UPN existence check  — retry guard, skip creation if user already exists
#   2. Create Entra ID user
#   3. Assign groups        — skips dynamic groups, idempotent
#   4. Assign RBAC roles    — idempotent
#
# IDEMPOTENCY:
#   Every write is preceded by a read. Safe to retry from the beginning
#   after any failure the check-before-write pattern in each step
#   prevents duplicate users, duplicate group memberships, and duplicate
#   role assignments.
#
# PARTIAL FAILURE:
#   Each completed step is recorded in the DecisionReport immediately.
#   On failure, the event is marked Failed with FailureStep recorded.
#   There is no rollback, retry resolves partial state via idempotent
#   operations. Post-provision validation confirms the final state is correct.
#
# AUDIT:
#   Every action including skips  is written to the DecisionReport
#   so the audit trail is complete regardless of outcome. The caller
#   owns writing the report to disk or storage.

import logging
from dataclasses import dataclass, field

from Audit.models import DecisionReport
from Ingestion.schema import IdentityPayload
from Mapping.mapping_resolver import EntitlementResult
from Provisioning.graph_client import (
    JmlGraphClient,
    GraphClientError,
    UserNotFoundError,
)

logger = logging.getLogger(__name__)


@dataclass
class ProvisioningResult:
    """
    Outcome of a provisioner run for a single identity.

    succeeded      — True only if every step completed without error
    entra_id       — Entra object ID of the user; populated even on partial failure
                     so the caller can reference the object in the audit report
    failure_step   — name of the step that failed (e.g. "UserCreation", "GroupAssignment")
    failure_detail — exception message from the failed step, for the audit report
    """
    succeeded:      bool = False
    entra_id:       str  = ""
    failure_step:   str  = ""
    failure_detail: str  = ""


def provision_joiner(
    payload:      IdentityPayload,
    entitlements: EntitlementResult,
    report:       DecisionReport,
    graph_client: JmlGraphClient,
    event_status: str = ""
) -> ProvisioningResult:
    """
    Execute the full Joiner provisioning sequence for one identity.

    Steps run in order and each step gates the next — if user creation
    fails, group and RBAC assignment are skipped entirely. This matches
    the dependency: you can't assign groups to a user that doesn't exist.

    The DecisionReport is written to as each step executes, so the audit
    trail reflects exactly how far provisioning got before any failure.
    The caller owns persisting the report so this function only writes to it.

    event_status is passed in so the UPN check can distinguish a retry
    (event was already Processing) from a genuine UPN conflict with a
    different identity.
    """
    result = ProvisioningResult()

    # Step 1 + 2 — UPN check and user creation are combined.
    # The retry guard logic lives inside _check_or_create_user because
    # both branches (skip vs create) need to return the same thing: an entra_id.
    entra_id = _check_or_create_user(
        payload=payload,
        report=report,
        graph_client=graph_client,
        event_status=event_status,
        result=result
    )

    if not entra_id:
        # _check_or_create_user already populated result.failure_step and
        # result.failure_detail — return early and let the caller handle it.
        return result

    result.entra_id = entra_id

    # Step 3 — Group assignment
    groups_ok = _assign_groups(
        user_id=entra_id,
        employee_id=payload.employee_id,
        groups=entitlements.groups,
        report=report,
        graph_client=graph_client,
        result=result
    )

    if not groups_ok:
        return result

    # Step 4 — RBAC role assignment
    rbac_ok = _assign_rbac_roles(
        user_id=entra_id,
        employee_id=payload.employee_id,
        rbac_roles=entitlements.rbac_roles,
        report=report,
        graph_client=graph_client,
        result=result
    )

    if not rbac_ok:
        return result

    result.succeeded = True
    logger.info(
        f"Provisioning complete — employee={payload.employee_id}, "
        f"upn={payload.upn}, entra_id={entra_id}"
    )
    return result


def _check_or_create_user(
    payload:      IdentityPayload,
    report:       DecisionReport,
    graph_client: JmlGraphClient,
    event_status: str,
    result:       ProvisioningResult
) -> str:
    """
    Handle the user creation step, including retry detection.

    Returns the Entra object ID (str) on success, or an empty string on failure.
    On failure, result.failure_step and result.failure_detail are populated
    before returning so the caller can record them without knowing the details.

    RETRY vs CONFLICT:
        If the UPN already exists AND event_status is "Processing", this is
        a retry after a crash the user was created on the previous attempt.
        Skip creation and return the existing object ID so the next steps
        (group and RBAC assignment) can continue from where the run left off.

        If the UPN already exists but this is NOT a retry, it is a real
        duplicate, a different identity may own that UPN. Treat as a
        failure rather than risk overwriting or merging with another user's
        account.
    """
    try:
        existing = graph_client.get_user(payload.upn)

        if event_status == "Processing":
            # Confirmed retry — user exists because we created them last time.
            # Resume from here rather than attempting creation again.
            report.add_action(
                action="UserCreationSkipped",
                detail=(
                    f"User {payload.upn} already exists — "
                    f"resuming from retry. object_id={existing['id']}"
                ),
                succeeded=True
            )
            logger.info(
                f"Retry detected — user exists, skipping creation — "
                f"employee={payload.employee_id}, entra_id={existing['id']}"
            )
            return existing["id"]

        else:
            # UPN exists but this is not a retry — potential duplicate identity.
            # Do not proceed. The pre-provision gate should have caught this,
            # but we guard here as a second line of defence.
            report.add_action(
                action="UserCreationFailed",
                detail=(
                    f"UPN {payload.upn} already exists in Entra ID "
                    "and this is not a retry. Possible duplicate identity."
                ),
                succeeded=False
            )
            result.failure_step   = "UserCreation"
            result.failure_detail = f"UPN conflict: {payload.upn} already exists"
            logger.error(
                f"UPN conflict — employee={payload.employee_id}, upn={payload.upn}"
            )
            return ""

    except UserNotFoundError:
        # Expected path for a fresh provisioning run — UPN is available.
        pass

    except GraphClientError as e:
        # The UPN check itself failed — can't determine whether user exists.
        # Fail here rather than risk creating a duplicate.
        report.add_action(
            action="UserCreationFailed",
            detail=f"Graph API error checking UPN existence: {e}",
            succeeded=False
        )
        result.failure_step   = "UPNCheck"
        result.failure_detail = str(e)
        return ""

    # Fresh provisioning path — create the user
    try:
        created = graph_client.create_user(payload)

        report.add_action(
            action="UserCreated",
            detail=f"upn={created['upn']}, object_id={created['id']}",
            succeeded=True
        )
        logger.info(
            f"User created — employee={payload.employee_id}, "
            f"upn={payload.upn}, entra_id={created['id']}"
        )
        return created["id"]

    except GraphClientError as e:
        report.add_action(
            action="UserCreationFailed",
            detail=str(e),
            succeeded=False
        )
        result.failure_step   = "UserCreation"
        result.failure_detail = str(e)
        logger.error(
            f"User creation failed — employee={payload.employee_id}: {e}"
        )
        return ""


def _assign_groups(
    user_id:      str,
    employee_id:  str,
    groups:       list[str],
    report:       DecisionReport,
    graph_client: JmlGraphClient,
    result:       ProvisioningResult
) -> bool:
    """
    Assign all groups from the entitlement resolver output.

    Returns True if all assignments succeeded or were safely skipped.
    Returns False on the first failure, result is populated before returning.

    Two skip conditions are handled gracefully (not treated as failures):
        Dynamic group  — Entra manages membership automatically via a rule.
                         Attempting a manual add would be rejected by the API anyway.
        Already member — User was assigned on a previous run that crashed.
                         Safe to skip; idempotent by design.

    Any GraphClientError on a static group stops the run and records the
    failure. Remaining groups are not attempted — partial group assignment
    is better surfaced clearly than silently continued.
    """
    if not groups:
        logger.info(f"No group assignments for employee={employee_id}")
        return True

    for group_id in groups:
        try:
            group_info = graph_client.get_group(group_id)

            if group_info["is_dynamic"]:
                # Dynamic groups reject manual membership changes via the API.
                # Entra evaluates the membership rule automatically — skip and record.
                report.add_action(
                    action="GroupAssignmentSkipped",
                    detail=(
                        f"group={group_info['display_name']} ({group_id}) "
                        "is dynamic — membership managed by Entra rule, "
                        "manual assignment skipped"
                    ),
                    succeeded=True
                )
                logger.warning(
                    f"Dynamic group skipped — employee={employee_id}, "
                    f"group={group_info['display_name']}"
                )
                continue

            already_member = graph_client.check_group_membership(user_id, group_id)

            if already_member:
                # Already assigned — retry scenario. Record the skip so the
                # audit trail shows this group was intentionally not re-added.
                report.add_action(
                    action="GroupAssignmentSkipped",
                    detail=(
                        f"group={group_info['display_name']} ({group_id}) "
                        "— user already a member, skipped (retry)"
                    ),
                    succeeded=True
                )
                continue

            graph_client.add_group_member(user_id, group_id)

            report.add_action(
                action="GroupAssigned",
                detail=f"group={group_info['display_name']} ({group_id})",
                succeeded=True
            )

        except GraphClientError as e:
            report.add_action(
                action="GroupAssignmentFailed",
                detail=f"group={group_id}: {e}",
                succeeded=False
            )
            result.failure_step   = "GroupAssignment"
            result.failure_detail = str(e)
            logger.error(
                f"Group assignment failed — employee={employee_id}, group={group_id}: {e}"
            )
            return False

    return True


def _assign_rbac_roles(
    user_id:      str,
    employee_id:  str,
    rbac_roles:   list[dict],
    report:       DecisionReport,
    graph_client: JmlGraphClient,
    result:       ProvisioningResult
) -> bool:
    """
    Assign all RBAC roles from the entitlement resolver output.

    Returns True if all assignments succeeded or were safely skipped.
    Returns False on the first failure, result is populated before returning.

    Each role entry in rbac_roles is a dict with:
        role  — role definition ID (required)
        scope — directory scope for the assignment (defaults to "/" if missing,
                which means tenant-wide)

    Missing role IDs are logged as warnings and skipped rather than failing
    the entire run, a malformed entry in the entitlement list should not
    block valid role assignments that follow it.

    Already-assigned roles are skipped without error (retry safety).
    """
    if not rbac_roles:
        logger.info(f"No RBAC role assignments for employee={employee_id}")
        return True

    for role in rbac_roles:
        role_id = role.get("role", "")
        scope   = role.get("scope", "/")  # Default to tenant-wide if not specified

        if not role_id:
            # Malformed entry — log and skip rather than fail the whole step
            logger.warning(
                f"RBAC role entry missing role ID — "
                f"employee={employee_id}, entry={role}"
            )
            continue

        try:
            already_assigned = graph_client.check_rbac_assignment(
                user_id, role_id, scope
            )

            if already_assigned:
                # Already assigned — retry scenario. Record the skip.
                report.add_action(
                    action="RbacAssignmentSkipped",
                    detail=(
                        f"role={role_id}, scope={scope} "
                        "— assignment already exists, skipped (retry)"
                    ),
                    succeeded=True
                )
                continue

            graph_client.create_rbac_assignment(user_id, role_id, scope)

            report.add_action(
                action="RbacAssigned",
                detail=f"role={role_id}, scope={scope}",
                succeeded=True
            )

        except GraphClientError as e:
            report.add_action(
                action="RbacAssignmentFailed",
                detail=f"role={role_id}, scope={scope}: {e}",
                succeeded=False
            )
            result.failure_step   = "RbacAssignment"
            result.failure_detail = str(e)
            logger.error(
                f"RBAC assignment failed — employee={employee_id}, role={role_id}: {e}"
            )
            return False

    return True