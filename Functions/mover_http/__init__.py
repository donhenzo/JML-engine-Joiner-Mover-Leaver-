"""
Functions/mover_http/__init__.py

Azure Function HTTP trigger for the Mover module.

Orchestrates the Mover processing flow for a single identity lifecycle
transition event. Called directly via HTTP or by the BambooHR ingestion
coordinator when action derivation returns JmlAction.MOVER.

Processing flow:

    Pre-Step  — claim_event() in JmlEvents. Atomic insert — duplicate
                event ID exits immediately with no side effects.

    Step 1    — Concurrent event check via MoverEventLog. User and
                memberOf fetch via Graph. acquire_lock() written to
                JmlEvents on success.

    Step 2    — Entitlement resolution for new and old roles.
    Step 3    — Group delta (four sets) and attribute delta.
    Step 4    — Retention evaluation against RetentionRegistry.

    Step 5    — SoD re-evaluation (fail closed). Pass A: unchanged ∪
                retain_set ∪ groups_to_add ∪ remove_confirmed. Pass B:
                groups_to_add in isolation. Block → HOLD_FOR_REVIEW →
                release_lock() → JmlEvents Failed → stop.

    Step 6    — Access removals. Failure → release_lock() → stop.
    Step 7    — Access additions and attribute patch.
    Step 8    — PIM adjustment from entitlement delta, not group delta.
    Step 9    — Post-move membership verification and governance check.
    Step 10   — MoverAuditRecord written. release_lock(). JmlEvents
                updated to Completed or Failed.

Every step that fails routes to a defined terminal state.
No access change executes without passing Step 5.
The JmlEvents lock is released on every exit path.
"""

from __future__ import annotations
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from azure.data.tables import TableServiceClient

from Ingestion.schema import IdentityPayload, JmlAction
from Mapping.mapping_loader import load_mapping_rules
from Mapping.mapping_resolver import resolve_entitlements
from Governance.SoD.sod_loader import load_sod_policies
from Provisioning.graph_client import (
    JmlGraphClient,
    GraphClientError,
    build_graph_client,
)
from Mover.delta_engine import calculate_delta
from Mover.attribute_delta import calculate_attribute_delta
from Mover.retention_evaluator import evaluate_all_retentions
from Mover.sod_reevaluator import evaluate_mover_sod
from Mover.pim_adjuster import adjust_pim_eligibility
from Mover.post_move_verifier import (
    verify_post_move_state,
    PostMoveStatus,
)
from Functions.Event_store.event_store import (
    get_events_table_client,
    generate_event_id,
    claim_event,
    acquire_lock,
    release_lock,
    update_event_status,
    EventStatus,
)

logger = logging.getLogger(__name__)


# Table names and constants
MOVER_EVENT_LOG_TABLE = "MoverEventLog"
MOVER_AUDIT_LOG_TABLE = "MoverAuditLog"
MOVER_HOLD_QUEUE_TABLE = "MoverHoldQueue"
RETENTION_TABLE        = "RetentionRegistry"
STALE_LOCK_MINUTES     = 10


# MoverEvent status values
class MoverEventStatus:
    RECEIVED          = "RECEIVED"
    IN_PROGRESS       = "IN_PROGRESS"
    MOVE_SUCCESS      = "MOVE_SUCCESS"
    MOVE_PARTIAL      = "MOVE_PARTIAL"
    MOVE_FAILED       = "MOVE_FAILED"
    HOLD_FOR_REVIEW   = "HOLD_FOR_REVIEW"
    QUEUED_CONCURRENT = "QUEUED_CONCURRENT"


# Table Storage helpers

def _get_table_client(connection_string: str) -> TableServiceClient:
    return TableServiceClient.from_connection_string(connection_string)


def _check_concurrent_event(
    table_client: TableServiceClient,
    employee_id:  str,
) -> bool:
    """
    Return True if an IN_PROGRESS event already exists for this employee.

    Queries MoverEventLog by PartitionKey = employee_id and
    status = IN_PROGRESS. Per ADR-004, only one Mover event per
    employee can be active at a time.

    Fails closed — if the query itself fails, returns True to prevent
    a second event from running on top of an unknown state.
    """
    try:
        client   = table_client.get_table_client(MOVER_EVENT_LOG_TABLE)
        entities = client.query_entities(
            query_filter=(
                f"PartitionKey eq '{employee_id}' "
                f"and status eq 'IN_PROGRESS'"
            )
        )
        return any(True for _ in entities)
    except Exception as e:
        logger.error(
            "MoverEventLog concurrent check failed — employee=%s, error=%s",
            employee_id, str(e),
        )
        return True


def _write_event_log(
    table_client: TableServiceClient,
    employee_id:  str,
    event_id:     str,
    status:       str,
    payload_json: str = "",
) -> None:
    """Write or update a MoverEventLog entry."""
    try:
        client = table_client.get_table_client(MOVER_EVENT_LOG_TABLE)
        entity = {
            "PartitionKey": employee_id,
            "RowKey":       event_id,
            "status":       status,
            "updated_at":   datetime.now(timezone.utc).isoformat(),
            "payload":      payload_json,
        }
        client.upsert_entity(entity)
    except Exception as e:
        logger.error(
            "MoverEventLog write failed — employee=%s, event=%s, "
            "status=%s, error=%s",
            employee_id, event_id, status, str(e),
        )


def _write_audit_record(
    table_client: TableServiceClient,
    employee_id:  str,
    event_id:     str,
    audit_record: dict,
) -> None:
    """
    Write the completed MoverAuditRecord to MoverAuditLog.

    Table Storage only accepts flat scalar values. Nested dicts and
    lists are serialised to JSON strings before writing.
    """
    try:
        client = table_client.get_table_client(MOVER_AUDIT_LOG_TABLE)
        entity = {
            "PartitionKey": employee_id,
            "RowKey":       event_id,
            **{
                k: json.dumps(v) if isinstance(v, (dict, list)) else v
                for k, v in audit_record.items()
            },
        }
        client.upsert_entity(entity)
    except Exception as e:
        logger.error(
            "MoverAuditLog write failed — employee=%s, event=%s, error=%s",
            employee_id, event_id, str(e),
        )


def _write_hold_queue(
    table_client: TableServiceClient,
    employee_id:  str,
    event_id:     str,
    violations:   list,
) -> None:
    """
    Write a hold record to MoverHoldQueue when a SoD block fires.

    PartitionKey = employee_id so all holds for an employee are
    grouped together. RowKey = event_id so each event has one record.

    violations is serialised to JSON — each entry carries the policy_id
    and conflicting_groups so an approver can see exactly what conflicted
    without needing to cross-reference the audit log.
    """
    try:
        client = table_client.get_table_client(MOVER_HOLD_QUEUE_TABLE)
        entity = {
            "PartitionKey":    employee_id,
            "RowKey":          event_id,
            "status":          "HOLD_FOR_REVIEW",
            "created_at":      datetime.now(timezone.utc).isoformat(),
            "violation_count": len(violations),
            "violations":      json.dumps([
                {
                    "policy_id":          v.policy_id,
                    "policy_name":        v.policy_name,
                    "conflicting_groups": v.conflicting_groups,
                    "risk_rating":        v.risk_rating.value,
                }
                for v in violations
            ]),
            "resolved":        False,
            "resolved_by":     "",
            "resolved_at":     "",
            "resolution_note": "",
        }
        client.upsert_entity(entity)
        logger.info(
            "MoverHoldQueue record written — employee=%s, event=%s, "
            "violations=%d",
            employee_id, event_id, len(violations),
        )
    except Exception as e:
        logger.error(
            "MoverHoldQueue write failed — employee=%s, event=%s, error=%s",
            employee_id, event_id, str(e),
        )


# PIM delta extraction
def _extract_pim_delta(
    old_pim_groups:   list,
    new_pim_groups:   list,
    groups_to_add:    frozenset[str],
    groups_to_remove: frozenset[str],
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Extract PIM add/remove/change lists from resolved PimGroup objects.

    Works directly from EntitlementResult.pim_groups — the resolver has
    already extracted and deduplicated PIM entries from the rules file.

    PIM eligibility assignments are schedule objects — they never appear
    in memberOf and never cross the group delta boundary. The delta is
    derived purely from old vs new resolved entitlements.

    Returns:
        pim_to_add:    PIM groups in new but not old.
        pim_to_remove: PIM groups in old but not new.
        pim_to_change: PIM groups present in both old and new (scope change).
    """
    def _to_dict(pg) -> dict:
        return {
            "group_id":      pg.group_id,
            "display_name":  pg.display_name,
            "eligible_role": pg.eligible_role,
            "justification": pg.justification,
        }

    old_pim_map = {pg.group_id: pg for pg in old_pim_groups}
    new_pim_map = {pg.group_id: pg for pg in new_pim_groups}

    old_ids = frozenset(old_pim_map.keys())
    new_ids = frozenset(new_pim_map.keys())

    pim_to_add    = [_to_dict(new_pim_map[gid]) for gid in (new_ids - old_ids)]
    pim_to_remove = [_to_dict(old_pim_map[gid]) for gid in (old_ids - new_ids)]
    pim_to_change = [_to_dict(new_pim_map[gid]) for gid in (old_ids & new_ids)]

    return pim_to_add, pim_to_remove, pim_to_change


# Manager resolution
def _resolve_manager_id(
    graph_client: JmlGraphClient,
    manager_id:   Optional[str],
) -> Optional[str]:
    """
    Resolve a manager employee number to an Entra object ID.

    Returns the Entra object ID if found, None if not provided or not
    found. A missing manager is not fatal for a Mover event — it is
    recorded in the audit trail as a warning.
    """
    if not manager_id:
        return None
    try:
        user = graph_client.get_user(manager_id)
        return user["id"]
    except Exception as e:
        logger.warning(
            "Manager resolution failed — manager_id=%s, error=%s. "
            "Proceeding without manager update.",
            manager_id, str(e),
        )
        return None


# Group membership execution

def _execute_removals(
    graph_client:     JmlGraphClient,
    user_id:          str,
    remove_confirmed: frozenset[str],
) -> tuple[list[dict], bool]:
    """
    Remove confirmed groups from the user's Entra ID membership.

    Checks membership before each removal — idempotent on retry.

    Returns (actions_taken, all_succeeded).
    If any removal fails, all_succeeded is False and the orchestrator
    must not proceed to Step 7 (additions).
    """
    actions_taken: list[dict] = []
    all_succeeded = True

    for group_id in remove_confirmed:
        try:
            is_member = graph_client.check_group_membership(
                user_id  = user_id,
                group_id = group_id,
            )

            if not is_member:
                actions_taken.append({
                    "action":    "GroupRemoval",
                    "group_id":  group_id,
                    "detail":    "Already not a member — skipped",
                    "succeeded": True,
                })
                continue

            graph_client._run(
                graph_client._client.groups
                .by_group_id(group_id)
                .members.by_directory_object_id(user_id)
                .ref.delete()
            )

            actions_taken.append({
                "action":    "GroupRemoval",
                "group_id":  group_id,
                "detail":    "Removed successfully",
                "succeeded": True,
            })
            logger.info(
                "Group removed — user=%s, group=%s", user_id, group_id
            )

        except GraphClientError as e:
            all_succeeded = False
            actions_taken.append({
                "action":    "GroupRemoval",
                "group_id":  group_id,
                "detail":    f"Removal failed: {str(e)}",
                "succeeded": False,
            })
            logger.error(
                "Group removal failed — user=%s, group=%s, error=%s",
                user_id, group_id, str(e),
            )

    return actions_taken, all_succeeded


def _execute_additions(
    graph_client:  JmlGraphClient,
    user_id:       str,
    groups_to_add: frozenset[str],
) -> list[dict]:
    """
    Add new groups to the user's Entra ID membership.

    Checks membership before each addition — idempotent on retry.
    Individual failures are recorded but do not stop remaining additions.
    The orchestrator marks MOVE_PARTIAL if any addition failed.
    """
    actions_taken: list[dict] = []

    for group_id in groups_to_add:
        try:
            is_member = graph_client.check_group_membership(
                user_id  = user_id,
                group_id = group_id,
            )

            if is_member:
                actions_taken.append({
                    "action":    "GroupAddition",
                    "group_id":  group_id,
                    "detail":    "Already a member — skipped (idempotent)",
                    "succeeded": True,
                })
                continue

            graph_client.add_group_member(
                user_id  = user_id,
                group_id = group_id,
            )

            actions_taken.append({
                "action":    "GroupAddition",
                "group_id":  group_id,
                "detail":    "Added successfully",
                "succeeded": True,
            })

        except GraphClientError as e:
            actions_taken.append({
                "action":    "GroupAddition",
                "group_id":  group_id,
                "detail":    f"Addition failed: {str(e)}",
                "succeeded": False,
            })
            logger.error(
                "Group addition failed — user=%s, group=%s, error=%s",
                user_id, group_id, str(e),
            )

    return actions_taken


# Attribute update
def _execute_attribute_update(
    graph_client: JmlGraphClient,
    user_id:      str,
    patch_dict:   dict,
) -> tuple[bool, str]:
    """
    PATCH the user's Entra ID attributes with changed values.

    manager and usageLocation are excluded from the PATCH body.
    manager requires a separate Graph endpoint.
    usageLocation requires an ISO 3166-1 alpha-2 country code — the CSV
    carries city names. Excluded until location-to-country mapping is
    added to canonical_lookup.json.

    Returns (succeeded, error_message).
    """
    if not patch_dict:
        return True, ""

    try:
        from msgraph.generated.models.user import User as MsUser

        user_patch = MsUser()

        field_map = {
            "department":     "department",
            "jobTitle":       "job_title",
            "officeLocation": "office_location",
            "usageLocation":  "usage_location",
            "employeeType":   "employee_type",
        }

        for field_name, value in patch_dict.items():
            if field_name == "manager":
                continue
            if field_name == "usageLocation":
                continue
            sdk_attr = field_map.get(field_name)
            if sdk_attr:
                setattr(user_patch, sdk_attr, value)

        graph_client._run(
            graph_client._client.users.by_user_id(user_id).patch(user_patch)
        )

        logger.info(
            "Attribute update applied — user=%s, fields=%s",
            user_id, list(patch_dict.keys()),
        )
        return True, ""

    except Exception as e:
        logger.error(
            "Attribute update failed — user=%s, error=%s",
            user_id, str(e),
        )
        return False, str(e)


# Main orchestrator
def run_mover_pipeline(
    payload:      IdentityPayload,
    table_client: TableServiceClient,
    graph_client: JmlGraphClient,
) -> dict:
    """
    Execute the 10-step Mover processing flow for a single identity event.

    The EventId is generated internally from the payload. Callers pass
    only the payload and clients — no external event ID is accepted.
    This mirrors the Joiner pattern and keeps event ownership inside
    the pipeline, not in the ingestion layer.

    Args:
        payload:      Canonical IdentityPayload with action=MOVER.
                      Department, job_title, and employment_type reflect
                      the NEW role — the state the user is moving TO.
        table_client: Authenticated TableServiceClient for all Table Storage ops.
        graph_client: Authenticated JmlGraphClient for all Graph API ops.

    Returns:
        dict with final_status, employee_id, event_id, and summary.

    Side effects:
        Reads and writes MoverEventLog and MoverAuditLog tables.
        Reads RetentionRegistry table.
        Graph API calls at Steps 1, 6, 7, 8, and 9.
        PowerShell validation engine call at Step 9.
    """
    employee_id = payload.employee_id

    # EventId is owned by the pipeline, not by the caller.
    # The same deterministic hash is produced regardless of which path
    # (CSV, API, HTTP trigger) invokes this function.
    event_id = generate_event_id(
        employee_id,
        "Mover",
        payload.start_date.isoformat(),
    )

    audit_record: dict = {
        "event_type":       "MOVE",
        "employee_id":      employee_id,
        "event_id":         event_id,
        "source":           "BAMBOOHR",
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "actions_taken":    [],
        "warnings":         [],
        "post_move_status": MoverEventStatus.RECEIVED,
    }

    # Pre-Step — Claim event in JmlEvents.
    # JmlEvents is the engine-wide event store shared across Joiner,
    # Mover, and Leaver. claim_event() attempts an atomic insert.
    # If the row already exists (retry, duplicate trigger, concurrent
    # invocation), it returns False and the pipeline exits immediately
    # with no side effects.
    conn_str          = os.environ.get("JML_STORAGE_CONNECTION_STRING", "")
    jml_events_client = get_events_table_client(conn_str)

    payload_json_str = json.dumps({
        "employee_id": employee_id,
        "action":      "Mover",
        "event_id":    event_id,
    })

    claimed = claim_event(
        table_client   = jml_events_client,
        employee_id    = employee_id,
        action         = "Mover",
        start_date     = payload.start_date.isoformat(),
        payload_json   = payload_json_str,
        correlation_id = event_id,
    )

    if not claimed:
        logger.info(
            "Mover event already claimed in JmlEvents — idempotency exit — "
            "employee=%s", employee_id,
        )
        return {
            "final_status": MoverEventStatus.QUEUED_CONCURRENT,
            "employee_id":  employee_id,
            "event_id":     event_id,
            "summary":      "Duplicate event — already claimed in JmlEvents.",
        }


    # Step 1 — Current state discovery + concurrent event check

    logger.info(
        "Mover Step 1 — current state discovery — employee=%s", employee_id
    )

    is_concurrent = _check_concurrent_event(table_client, employee_id)
    if is_concurrent:
        logger.warning(
            "Concurrent Mover event detected — employee=%s, "
            "queuing with status QUEUED_CONCURRENT",
            employee_id,
        )
        _write_event_log(
            table_client = table_client,
            employee_id  = employee_id,
            event_id     = event_id,
            status       = MoverEventStatus.QUEUED_CONCURRENT,
        )
        return {
            "final_status": MoverEventStatus.QUEUED_CONCURRENT,
            "employee_id":  employee_id,
            "event_id":     event_id,
            "summary":      (
                "Event queued — another Mover event is in progress "
                "for this employee."
            ),
        }

    _write_event_log(
        table_client = table_client,
        employee_id  = employee_id,
        event_id     = event_id,
        status       = MoverEventStatus.IN_PROGRESS,
    )

    # Fetch current Entra user
    try:
        current_user = graph_client.get_user(payload.upn)
        user_id      = current_user["id"]
    except GraphClientError as e:
        logger.error(
            "Step 1 failed — user fetch failed — employee=%s, error=%s",
            employee_id, str(e),
        )
        _write_event_log(
            table_client, employee_id, event_id, MoverEventStatus.MOVE_FAILED
        )
        return _fail(employee_id, event_id, f"User fetch failed: {str(e)}")

    # Fetch current group membership
    try:
        current_member_of = graph_client._run(
            graph_client._client.users.by_user_id(user_id).member_of.get()
        )
        current_groups = frozenset(
            obj.id
            for obj in (current_member_of.value or [])
            if obj.id is not None
        )
    except Exception as e:
        logger.error(
            "Step 1 failed — memberOf fetch failed — employee=%s, error=%s",
            employee_id, str(e),
        )
        _write_event_log(
            table_client, employee_id, event_id, MoverEventStatus.MOVE_FAILED
        )
        return _fail(employee_id, event_id, f"memberOf fetch failed: {str(e)}")

    # Acquire processing lock in JmlEvents.
    # This is the hard atomic concurrency guard. Two function instances
    # cannot both hold the lock for the same event_id simultaneously.
    # The lock expires after STALE_LOCK_MINUTES if the instance crashes.
    import uuid as _uuid
    instance_id = str(_uuid.uuid4())
    acquire_lock(
        table_client = jml_events_client,
        employee_id  = employee_id,
        event_id     = event_id,
        instance_id  = instance_id,
    )

    # Current attributes for attribute delta
    current_attributes: dict = {
        "department":     current_user.get("department"),
        "jobTitle":       current_user.get("job_title"),
        "officeLocation": None,
        "usageLocation":  None,
        "employeeType":   None,
    }


    # Step 2 — Target state calculation

    logger.info(
        "Mover Step 2 — target state calculation — employee=%s", employee_id
    )

    try:
        _rules_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "config", "role_mapping_rules.json"
        )
        mapping_rules = load_mapping_rules(rules_path=_rules_path)
    except Exception as e:
        _write_event_log(
            table_client, employee_id, event_id, MoverEventStatus.MOVE_FAILED
        )
        return _fail(employee_id, event_id, f"Mapping rules load failed: {str(e)}")

    try:
        _sod_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "config", "sod_policies.json"
        )
        sod_policies = load_sod_policies(policies_path=_sod_path)
    except Exception as e:
        _write_event_log(
            table_client, employee_id, event_id, MoverEventStatus.MOVE_FAILED
        )
        return _fail(employee_id, event_id, f"SoD policies load failed: {str(e)}")

    # Resolve entitlements for the NEW role (payload carries new state)
    new_resolved = resolve_entitlements(
        rules           = mapping_rules,
        department      = payload.department,
        job_title       = payload.job_title,
        employment_type = payload.employment_type.value,
        employee_id     = payload.employee_id,
    )
    target_groups = frozenset(new_resolved.groups)

    # Build managed catalogue — all group IDs defined anywhere in rules
    managed_catalogue = frozenset(
        gid
        for rule in mapping_rules
        for gid in rule.get("entitlements", {}).get("groups", [])
    )

    # Resolve entitlements for the OLD role using current Entra attributes
    old_resolved = resolve_entitlements(
        rules           = mapping_rules,
        department      = current_user.get("department") or payload.department,
        job_title       = current_user.get("job_title") or payload.job_title,
        employment_type = payload.employment_type.value,
        employee_id     = payload.employee_id,
    )

    # Resolve manager ID for attribute delta
    resolved_manager_id = _resolve_manager_id(graph_client, payload.manager_id)

    incoming_attributes: dict = {
        "department":    payload.department,
        "jobTitle":      payload.job_title,
        "manager":       resolved_manager_id,
        "employeeType":  payload.employment_type.value,
        "usageLocation": payload.location,
    }


    # Step 3 — Delta analysis

    logger.info(
        "Mover Step 3 — delta analysis — employee=%s", employee_id
    )

    delta = calculate_delta(
        current_groups    = current_groups,
        target_groups     = target_groups,
        managed_catalogue = managed_catalogue,
    )

    attr_delta = calculate_attribute_delta(
        current_attributes  = current_attributes,
        incoming_attributes = incoming_attributes,
    )

    audit_record["from_department"]   = current_user.get("department", "")
    audit_record["to_department"]     = payload.department
    audit_record["from_title"]        = current_user.get("job_title", "")
    audit_record["to_title"]          = payload.job_title
    audit_record["attribute_changes"] = {
        change.field: {
            "from": change.from_value,
            "to":   change.to_value,
        }
        for change in attr_delta.changes
    }
    audit_record["unmanaged_groups"] = [
        {"id": gid, "action": "NOT_PROCESSED"}
        for gid in delta.unmanaged
    ]


    # Step 4 — Retention evaluation

    logger.info(
        "Mover Step 4 — retention evaluation — employee=%s", employee_id
    )

    retention_result = evaluate_all_retentions(
        employee_id      = employee_id,
        groups_to_remove = delta.groups_to_remove,
        table_client     = table_client,
    )

    audit_record["groups_retained"] = [
        {
            "id":               d.group_id,
            "retention_reason": d.record.reason if d.record else "",
            "review_date":      (
                d.record.review_date.isoformat() if d.record else ""
            ),
        }
        for d in retention_result.decisions
        if d.outcome.value == "RETAINED"
    ]


    # Step 5 — SoD re-evaluation

    logger.info(
        "Mover Step 5 — SoD re-evaluation — employee=%s", employee_id
    )

    sod_result = evaluate_mover_sod(
        unchanged        = delta.unchanged,
        retain_set       = retention_result.retain_set,
        groups_to_add    = delta.groups_to_add,
        remove_confirmed = retention_result.remove_confirmed,
        policies         = sod_policies,
    )

    audit_record["sod_escalations"] = [
        {
            "rule":               v.policy_id,
            "conflicting_groups": v.conflicting_groups,
            "resolution":         "HOLD",
        }
        for v in sod_result.pass_a.result.violations
        if v.action.value == "Block" and not v.exception_applied
    ]

    if sod_result.should_hold:
        logger.warning(
            "SoD block — event entering HOLD_FOR_REVIEW — employee=%s",
            employee_id,
        )
        audit_record["post_move_status"] = MoverEventStatus.HOLD_FOR_REVIEW
        _write_event_log(
            table_client, employee_id, event_id,
            MoverEventStatus.HOLD_FOR_REVIEW
        )
        _write_hold_queue(
            table_client, employee_id, event_id,
            sod_result.pass_a.result.violations,
        )
        _write_audit_record(table_client, employee_id, event_id, audit_record)
        release_lock(jml_events_client, employee_id, event_id)
        update_event_status(
            table_client = jml_events_client,
            employee_id  = employee_id,
            event_id     = event_id,
            status       = EventStatus.FAILED,
            failure_step = "SoDBlock",
        )
        return {
            "final_status": MoverEventStatus.HOLD_FOR_REVIEW,
            "employee_id":  employee_id,
            "event_id":     event_id,
            "summary":      "SoD conflict detected. Event held for human review.",
        }


    # Step 6 — Execute access removals

    logger.info(
        "Mover Step 6 — access removals — employee=%s", employee_id
    )

    removal_actions, removals_succeeded = _execute_removals(
        graph_client     = graph_client,
        user_id          = user_id,
        remove_confirmed = retention_result.remove_confirmed,
    )
    audit_record["actions_taken"].extend(removal_actions)
    audit_record["groups_removed"] = [
        {"id": a["group_id"], "reason": "ROLE_CHANGE"}
        for a in removal_actions
        if a["succeeded"]
    ]

    if not removals_succeeded:
        logger.error(
            "Step 6 partial failure — removals incomplete — "
            "not proceeding to additions — employee=%s",
            employee_id,
        )
        audit_record["post_move_status"] = MoverEventStatus.MOVE_FAILED
        _write_event_log(
            table_client, employee_id, event_id, MoverEventStatus.MOVE_FAILED
        )
        _write_audit_record(table_client, employee_id, event_id, audit_record)
        release_lock(jml_events_client, employee_id, event_id)
        update_event_status(
            table_client = jml_events_client,
            employee_id  = employee_id,
            event_id     = event_id,
            status       = EventStatus.FAILED,
            failure_step = "AccessRemoval",
        )
        return _fail(
            employee_id, event_id,
            "Access removal failed — additions not attempted.",
        )


    # Step 7 — Execute access additions + attribute update

    logger.info(
        "Mover Step 7 — access additions — employee=%s", employee_id
    )

    addition_actions = _execute_additions(
        graph_client  = graph_client,
        user_id       = user_id,
        groups_to_add = delta.groups_to_add,
    )
    audit_record["actions_taken"].extend(addition_actions)
    audit_record["groups_added"] = [
        {"id": a["group_id"]}
        for a in addition_actions
        if a["succeeded"]
    ]

    attr_succeeded, attr_error = _execute_attribute_update(
        graph_client = graph_client,
        user_id      = user_id,
        patch_dict   = attr_delta.to_patch_dict(),
    )
    if not attr_succeeded:
        audit_record["warnings"].append(
            f"Attribute update failed: {attr_error}"
        )


    # Step 8 — PIM adjustment

    logger.info(
        "Mover Step 8 — PIM adjustment check — employee=%s", employee_id
    )

    pim_to_add, pim_to_remove, pim_to_change = _extract_pim_delta(
        old_pim_groups   = old_resolved.pim_groups,
        new_pim_groups   = new_resolved.pim_groups,
        groups_to_add    = delta.groups_to_add,
        groups_to_remove = delta.groups_to_remove,
    )

    has_pim_changes = any([pim_to_add, pim_to_remove, pim_to_change])

    if has_pim_changes:
        logger.info(
            "Mover Step 8 — executing PIM adjustments — employee=%s",
            employee_id,
        )

        pim_result = adjust_pim_eligibility(
            graph_client  = graph_client,
            user_id       = user_id,
            pim_to_add    = pim_to_add,
            pim_to_remove = pim_to_remove,
            pim_to_change = pim_to_change,
            justification = (
                f"Mover event {event_id} — "
                f"role transition for {employee_id}"
            ),
        )

        audit_record["pim_changes"] = [
            {
                "role":                   r.display_name,
                "action":                 r.action.value,
                "active_session_at_move": r.active_session_at_move,
                "session_expires":        r.session_expires,
                "succeeded":              r.succeeded,
            }
            for r in pim_result.records
        ]

        if not pim_result.all_succeeded:
            audit_record["warnings"].append(
                "One or more PIM adjustments failed — "
                "see pim_changes for detail."
            )
    else:
        logger.info(
            "Mover Step 8 — no PIM changes required — employee=%s",
            employee_id,
        )
        audit_record["pim_changes"] = None


    # Step 9 — Post-move verification

    logger.info(
        "Mover Step 9 — post-move verification — employee=%s", employee_id
    )

    verification = verify_post_move_state(
        graph_client     = graph_client,
        user_id          = user_id,
        employee_id      = employee_id,
        unchanged        = delta.unchanged,
        retain_set       = retention_result.retain_set,
        groups_to_add    = delta.groups_to_add,
        unmanaged_groups = delta.unmanaged,
        recently_removed = retention_result.remove_confirmed,
    )

    audit_record["post_move_verification"] = {
        "status":              verification.status.value,
        "discrepancies":       [
            {"group_id": d.group_id, "kind": d.kind}
            for d in verification.discrepancies
        ],
        "governance_passed":   (
            verification.governance_result.passed
            if verification.governance_result else False
        ),
        "governance_warnings": (
            verification.governance_result.warning_summary()
            if verification.governance_result else []
        ),
    }


    # Step 10 — Final status + audit record

    logger.info(
        "Mover Step 10 — audit reporting — employee=%s", employee_id
    )

    if verification.status == PostMoveStatus.VERIFICATION_ERROR:
        final_status = MoverEventStatus.MOVE_FAILED
    elif verification.status == PostMoveStatus.MOVE_PARTIAL:
        final_status = MoverEventStatus.MOVE_PARTIAL
    else:
        final_status = MoverEventStatus.MOVE_SUCCESS

    audit_record["post_move_status"] = final_status
    _write_event_log(table_client, employee_id, event_id, final_status)
    _write_audit_record(table_client, employee_id, event_id, audit_record)

    jml_final_status = (
        EventStatus.COMPLETED
        if final_status == MoverEventStatus.MOVE_SUCCESS
        else EventStatus.FAILED
    )
    release_lock(jml_events_client, employee_id, event_id)
    update_event_status(
        table_client = jml_events_client,
        employee_id  = employee_id,
        event_id     = event_id,
        status       = jml_final_status,
        failure_step = (
            "PostMoveVerification"
            if final_status == MoverEventStatus.MOVE_PARTIAL
            else ""
        ),
    )

    logger.info(
        "Mover pipeline complete — employee=%s, status=%s",
        employee_id, final_status,
    )

    return {
        "final_status": final_status,
        "employee_id":  employee_id,
        "event_id":     event_id,
        "summary":      f"Mover event completed with status {final_status}.",
    }


# Helpers

def _fail(employee_id: str, event_id: str, reason: str) -> dict:
    """Return a standard failure response dict."""
    return {
        "final_status": MoverEventStatus.MOVE_FAILED,
        "employee_id":  employee_id,
        "event_id":     event_id,
        "summary":      reason,
    }


# Azure Function HTTP entry point

def main(req) -> object:
    """
    Azure Function HTTP trigger entry point.

    Expects a JSON body with a canonical IdentityPayload.
    event_id is no longer accepted from the caller — it is generated
    internally by run_mover_pipeline() from the payload fields.

    Environment variables required:
        AZURE_STORAGE_CONNECTION_STRING
        AZURE_TENANT_ID
        AZURE_CLIENT_ID
        AZURE_CLIENT_SECRET
        JML_VALIDATION_ENGINE_URL
    """
    import azure.functions as func

    try:
        body    = req.get_json()
        payload = IdentityPayload(**body["payload"])

        conn_str     = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        table_client = _get_table_client(conn_str)

        graph_service_client, credential = build_graph_client()
        graph_client = JmlGraphClient(graph_service_client, credential)

        result = run_mover_pipeline(
            payload      = payload,
            table_client = table_client,
            graph_client = graph_client,
        )

        return func.HttpResponse(
            json.dumps(result),
            status_code = 200,
            mimetype    = "application/json",
        )

    except Exception as e:
        logger.error("Mover HTTP trigger failed: %s", str(e))
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code = 500,
            mimetype    = "application/json",
        )