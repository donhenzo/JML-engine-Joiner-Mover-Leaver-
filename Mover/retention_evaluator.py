"""

Evaluates retention records for groups flagged for removal in a Mover event.

Splits groups_to_remove from MoverDelta into two sets:
    - remove_confirmed: groups with no retention record, or an expired one
    - retain_set:       groups with a valid, unexpired retention record

I/O is isolated to fetch_retention_record(). The decision logic in
evaluate_retention() is pure and unit-testable without mocking Table Storage.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Optional

from azure.data.tables import TableServiceClient



# Data models

class RetentionOutcome(Enum):
    RETAINED            = "RETAINED"             # valid record, date in future
    EXPIRED             = "EXPIRED"              # record exists but date has passed
    NO_RECORD           = "NO_RECORD"            # nothing in RetentionRegistry


@dataclass(frozen=True)
class RetentionRecord:
    """
    A single entry from the RetentionRegistry table in Azure Table Storage.

    Fields mirror the ADR-002 schema exactly.
    This object is constructed from a raw Table Storage entity dict
    by fetch_retention_record() and passed into evaluate_retention().

    Fields:
        employee_id:      PartitionKey — HR source identifier
        group_object_id:  RowKey — Entra group object ID
        granted_by:       UPN of the approver who created the retention
        granted_date:     ISO-8601 date the retention was created
        review_date:      ISO-8601 expiry date — retention is valid up to and including this date
        reason:           Business justification string
        source:           MANUAL | ACCESS_REQUEST | EXCEPTION
        retained_through: event_id of the move that triggered this retention decision
    """
    employee_id:      str
    group_object_id:  str
    granted_by:       str
    granted_date:     date
    review_date:      date
    reason:           str
    source:           str
    retained_through: str


@dataclass
class GroupRetentionDecision:
    """
    The outcome of evaluating a single group against the RetentionRegistry.

    Fields:
        group_id:   Entra group object ID
        outcome:    RetentionOutcome enum value
        record:     The RetentionRecord if one existed, None otherwise.
                    Preserved for audit trail regardless of outcome.
    """
    group_id: str
    outcome:  RetentionOutcome
    record:   Optional[RetentionRecord]


@dataclass
class RetentionResult:
    """
    The complete output of evaluate_all_retentions().

    Fields:
        remove_confirmed: Groups with no record or an expired record.
                          These proceed to Step 6 (access removal).
        retain_set:       Groups with a valid, unexpired retention record.
                          These are excluded from removal and carried into
                          the SoD re-evaluation as part of post-move access.
        decisions:        Full per-group decision list for the audit record.
                          Every group in groups_to_remove gets an entry here.
    """
    remove_confirmed: frozenset[str]
    retain_set:       frozenset[str]
    decisions:        list[GroupRetentionDecision]



# Table Storage fetch

def fetch_retention_record(
    employee_id:     str,
    group_object_id: str,
    table_client:    TableServiceClient,
    table_name:      str = "RetentionRegistry",
) -> Optional[RetentionRecord]:
    """
    Fetch a single retention record from Azure Table Storage.

    PartitionKey = employee_id
    RowKey       = group_object_id

    Returns a RetentionRecord if found, None if no entry exists.

    Args:
        employee_id:     HR source identifier for the employee.
        group_object_id: Entra group object ID being checked.
        table_client:    Injected TableServiceClient — not created internally.
        table_name:      Table name, defaults to RetentionRegistry.

    Side effects:
        One GET request to Azure Table Storage per call.
    """
    try:
        client = table_client.get_table_client(table_name)
        entity = client.get_entity(
            partition_key=employee_id,
            row_key=group_object_id,
        )

        return RetentionRecord(
            employee_id      = entity["PartitionKey"],
            group_object_id  = entity["RowKey"],
            granted_by       = entity["granted_by"],
            granted_date     = date.fromisoformat(entity["granted_date"]),
            review_date      = date.fromisoformat(entity["review_date"]),
            reason           = entity["reason"],
            source           = entity["source"],
            retained_through = entity.get("retained_through", ""),
        )

    except Exception:
        # No record found — this is the expected path for most groups.
        return None



# Decision logic
def evaluate_retention(
    group_id: str,
    record:   Optional[RetentionRecord],
    today:    date,
) -> GroupRetentionDecision:
    """
    Apply retention decision logic for a single group.

    Pure function — no I/O. Takes a record (or None) and today's date,
    returns a GroupRetentionDecision.

    Args:
        group_id: Entra group object ID being evaluated.
        record:   RetentionRecord from Table Storage, or None if no entry exists.
        today:    The reference date for expiry comparison.
                  Passed in explicitly so tests can control it without mocking.

    Returns:
        GroupRetentionDecision with outcome RETAINED, EXPIRED, or NO_RECORD.
    """
    if record is None:
        return GroupRetentionDecision(
            group_id=group_id,
            outcome=RetentionOutcome.NO_RECORD,
            record=None,
        )

    if record.review_date >= today:
        return GroupRetentionDecision(
            group_id=group_id,
            outcome=RetentionOutcome.RETAINED,
            record=record,
        )

    # Record exists but review_date has passed — retention expired.
    return GroupRetentionDecision(
        group_id=group_id,
        outcome=RetentionOutcome.EXPIRED,
        record=record,
    )


# Orchestration
def evaluate_all_retentions(
    employee_id:    str,
    groups_to_remove: frozenset[str],
    table_client:   TableServiceClient,
    today:          Optional[date] = None,
    table_name:     str = "RetentionRegistry",
) -> RetentionResult:
    """
    Evaluate retention records for every group flagged for removal.

    Calls fetch_retention_record() and evaluate_retention() per group,
    then splits results into remove_confirmed and retain_set.

    Args:
        employee_id:      HR source identifier — used as PartitionKey.
        groups_to_remove: groups_to_remove from MoverDelta.
        table_client:     Injected TableServiceClient.
        today:            Reference date for expiry checks. Defaults to
                          date.today() if not provided. Pass explicitly in tests.
        table_name:       RetentionRegistry table name.

    Returns:
        RetentionResult with remove_confirmed, retain_set, and full decisions list.

    Side effects:
        One Table Storage GET per group in groups_to_remove.
    """
    if today is None:
        today = date.today()

    decisions:        list[GroupRetentionDecision] = []
    remove_confirmed: set[str] = set()
    retain_set:       set[str] = set()

    for group_id in groups_to_remove:
        record = fetch_retention_record(
            employee_id=employee_id,
            group_object_id=group_id,
            table_client=table_client,
            table_name=table_name,
        )

        decision = evaluate_retention(
            group_id=group_id,
            record=record,
            today=today,
        )

        decisions.append(decision)

        if decision.outcome == RetentionOutcome.RETAINED:
            retain_set.add(group_id)
        else:
            # NO_RECORD and EXPIRED both result in removal.
            remove_confirmed.add(group_id)

    return RetentionResult(
        remove_confirmed=frozenset(remove_confirmed),
        retain_set=frozenset(retain_set),
        decisions=decisions,
    )