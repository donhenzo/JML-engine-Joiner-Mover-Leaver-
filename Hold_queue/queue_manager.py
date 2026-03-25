"""
Hold_queue/queue_manager.py

Manages hold queue state transitions and record lifecycle.

Enforces valid state transitions. Logs all transitions. Persists records
to Azure Table Storage in production; uses in-memory dict for local/test.

Phase 0 scope: in-memory store with a swap interface for Table Storage.
The storage backend is injected so the manager logic is testable without
an Azure dependency.
"""

from __future__ import annotations
import json
import logging
import uuid
from datetime import datetime
from typing import Protocol

from Hold_queue.models import (
    APPROVABLE_STATES,
    TERMINAL_STATES,
    HoldRecord,
    HoldStatus,
)
from Ingestion.schema import IdentityPayload

logger = logging.getLogger(__name__)


# Storage backend protocol
class HoldQueueStore(Protocol):
    """
    Interface for hold queue persistence.

    Implement this for Azure Table Storage in production. The in-memory
    implementation below satisfies this protocol for local use and tests.
    """

    def save(self, record: HoldRecord) -> None: ...
    def get(self, record_id: str) -> HoldRecord | None: ...
    def list_by_status(self, status: HoldStatus) -> list[HoldRecord]: ...
    def list_by_employee(self, employee_id: str) -> list[HoldRecord]: ...



# In-memory store 
class InMemoryHoldQueueStore:
    """
    In-memory hold queue store for local development and testing.

    Not suitable for production — no persistence across function invocations.
    Swap with AzureTableHoldQueueStore when connecting to Azure Storage.
    """

    def __init__(self) -> None:
        self._records: dict[str, HoldRecord] = {}

    def save(self, record: HoldRecord) -> None:
        self._records[record.record_id] = record

    def get(self, record_id: str) -> HoldRecord | None:
        return self._records.get(record_id)

    def list_by_status(self, status: HoldStatus) -> list[HoldRecord]:
        return [r for r in self._records.values() if r.status == status]

    def list_by_employee(self, employee_id: str) -> list[HoldRecord]:
        return [r for r in self._records.values() if r.employee_id == employee_id]

    def all(self) -> list[HoldRecord]:
        """Return all records. For testing / inspection only."""
        return list(self._records.values())



# Valid state transitions
# Maps current state → set of valid next states.
# Any transition not in this map is rejected.
VALID_TRANSITIONS: dict[HoldStatus, set[HoldStatus]] = {
    HoldStatus.RECEIVED:             {HoldStatus.NORMALIZED, HoldStatus.NORMALIZATION_FAILED},
    HoldStatus.NORMALIZED:           {HoldStatus.HELD, HoldStatus.VALIDATION_FAILED},
    HoldStatus.NORMALIZATION_FAILED: {HoldStatus.HELD},
    HoldStatus.VALIDATION_FAILED:    {HoldStatus.HELD},
    HoldStatus.HELD:                 {HoldStatus.APPROVED, HoldStatus.FAILED},
    HoldStatus.APPROVED:             {HoldStatus.PROVISIONING},
    HoldStatus.PROVISIONING:         {HoldStatus.PROVISIONED, HoldStatus.FAILED},
    HoldStatus.PROVISIONED:          {HoldStatus.COMPLETED, HoldStatus.FAILED},
    HoldStatus.COMPLETED:            set(),  # Terminal
    HoldStatus.FAILED:               {HoldStatus.HELD},  # Can be reset for retry
}



# Queue manager
class HoldQueueManager:
    """
    Manages the hold queue state machine.

    Purpose:
        Create, transition, and retrieve hold records. Enforce valid state
        transitions. All changes are persisted via the injected store.

    Inputs:
        store: Any object satisfying the HoldQueueStore protocol.

    Security considerations:
        Manual overrides are explicitly flagged and must be traceable in
        the audit log. The manager records override metadata but does not
        make the provisioning decision — that responsibility stays in the
        calling layer.
    """

    def __init__(self, store: HoldQueueStore) -> None:
        self._store = store

    # Record creation 
    def create_from_parse_error(
        self,
        employee_id: str,
        upn: str,
        reasons: list[str],
        raw_row: dict | None = None,
    ) -> HoldRecord:
        """
        Create a hold record for a CSV parse failure.

        The record starts at RECEIVED and immediately transitions to HELD
        with the parse error reasons. Parse errors cannot proceed to
        normalization.
        """
        record = HoldRecord(
            record_id=_new_id(),
            employee_id=employee_id,
            upn=upn,
            status=HoldStatus.RECEIVED,
            failure_reasons=reasons,
            payload_snapshot=json.dumps(raw_row) if raw_row else None,
        )
        self._store.save(record)
        logger.info(
            "Hold record created from parse error: %s (employee=%s)",
            record.record_id,
            employee_id,
        )
        self.transition(record, HoldStatus.NORMALIZATION_FAILED)
        self.transition(record, HoldStatus.HELD)
        return record

    def create_from_normalization_failure(
        self,
        payload: IdentityPayload,
        reasons: list[str],
    ) -> HoldRecord:
        """
        Create a hold record for a normalization failure.

        Starts at RECEIVED → NORMALIZATION_FAILED → HELD.
        """
        record = HoldRecord(
            record_id=_new_id(),
            employee_id=payload.employee_id,
            upn=payload.upn,
            status=HoldStatus.RECEIVED,
            failure_reasons=reasons,
            payload_snapshot=_serialize_payload(payload),
        )
        self._store.save(record)
        self.transition(record, HoldStatus.NORMALIZATION_FAILED)
        self.transition(record, HoldStatus.HELD)
        logger.info(
            "Hold record created from normalization failure: %s (employee=%s)",
            record.record_id,
            payload.employee_id,
        )
        return record

    def create_from_validation_failure(
        self,
        payload: IdentityPayload,
        reasons: list[str],
    ) -> HoldRecord:
        """
        Create a hold record for a validation failure.

        Starts at RECEIVED → NORMALIZED → VALIDATION_FAILED → HELD.
        """
        record = HoldRecord(
            record_id=_new_id(),
            employee_id=payload.employee_id,
            upn=payload.upn,
            status=HoldStatus.RECEIVED,
            failure_reasons=reasons,
            payload_snapshot=_serialize_payload(payload),
        )
        self._store.save(record)
        self.transition(record, HoldStatus.NORMALIZED)
        self.transition(record, HoldStatus.VALIDATION_FAILED)
        self.transition(record, HoldStatus.HELD)
        logger.info(
            "Hold record created from validation failure: %s (employee=%s)",
            record.record_id,
            payload.employee_id,
        )
        return record

    # State transitions 
    def transition(self, record: HoldRecord, new_status: HoldStatus) -> None:
        """
        Transition a hold record to a new state.

        Purpose:
            Enforce valid state transitions and persist the change.

        Raises:
            ValueError if the transition is not permitted from the current state.
        """
        allowed = VALID_TRANSITIONS.get(record.status, set())
        if new_status not in allowed:
            raise ValueError(
                f"Invalid transition for record {record.record_id}: "
                f"{record.status.value} → {new_status.value}. "
                f"Allowed from {record.status.value}: "
                f"{[s.value for s in allowed]}"
            )

        old_status = record.status
        record.status = new_status
        record.last_updated = datetime.utcnow()
        self._store.save(record)

        logger.info(
            "Hold record %s: %s → %s (employee=%s)",
            record.record_id,
            old_status.value,
            new_status.value,
            record.employee_id,
        )

    def approve(
        self,
        record: HoldRecord,
        override_note: str | None = None,
    ) -> None:
        """
        Manually approve a held record for provisioning.

        Purpose:
            Allow an operator to release a held record. Sets manual_override
            flag so the audit log can trace the exception.

        Raises:
            ValueError if the record is not in an approvable state.
        """
        if record.status not in APPROVABLE_STATES:
            raise ValueError(
                f"Record {record.record_id} cannot be approved from state "
                f"'{record.status.value}'. Must be in: "
                f"{[s.value for s in APPROVABLE_STATES]}"
            )

        record.manual_override = True
        record.override_note = override_note
        self.transition(record, HoldStatus.APPROVED)

        logger.warning(
            "Hold record %s manually approved by operator "
            "(employee=%s, note=%r). "
            "This override must appear in the audit log.",
            record.record_id,
            record.employee_id,
            override_note,
        )

    def record_attempt(self, record: HoldRecord) -> None:
        """
        Record a provisioning attempt against a hold record.

        Increments retry_count and sets last_attempt timestamp.
        Does not change state — caller transitions to PROVISIONING separately.
        """
        record.retry_count += 1
        record.last_attempt = datetime.utcnow()
        record.last_updated = datetime.utcnow()
        self._store.save(record)

    # Queries 
    def get_held_records(self) -> list[HoldRecord]:
        """Return all records currently in the HELD state."""
        return self._store.list_by_status(HoldStatus.HELD)

    def get_failed_records(self) -> list[HoldRecord]:
        """Return all records in the FAILED state."""
        return self._store.list_by_status(HoldStatus.FAILED)

    def get_by_employee(self, employee_id: str) -> list[HoldRecord]:
        """Return all hold records for a given employee."""
        return self._store.list_by_employee(employee_id)



# Helpers
def _new_id() -> str:
    return str(uuid.uuid4())


def _serialize_payload(payload: IdentityPayload) -> str:
    """
    Serialize an IdentityPayload to JSON for storage in the hold record.

    Stores a snapshot so the record is self-contained for operator review
    even if the original CSV is no longer available.
    """
    return json.dumps(
        {
            "employee_id": payload.employee_id,
            "upn": payload.upn,
            "display_name": payload.display_name,
            "department": payload.department,
            "job_title": payload.job_title,
            "manager_id": payload.manager_id,
            "start_date": payload.start_date.isoformat(),
            "employment_type": payload.employment_type.value,
            "location": payload.location,
            "action": payload.action.value,
            "retain_roles": payload.retain_roles,
            "retain_list": payload.retain_list,
        }
    )