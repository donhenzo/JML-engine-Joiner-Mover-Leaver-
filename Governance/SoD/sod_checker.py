# Governance/sod_checker.py
#
# WHY THIS EXISTS:
#   The entitlement resolver decides what groups a Joiner or Mover receives.
#   The SoD loader provides the conflict policy catalogue.
#   This file owns the evaluation — it is the only place SoD intersection
#   logic exists. Called by the Joiner pipeline, the Mover pipeline, and
#   the reconciliation function. Never duplicated across runtimes.
#
# EVALUATION MODEL:
#   effective_access = current_groups ∪ requested_groups
#
#   This models the projected post-change state, not just the delta.
#   For Joiner: current_groups is always empty — effective_access equals
#   requested_groups only.
#   For Mover: current_groups reflects actual tenant membership before the
#   delta is applied. This is where real SoD violations hide — a role change
#   can create a conflict with access the user already holds.
#
# FAIL-CLOSED CONTRACT:
#   If current_groups_fetch_status is not COMPLETE and the context is
#   MOVER or LEAVER, this module returns a block immediately without
#   evaluating policies. A false block is recoverable via human review.
#   A missed SoD violation is not recoverable from an audit perspective.
#
# MULTI-VIOLATION:
#   Evaluation never stops at the first violation. All policies are
#   evaluated before the final action is resolved. The audit report
#   must show the complete picture — stopping early is a security gap
#   dressed as an optimisation.

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .sod_models import (
    DetectionPath,
    EvaluationContext,
    FetchStatus,
    SoDAction,
    SoDMode,
    SoDPolicy,
    SoDViolation,
    SoDCheckResult,
)

logger = logging.getLogger(__name__)


def evaluate_sod(
    requested_groups:             list[str],
    current_groups:               list[str],
    current_groups_fetch_status:  FetchStatus,
    policies:                     list[SoDPolicy],
    context:                      EvaluationContext,
    detection_path:               DetectionPath,
) -> SoDCheckResult:
    """
    Evaluate SoD policies against the projected effective access for an identity.

    Inputs:
        requested_groups            — groups the identity will receive after
                                      this operation. From mapping_resolver.
        current_groups              — groups the identity currently holds in
                                      the tenant. Empty list for Joiner.
        current_groups_fetch_status — whether the Graph API call to fetch
                                      current membership was reliable.
                                      Always pass FetchStatus.COMPLETE for
                                      Joiner — no fetch is needed.
        policies                    — validated SoDPolicy list from sod_loader.
                                      Caller is responsible for loading.
        context                     — lifecycle stage triggering this evaluation.
                                      Written to every SoDViolation.
        detection_path              — pipeline point where evaluation runs.
                                      Written to every SoDViolation.

    Output:
        SoDCheckResult with all violations found and a resolved final_action.
        final_action is one of: "block" | "warn" | "clean"

    Side effects:
        Logs evaluation summary at INFO level.
        Logs each violation at WARNING level.
        Logs data-quality blocks at ERROR level.
    """
    # Fail-closed gate
    #
    # A degraded or failed membership fetch means effective_access cannot
    # be guaranteed complete. For Mover and Leaver, an incomplete
    # effective_access can miss real violations, it should block immediately.
    #
    # Joiner is exempt: current_groups is always empty by definition.
    # FetchStatus is irrelevant for Joiner and the gate is never triggered.
    #
    if (
        current_groups_fetch_status != FetchStatus.COMPLETE
        and context in (EvaluationContext.MOVER, EvaluationContext.LEAVER)
    ):
        logger.error(
            "SoD evaluation blocked — current_groups fetch was %s for "
            "context=%s. Cannot guarantee effective_access is complete. "
            "Fail-closed: returning block without evaluating policies.",
            current_groups_fetch_status.value,
            context.value,
        )
        return SoDCheckResult(
            violations=[],
            final_action="block",
            has_exceptions=False,
            blocked_reason="current_groups_fetch_degraded",
        )

    # Build effective access 
    effective_access: set[str] = set(current_groups) | set(requested_groups)

    logger.info(
        "SoD evaluation started — context=%s, detection_path=%s, "
        "requested=%d groups, current=%d groups, effective=%d groups, "
        "policies=%d",
        context.value,
        detection_path.value,
        len(requested_groups),
        len(current_groups),
        len(effective_access),
        len(policies),
    )

    # Policy evaluation 
    violations: list[SoDViolation] = []

    for policy in policies:

        # Defensive guard — loader should have rejected unimplemented modes,
        # but a SoDPolicy constructed directly in tests could bypass the loader.
        if policy.mode != SoDMode.ANY_TO_ANY:
            logger.warning(
                "SoD policy '%s' uses mode '%s' which is not implemented "
                "in sod_checker.py — skipping. This policy should have been "
                "rejected by sod_loader.py.",
                policy.id,
                policy.mode.value,
            )
            continue

        violation = _evaluate_any_to_any(
            policy=policy,
            effective_access=effective_access,
            context=context,
            detection_path=detection_path,
        )

        if violation:
            violations.append(violation)
            logger.warning(
                "SoD violation detected — policy=%s (%s), action=%s, "
                "matched_a=%s, matched_b=%s, context=%s",
                policy.id,
                policy.name,
                policy.action.value,
                violation.matched_a,
                violation.matched_b,
                context.value,
            )

    #  Resolve final action across all violations found.
    result = _resolve_final_action(violations)

    logger.info(
        "SoD evaluation complete — context=%s, violations=%d, "
        "final_action=%s, has_exceptions=%s",
        context.value,
        len(violations),
        result.final_action,
        result.has_exceptions,
    )

    return result


# Internal helpers 

def _evaluate_any_to_any(
    policy:           SoDPolicy,
    effective_access: set[str],
    context:          EvaluationContext,
    detection_path:   DetectionPath,
) -> SoDViolation | None:
    """
    Evaluate a single ANY_TO_ANY policy against the effective access set.

    A violation fires if the identity holds at least one group from set_a
    AND at least one group from set_b simultaneously.

    Returns a SoDViolation if a conflict is detected, None otherwise.
    The exception stub is called here (see _exception_exists() for detail.)
    """
    matched_a = [g for g in policy.set_a if g in effective_access]
    matched_b = [g for g in policy.set_b if g in effective_access]

    if not matched_a or not matched_b:
        # No conflict — at least one side of the policy has no match.
        return None

    exception_applied = _exception_exists(
        policy_id=policy.id,
        context=context,
    )

    return SoDViolation(
        policy_id=            policy.id,
        policy_name=          policy.name,
        risk_rating=          policy.risk_rating,
        action=               policy.action,
        matched_a=            matched_a,
        matched_b=            matched_b,
        conflicting_groups=   matched_a + matched_b,
        evaluation_context=   context,
        detection_path=       detection_path,
        detected_at=          datetime.now(timezone.utc),
        compensating_control= policy.compensating_control,
        exception_applied=    exception_applied,
    )


def _resolve_final_action(violations: list[SoDViolation]) -> SoDCheckResult:
    """
    Resolve the final action across all violations found.

    Active violations are those where exception_applied is False.
    Exception-applied violations do not contribute to final_action —
    they are flagged in the audit report but do not block provisioning.

    Resolution rules (evaluated in order):
        1. Any active BLOCK violation  → final_action = "block"
        2. Any active WARN violation   → final_action = "warn"
        3. No active violations        → final_action = "clean"

    A single BLOCK violation blocks the entire operation regardless of
    how many WARN violations accompany it.
    """
    has_exceptions = any(v.exception_applied for v in violations)
    active = [v for v in violations if not v.exception_applied]

    if any(v.action == SoDAction.BLOCK for v in active):
        final_action = "block"
    elif any(v.action == SoDAction.WARN for v in active):
        final_action = "warn"
    else:
        final_action = "clean"

    return SoDCheckResult(
        violations=violations,
        final_action=final_action,
        has_exceptions=has_exceptions,
    )


def _exception_exists(
    policy_id: str,
    context:   EvaluationContext,
) -> bool:
    """
    Stub. Returns False unconditionally.

    WHY THIS IS A STUB:
        SoD exception management has two distinct layers. The first is
        detection — identifying a conflict, blocking provisioning, and
        recording the violation in the audit trail. This module fully
        implements the detection layer.

        The second layer is the exception workflow — the approval process
        that permits a specific identity to hold a conflicting entitlement
        under defined conditions. This layer is deliberately deferred.

        In production SoD implementations, the exception workflow is the
        most politically complex component of the programme. The following
        questions must be answered at the organisational level before the
        technical store can be built meaningfully:
            — Who has authority to approve exceptions?
            — What justification is required?
            — How long does an exception remain valid?
            — Who reviews the approvers?

        Building the technical store before that governance conversation
        has happened produces a store with no controlled input process,
        which is worse than no store at all.

    WHAT A REAL IMPLEMENTATION REQUIRES:
        A real implementation queries an exception store (Azure Table
        Storage) for a valid, unexpired exception record matching this
        policy_id and identity. Exception records carry:
            granted_by, granted_at, expires_at,
            business_justification, approver_id, status (Active | Expired | Revoked)

        Expiry must be checked in application code — Azure Table Storage
        has no native TTL. Expired records return False here even if the
        row exists. A nightly cleanup function marks expired records so
        the audit trail is preserved.

        The system that detects violations must not be able to write its
        own exception records — separation of duties applies to the
        exception workflow itself.

    WHAT THE STUB PRESERVES:
        Every violation is detected correctly, recorded in the audit
        report with full detail, and blocks provisioning on hard
        violations. When the exception workflow is built, this function
        is replaced with a real implementation. No other module changes.

    
    """
    return False