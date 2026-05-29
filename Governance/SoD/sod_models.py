"""
Governance/sod_models.py

Data structures for the Separation of Duties (SoD) evaluation layer.

This module contains enums and dataclasses only. No logic, no I/O,
no Graph API calls. Every other SoD module imports from here.

Design note — why str, Enum:
    Inheriting from str means enum members serialise directly to their
    string value in JSON output and audit reports without a custom
    encoder. Consistent with JmlAction and EmploymentType in schema.py.

Design note — why enums for controlled vocabularies:
    Every field that has a fixed set of valid values uses an enum.
    An invalid value raises a ValueError at construction time, not
    silently downstream. This is the same principle that makes
    IdentityPayload a dataclass rather than a plain dict.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


# Enums 

class RiskRating(str, Enum):
    """Severity classification of a SoD policy violation."""
    LOW      = "Low"
    MEDIUM   = "Medium"
    HIGH     = "High"
    CRITICAL = "Critical"


class EvaluationContext(str, Enum):
    """
    The lifecycle stage that triggered the SoD evaluation.

    Written to every SoDViolation so an auditor reading the violation
    record knows whether the conflict was detected at onboarding,
    during a role change, at offboarding, or via a background scan.
    The risk profile of the same conflict differs across these contexts.
    """
    JOINER         = "Joiner"
    MOVER          = "Mover"
    LEAVER         = "Leaver"
    RECONCILIATION = "Reconciliation"


class SoDAction(str, Enum):
    """
    The enforcement action taken when a policy violation is detected.

    Block  — provisioning halts. Record routes to hold queue
             (ValidationFailed → Held). Operator review required.
    Warn   — provisioning continues. Violation is recorded in the
             audit report warnings and sod_violations fields.
    """
    BLOCK = "Block"
    WARN  = "Warn"


class SoDMode(str, Enum):
    """
    The intersection logic used to evaluate a policy against
    an identity's effective access.

    ANY_TO_ANY  — violation if the identity holds any group from
                  set_a AND any group from set_b. Implemented.

    ALL_TO_ANY  — violation only if the identity holds ALL groups
                  from set_a AND any group from set_b. Not yet
                  implemented. sod_loader.py rejects policies using
                  this mode until it is built in sod_checker.py.

    THRESHOLD   — violation if the identity holds more than N groups
                  from a defined sensitive set. Requires a different
                  policy schema (single set + numeric threshold).
                  Not yet implemented.

    When ALL_TO_ANY or THRESHOLD are implemented, add them to
    IMPLEMENTED_MODES in sod_loader.py. The loader then accepts
    those modes automatically — no other change required.
    """
    ANY_TO_ANY = "AnyToAny"
    ALL_TO_ANY = "AllToAny"
    THRESHOLD  = "Threshold"


class FetchStatus(str, Enum):
    """
    Signals whether the Graph API call to fetch an identity's current
    group membership returned a complete and reliable result.

    Used by sod_checker.py to enforce the fail-closed contract:
    if current_groups cannot be trusted for a Mover or Leaver event,
    SoD evaluation blocks rather than proceeding on incomplete data.

    Complete  — full membership returned, no errors.
    Degraded  — partial results returned, no exception raised.
                effective_access cannot be guaranteed complete.
    Failed    — Graph call raised an exception or timed out.
                current_groups must be treated as empty.
    """
    COMPLETE  = "Complete"
    DEGRADED  = "Degraded"
    FAILED    = "Failed"


class DetectionPath(str, Enum):
    """
    The point in the pipeline where the SoD violation was detected.

    PreProvision   — detected before any Entra ID object was created
                     or modified. Provisioning was blocked.
    PostProvision  — detected after provisioning completed, during
                     the post-provision validation pass.
    Reconciliation — detected by the nightly reconciliation scan
                     against actual tenant state.
    """
    PRE_PROVISION  = "PreProvision"
    POST_PROVISION = "PostProvision"
    RECONCILIATION = "Reconciliation"


# Dataclasses

@dataclass
class SoDPolicy:
    """
    A single SoD conflict rule loaded from sod_policies.json.

    Each policy defines two sets of groups that must never coexist
    on the same identity's effective access. The mode field controls
    how the intersection is evaluated.

    compensating_control is surfaced in the audit report to tell a
    reviewer what must be verified before an exception can be approved.
    The engine does not evaluate it — it is human-readable guidance only.
    """
    id:                   str                # Unique policy identifier e.g. "SOD-001"
    name:                 str                # Short human-readable name
    description:          str                # Full description of the conflict
    risk_rating:          RiskRating         # LOW / MEDIUM / HIGH / CRITICAL
    mode:                 SoDMode            # Intersection logic — see SoDMode
    set_a:                list[str]          # First group set — must be non-empty
    set_b:                list[str]          # Second group set — must be non-empty
    action:               SoDAction          # BLOCK or WARN on violation
    compensating_control: Optional[str]      # Guidance for exception reviewers; nullable


@dataclass
class SoDViolation:
    """
    A single SoD policy violation detected during evaluation.

    Records exactly which groups triggered the conflict, where in the
    pipeline the detection occurred, and whether an exception was
    applied. Written to the audit report regardless of whether the
    violation blocked provisioning or produced a warning.

    Design note — exceptions do not suppress violations:
        exception_applied = True signals that a pre-registered
        exception was found for this policy and identity. The violation
        still appears in the audit report with full detail. The
        exception explains why it was permitted — it does not erase
        the compliance record.
    """
    policy_id:            str                # Matches SoDPolicy.id
    policy_name:          str                # Matches SoDPolicy.name
    risk_rating:          RiskRating         # Inherited from the policy
    action:               SoDAction          # Inherited from the policy
    matched_a:            list[str]          # Actual groups matched within set_a
    matched_b:            list[str]          # Actual groups matched within set_b
    conflicting_groups:   list[str]          # Union of matched_a + matched_b
    evaluation_context:   EvaluationContext  # JOINER / MOVER / LEAVER / RECONCILIATION
    detection_path:       DetectionPath      # PRE_PROVISION / POST_PROVISION / RECONCILIATION
    detected_at:          datetime           # UTC timestamp of detection
    compensating_control: Optional[str]      # Surfaced from the policy; nullable
    exception_applied:    bool = False       # True if a valid exception record was found


@dataclass
class SoDCheckResult:
    """
    The complete output of a single sod_checker.evaluate_sod() call.

    violations contains every violation found across all policies —
    evaluation never stops at the first violation. The full list is
    required so the audit report shows the complete picture.

    final_action is resolved across all active violations (violations
    where exception_applied is False):
        "block" — at least one active BLOCK violation present
        "warn"  — active WARN violations only, no BLOCK
        "clean" — no active violations

    blocked_reason is only populated when a block is caused by a data
    quality failure (e.g. degraded Graph fetch) rather than a policy
    violation. This distinguishes an infrastructure failure from a
    governance decision in the audit report.
    """
    violations:     list[SoDViolation]  # All violations found — never truncated
    final_action:   str                 # "block" | "warn" | "clean"
    has_exceptions: bool                # True if any violation has exception_applied
    blocked_reason: Optional[str] = None  # Populated on data-quality blocks only