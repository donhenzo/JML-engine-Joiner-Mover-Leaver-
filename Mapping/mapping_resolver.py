# mapping/mapping_resolver.py
#
# Evaluates all mapping rules against a canonical IdentityPayload.
# Returns the union of matched entitlements across all matching rules,
# plus the rule IDs that matched — required for the audit trail.
#
# Rules are evaluated in priority order (ascending).
# All matching rules contribute entitlements — evaluation never stops
# at first match. This is what allows baseline + role rules to combine.

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class EntitlementResult:
    """
    Output of the resolver for a single identity.

    groups          — deduplicated list of group IDs to assign
    rbac_roles      — deduplicated list of RBAC role assignments
    matched_rule_ids — IDs of every rule that matched, in evaluation order
    """
    groups: list[str] = field(default_factory=list)
    rbac_roles: list[dict] = field(default_factory=list)
    matched_rule_ids: list[str] = field(default_factory=list)


def _condition_matches(condition: dict, identity_value: str | None) -> bool:
    """
    Evaluate a single rule condition against an identity field value.

    Only 'exact' operator is supported. Case-insensitive comparison.
    Returns False if the identity value is None or empty.
    """
    if not identity_value:
        return False

    operator = condition.get("operator")
    rule_value = condition.get("value", "")

    if operator == "exact":
        return identity_value.strip().lower() == rule_value.strip().lower()

    # Unknown operator — log and treat as no match rather than erroring.
    logger.warning(f"Unknown rule condition operator '{operator}' — skipping condition")
    return False


def _rule_matches(rule: dict, department: str | None, job_title: str | None, employment_type: str | None = None) -> bool:
    """
    Evaluate all conditions in a rule against the identity.
    All conditions must match for the rule to match (AND logic).
    A rule with no conditions never matches.
    """
    conditions = rule.get("conditions", {})

    if not conditions:
        logger.warning(f"Rule {rule.get('id')} has no conditions — skipping")
        return False

    if "department" in conditions:
        if not _condition_matches(conditions["department"], department):
            return False

    if "job_title" in conditions:
        if not _condition_matches(conditions["job_title"], job_title):
            return False

    # Employment type condition — allows rules to target Employee vs
    # Contractor vs Guest without relying on job title as a proxy.
    if "employment_type" in conditions:
        if not _condition_matches(conditions["employment_type"], employment_type):
            return False

    return True


def resolve_entitlements(
    rules:           list[dict],
    department:      str | None,
    job_title:       str | None,
    employment_type: str | None = None,
    employee_id:     str = ""
) -> EntitlementResult:
    """
    Evaluate all rules against an identity and return the union of
    matched entitlements.

    Inputs:
        rules           — sorted rule list from mapping_loader
        department      — canonical department value from IdentityPayload
        job_title       — canonical job title value from IdentityPayload
        employment_type — canonical employment type (Employee/Contractor/Guest)
        employee_id     — used only for log context

    Output:
        EntitlementResult with deduplicated groups, rbac_roles,
        and matched_rule_ids.

    No rules match → returns empty EntitlementResult. Not an error.
    The pipeline treats an empty result as a warning, not a failure.
    """
    result = EntitlementResult()

    # Accumulate before deduplication
    all_groups:     list[str]  = []
    all_rbac_roles: list[dict] = []

    for rule in rules:
        rule_id = rule.get("id", "UNKNOWN")

        if _rule_matches(rule, department, job_title, employment_type):
            entitlements = rule.get("entitlements", {})

            matched_groups = entitlements.get("groups", [])
            matched_roles  = entitlements.get("rbac_roles", [])

            all_groups.extend(matched_groups)
            all_rbac_roles.extend(matched_roles)
            result.matched_rule_ids.append(rule_id)

            logger.debug(
                f"Rule {rule_id} matched for employee {employee_id} — "
                f"groups: {matched_groups}, roles: {matched_roles}"
            )

    # Deduplicate groups — preserve first-seen order
    seen_groups: set[str] = set()
    for g in all_groups:
        if g not in seen_groups:
            result.groups.append(g)
            seen_groups.add(g)

    # Deduplicate RBAC roles — role + scope pair must both match
    seen_roles: set[tuple] = set()
    for r in all_rbac_roles:
        key = (r.get("role"), r.get("scope"))
        if key not in seen_roles:
            result.rbac_roles.append(r)
            seen_roles.add(key)

    if result.matched_rule_ids:
        logger.info(
            f"Entitlement resolution complete for employee {employee_id} — "
            f"matched rules: {result.matched_rule_ids}, "
            f"groups: {result.groups}, "
            f"rbac_roles: {len(result.rbac_roles)}"
        )
    else:
        logger.warning(
            f"No rules matched for employee {employee_id} — "
            f"department='{department}', job_title='{job_title}', "
            f"employment_type='{employment_type}'"
        )

    return result