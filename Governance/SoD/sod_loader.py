# Governance/sod_loader.py
#
# Loads the SoD policy catalogue from disk.
# Validates every policy object and returns a clean list of SoDPolicy.
#
# Phase 1 loads from disk. Azure Storage path added when the
# Function app is deployed — same pattern as mapping_loader.py
# and lookup_loader.py.

from __future__ import annotations

import json
import logging
from pathlib import Path

from .sod_models import FetchStatus, RiskRating, SoDAction, SoDMode, SoDPolicy

logger = logging.getLogger(__name__)

# Implemented modes
#
# Only modes listed here are accepted by the loader.
# ALL_TO_ANY and THRESHOLD are defined in SoDMode but not yet implemented
# in sod_checker.py. Any policy using those modes raises a ValueError at
# load time — it will not silently pass through and do nothing.
#
# When a new mode is implemented in sod_checker.py, add it here.
# The loader then accepts it automatically — no other change required.
#
IMPLEMENTED_MODES: frozenset[SoDMode] = frozenset({SoDMode.ANY_TO_ANY})


def load_sod_policies(policies_path: str) -> list[SoDPolicy]:
    """
    Load and validate the SoD policy catalogue from a JSON file.

    Inputs:
        policies_path — path to sod_policies.json

    Output:
        Validated list of SoDPolicy objects. Order matches the file.

    Raises:
        FileNotFoundError  if the path does not exist.
        ValueError         if the JSON is structurally invalid, any required
                           field is missing, any policy fails validation, or
                           any policy uses an unimplemented mode.
        json.JSONDecodeError  if the file is not valid JSON.
    """
    path = Path(policies_path)

    if not path.exists():
        raise FileNotFoundError(
            f"SoD policy file not found: {policies_path}"
        )

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if "policies" not in raw:
        raise ValueError(
            f"SoD policy file is missing top-level 'policies' key: {policies_path}"
        )

    entries = raw["policies"]

    if not entries:
        raise ValueError(
            f"SoD policy file contains an empty policy list: {policies_path}"
        )

    policies: list[SoDPolicy] = []
    seen_ids: set[str] = set()

    for entry in entries:
        policy = _validate_and_build(entry, seen_ids, policies_path)
        policies.append(policy)

    logger.info(
        "SoD policies loaded — %d policies from %s",
        len(policies),
        policies_path,
    )

    return policies


# Internal validation 

def _validate_and_build(
    entry: dict,
    seen_ids: set[str],
    source: str,
) -> SoDPolicy:
    """
    Validate a single raw policy dict and construct a SoDPolicy.

    All validation errors name the policy ID (or '<missing id>') and
    the source file so the operator knows exactly which entry failed
    and where to find it.

    Raises:
        ValueError on any validation failure.
    """
    # id
    policy_id = entry.get("id", "").strip()
    if not policy_id:
        raise ValueError(
            f"SoD policy in '{source}' is missing required field: 'id'"
        )

    if policy_id in seen_ids:
        raise ValueError(
            f"SoD policy '{policy_id}' in '{source}' is a duplicate. "
            "Policy IDs must be unique across the file."
        )
    seen_ids.add(policy_id)

    # name
    name = entry.get("name", "").strip()
    if not name:
        raise ValueError(
            f"SoD policy '{policy_id}' in '{source}' is missing required field: 'name'"
        )

    # description 
    description = entry.get("description", "").strip()
    if not description:
        raise ValueError(
            f"SoD policy '{policy_id}' in '{source}' is missing required field: 'description'"
        )

    # risk_rating 
    risk_raw = entry.get("risk_rating", "")
    try:
        risk_rating = RiskRating(risk_raw)
    except ValueError:
        valid = [r.value for r in RiskRating]
        raise ValueError(
            f"SoD policy '{policy_id}' in '{source}' has invalid 'risk_rating': "
            f"'{risk_raw}'. Must be one of {valid}."
        )

    # mode
    mode_raw = entry.get("mode", "")
    try:
        mode = SoDMode(mode_raw)
    except ValueError:
        valid = [m.value for m in SoDMode]
        raise ValueError(
            f"SoD policy '{policy_id}' in '{source}' has invalid 'mode': "
            f"'{mode_raw}'. Must be one of {valid}."
        )

    if mode not in IMPLEMENTED_MODES:
        implemented = [m.value for m in IMPLEMENTED_MODES]
        raise ValueError(
            f"SoD policy '{policy_id}' in '{source}' uses mode '{mode.value}' "
            f"which is defined but not yet implemented in sod_checker.py. "
            f"Currently implemented modes: {implemented}. "
            "Update IMPLEMENTED_MODES in sod_loader.py when the mode is built."
        )

    # set_a
    set_a = entry.get("set_a", [])
    if not set_a:
        raise ValueError(
            f"SoD policy '{policy_id}' in '{source}' has an empty 'set_a'. "
            "Both sets must contain at least one group."
        )

    # set_b
    set_b = entry.get("set_b", [])
    if not set_b:
        raise ValueError(
            f"SoD policy '{policy_id}' in '{source}' has an empty 'set_b'. "
            "Both sets must contain at least one group."
        )

    # set overlap 
    #
    # A group that appears in both set_a and set_b means the policy fires
    # the moment any identity holds that single group — regardless of what
    # else they have. That is never a meaningful SoD signal.
    #
    overlap = set(set_a) & set(set_b)
    if overlap:
        raise ValueError(
            f"SoD policy '{policy_id}' in '{source}' has groups that appear "
            f"in both set_a and set_b: {sorted(overlap)}. "
            "A group in both sets cannot produce a clean violation signal. "
            "Remove it from one set."
        )

    # action 
    action_raw = entry.get("action", "")
    try:
        action = SoDAction(action_raw)
    except ValueError:
        valid = [a.value for a in SoDAction]
        raise ValueError(
            f"SoD policy '{policy_id}' in '{source}' has invalid 'action': "
            f"'{action_raw}'. Must be one of {valid}."
        )

    # compensating_control — optional, nullable
    compensating_control = entry.get("compensating_control") or None

    return SoDPolicy(
        id=policy_id,
        name=name,
        description=description,
        risk_rating=risk_rating,
        mode=mode,
        set_a=set_a,
        set_b=set_b,
        action=action,
        compensating_control=compensating_control,
    )