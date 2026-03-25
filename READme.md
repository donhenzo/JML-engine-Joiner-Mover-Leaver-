# JML Identity Lifecycle Engine
### Joiner · Mover · Leaver — Azure / Entra ID

A policy-driven Identity Lifecycle Management engine for Microsoft Entra ID, built on Azure Functions and the Microsoft Graph API. Designed to demonstrate enterprise IAM engineering — deterministic provisioning, governance validation, and full auditability across the identity lifecycle.

| Phase   | Capability                                                        | Status    |
| ------- | ----------------------------------------------------------------- | --------- |
| Phase 0 | Data contracts, normalization, event store, governance validation | Complete  |
| Phase 1 | Joiner provisioning pipeline                                      | Complete  |
| Phase 2 | PIM eligible role assignment                                      | In design |
| Phase 3 | Mover access recalculation                                        | In design |
| Phase 4 | Leaver deprovisioning                                             | In design |

---

## Why This Project Exists

Identity lifecycle automation in many environments is implemented through ad-hoc scripts or workflow automation tools with limited transparency into how access decisions are made.

This project explores an alternative model: treating identity lifecycle management as a deterministic pipeline with explicit data contracts, policy evaluation, and audit reporting.

The goal is not to replicate existing products, but to demonstrate how an IAM engineering team might build a lifecycle engine with strong governance guarantees.


## System Summary

**Input:** HR identity events (CSV / API)

**Processing:**
- Canonical normalization of raw HR field values
- Pre-provision governance validation gate
- Entitlement rule evaluation against identity attributes
- Idempotent event processing via deterministic event store

**Output:**
- Entra ID user provisioning
- Security group, licence group, and Azure RBAC assignment
- Immutable per-identity audit report

---

## What This Is

This repository contains an Identity Lifecycle Management (JML) engine for Microsoft Entra ID, implemented as a policy-driven provisioning pipeline running on Azure Functions. The system processes identity lifecycle events from an upstream HR source and deterministically converts them into provisioning actions, governance validation checks, and auditable identity state changes within Entra ID.

Rather than relying on low-code workflow tooling, the engine implements lifecycle management as a structured pipeline with explicit contracts between stages:

1. A canonical identity schema for all lifecycle events
2. A normalization layer that resolves raw HR data into controlled values
3. A policy rules engine that maps identity attributes to entitlements
4. An event store providing idempotency and concurrency control
5. Pre- and post-provision governance validation gates
6. An immutable per-identity audit report

The goal of the project is to model how an IAM engineering team might implement identity lifecycle management as a deterministic system, rather than as ad-hoc scripts or workflow automation.

---

## Design Principles

**Deterministic provisioning**
Every lifecycle event produces a predictable, repeatable result driven by policy — not by runtime logic that can diverge.

**Idempotent execution**
Repeated execution of the same event produces no duplicate actions. The same input always resolves to the same EventId and exits cleanly if already processed.

**Explicit governance gates**
Provisioning cannot occur without passing pre-provision policy validation. The validation engine is a hard gate, not a post-hoc check.

**Separation of concerns**
Each pipeline stage owns a single responsibility. Normalization does not provision. Provisioning does not validate. The audit layer does not make decisions.

**Complete auditability**
Every lifecycle decision produces an immutable JSON report capturing actions taken, validation outcomes, rule IDs, and failure reasons — regardless of whether the event succeeded or was held.

---

## Example Use Cases

- Automated onboarding triggered by HR identity events
- Deterministic access provisioning based on role and department attributes
- Governance validation before Entra ID account creation
- Lifecycle audit reporting for compliance and access review

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        INPUT LAYER                              │
│   HR CSV / API Feed → CSV Parser → Canonical IdentityPayload    │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                    NORMALIZATION LAYER                          │
│   Canonical Lookup Table (Azure Storage JSON)                   │
│   Raw field values → Standardised department / job title        │
│   Unknown values → Hold Queue (not provisioning)                │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│               PRE-PROVISION VALIDATION GATE                     │
│   Identity Governance Validation Engine (PowerShell)            │
│   Payload mode — evaluates canonical object before any          │
│   Entra ID object exists. MUST PASS to proceed.                 │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                     EVENT STORE                                 │
│   Azure Table Storage — JmlEvents table                         │
│   Deterministic SHA-256 EventId · Optimistic concurrency        │
│   Status: Pending → Processing → Completed / Failed             │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                  PROVISIONING LAYER (Phase 1)                   │
│   mapping_resolver.py evaluates Rules.json against payload      │
│   Graph API: Create user · Assign groups · Assign RBAC roles    │
│   All operations are idempotent                                 │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│             POST-PROVISION VALIDATION GATE                      │
│   Validation engine re-runs against actual Entra ID state       │
│   Confirms provisioned state matches expected entitlements      │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                       AUDIT LAYER                               │
│   Per-identity JSON decision report written on every outcome    │
│   Captures: actions taken · validation status · hold reasons    │
│   Immutable. One file per identity event.                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Joiner Provisioning Flow

```
CSV Input / HR API Input
    │
    ▼
Parse & validate structure
    │  ── Structural failure ──► Hold Queue (NormalizationFailed → Held)
    ▼
Normalization Layer
    │  ── Unknown field value ──► Hold Queue (NormalizationFailed → Held)
    ▼
Pre-Provision Validation Gate  ◄── MUST PASS
    │  ── Validation failure ──► Hold Queue (ValidationFailed → Held)
    ▼
Claim Event in JmlEvents store
    │  ── Duplicate EventId ──► Exit cleanly (idempotent)
    ▼
mapping_resolver.py → Resolve entitlements from Rules.json
    ▼
Graph API Provisioning
    ├── Create Entra ID user
    ├── Assign security groups (SG_*, LIC_*, CA_*)
    └── Assign Azure RBAC roles via groups
    │  ── Partial failure ──► ActionsTaken recorded · Event.Status = Failed
    ▼
Post-Provision Validation Gate
    ▼
Audit Report written (JSON, per identity)
```

---

## Key Engineering Decisions

### Policy Rules Engine — Not a Static Lookup Table

Entitlements are resolved by evaluating externally loaded rule objects against the canonical identity payload. `mapping_resolver.py` evaluates all matching rules and unions the resulting entitlements. Adding a new role mapping is a JSON edit — no redeployment.

```json
{
  "id": "RULE-017",
  "description": "Sales Manager baseline access",
  "conditions": {
    "department": { "equals": "Sales" },
    "jobTitle": { "equals": "Sales Manager" }
  },
  "entitlements": {
    "groups": ["SG_Sales_Core", "SG_Sales_Managers", "LIC_M365_E3"],
    "rbacRoles": ["Contributor:sales-rg"]
  }
}
```

Every entitlement decision is traceable to a named rule ID in the audit report.

### Idempotency — Deterministic EventId

The EventId is a SHA-256 hash of `EmployeeId + Action + StartDate`. The same input always produces the same ID. Inserting the event row into Azure Table Storage fails if the row already exists — second run exits cleanly, no double-provisioning.

StartDate is included in the hash so a re-hire after a Leaver produces a distinct event, not a duplicate.

### Concurrency Control

A processing lock is written to the event row at the start of each run (`LockedAt`, `LockedBy`). Stale lock timeout is 10 minutes — if a function instance crashes, the lock auto-releases. Event row locking prevents concurrent processing of the same lifecycle event.

### Pre-Provision Validation — Payload Mode

The validation engine supports a payload input path (`New-IdentitySnapshotFromPayload`) that builds a synthetic identity snapshot without requiring an Entra ID object to exist. The engine evaluates the canonical payload before provisioning runs. Hygiene and sign-in rules that have no meaning pre-creation are skipped automatically via the `IsPayloadScan` flag.

### Hold Queue as a State Machine

A failed record is not discarded. It enters a formal state machine:

```
Received → NormalizationFailed → Held → Approved → Provisioning → Completed
                                                                 → Failed
```

The transition `Received → Held` directly is not permitted and raises at runtime. Every held record carries a reason, retry count, and manual override flag.

---

## Repository Structure

```
JML-Engine/
│
├── Functions/
│   └── joiner_http/
│       └── __init__.py          # Azure Function HTTP trigger, run_pipeline()
│
├── Ingestion/
│   ├── csv_parser.py            # CSV ingestion and structural validation
│   └── schema.py                # IdentityPayload, JmlAction, EmploymentType
│
├── Normalization/
│   ├── lookup_loader.py         # Loads canonical lookup JSON
│   └── normalizer.py            # Resolves raw field values to canonical values
│
├── Mapping/
│   ├── mapping_loader.py        # Loads Rules.json from Azure Storage
│   └── mapping_resolver.py      # Evaluates rules against identity payload
│
├── Hold_queue/
│   ├── models.py                # HoldStatus enum, HoldRecord dataclass
│   └── queue_manager.py         # HoldQueueManager, state machine transitions
│
├── Audit/
│   ├── models.py                # DecisionReport, ActionRecord, status enums
│   └── report_writer.py         # Writes per-identity JSON audit reports
│
├── EventStore/
│   └── event_store.py           # Azure Table Storage, claim_event(), lock management
│
├── config/
│   ├── canonical_lookup.json    # Canonical field value mappings
│   └── Rules.json               # Entitlement mapping rules (v3.0.0, 25 rules)
│
├── scripts/
│   └── run_local.py             # Local pipeline runner (--output reports)
│
├── reports/                     # Audit report output directory
└── Tests/                       # Unit tests for all modules
```

---

## Canonical Identity Schema

Every component contracts against a single internal identity object. No component accepts raw CSV field names or ad-hoc dictionaries.

| Field | Type | Notes |
|---|---|---|
| `employee_id` | str | Unique HR identifier. Required. |
| `upn` | str | User principal name. |
| `display_name` | str | Normalised full name. |
| `department` | str | Normalised via canonical lookup. |
| `job_title` | str | Normalised via canonical lookup. |
| `manager_id` | str \| None | EmployeeId of manager. Optional. |
| `start_date` | str | ISO 8601. |
| `employment_type` | EmploymentType | Enum: Employee \| Contractor \| Guest. |
| `location` | str \| None | Normalised via lookup. Optional. |
| `action` | JmlAction | Enum: Joiner \| Mover \| Leaver. |
| `retain_roles` | bool | Full retention toggle. Default: False. |
| `retain_list` | list[str] | Selective role/group IDs to retain (Mover). |

---

## Audit Report

Every identity event produces a structured JSON report regardless of outcome. Written per event — immutable, one file per identity.

```json
{
  "identity": "jsmith@contoso.com",
  "employee_id": "E101",
  "event": "Joiner",
  "validation_status": "Passed",
  "normalization_status": "Passed",
  "actions_taken": [
    { "action": "UserCreated", "detail": "jsmith@contoso.com", "succeeded": true },
    { "action": "AddedToGroup", "detail": "SG_Sales_Core", "rule_id": "RULE-017", "succeeded": true },
    { "action": "RbacAssigned", "detail": "Contributor:sales-rg", "rule_id": "RULE-017", "succeeded": true }
  ],
  "warnings": [],
  "hold_reasons": [],
  "timestamp": "2026-03-09T10:22:00Z",
  "engine_version": "1.0.0"
}
```

---

## Group Naming Convention

The engine provisions into standardised groups only. Legacy groups are never assigned to new identities.

| Prefix | Purpose | Example |
|---|---|---|
| `SG_*` | Security groups — department and role baseline | `SG_Sales_Core` |
| `LIC_*` | Licence assignment groups | `LIC_M365_E3` |
| `CA_*` | Conditional Access policy groups | `CA_MFA_Required` |

---

## Premium Licence Dependencies

| Feature | Licence | Required From |
|---|---|---|
| Core provisioning (users, groups, RBAC) | Entra ID Free | Phase 0–1 |
| Dynamic membership rules | Entra ID P1 | Phase 1 (optional) |
| Privileged Identity Management (PIM) | Entra ID P2 | Phase 2 |

The core engine requires no premium licensing. Premium features are additive layers.

---

## What Is Not Built Yet

| Phase | Scope | Status |
|---|---|---|
| Phase 2 — PIM Eligibility | Extend Joiner provisioning to include PIM eligible role assignments | Designed, not started |
| Phase 3 — Mover | Delta calculation, permission recalibration, RetainList support | Designed, not started |
| Phase 4 — Leaver | Full revocation, session termination, M365/app assignment removal | Designed, not started |
| Hold Queue UI | Manual review and release interface | Deferred |
| HR API Integration | Live HR feed replacing CSV | Deferred |

Full architecture decisions for all phases are documented in the Decision Log.

---

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run pipeline with sample data
python scripts/run_local.py --input data/sample_hr.csv --output reports/

# Reports written to reports/{employee_id}_{event}_{timestamp}.json
```

---

## Related Repository

The **Identity Governance Validation Engine** (PowerShell) that powers the pre- and post-provision validation gates lives in a separate repository. It operates independently and can be run against any Entra ID tenant for governance scanning.

---

## Tech Stack

- **Runtime:** Azure Functions (Python 3.11)
- **Identity Platform:** Microsoft Entra ID
- **API:** Microsoft Graph API (sole integration interface)
- **Storage:** Azure Table Storage (event store, hold queue backend)
- **Config:** Azure Storage Account (canonical lookup, rules JSON)
- **Auth:** Managed Identity — no credential management
- **Validation Engine:** PowerShell (separate repo)