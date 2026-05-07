# Eliminating Day-One Access Risk with a Policy-Driven Identity Lifecycle Engine

### Joiner · Mover · Leaver — Microsoft Entra ID · Azure Functions · Microsoft Graph API

---

## Executive Summary

Most identity provisioning systems create users first and validate access later. In that window — however brief — an identity can exist with incorrect group memberships, excessive permissions, or attribute conflicts that violate access policy. In regulated environments, that window is auditable. It shows up in access reviews. It has to be explained.

This project implements a pre-validation identity lifecycle engine for Microsoft Entra ID. No identity is created unless it first passes a pre-provision governance validation gate on its canonical attributes. Contractors cannot be provisioned into management-tier groups. Invalid HR data is prevented from reaching provisioning. Every decision is recorded in an immutable audit report regardless of outcome.

Post-provision remediation is eliminated for all violations detectable at the pre-provision stage, and strictly reduced for entitlement-level conflicts through targeted post-provision validation. The result is improved audit readiness and a provisioning pipeline where access correctness is enforced at the point of creation — not discovered after the fact.

---

## The Business Problem

In most enterprise environments, identity provisioning is reactive by design. A new hire joins. IT receives a ticket. A user is created. Group memberships are assigned based on whoever processed the request, what template was used last time, or what the previous person in that role had. Validation, if it happens at all, runs after the identity already exists.

This creates three compounding failure modes:

**Incorrect access from day one.** Without a defined policy engine, group assignment is inconsistent. Two people with the same job title in different departments can receive materially different access depending on who provisioned them. The same misconfiguration that slips through once becomes a pattern at scale.

**A window of unauthorized access.** Even in systems that validate post-provision, the identity exists with potentially incorrect access during the window between creation and remediation. In a regulated environment — financial services, healthcare, government — that window is not a technicality. It appears in audit logs. It requires explanation during access reviews. It can constitute a control failure.

**No structured audit trail.** When provisioning is handled through tickets, scripts, or disconnected workflow tools, there is no structured record of what was provisioned, what policy drove the decision, or what happened when a step failed. Compliance evidence is reconstructed retrospectively, which introduces risk and operational cost.

---

## Why Existing Approaches Fail

**Low-code workflow tooling** — such as Microsoft Entra ID Lifecycle Workflows — is optimised for rapid deployment and operational orchestration. However, it lacks a centralised policy evaluation layer. Decision logic is distributed across workflows, group rules, and role assignments, making complex access decisions difficult to reason about, test, and audit consistently. While attribute-based rules can be implemented, they become fragmented and difficult to maintain at scale. Audit logs capture events, but not structured policy decisions, which limits the ability to reconstruct why a specific access outcome occurred.

**Manual provisioning** — IT ticket-based workflows — fail not because people make mistakes, but because they introduce structural inconsistency. Policy lives in the mind of the engineer processing the ticket. It cannot be tested, versioned, or audited in any meaningful way.

**Post-provision validation** — running compliance scans after identities are created — addresses the symptom rather than the cause. The incorrectly provisioned identity already exists. Remediating it requires additional work, additional audit entries, and in some cases, a formal incident record.

None of these approaches treat access correctness as a provisioning prerequisite. This engine does.

---

## Positioning

This engine is not a replacement for identity platforms such as Microsoft Entra ID or governance suites like SailPoint IdentityIQ or Saviynt Identity Cloud.

It operates as a policy enforcement layer that sits between HR systems and identity platforms, ensuring that all provisioning requests are policy-compliant before execution. The focus is on the decision layer — how access entitlements are computed, validated, and recorded — rather than on directory management or access request workflows.

---

## Solution Overview

The core design principle is policy-as-a-prerequisite, not policy-as-validation. Access decisions are computed and verified before identity creation, not evaluated after provisioning completes.

This project implements a policy-driven identity lifecycle engine that evaluates every Joiner identity event against a governance rule set before any Entra ID object is created. The pipeline is linear, sequenced, and exits immediately when a constraint is violated — routing the record to a hold queue with a structured reason rather than proceeding to provisioning.

The engine enforces:

- **Pre-provision governance validation** — no identity is created without clearing a hard policy gate
- **Canonical data normalisation** — raw HR field values are resolved to controlled canonical values before any decision is made; unresolvable values are prevented from reaching provisioning
- **Policy-driven entitlement resolution** — group and RBAC assignments are derived from externally configurable rule objects, not hardcoded logic
- **Deterministic idempotency** — the same HR event processed twice produces exactly one outcome; retries are safe
- **Immutable audit reporting** — every decision, every rule ID, every failure reason is written to a per-identity JSON report regardless of outcome

---

## Key Capabilities

**Pre-Provision Validation Gate**
The governance validation engine evaluates the canonical identity payload against 27 rules before any Entra ID object is created. A Contractor attempting to be provisioned into a Manager-tier group is blocked before a user object exists. A payload with a missing manager relationship is held for human review. Provisioning cannot proceed unless the gate passes.

**Policy Rules Engine**
Entitlement decisions are resolved by evaluating externally loaded rule objects against the canonical identity payload. Adding a new role mapping — new job title, new department, new group assignment — is a configuration file edit with no redeployment. Every entitlement decision is traceable to a named rule ID in the audit report.

**Employment Type Enforcement**
The engine enforces employment type constraints at the payload level. Contractors and Interns cannot be provisioned into management-tier or privileged groups. This check runs in the pre-provision gate against the payload itself — the user is never created if the combination violates policy.

**Canonical Normalisation Layer**
Raw HR field values — variant spellings, case differences, abbreviations — are resolved to canonical values before any downstream component sees them. Unknown values route to the hold queue, not to provisioning. Policy changes to the canonical lookup require no code changes.

**Deterministic Idempotency**
The EventId is a SHA-256 hash of EmployeeId, Action, and StartDate. The same input always produces the same ID. Processing the same CSV twice produces one outcome. Function retries resolve safely without double-provisioning.

**Hold Queue as a State Machine**
Records that fail normalisation or validation are not discarded. They enter a formal state machine with explicit transitions, reason codes, retry counts, and a manual release path. Every held record is explainable and actionable.

**Immutable Per-Identity Audit Reports**
Every lifecycle event produces a structured JSON report regardless of outcome — pass, hold, or fail. Reports capture every action taken, every gate result, every rule ID that fired, and every failure reason. Each report provides full decision traceability, linking every provisioning outcome to the exact rule set and evaluation path that produced it. One file per identity event, written at the time of processing, never modified.

**Post-Provision Validation**
After provisioning completes, the validation engine re-runs against the real Entra ID object to confirm the provisioned state matches the expected entitlements. This uses a targeted O(1) Graph API path — three calls regardless of tenant size — rather than a full tenant scan. This design prioritises deterministic validation speed at the pre-provision stage, at the cost of deferring entitlement-state validation to the post-provision gate.

---

## IAM Principles Demonstrated

**Least Privilege**
Access is derived from validated identity attributes against a declarative policy. No speculative or convenience-based group assignments. Entitlements are the minimum required for the role and employment type.

**Separation of Duties**
The policy rule set enforces employment type constraints across privilege tiers. Contractors cannot hold Manager-tier group memberships. The engine enforces this structurally — it is not dependent on human review.

**Governance Before Access**
Provisioning is conditional on governance validation. The pre-provision gate is a hard block, not a recommendation. This closes the window of incorrect access that post-hoc validation leaves open.

**Zero Trust Alignment**
No identity is trusted by default. Every lifecycle event is validated against policy before access is granted. Access is explicitly derived from attributes, not inherited from templates or manual selection.

**Complete Auditability**
Every access decision is traceable. Every rule that contributed to a provisioning outcome is recorded by ID. Every failure reason is written to the hold queue and the audit report. Compliance evidence is produced at provisioning time, not reconstructed later.

---

## Architecture Overview

The pipeline is linear and strictly sequenced. No record reaches provisioning without passing every gate. Each layer has a single responsibility and a defined output contract.

```
┌─────────────────────────────────────────────────────────────────┐
│                        INPUT LAYER                              │
│   HR CSV / API Feed → CSV Parser → Canonical IdentityPayload    │
│   Structural failures → Hold Queue (NormalizationFailed)        │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                    NORMALIZATION LAYER                          │
│   Canonical Lookup Table (Azure Storage JSON)                   │
│   Raw field values → Standardised department / job title        │
│   Unknown values → Hold Queue — never reach provisioning        │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│               PRE-PROVISION VALIDATION GATE ◄── MUST PASS       │
│   Identity Governance Validation Engine (PowerShell)            │
│   27 rules evaluated against canonical payload                  │
│   ENT-004: Contractor/Intern in Manager-tier role → blocked     │
│   Zero Graph API calls — no Entra object exists yet             │
│   Failures → Hold Queue (ValidationFailed)                      │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                     EVENT STORE                                 │
│   Azure Table Storage — JmlEvents table                         │
│   SHA-256 deterministic EventId · Optimistic concurrency        │
│   Status: Pending → Processing → Completed / Failed             │
│   Duplicate EventId → exit cleanly (idempotent)                 │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│               ENTITLEMENT RESOLUTION LAYER                      │
│   mapping_resolver.py evaluates Rules.json against payload      │
│   JobTitle + Department + EmploymentType → Groups + RBAC        │
│   Multiple rules can contribute entitlements per identity       │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│              GRAPH API PROVISIONING LAYER                       │
│   Create Entra ID user (with employeeType written to Graph)     │
│   Assign security groups  (SG_*, LIC_*, CA_*)                   │
│   Assign Azure RBAC roles via group membership                  │
│   All operations idempotent · ActionsTaken recorded live        │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│             POST-PROVISION VALIDATION GATE                      │
│   Validation engine re-runs against actual Entra ID state       │
│   Get-UserSnapshot: 3 Graph calls, O(1) regardless of scale     │
│   Confirms provisioned state matches expected entitlements      │
│   ENT-002: Contractor in Manager-tier group → event failed      │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                       AUDIT LAYER                               │
│   Per-identity JSON decision report · every outcome             │
│   Actions taken · validation status · rule IDs · hold reasons   │
│   Immutable · one file per identity event                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Technical Deep Dive

### Canonical Identity Schema

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

### Governance Rule Set

The validation engine evaluates 27 rules across six categories. Rules are declared in `Rules.json` alongside the entitlement model and mapping rules — no rule logic is hardcoded.

| Category | Rules | Blocking |
|---|---|---|
| Identity | IDENT-001/002/003 · JOIN-001/002 | IDENT-001/002 · JOIN-001/002 |
| Access | ACCESS-001/002/003 · ENT-001/002/003/004 | ACCESS-002 · ENT-002 · ENT-004 |
| Architecture | ARCH-001/002/003 | ARCH-001 |
| Hygiene | HYG-001/002/003/004 | HYG-004 (FullScan) |
| RBAC | RBAC-001/002/003 | RBAC-003 |
| Correlation | CORR-001/002/003/004/005 | CORR-001/002/003 |

**ENT-004** is the pre-provision payload check — it evaluates employment type against job title before any Entra object exists. Contractors and Interns attempting to be provisioned into Manager, Director, HOD, or Executive roles are blocked at this gate.

**ENT-002** is the post-provision entitlement check — it evaluates actual group memberships against the entitlement model's `allowedEmployment` policy after provisioning completes.

### Employment Type Vocabulary

The JML engine uses `Employee | Contractor | Guest` (HR API conventions). The validation engine's entitlement model uses the same vocabulary. All `allowedEmployment` arrays in `Rules.json` reflect this canonical set. The normaliser in `IDRuleProcessor.ps1` accepts both `employee` and `full-time` during transition, mapping both to `Employee`.

### Idempotency

```
EventId = SHA-256(EmployeeId + Action + StartDate) → truncated to 32 chars
```

The same CSV processed twice produces the same EventId. Azure Table Storage insert fails atomically if the row exists — second run exits cleanly. StartDate is included so a re-hire after a Leaver produces a distinct event, not a duplicate.

### Concurrency Control

A processing lock is written to the event row at the start of each run (`LockedAt`, `LockedBy`). Stale lock timeout is 10 minutes — if a function instance crashes, the lock auto-releases. Concurrent function instances processing the same event exit cleanly on the lock check.

### Post-Provision Graph Efficiency

The original post-provision path used `Get-IdentitySnapshot` — full tenant collection scaling as O(groups × members). On tenants with 50+ groups this consistently exceeded timeout thresholds.

`Get-UserSnapshot` replaces this with three targeted Graph calls:
1. `GET /users/{id}` — fetch the provisioned user
2. `GET /users/{id}/memberOf` — fetch their group memberships directly
3. `GET /groups/{id}` per membership — resolve display names

Runtime dropped from 60+ seconds (timeout) to approximately 12 seconds end-to-end, with the Graph calls themselves completing in under 2 seconds.

### Audit Report Structure

```json
{
  "identity": "felix.wagner@contoso.com",
  "employee_id": "E406",
  "event": "Joiner",
  "validation_status": "Failed",
  "normalization_status": "Passed",
  "actions_taken": [],
  "warnings": [],
  "hold_reasons": [
    "[ENT-004] Employment type 'Contractor' is not permitted for Manager-tier role 'Sales Manager'. Contractors and Interns cannot be provisioned into management positions."
  ],
  "timestamp": "2026-05-04T14:17:17Z",
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
| `CA_*` | Conditional Access policy groups | `CA_Contractors` |

### Employment Tier Model

| Tier | Description | Privileged | EmploymentType Restriction |
|---|---|---|---|
| Base | Standard department membership | No | Employee, Contractor, Intern |
| Base/Staff | Operational access, elevated permissions | No | Employee, Contractor |
| Manager | Management-level access | Yes | Employee only |
| Manager/Staff | Combined management and operational | Yes | Employee only |
| Administrative | Licence / Conditional Access groups | No | Varies |

---

## Business Impact

**No identity that violates pre-provision policy constraints can be provisioned.** The pre-provision gate is a hard block. A Contractor cannot be assigned to a Manager-tier group. An identity with unresolvable HR attributes is prevented from reaching provisioning. These constraints are enforced structurally, not by process.

**Every access decision is explainable.** Every group assignment is traceable to a named rule ID. Every failure reason is recorded at the time of processing. Compliance evidence does not need to be reconstructed — it exists in the audit report.

**Post-provision remediation is eliminated for all violations detectable at the pre-provision stage.** Because access correctness for payload-level constraints is enforced before the identity exists, there is no incorrect state to remediate for those cases. The hold queue surfaces exceptions for human review; provisioned identities meet pre-provision policy by construction.

**Policy changes require no redeployment.** Adding a new role, changing a group assignment, updating employment type constraints — all are configuration file edits. The engine picks up changes on the next run.

---

## Limitations and Trade-offs

**Dependent on HR data quality.** The normalisation layer resolves known variants but unknown values route to the hold queue. If the HR system produces field values not in the canonical lookup table, records will be held until the lookup is updated. Garbage in, hold queue out — not garbage in, provisioning out.

**Pre-provision validation evaluates payload, not entitlements.** ENT-004 catches employment type and job title conflicts at the payload level. ENT-002 catches group membership conflicts at the post-provision level. Between the two gates there is a window where provisioning runs — if a mapping rule produces an entitlement that would violate policy, the post-provision gate catches it but the user object is created. The design decision was to keep the pre-provision gate fast (zero Graph calls) and the post-provision gate complete.

**Policy complexity scales with the mapping rule set.** As the number of departments, job titles, and employment types grows, the `Rules.json` entitlement model grows with it. Without a role abstraction layer (planned), HR title changes require mapping rule updates.

**No real-time HR integration yet.** The engine consumes CSV input designed to mirror what an HR API would return. Live webhook integration is deferred — the CSV schema is designed as a drop-in replacement for the live feed.

**Hold Queue UI not implemented.** Held records are visible in Azure Table Storage and the audit reports. A manual review and release interface is deferred.

---

## Phase Status

| Phase | Capability | Status |
|---|---|---|
| Phase 0 | Data contracts, normalisation, event store, hold queue, audit system | Complete |
| Phase 1 | Joiner provisioning pipeline, governance gates, policy-driven entitlements | Complete |
| Phase 2 | PIM eligible role assignment (requires Entra ID P2) | Designed, not started |
| Phase 3 | Mover — delta calculation, permission recalibration, RetainList support | Designed, not started |
| Phase 4 | Leaver — full revocation, session termination, M365/app removal | Designed, not started |

Full architecture decisions for all phases are documented in the Decision Log.

---

## Premium Licence Dependencies

| Feature | Licence | Required From |
|---|---|---|
| Core provisioning (users, groups, RBAC) | Entra ID Free | Phase 0–1 |
| Dynamic membership rules | Entra ID P1 | Phase 1 (optional enhancement) |
| Privileged Identity Management (PIM) | Entra ID P2 | Phase 2 |

The core engine requires no premium licensing. Premium features are additive layers.

---

## Running Locally

```bash
# Install Python dependencies
pip install -r requirements.txt

# Terminal 1 — start the PowerShell validation engine
cd Validation_engine
func start
# Wait for: profile.ps1: Connected to Microsoft Graph successfully

# Terminal 2 — run the JML pipeline
cd JML-engine
python scripts/run_local.py --clean --output reports --csv Data/sample_hr.csv

# Audit reports written to reports/{employee_id}_{event}_{timestamp}.json
```

---

## Repository Structure

```
JML-Engine/
├── Functions/
│   ├── joiner_http/
│   │   └── __init__.py              # Azure Function HTTP trigger · run_pipeline()
│   └── Event_store/
│       ├── event_store.py           # SHA-256 EventId · claim_event() · lock management
│       └── conflict_queue.py        # Conflicting event FIFO queue
├── Ingestion/
│   ├── csv_parser.py                # CSV ingestion · structural validation
│   └── schema.py                    # IdentityPayload · JmlAction · EmploymentType enums
├── Normalization/
│   ├── lookup_loader.py             # Loads canonical_lookup.json
│   └── normalizer.py                # Resolves raw field values · accumulates failures
├── Mapping/
│   ├── mapping_loader.py            # Loads role_mapping_rules.json from Azure Storage
│   └── mapping_resolver.py          # Evaluates rules against identity payload
├── Provisioning/
│   ├── graph_client.py              # Microsoft Graph API client · writes employeeType to Entra
│   └── provisioner.py               # Entra ID user · group · RBAC provisioning
├── Validation/
│   └── validation_gate.py           # Pre- and post-provision validation gate (HTTP)
├── Hold_queue/
│   ├── models.py                    # HoldStatus enum · HoldRecord · state constants
│   ├── queue_manager.py             # State machine · VALID_TRANSITIONS enforced
│   └── azure_table_hold_queue_store.py  # Azure Table Storage backend
├── Audit/
│   ├── models.py                    # DecisionReport · ActionRecord · status enums
│   ├── report_writer.py             # Per-identity JSON audit reports
│   └── run_summary_writer.py        # Per-run summary report
├── config/
│   ├── canonical_lookup.json        # Field variant → canonical value mappings
│   └── role_mapping_rules.json      # JobTitle / Dept / EmploymentType → groups + RBAC
├── scripts/
│   └── run_local.py                 # Local pipeline runner
├── reports/                         # Audit report output directory
└── Tests/                           # Unit tests for all modules
```

---

## Related Repository

The **Identity Governance Validation Engine** (PowerShell) that powers the pre- and post-provision validation gates lives in a separate repository. It operates independently and can be run against any Entra ID tenant for governance scanning, drift detection, and compliance reporting.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Azure Functions (Python 3.11) |
| Identity Platform | Microsoft Entra ID |
| API | Microsoft Graph API (sole integration interface) |
| Storage | Azure Table Storage (event store, hold queue backend) |
| Config | Azure Storage Account (canonical lookup, rules JSON) |
| Auth | Managed Identity — no credential management |
| Validation Engine | PowerShell Azure Function (separate repo) |