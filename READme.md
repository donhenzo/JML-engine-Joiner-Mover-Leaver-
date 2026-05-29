# Policy-Driven Identity Lifecycle Engine

### Joiner · Mover · Leaver — Microsoft Entra ID · Azure Functions · Microsoft Graph API

---

## What This Is

Most provisioning systems create a user account first and check whether the access is correct afterwards. That gap, however short, is a real problem in regulated environments. It shows up in access reviews. Auditors ask questions about it. In some cases it constitutes a control failure.

This project takes a different approach. No identity is created until it has cleared a governance validation gate and a Separation of Duties check against a live conflict catalogue. If the HR data is incomplete, the record goes to a hold queue. If the resolved entitlements would put a user in two groups that should never coexist, provisioning is blocked before a user object exists. Every outcome, pass or fail, is written to a structured audit report.

The engine connects directly to BambooHR for live ingestion, normalises raw HR data against a configurable lookup table, resolves entitlements through a policy rules engine, and provisions to Microsoft Entra ID via the Graph API.

---

## The Problem It Solves

Identity provisioning fails in predictable ways. A new starter joins and IT raises a ticket. Someone assigns group memberships based on what the previous person in that role had, or what seemed right at the time. Nobody checks whether those groups are appropriate for the employment type. Nobody checks whether the combination of groups creates a Separation of Duties conflict. Validation, if it runs at all, happens after the identity already exists.

Three things go wrong consistently.

**Access is inconsistent.** Two people with the same job title in different departments end up with different group memberships depending on who processed their request. The misconfiguration that gets through once tends to repeat.

**The audit window is real.** Even when a post-provision scan eventually catches a problem, the identity existed with incorrect access in the meantime. That window shows up in audit logs and has to be explained.

**There is no structured record.** Ticket-based provisioning leaves no machine-readable evidence of what was provisioned, what policy justified it, or what happened when something went wrong. Compliance evidence gets reconstructed from memory and log files.

---

## Why Existing Tools Do Not Fix This

**Entra ID Lifecycle Workflows** handles orchestration well but decision logic ends up scattered across workflow steps, group rules, and role assignments. Complex attribute-based policy is hard to test and harder to audit. The logs record that something happened, not why a specific access decision was made.

**Manual provisioning** is not really about human error. The problem is that policy lives in someone's head. It cannot be versioned, tested, or consistently applied across a team.

**Running compliance scans after the fact** catches problems that already exist. The remediation work still has to happen, more audit entries get written, and in some environments a formal incident record follows.

---

## How This Engine Works

Access decisions are made before identity creation, not after.

When a provisioning event arrives, the pipeline runs in strict order. The HR record is parsed and normalised. The entitlements are resolved through a JSON policy rules engine. Those entitlements are checked against a SoD conflict catalogue. The canonical payload is evaluated against 27 governance rules. Only if all of that passes does the engine call the Graph API to create the user and assign groups. After provisioning, the validation engine runs again against the real Entra ID object to confirm the tenant state matches what was intended.

Every step produces an audit record. Every gate that fails routes the record to a hold queue with a structured reason. Nothing is discarded.

---

## Capabilities

**Pre-provision governance gate.** The PowerShell validation engine evaluates 27 rules against the canonical identity payload before any Graph API call is made. Missing manager association, duplicate UPN, employment type in a Manager-tier role, privileged group correlation checks. If the gate fails, the record is held. Provisioning does not run.

**Separation of Duties.** Resolved entitlements are checked against `sod_policies.json` before the user is created. The evaluation model is `effective_access = current_groups + requested_groups`, so for Mover events the check runs against the full post-change state, not just the delta. Block-level violations stop provisioning and route the identity to a dedicated SoD hold record. Warn-level violations are recorded in the audit report and provisioning continues. The SoD catalogue is a JSON file with no code changes required to add new conflict pairs.

**Policy rules engine.** Group and RBAC assignments are derived from `role_mapping_rules.json`, evaluated against the canonical payload at runtime. Adding a new job title mapping or department rule is a config file edit. Every entitlement decision is traced to a named rule ID in the audit report.

**Employment type enforcement.** Contractors and Interns cannot be provisioned into Manager-tier or privileged groups. This is enforced in the pre-provision gate against the payload, before the user object exists.

**BambooHR integration.** The engine polls BambooHR directly and derives the lifecycle action (Joiner, Mover, Skip) by comparing the HR record against live Entra ID state. Delta polling fetches only employees changed since the last checkpoint. Three idempotency layers prevent double-provisioning regardless of ingestion mode.

**Canonical normalisation.** Raw HR field values (abbreviations, case variants, misspellings) are resolved to controlled canonical values before any downstream component sees them. Unknown values go to the hold queue.

**Deterministic idempotency.** The EventId is a SHA-256 hash of EmployeeId, Action, and StartDate. The same input always produces the same ID. Running the same record twice produces one outcome. Retries are safe.

**Hold queue state machine.** Records that fail any gate enter a formal state machine with defined transitions, reason codes, retry counts, and a manual release path. SoD violations get a dedicated hold record type so operators can distinguish a governance block from a data quality problem.

**Immutable audit reports.** Every identity event produces a structured JSON report regardless of outcome. The report captures every action taken, every gate result, every rule ID, every SoD violation with conflicting groups and compensating control text. One file per event, written once, never modified.

**Post-provision validation.** After provisioning, the validation engine re-runs against the real Entra ID object using three targeted Graph calls, regardless of tenant size. The PowerShell engine also checks actual group memberships against the SoD catalogue, providing a second independent check against real tenant state.

**Graph API throttling recovery.** All Graph calls use automatic retry. HTTP 429 responses respect the `Retry-After` header and retry up to three times. Server errors use exponential backoff. Client errors fail immediately.

---

## IAM Design Principles

**Least privilege.** Entitlements come from validated attributes evaluated against a policy. No template-based assignments. No convenience memberships.

**Separation of duties.** The SoD catalogue is the single source of truth for conflict definitions. Both the Python engine (preventive, pre-provision) and the PowerShell engine (detective, FullScan and post-provision) read from the same file. Adding a new conflict pair is a one-line JSON edit.

**Governance before access.** Both the validation gate and the SoD check are hard blocks. Neither is advisory. Provisioning cannot proceed until both pass.

**Zero trust.** Every identity event is evaluated against policy before access is granted. Nothing is inherited from templates or inferred from role history.

**Auditability.** Every provisioning decision is traceable to a rule ID. Every SoD violation is recorded with the conflicting groups, the policy that fired, and the compensating control requirement. Compliance evidence is produced at the time of provisioning.

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        INPUT LAYER                              │
│   BambooHR API / CSV → Parser → Canonical IdentityPayload       │
│   Structural failures → Hold Queue (NormalizationFailed)        │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                    NORMALIZATION LAYER                          │
│   Canonical Lookup Table (Azure Storage JSON)                   │
│   Raw field values → Standardised department / job title        │
│   Unknown values → Hold Queue                                   │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                       EVENT STORE                               │
│   Azure Table Storage - JmlEvents table                         │
│   SHA-256 deterministic EventId · claim_event() idempotency     │
│   Duplicate EventId → exit cleanly                              │
│   Stale lock detection - auto-reclaim if locked > 10 minutes    │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                     CONFLICT QUEUE                              │
│   Check for active events on same EmployeeId                    │
│   Active event exists → queue new event (FIFO per identity)     │
│   Leaver arrives → supersede all pending events                 │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│               ENTITLEMENT RESOLUTION LAYER                      │
│   mapping_resolver.py evaluates role_mapping_rules.json         │
│   JobTitle + Department + EmploymentType → Groups + RBAC        │
│   Multiple rules can contribute entitlements per identity       │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│          SEPARATION OF DUTIES EVALUATION                        │
│   sod_checker.evaluate_sod() - Python, no Graph API calls       │
│   effective_access = current_groups + requested_groups          │
│   ANY_TO_ANY intersection against sod_policies.json             │
│   Block → Hold Queue (SoDViolation) → audit record              │
│   Warn → violations in audit report → continue                  │
│   Clean → continue                                              │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│               PRE-PROVISION VALIDATION GATE                     │
│   Identity Governance Validation Engine (PowerShell)            │
│   27 rules evaluated against canonical payload                  │
│   ENT-004: Contractor/Intern in Manager-tier role → blocked     │
│   Zero Graph API calls - no Entra object exists yet             │
│   Failures → Hold Queue (ValidationFailed)                      │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│              GRAPH API PROVISIONING LAYER                       │
│   Create Entra ID user                                          │
│   Assign security groups (SG_*, LIC_*, CA_*)                    │
│   Assign Azure RBAC roles via group membership                  │
│   All operations idempotent · ActionsTaken recorded live        │
│   429 throttling: automatic retry with Retry-After backoff      │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│              PIM ELIGIBILITY LAYER (Phase 2)                    │
│   Group-based PIM pattern - engine assigns eligible membership  │
│   Requires Entra ID P2 - skipped gracefully if absent           │
│   pimGroups entries in mapping rules drive this step            │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│             POST-PROVISION VALIDATION GATE                      │
│   Validation engine re-runs against actual Entra ID state       │
│   Get-UserSnapshot: 3 Graph calls, O(groups for this user)      │
│   SoD evaluation runs against real memberOf                     │
│   ENT-002: Contractor in Manager-tier group → event failed      │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                       AUDIT LAYER                               │
│   Per-identity JSON decision report · every outcome             │
│   Actions taken · validation status · SoD violations · warnings │
│   SoD violations: policy ID · conflicting groups ·              │
│   compensating control · exception_applied flag                 │
│   Immutable · one file per identity event                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Separation of Duties

### How the evaluation works

The effective access set is `current_groups + requested_groups`. For a Joiner, `current_groups` is always empty, so effective access equals the resolved entitlements. For a Mover, `current_groups` is the user's actual tenant membership before the delta is applied. This is where real SoD violations hide in production: a role change adds a second entitlement that, combined with something the user already holds, creates a conflict.

### Conflict catalogue

Conflict pairs live in `sod_policies.json`. Both the Python engine and the PowerShell validation engine read from this file. No code changes are required to add or modify conflict pairs.

| Policy | Conflict | Risk | Action |
|---|---|---|---|
| SOD-001 | Payment Approver + Payment Processor | Critical | Block |
| SOD-002 | IT User Provisioner + IT Access Approver | Critical | Block |
| SOD-003 | Payment Processor + Finance Auditor | High | Warn |
| SOD-004 | Payment Approver + Finance Auditor | High | Warn |
| SOD-005 | Journal Poster + Finance Auditor | High | Warn |
| SOD-006 | HR Salary Admin + HR Data Export | High | Warn |

### Two controls, one catalogue

The Python `sod_checker.evaluate_sod()` is the preventive control. It runs before any Graph API call. A block violation stops provisioning before the user object exists.

The PowerShell `Evaluate-SoDConflict` in `IDRuleProcessor.ps1` is the detective control. It runs during FullScan and post-provision verification, evaluating real tenant group memberships against the same catalogue. This catches violations that entered the tenant through manual assignment or before SoD was active.

### Fail-closed on Mover

If the Graph API call to fetch current group memberships returns incomplete or failed results for a Mover event, the SoD check blocks immediately without evaluating policies. A partial effective access set can miss real violations. A false block is recoverable through human review. A missed violation is not.

---

## HR API Integration

```
BambooHR API
  ↓ bamboohr_client.py        fetch employee · delta poll · directory cache
  ↓ bamboohr_mapper.py        BambooHR fields → raw IdentityPayload shape
  ↓ action_deriver.py         Joiner / Mover / Skip - derived from live Entra state
  ↓ ingestion_coordinator.py  run_single · run_delta · run_bulk
  ↓ pipeline_adapter.py       bridges HR API records into the existing pipeline
  ↓ existing pipeline         normalise → SoD → validate → provision → audit
```

| Mode | Command | Description |
|---|---|---|
| Single / batch | `--source api --id Acc003,Acc004` | Process specific employees by employee number or UPN |
| Delta poll | `--source api --mode delta` | Process all employees changed since last checkpoint |
| CSV | `--csv Data/sample.csv` | Original path, unchanged |

Three idempotency layers prevent double-provisioning: delta timestamp narrows the BambooHR query window; action derivation filters records with no meaningful change; EventId + `claim_event()` provides the hard guarantee via Azure Table Storage atomic insert.

---

## Canonical Identity Schema

Every component in the pipeline works against a single internal data contract. No component accepts raw CSV field names or unstructured dictionaries.

| Field | Type | Notes |
|---|---|---|
| `employee_id` | str | Unique HR identifier |
| `upn` | str | User principal name |
| `display_name` | str | Normalised full name |
| `department` | str | Normalised via canonical lookup |
| `job_title` | str | Normalised via canonical lookup |
| `manager_id` | str / None | EmployeeId of manager |
| `start_date` | date | ISO 8601 |
| `employment_type` | EmploymentType | Employee, Contractor, or Guest |
| `location` | str / None | Normalised via lookup |
| `action` | JmlAction | Joiner, Mover, or Leaver |
| `retain_roles` | bool | Full retention toggle for Mover |
| `retain_list` | list[str] | Specific role/group IDs to retain |

---

## Known Limitations

**PIM requires Entra ID P2.** PIM eligibility assignments are non-blocking. If the tenant has no P2 licence, the PIM step is skipped with a warning and the event completes successfully. Group assignments are unaffected.

**SoD exception store is a stub.** The `_exception_exists()` function returns False unconditionally. Building a real exception store requires agreement on who can approve exceptions, for how long, and with what justification. The detection layer works correctly regardless.

**No automatic rollback on partial failure.** All Graph operations are idempotent, so retrying from the beginning is safe. Partial state between failure and retry is a transient condition, not a persistent one.

**Queued events need a trigger to drain.** When a queued event is auto-released, it moves to Pending but waits for the next pipeline run. A timer-triggered function is the production solution.

**HR API is polling-based.** Delta polling narrows the query window. A webhook handler is the planned next step and would only require a new HTTP trigger entry point.

---

## Phase Status

| Phase | Capability | Status |
|---|---|---|
| Phase 0 | Data contracts, normalisation, event store, hold queue, audit system | Complete |
| Phase 1 | Joiner provisioning, governance gates, policy-driven entitlements | Complete |
| Phase 2 | PIM eligible role assignment (requires Entra ID P2) | Complete |
| Phase 2.5 | Separation of Duties - preventive and detective controls | Complete |
| Phase 3 | Mover - delta calculation, permission recalibration | Designed, not started |
| Phase 4 | Leaver - full revocation, session termination | Designed, not started |

---

## Running Locally

```bash
# Install Python dependencies
pip install -r requirements.txt

# Terminal 1 - start the PowerShell validation engine
cd Validation_engine
func start

# Terminal 2 - CSV mode
python scripts/run_local.py --clean --output reports --csv Data/sample_hr.csv

# Terminal 2 - single employee from BambooHR
python scripts/run_local.py --source api --id Acc003

# Terminal 2 - delta poll
python scripts/run_local.py --source api --mode delta
```

---

## Repository Structure

```
JML-Engine/
├── Functions/
│   ├── joiner_http/
│   │   └── __init__.py              # Azure Function HTTP trigger · run_pipeline()
│   └── Event_store/
│       ├── event_store.py           # SHA-256 EventId · claim_event() · stale lock recovery
│       └── conflict_queue.py        # FIFO queue · auto-release on completion
├── Ingestion/
│   ├── csv_parser.py                # CSV ingestion · structural validation
│   ├── schema.py                    # IdentityPayload · JmlAction · EmploymentType enums
│   └── hr_api/
│       ├── action_deriver.py        # Joiner / Mover / Skip derivation · provider-agnostic
│       ├── system_state.py          # Poll checkpoint · Azure Table Storage · provider-agnostic
│       └── bamboohr/
│           ├── bamboohr_client.py   # BambooHR API client · directory cache
│           ├── bamboohr_mapper.py   # Field translation · employeeNumber as employee_id
│           ├── pipeline_adapter.py  # Bridges HR records into pipeline
│           └── ingestion_coordinator.py  # Orchestrates fetch · derive · pipeline · checkpoint
├── Normalization/
│   ├── lookup_loader.py             # Loads canonical_lookup.json
│   └── normalizer.py                # Resolves raw field values · accumulates failures
├── Mapping/
│   ├── mapping_loader.py            # Loads role_mapping_rules.json
│   └── mapping_resolver.py          # Evaluates rules against identity payload
├── Governance/
│   └── SoD/
│       ├── sod_models.py            # Enums · SoDPolicy · SoDViolation · SoDCheckResult
│       ├── sod_loader.py            # Loads and validates sod_policies.json
│       └── sod_checker.py           # evaluate_sod() · ANY_TO_ANY · fail-closed contract
├── Provisioning/
│   ├── graph_client.py              # Graph API client · retry on 429
│   ├── pim_client.py                # PIM group eligibility assignment
│   └── provisioner.py               # Entra ID user · group · RBAC · PIM provisioning
├── Validation/
│   └── validation_gate.py           # Pre- and post-provision gate (HTTP)
├── Hold_queue/
│   ├── models.py                    # HoldStatus enum · HoldRecord · state constants
│   ├── queue_manager.py             # State machine · create_from_sod_violation()
│   └── azure_table_hold_queue_store.py
├── Audit/
│   ├── models.py                    # DecisionReport · ActionRecord · sod_violations
│   ├── report_writer.py             # Per-identity JSON audit reports
│   └── run_summary_writer.py        # Per-run summary
├── config/
│   ├── canonical_lookup.json        # Field variant to canonical value mappings
│   ├── role_mapping_rules.json      # JobTitle / Dept / EmploymentType to groups + RBAC
│   └── sod_policies.json            # SoD conflict pairs · risk ratings · compensating controls
├── scripts/
│   └── run_local.py
├── reports/
└── Tests/
    ├── test_normalizer.py
    ├── test_sod.py                  # 63 tests covering sod_loader and sod_checker
    └── ...
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Azure Functions (Python 3.11) |
| Identity Platform | Microsoft Entra ID |
| API | Microsoft Graph API |
| Storage | Azure Table Storage |
| Config | Azure Storage Account |
| Auth | Managed Identity |
| Validation Engine | PowerShell Azure Function (separate repo) |
| HR Integration | BambooHR API (live) · OrangeHR webhook (planned) |

---

## Licence Dependencies

| Feature | Licence | Required From |
|---|---|---|
| Core provisioning (users, groups, RBAC) | Entra ID Free | Phase 0-1 |
| SoD enforcement | Entra ID Free | Phase 2.5 |
| Dynamic membership rules | Entra ID P1 | Phase 1 (optional) |
| Privileged Identity Management | Entra ID P2 | Phase 2 |