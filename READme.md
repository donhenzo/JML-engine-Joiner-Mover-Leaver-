# Policy-Driven Identity Lifecycle Engine

### Joiner · Mover · Leaver — Microsoft Entra ID · Azure Functions · Microsoft Graph API

---

## What This Is

Most provisioning systems create a user account first and check whether the access is correct afterwards. That gap, however short, is a real problem in regulated environments. It shows up in access reviews. Auditors ask questions about it. In some cases it constitutes a control failure.

This project takes a different approach. No identity is created until it has cleared a governance validation gate and a Separation of Duties check against a live conflict catalogue. If the HR data is incomplete, the record goes to a hold queue. If the resolved entitlements would put a user in two groups that should never coexist, provisioning is blocked before a user object exists. Every outcome, pass or fail, is written to a structured audit report.

When an employee changes role, the engine recalculates the full access delta, evaluates the proposed post-move access set against the SoD catalogue before applying any change, checks time-bounded retention records for groups the user should keep, adjusts PIM eligible assignments, and verifies the tenant state after the move completes.

The engine connects directly to BambooHR for live ingestion, normalises raw HR data against a configurable lookup table, resolves entitlements through a policy rules engine, and provisions to Microsoft Entra ID via the Graph API.

---

## The Problem It Solves

Identity provisioning fails the same way every time. A new starter joins and IT raises a ticket. Groups get assigned based on what the previous person in that role had, or what seemed right at the time. Nobody checks whether those groups are appropriate for the employment type. Nobody checks whether the combination creates a SoD conflict. Validation, if it runs at all, happens after the identity already exists.

When someone changes role, the same thing happens again. Old access accumulates. New access goes on top. The combination is never evaluated against policy. An employee who moves from Payment Processing to Payment Approval carries both entitlements indefinitely unless someone manually audits them.

Three things go wrong consistently.

**Access is inconsistent.** Two people with the same job title end up with different group memberships depending on who processed the request. The misconfiguration that gets through once tends to repeat.

**The audit window is real.** Even when a scan eventually catches a problem, the identity existed with incorrect access in the meantime. That window shows up in audit logs and has to be explained.

**There is no structured record.** Ticket-based provisioning leaves no machine-readable evidence of what was provisioned, what policy justified it, or what happened when something went wrong. Compliance evidence gets reconstructed from memory and log files.

---

## Why Existing Tools Do Not Fix This

**Entra ID Lifecycle Workflows** handles orchestration well but decision logic ends up scattered across workflow steps, group rules, and role assignments. Complex attribute-based policy is hard to test and harder to audit. The logs tell you that something happened — not why a specific access decision was made.

**Manual provisioning** is not really about human error. Policy lives in someone's head. It cannot be versioned, tested, or consistently applied across a team.

**Running compliance scans after the fact** catches problems that already exist. The remediation work still has to happen, more audit entries get written, and in some environments a formal incident record follows.

---

## How This Engine Works

Access decisions are made before identity creation, not after. Access change decisions are made before any group modification, not after.

For a Joiner event, the pipeline runs in strict order. The HR record is parsed and normalised. Entitlements are resolved through a JSON policy rules engine. Those entitlements are checked against a SoD conflict catalogue. The canonical payload is evaluated against 27 governance rules. Only if all of that passes does the engine call the Graph API to create the user and assign groups. After provisioning, the validation engine runs again against the real Entra ID object to confirm the tenant state matches what was intended.

For a Mover event, the engine fetches the user's current Entra state and calculates the delta between the old and new role. It checks retention records for groups flagged to keep, runs two-pass SoD re-evaluation against the full proposed post-move access set, then executes removals and additions in order. A SoD block at Step 5 routes the event to HOLD_FOR_REVIEW and stops. Nothing touches Entra until the governance checks clear.

Every step produces an audit record. Every gate that fails routes the record to a hold queue with a structured reason. Nothing is discarded.

---

## Capabilities

**Pre-provision governance gate.** The PowerShell validation engine evaluates 27 rules against the canonical identity payload before any Graph API call is made. Missing manager association, duplicate UPN, employment type in a Manager-tier role, privileged group correlation checks. If the gate fails, the record is held. Provisioning does not run.

**Separation of Duties — preventive and detective.** Entitlements are checked against `sod_policies.json` before any Graph write. For Joiner events, effective access equals the resolved entitlements. For Mover events, the effective access set is `unchanged ∪ retain_set ∪ groups_to_add ∪ remove_confirmed` — groups pending removal are included because they still exist on the user at evaluation time. Block-level violations stop the event and route to a dedicated SoD hold record. Warn-level violations are recorded and the event continues. The PowerShell engine runs the same catalogue in FullScan and post-provision modes as the detective layer.

**Mover delta engine.** Group changes are computed as four non-overlapping sets: groups to add, groups to remove, unchanged groups, and unmanaged groups outside the managed catalogue. Unmanaged groups are excluded from all delta logic and recorded in the audit record as NOT_PROCESSED. Attribute changes across seven tracked fields are computed in parallel.

**Retention registry.** Before any removal executes, the engine checks `RetentionRegistry` in Azure Table Storage for each group flagged for removal. A valid retention record moves the group to the retain set. An expired record or no record routes it to confirmed removal. Every decision is recorded individually.

**Two-pass Mover SoD re-evaluation.** Pass A evaluates the full proposed post-move access set including groups pending removal. Pass B evaluates the incoming groups in isolation to catch self-conflicting role mappings. Both passes always run. A block in either pass routes the event to HOLD_FOR_REVIEW, releases the processing lock, and updates JmlEvents to Failed. Nothing touches Entra until a human resolves the conflict.

**Engine-wide event store.** JmlEvents is shared across all lifecycle event types. `claim_event()` performs an atomic insert — duplicate event IDs exit immediately with no side effects. `acquire_lock()` prevents concurrent processing of the same event. `release_lock()` is called on every exit path, including HOLD_FOR_REVIEW and removal failures, so no event row remains locked after processing ends.

**PIM adjustment.** When a role change adds or removes PIM-eligible group mappings, the engine creates or removes eligible assignments via the Graph privileged access API. PIM groups are schedule objects and never appear in memberOf — the delta comes from entitlement resolution output, not the group delta. Active PIM sessions are allowed to expire naturally per ADR-003. PIM requires Entra ID P2.

**Post-move verification.** After a Mover event completes, the engine re-fetches actual group membership, compares it against the expected state, and calls the PowerShell validation engine in post-provision mode. Two categories are excluded from the unexpected discrepancy check: unmanaged groups (outside the managed catalogue by design) and recently removed groups (confirmed deleted at Step 6 but not yet propagated — Graph membership writes do not always reflect immediately).

**Policy rules engine.** Group and RBAC assignments come from `role_mapping_rules.json`, evaluated against the canonical payload at runtime. Adding a new job title mapping is a config file edit. Every entitlement decision is traced to a named rule ID in the audit report.

**Employment type enforcement.** Contractors and Interns cannot be provisioned into Manager-tier or privileged groups. This runs in the pre-provision gate against the payload, before the user object exists.

**BambooHR integration.** The engine polls BambooHR directly and derives the lifecycle action (Joiner, Mover, Skip) by comparing the HR record against live Entra ID state. Delta polling fetches only employees changed since the last checkpoint. Three idempotency layers prevent double-provisioning regardless of ingestion mode.

**Canonical normalisation.** Raw HR field values are resolved to controlled canonical values before any downstream component sees them. Unknown values go to the hold queue.

**Hold queue state machine.** Records that fail any gate enter a formal state machine with defined transitions, reason codes, retry counts, and a manual release path. SoD violations get a dedicated hold record type so operators can distinguish a governance block from a data quality problem.

**Immutable audit reports.** Every identity event produces a structured JSON report regardless of outcome. Every action taken, every gate result, every rule ID, every SoD violation with conflicting groups and compensating control text. One file per event, written once, never modified.

**Graph API throttling recovery.** All Graph calls use automatic retry. HTTP 429 responses respect the `Retry-After` header and retry up to three times. Server errors use exponential backoff. Client errors fail immediately.

---

## Business Value

Each control maps to a specific audit finding, compliance exposure, or operational cost that it prevents.

| Control | Business Outcome |
|---|---|
| Pre-provision SoD check | Violations caught before provisioning never appear in access review reports or audit logs. No remediation, no incident record. |
| Pre-provision governance gate | Policy violations are blocked before any directory object exists. Nothing to remediate, no additional audit entries. |
| Retention registry | Temporary access approvals are formally tracked, time-bounded, and automatically expired. The alternative is ungoverned carry-forward accumulating across every role transition. |
| Immutable audit reports | Compliance evidence is produced at provisioning time, not reconstructed from logs during an audit. Eliminates gaps where log rotation has removed relevant entries. |
| Engine-wide event store | Duplicate provisioning is an architectural impossibility, not an operational procedure. Retries, replays, and concurrent invocations all resolve to a single outcome. |
| Deterministic entitlement resolution | Two people with the same job title in the same department receive identical access. Policy is the source of truth, not the person who processed the ticket. |
| Mover SoD re-evaluation | Role transitions do not silently accumulate SoD violations. A conflict between incoming and outgoing access is caught before any write occurs — not discovered in the next access certification cycle. |
| Post-move verification | The engine confirms what Entra ID actually contains after every change. Graph API calls can partially succeed. Independent verification catches the difference before the audit record is written. |

---

## IAM Design Principles

**Least privilege.** Entitlements come from validated attributes evaluated against a policy. No template-based assignments. No convenience memberships.

**Separation of duties.** The SoD catalogue is the single source of truth for conflict definitions. Both the Python engine (preventive) and the PowerShell engine (detective) read from the same file. Adding a new conflict pair is a one-line JSON edit.

**Governance before access.** Both the validation gate and the SoD check are hard blocks. Neither is advisory. Provisioning cannot proceed until both pass. For Mover events, no access change executes until the SoD re-evaluation clears.

**Zero trust.** Every identity event is evaluated against policy before access is granted or changed. Nothing is inherited from templates or inferred from role history.

**Auditability.** Every provisioning decision is traceable to a rule ID. Every SoD violation is recorded with the conflicting groups, the policy that fired, and the compensating control requirement. Compliance evidence is produced at the time of provisioning.

**Fail closed.** SoD evaluation blocks on degraded data. A partial effective access set can miss real violations. A false block is recoverable through human review. A missed violation is not.

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        INPUT LAYER                              │
│   BambooHR API / CSV → Parser → Canonical IdentityPayload       │
│   Action derivation: Joiner / Mover / Skip                      │
│   Structural failures → Hold Queue (NormalizationFailed)        │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                    NORMALIZATION LAYER                          │
│   Canonical Lookup Table (config/canonical_lookup.json)         │
│   Raw field values → Standardised department / job title        │
│   Unknown values → Hold Queue                                   │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│              ENGINE-WIDE EVENT STORE (JmlEvents)                │
│   Shared across Joiner · Mover · Leaver                         │
│   claim_event()   — atomic insert · duplicate exits immediately │
│   acquire_lock()  — written after user fetch / gate pass        │
│   release_lock()  — called on every exit path                   │
│   SHA-256 deterministic EventId · stale lock auto-reclaim       │
└──────────────┬──────────────────────────┬───────────────────────┘
               │                          │
     ┌─────────▼──────┐         ┌─────────▼──────────┐
     │  JOINER PATH   │         │    MOVER PATH      │
     │                │         │                    │
     │ Conflict queue │         │ MoverEventLog      │
     │ FIFO per       │         │ concurrent check   │
     │ identity       │         │ (JmlEvents lock is │
     └─────────┬──────┘         │  the hard guarantee│
               │                └─────────┬──────────┘
     ┌─────────▼──────┐                   │
     │ ENTITLEMENT    │         ┌─────────▼──────────┐
     │ RESOLUTION     │         │ DELTA ENGINE       │
     │ rules.json     │         │ groups_to_add      │
     └─────────┬──────┘         │ groups_to_remove   │
               │                │ unchanged · unmanaged
               │                └─────────┬──────────┘
               │                          │
               │                ┌─────────▼──────────┐
               │                │ RETENTION EVAL     │
               │                │ RetentionRegistry  │
               │                │ RETAINED / EXPIRED │
               │                └─────────┬──────────┘
               │                          │
     ┌─────────▼──────────────────────────▼──────────┐
     │              SEPARATION OF DUTIES             │
     │   Joiner: effective = requested entitlements  │
     │   Mover Pass A: unchanged ∪ retain_set ∪      │
     │     groups_to_add ∪ remove_confirmed          │
     │   Mover Pass B: groups_to_add in isolation    │
     │   Block → HOLD_FOR_REVIEW → release_lock()    │
     │         → JmlEvents Failed → STOP             │
     │   Warn  → record → continue                   │
     └─────────────────────┬──────────────────────────┘
                           │
     ┌─────────────────────▼──────────────────────────┐
     │          PRE-PROVISION VALIDATION GATE         │
     │   PowerShell Identity Governance Engine        │
     │   27 rules · zero Graph API calls              │
     │   Joiner: acquire_lock() on gate pass          │
     │   Failures → Hold Queue (ValidationFailed)     │
     └─────────────────────┬──────────────────────────┘
                           │
     ┌─────────────────────▼──────────────────────────┐
     │             GRAPH API OPERATIONS               │
     │   Joiner: create user · add groups             │
     │   Mover:  remove confirmed · add new groups    │
     │           patch attributes                     │
     │   All operations idempotent                    │
     │   429 throttling: Retry-After backoff          │
     │   Mover removal failure → release_lock() → END│
     └─────────────────────┬──────────────────────────┘
                           │
     ┌─────────────────────▼──────────────────────────┐
     │               PIM ELIGIBILITY                  │
     │   Add / remove eligible assignments            │
     │   Delta from entitlement resolution output     │
     │   (PIM groups are schedule objects, not        │
     │    memberOf — not visible in group delta)      │
     │   Requires Entra ID P2 · skipped if absent     │
     └─────────────────────┬──────────────────────────┘
                           │
     ┌─────────────────────▼──────────────────────────┐
     │           POST-PROVISION VALIDATION            │
     │   Validation engine re-runs vs actual state    │
     │   Mover: membership verification               │
     │          unmanaged groups excluded             │
     └─────────────────────┬──────────────────────────┘
                           │
     ┌─────────────────────▼──────────────────────────┐
     │                 AUDIT LAYER                    │
     │   Joiner: DecisionReport (JSON file)           │
     │   Mover:  MoverAuditRecord (Table Storage)     │
     │   release_lock() · JmlEvents → Completed/Failed│
     │   Every outcome · immutable                    │
     └────────────────────────────────────────────────┘
```

---

## Sequence Diagrams

### Joiner Sequence

```
BambooHR / CSV
  │
  │  raw employee record
  ▼
Action Deriver ──── Entra ID (live state check)
  │
  │  action = Joiner
  ▼
Normalization Layer ──── canonical_lookup.json
  │
  │  resolved IdentityPayload
  ▼
JmlEvents ─── claim_event() ──── duplicate? → EXIT (no side effects)
  │
  │  claimed
  ▼
Entitlement Resolution ──── role_mapping_rules.json
  │
  │  groups + RBAC + PIM entitlements
  ▼
SoD Checker ──── sod_policies.json
  │
  ├── BLOCK → Hold Queue (SoDViolation) → audit report → EXIT
  │
  │  clean / warn recorded
  ▼
Pre-Provision Validation Gate ──── PowerShell engine (27 rules, 0 Graph calls)
  │
  ├── FAIL → Hold Queue (ValidationFailed) → audit report → EXIT
  │
  │  PASS → acquire_lock() → JmlEvents
  ▼
Graph API Provisioning
  │  create user · add groups · RBAC via group membership
  │
  ▼
PIM Eligibility Assignment ──── Entra ID P2 required · skipped gracefully if absent
  │
  ▼
Post-Provision Validation Gate ──── PowerShell engine (real Entra state)
  │
  ▼
Audit Layer
  │  DecisionReport (JSON) written · release_lock() · JmlEvents → Completed
  ▼
  END
```

### Mover Sequence

```
BambooHR attribute change detected
  │
  │  action = Mover
  ▼
Normalization Layer ──── canonical_lookup.json
  │
  ▼
JmlEvents ─── claim_event() ──── duplicate? → EXIT (no side effects)
  │
  │  claimed
  ▼
Step 1: Current-State Discovery
  │  MoverEventLog concurrent check ──── active event? → QUEUED_CONCURRENT → EXIT
  │  Graph API: fetch attributes + memberOf
  │  acquire_lock() → JmlEvents
  │
  ▼
Step 2: Target-State Calculation
  │  resolve new role entitlements · resolve old role entitlements
  │  build managed catalogue from role_mapping_rules.json
  │
  ▼
Step 3: Delta Analysis
  │  groups_to_add · groups_to_remove · unchanged · unmanaged
  │  attribute delta across 7 TRACKED_ATTRIBUTES
  │
  ▼
Step 4: Retention Evaluation ──── RetentionRegistry (Azure Table Storage)
  │  groups_to_remove → RETAINED (retain_set) or remove_confirmed
  │
  ▼
Step 5: SoD Re-Evaluation ──── sod_policies.json (fail closed)
  │  Pass A: unchanged ∪ retain_set ∪ groups_to_add ∪ remove_confirmed
  │  Pass B: groups_to_add in isolation
  │
  ├── BLOCK → HOLD_FOR_REVIEW → MoverHoldQueue written
  │           release_lock() · JmlEvents Failed → EXIT
  │
  │  clean / warn recorded
  ▼
Step 6: Access Removals ──── Graph API (idempotent)
  │
  ├── FAILURE → release_lock() · JmlEvents Failed → EXIT
  │
  ▼
Step 7: Access Additions + Attribute Patch ──── Graph API
  │
  ▼
Step 8: PIM Adjustment ──── Graph privileged access API
  │  delta from entitlement resolution (PIM groups are schedule objects, not memberOf)
  │  P2 absent → warning recorded · continue
  │
  ▼
Step 9: Post-Move Verification
  │  re-fetch memberOf (10s consistency delay)
  │  compare against unchanged ∪ retain_set ∪ groups_to_add
  │  unmanaged groups excluded · recently removed groups excluded
  │  governance validation engine called
  │
  ▼
Step 10: Audit Reporting
  │  MoverAuditRecord → MoverAuditLog · MoverEventLog → terminal status
  │  release_lock() · JmlEvents → Completed or Failed
  ▼
  END
```

---

## Mover — Processing Flow

```
Pre-Step  claim_event() in JmlEvents
          → Atomic insert — duplicate event ID exits immediately
          → JmlEvents is engine-wide, shared across Joiner, Mover, Leaver

Step 1    Current-state discovery
          → Concurrent event check via MoverEventLog (first-layer guard)
          → Fetch current Entra attributes and memberOf via Graph API
          → acquire_lock() written to JmlEvents on success

Step 2    Target-state calculation
          → Resolve entitlements for new role (new department + title)
          → Resolve entitlements for old role (current Entra attributes)
          → Build managed catalogue from all rules

Step 3    Delta analysis
          Group delta:     groups_to_add, groups_to_remove, unchanged, unmanaged
          Attribute delta: seven TRACKED_ATTRIBUTES compared field by field

Step 4    Retention evaluation
          → For each group in groups_to_remove: check RetentionRegistry
          → Valid retention   → retain_set (excluded from removal)
          → Expired/no record → remove_confirmed

Step 5    SoD re-evaluation (fail closed — no writes until this clears)
          Pass A: unchanged ∪ retain_set ∪ groups_to_add ∪ remove_confirmed
                  → Block found: HOLD_FOR_REVIEW → MoverHoldQueue written
                  → release_lock() → JmlEvents Failed → STOP
          Pass B: groups_to_add in isolation → self-conflict check

Step 6    Execute access removals
          → remove_confirmed via Graph API
          → Partial failure → release_lock() → JmlEvents Failed → STOP

Step 7    Execute access additions + attribute update
          → groups_to_add via Graph API
          → PATCH tracked attributes (manager and usageLocation excluded)

Step 8    PIM adjustment
          → Compare old_resolved.pim_groups vs new_resolved.pim_groups
          → Add new PIM eligible assignments
          → Remove dropped PIM eligible assignments
          → Active sessions allowed to expire naturally (ADR-003)

Step 9    Post-move verification
          → Re-fetch memberOf (10s delay for Graph eventual consistency)
          → Compare against unchanged ∪ retain_set ∪ groups_to_add
          → Unmanaged groups excluded from discrepancy check
          → Recently removed groups excluded (Graph propagation lag)
          → Governance validation against real Entra object

Step 10   Audit reporting
          → Write MoverAuditRecord to MoverAuditLog
          → Update MoverEventLog to terminal status
          → release_lock() → JmlEvents updated to Completed or Failed
```

---

## Separation of Duties

### How the evaluation works

For Joiner events, effective access equals the resolved entitlements — the user does not yet exist, so current groups are always empty. For Mover events, Pass A evaluates `unchanged ∪ retain_set ∪ groups_to_add ∪ remove_confirmed`. Groups pending removal are included because they still exist on the user in Entra at evaluation time. A conflict between an incoming group and an outgoing group is a real SoD violation that must be caught before any write occurs.

### Conflict catalogue

Conflict pairs live in `sod_policies.json`. Both the Python engine and the PowerShell validation engine read from this file. Adding or modifying a conflict pair requires no code change.

| Policy | Conflict | Risk | Action |
|---|---|---|---|
| SOD-001 | Payment Approver + Payment Processor | Critical | Block |
| SOD-002 | IT User Provisioner + IT Access Approver | Critical | Block |
| SOD-003 | Payment Processor + Finance Auditor | High | Warn |
| SOD-004 | Payment Approver + Finance Auditor | High | Warn |
| SOD-005 | Journal Poster + Finance Auditor | High | Warn |
| SOD-006 | HR Salary Admin + HR Data Export | High | Warn |

### Two controls, one catalogue

`sod_checker.evaluate_sod()` is the preventive control. It runs before any Graph API call. A block violation stops the event before it occurs.

`Evaluate-SoDConflict` in `IDRuleProcessor.ps1` is the detective control. It runs during FullScan and post-provision verification against real tenant group memberships. This catches violations that entered through manual assignment or before SoD was active.

### Fail-closed on Mover

If the Graph API call to fetch current group memberships returns incomplete or failed results, the SoD check blocks immediately without evaluating policies. A partial effective access set can miss real violations. A false block is recoverable through human review. A missed violation is not.

---

## Mover ADR Summary

| ADR | Decision | Rationale |
|---|---|---|
| ADR-001 | SoD conflict → HOLD_FOR_REVIEW | Fail closed. False block is recoverable. Missed violation is not. |
| ADR-002 | Retention requires explicit registry record | No record = no retention. Every retention is time-bounded. |
| ADR-003 | PIM active sessions expire naturally | Engine removes eligible assignment only. Sessions are not cancelled. |
| ADR-004 | Concurrent events serialised | One active Mover event per employee at a time. Second event queues. |
| ADR-005 | Unmanaged groups not touched | Groups outside the managed catalogue are logged only. Not modified. |

---

## HR API Integration

```
BambooHR API
  ↓ bamboohr_client.py        fetch employee · delta poll · directory cache
  ↓ bamboohr_mapper.py        BambooHR fields → raw IdentityPayload shape
  ↓ action_deriver.py         Joiner / Mover / Skip — derived from live Entra state
  ↓ ingestion_coordinator.py  run_single · run_delta · run_bulk
  ↓ pipeline_adapter.py       routes Joiner → joiner pipeline · Mover → mover pipeline
  ↓ joiner_http/__init__.py   Joiner provisioning flow
  ↓ mover_http/__init__.py    Mover processing flow
```

| Mode | Command | Description |
|---|---|---|
| Single / batch | `--source api --id Acc003,Acc004` | Process specific employees by employee number |
| Delta poll | `--source api --mode delta` | Process all employees changed since last checkpoint |
| Joiner CSV | `--csv Data/sample_hr.csv` | CSV-based Joiner ingestion |
| Mover CSV | `--source mover --csv Data/sample_movers.csv` | CSV-based Mover ingestion |

---

## Idempotency and Concurrency

Three layers prevent double-provisioning regardless of how an event arrives — retry, duplicate HR trigger, or concurrent function invocation.

**Layer 1 — Delta timestamp.** BambooHR delta polling fetches only employees changed since the last checkpoint. Records with no meaningful attribute change are classified as Skip before any pipeline logic runs.

**Layer 2 — Action derivation.** For every record that passes the timestamp filter, the action deriver compares the incoming HR data against the user's live Entra state. If nothing has changed, the record is Skip. Only genuine differences proceed.

**Layer 3 — Deterministic EventId and atomic claim.** The EventId is a SHA-256 hash of EmployeeId, Action, and StartDate. `claim_event()` attempts an atomic insert into JmlEvents. Table Storage rejects duplicate row keys atomically — if the row already exists, the insert fails and the pipeline exits with no side effects. A retry, a duplicate submission, and a concurrent invocation all hit the same rejection.

**Concurrency locking.** Two function instances could both pass `claim_event()` before either acquires a lock. `acquire_lock()` handles that — it writes a `LockedAt` timestamp and `LockedBy` instance ID to the JmlEvents row. On the Joiner path the lock is acquired after the governance gate passes. On the Mover path it is acquired after the user fetch succeeds. The second instance reads the active lock and exits. Locks expire after ten minutes automatically, so a crashed instance does not block the next run. `release_lock()` is called on every exit path — success, SoD block, and removal failure. No event row stays locked after processing ends.

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
| `retain_list` | list[str] | Specific group IDs to retain |

---

## Known Limitations

**PIM requires Entra ID P2.** PIM eligibility assignments are non-blocking. If the tenant has no P2 licence, the PIM step records a warning and the event completes successfully. Group assignments are unaffected.

**SoD exception store is a stub.** `_exception_exists()` returns False unconditionally. Building a real exception store requires organisational agreement on approval authority, duration, and justification requirements. The detection layer works correctly regardless.

**usageLocation patching is deferred.** Graph API requires an ISO 3166-1 alpha-2 country code. BambooHR sends city names. The field is excluded from the PATCH body until a location-to-country mapping is added to `canonical_lookup.json`.

**manager update is deferred.** Updating the manager relationship requires a separate Graph endpoint from the standard user PATCH. Excluded from the attribute update step until that endpoint is implemented.

**RetentionRegistry entries are created manually.** The engine reads from the table but does not write to it. Production population requires an access request workflow.

**Graph membership propagation lag.** Group membership DELETE calls return 204 immediately but the change does not always appear in memberOf fetches for several seconds. The post-move verifier excludes recently removed groups from the unexpected discrepancy check to avoid false MOVE_PARTIAL results. If a removed group persists beyond the consistency window, the nightly FullScan will surface it as a hygiene finding.

**No automatic rollback on partial failure.** All Graph operations are idempotent so retrying from the beginning is safe. Partial state between failure and retry is a transient condition, not a persistent one.

**Queued events need a trigger to drain.** When a queued event is auto-released it moves to Pending but waits for the next pipeline run. A timer-triggered function is the production solution.

**HR API is polling-based.** Delta polling narrows the query window. A webhook handler is the planned next step and would only require a new HTTP trigger entry point.

---

## Phase Status

| Phase | Capability | Status |
|---|---|---|
| Phase 0 | Data contracts, normalisation, event store, hold queue, audit system | Complete |
| Phase 1 | Joiner provisioning, governance gates, policy-driven entitlements | Complete |
| Phase 2 | PIM eligible role assignment (requires Entra ID P2) | Complete |
| Phase 2.5 | Separation of Duties — preventive and detective controls | Complete |
| Phase 3 | Mover — delta calculation, SoD re-evaluation, retention, PIM adjustment | Complete |
| Phase 4 | Leaver — full revocation, session termination | Designed, not started |

---

## Running Locally

```bash
# Install Python dependencies
pip install -r requirements.txt

# Terminal 1 — start the PowerShell validation engine
cd Validation_engine
func start

# Terminal 2 — Joiner CSV mode
python scripts/run_local.py --clean --output reports --csv Data/sample_hr.csv

# Terminal 2 — Mover CSV mode
python scripts/run_local.py --source mover --csv Data/sample_movers.csv --clean

# Terminal 2 — single employee from BambooHR (Joiner or Mover, derived automatically)
python scripts/run_local.py --source api --id Acc003

# Terminal 2 — batch from BambooHR
python scripts/run_local.py --source api --id AccIT223,AccFA456,AccHr332

# Terminal 2 — delta poll
python scripts/run_local.py --source api --mode delta
```

---

## Repository Structure

```
JML-Engine/
├── Functions/
│   ├── joiner_http/
│   │   └── __init__.py              # Azure Function HTTP trigger · run_pipeline()
│   ├── mover_http/
│   │   └── __init__.py              # Azure Function HTTP trigger · run_mover_pipeline()
│   └── Event_store/
│       ├── event_store.py           # JmlEvents · claim_event() · acquire_lock() · release_lock()
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
│           ├── pipeline_adapter.py  # Routes Joiner → joiner pipeline · Mover → mover pipeline
│           └── ingestion_coordinator.py  # Orchestrates fetch · derive · pipeline · checkpoint
├── Normalization/
│   ├── lookup_loader.py             # Loads canonical_lookup.json
│   └── normalizer.py               # Resolves raw field values · accumulates failures
├── Mapping/
│   ├── mapping_loader.py            # Loads role_mapping_rules.json
│   └── mapping_resolver.py         # Evaluates rules against identity payload
├── Governance/
│   └── SoD/
│       ├── sod_models.py            # Enums · SoDPolicy · SoDViolation · SoDCheckResult
│       ├── sod_loader.py            # Loads and validates sod_policies.json
│       └── sod_checker.py          # evaluate_sod() · ANY_TO_ANY · fail-closed contract
├── Mover/
│   ├── delta_engine.py             # Pure group delta · four non-overlapping sets · no I/O
│   ├── attribute_delta.py          # Attribute-level diff · TRACKED_ATTRIBUTES · to_patch_dict()
│   ├── retention_evaluator.py      # RetentionRegistry lookup · RETAINED/EXPIRED/NO_RECORD
│   ├── sod_reevaluator.py          # Two-pass SoD re-evaluation · Pass A includes remove_confirmed
│   ├── pim_adjuster.py             # PIM eligible assignment add/remove/scope-change · ADR-003
│   └── post_move_verifier.py       # Post-move membership check · governance validation · unmanaged exclusion
├── Provisioning/
│   ├── graph_client.py             # Graph API client · retry on 429 · PIM endpoints
│   ├── pim_client.py               # PIM group eligibility assignment
│   └── provisioner.py              # Entra ID user · group · RBAC · PIM provisioning
├── Validation/
│   └── validation_gate.py          # Pre- and post-provision gate (HTTP)
├── Hold_queue/
│   ├── models.py                   # HoldStatus enum · HoldRecord · state constants
│   ├── queue_manager.py            # State machine · create_from_sod_violation()
│   └── azure_table_hold_queue_store.py
├── Audit/
│   ├── models.py                   # DecisionReport · ActionRecord · sod_violations
│   ├── report_writer.py            # Per-identity JSON audit reports
│   └── run_summary_writer.py       # Per-run summary
├── config/
│   ├── canonical_lookup.json       # Field variant to canonical value mappings
│   ├── role_mapping_rules.json     # JobTitle / Dept / EmploymentType to groups + RBAC + PIM
│   └── sod_policies.json           # SoD conflict pairs · risk ratings · compensating controls
├── scripts/
│   ├── run_local.py                # Local runner · CSV · API · Mover CSV modes
│   └── create_mover_tables.py      # One-time table creation for four Mover tables
├── reports/
└── Tests/
    ├── test_normalizer.py
    ├── test_sod.py                 # 63 tests — sod_loader and sod_checker
    ├── test_delta_engine.py        # 29 tests — set arithmetic, unmanaged isolation, determinism
    ├── test_attribute_delta.py     # 29 tests — field diffing, patch dict, boundary cases
    ├── test_retention_evaluator.py # 20 tests — decision logic, expiry boundary, mixed outcomes
    └── test_sod_reevaluator.py     # 24 tests — two-pass evaluation, remove_confirmed, Claire scenario
```

---

## Azure Table Storage

| Table | Purpose | Key Structure |
|---|---|---|
| JmlEvents | Engine-wide event store · all lifecycle events · idempotency · concurrency lock | PartitionKey = employee_id · RowKey = event_id |
| JmlHoldQueue | Hold records · normalization and validation failures | PartitionKey = employee_id · RowKey = record_id |
| JmlSystemState | Delta poll checkpoint · last successful poll timestamp | PartitionKey = system · RowKey = bamboohr |
| MoverEventLog | Mover event status lifecycle · concurrent event guard | PartitionKey = employee_id · RowKey = event_id |
| MoverAuditLog | Completed Mover change records · full audit trail | PartitionKey = employee_id · RowKey = event_id |
| MoverHoldQueue | Mover events blocked on SoD conflict · awaiting human resolution | PartitionKey = employee_id · RowKey = event_id |
| RetentionRegistry | Access retention records · time-bounded | PartitionKey = employee_id · RowKey = group_object_id |

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
| HR Integration | BambooHR API (live) · OrangeHRM webhook (planned) |

---

## Licence Dependencies

| Feature | Licence | Required From |
|---|---|---|
| Core provisioning (users, groups, RBAC) | Entra ID Free | Phase 0-1 |
| SoD enforcement | Entra ID Free | Phase 2.5 |
| Dynamic membership rules | Entra ID P1 | Phase 1 (optional) |
| Privileged Identity Management | Entra ID P2 | Phase 2 |