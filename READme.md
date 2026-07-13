# Policy-Driven Identity Lifecycle Engine

Governance-first Joiner, Mover, and Leaver automation for Microsoft Entra ID.

Most provisioning systems create identities first and validate access afterwards. I built this engine to reverse that model. Governance validation, policy evaluation, and Separation of Duties (SoD) checks run before any Microsoft Graph operation. If a request fails policy, no identity is created and no access is modified. Every decision is recorded in an audit report.

> **Key idea:** Governance should decide whether provisioning happens, not verify it afterwards. I resolve entitlements, validate policy, and evaluate Separation of Duties before any identity or access change is written to Microsoft Entra ID.

---

## Architecture Overview

![Architecture overview](docs/architecture-overview.svg)

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full system design.

---

## Features

- Policy-driven Joiner provisioning
- Mover processing with access delta calculation
- Governance validation against a 33-rule policy set before any Graph write
- Separation of Duties, preventive and detective
- Time-bounded access retention on role change
- PIM eligible assignment management
- Deterministic, idempotent event processing
- Immutable per-event audit reports
- BambooHR live ingestion and CSV offline mode
- Azure Functions deployment

---

## Pipeline

```
HR
 ↓
Canonical Identity
 ↓
Entitlement Resolution
 ↓
SoD Evaluation
 ↓
Governance Validation
 ↓
Microsoft Graph
 ↓
Post-Provision Validation
 ↓
Audit Report
```

Joiner and Mover share the same governance pipeline, diverging only during entitlement resolution, where a Joiner resolves new access and a Mover calculates an access delta against the current state.

---

## Documentation

| Document | Purpose |
|---|---|
| README.md | Project overview (this file) |
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design, pipeline layers, sequence diagrams |
| [DEVELOPER.md](DEVELOPER.md) | Repository layout, local setup, module reference |
| [docs/GOVERNANCE.md](docs/GOVERNANCE.md) | Governance model, preventive vs detective controls |
| [docs/ADR.md](docs/ADR.md) | Architecture Decision Records |

---

## Repository Structure

```
Functions/        Azure Function triggers · engine-wide event store
Ingestion/        HR feed parsing · BambooHR client · action derivation
Normalization/    Canonical lookup and field resolution
Mapping/          Policy rules engine
Governance/SoD/   Separation of Duties evaluation
Mover/            Delta · retention · PIM adjustment · post-move verification
Provisioning/     Graph API client · PIM client · provisioner
Validation/       Validation engine HTTP gate
Hold_queue/       Hold state machine
Audit/            Per-event decision reports
config/           canonical_lookup · role_mapping_rules · sod_policies
Tests/
```

Full module-by-module breakdown is in [DEVELOPER.md](DEVELOPER.md).

---

## Running Locally

**Prerequisites**
- Python 3.11
- Azure Functions Core Tools
- PowerShell 7

```bash
pip install -r requirements.txt

# start the PowerShell validation engine
cd Validation_engine && func start

# Joiner from CSV
python scripts/run_local.py --csv Data/sample_hr.csv

# Mover from CSV
python scripts/run_local.py --source mover --csv Data/sample_movers.csv

# from BambooHR, action derived automatically
python scripts/run_local.py --source api --id Acc003
python scripts/run_local.py --source api --mode delta
```

All modes and flags are documented in [DEVELOPER.md](DEVELOPER.md).

---

## Technology

| Layer | Technology |
|---|---|
| Runtime | Python 3.11 |
| Compute | Azure Functions |
| Identity | Microsoft Entra ID |
| API | Microsoft Graph |
| Storage | Azure Table Storage |
| Authentication | Managed Identity |
| Validation | PowerShell Azure Function |
| HR Source | BambooHR |

---

## Project Status

| Phase | Capability | Status |
|---|---|---|
| 0 | Foundation, event store, hold queue, audit | Complete |
| 1 | Joiner provisioning and governance gates | Complete |
| 2 | PIM eligible assignment (Entra ID P2) | Complete |
| 2.5 | Separation of Duties, preventive and detective | Complete |
| 3 | Mover delta, retention, SoD re-evaluation, PIM | Complete |
| 4 | Leaver revocation and session termination | In progress |
| — | HR webhook ingestion | Planned |

---

## Related Projects

This engine is part of a broader identity governance portfolio.

| Project | Purpose |
|---|---|
| Validation Engine | Detect governance violations across Microsoft Entra ID tenants. |
| Catalog Recommendation Engine | Analyze existing entitlements and recommend access packages. |
| JML Engine | Enforce governance during identity lifecycle automation. |

---

## Design Principles

- **Governance before access.** The validation gate and the SoD check are hard blocks, not advisories.
- **Least privilege.** Entitlements come from validated attributes against policy. No template or convenience access.
- **Separation of duties.** Conflict pairs are defined as policy and enforced before access is granted, not discovered in a later certification cycle.
- **Fail closed.** Degraded data blocks the event. A false block is recoverable; a missed violation is not.
- **Deterministic entitlement resolution.** Same input, same access, every run.
- **Auditability by design.** Every decision traces to a rule ID. Evidence is produced at provisioning time, not reconstructed from logs.