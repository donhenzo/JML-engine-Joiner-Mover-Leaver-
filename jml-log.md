```log
 Ingestion.hr_api.action_deriver — User sarah.mitchell@tenant.onmicrosoft.com not found in Entra — action: Joiner (employee: Acc003)
INFO     Ingestion.hr_api.ingestion_coordinator — Employee Acc003 (sarah.mitchell@tenant.onmicrosoft.com) — action: Joiner — entering pipeline.
WARNING  Normalization.normalizer — Location 'Sydney, Australia' not in lookup table for employee Acc003 — keeping raw value.
INFO     Normalization.normalizer — Normalization passed for employee Acc003 (sarah.mitchell@tenant.onmicrosoft.com)
INFO     Functions.Event_store.event_store — Event claimed — employee=Acc003, action=Joiner, event_id=b21f2b325d162d309ed24fdaeccc7471
```