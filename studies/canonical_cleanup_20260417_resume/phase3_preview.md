# Phase 3 — proposed actions (audit-only preview)

_Generated 2026-04-18T04:02:47.265775+00:00_
_Audit scope: every object in `main` (BASE TABLE + VIEW). No DDL/DML executed in this stage._

## Action counts

| action | n |
|:---|---:|
| DELETE | 0 |
| DEPRECATE | 0 |
| ARCHIVE | 0 |
| KEEP_REVIEW | 0 |
| LIVE | 118 |
| **TOTAL** | 118 |

## Stop-gate evaluation (per Logan's spec)

- DELETEs proposed: **0** (gate threshold: 10)
- ARCHIVEs proposed: **0** (gate threshold: 5)
- Stop gate NOT tripped. Logan can authorise full execution (DEPRECATE → ARCHIVE → DELETE) without an extra checkpoint.

