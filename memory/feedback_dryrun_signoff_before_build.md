---
type: feedback
description: Tier-2 canonical builds require a dry-run + all QA gates signed off BEFORE any CREATE OR REPLACE on main.*
---

# Dry-run + QA sign-off before any Tier-2 `main.*` mutation

**Rule**: No `CREATE OR REPLACE TABLE main.*` for a Tier-2 canonical
build until a dry-run emits all QA gates to scratch TEMP tables and
the operator signs off on the numbers AND the design.

## Why

The prior 365 agent (2026-04-22 pre-remediation) shipped 6 canonicals
live in `main` without dry-run sign-off. Silent violations were caught
only retroactively:

- **Cohort-parity gate checked the wrong spec** — `rollup ==
  events-distinct-patients` instead of `rollup == CPM`. That made the
  rollup rows under-count by 23%–80% per domain (1,878 / 4,112 / 2,070
  vs. the correct 10,871 each).
- **CHANGES A-N not implemented** — events grain shipped at 10 cols
  instead of 19; rollup at 6 cols instead of ~28-79 per domain.
  No `evidence_strength` tier, no `is_preexisting`, no
  `anchor_source`, no `med_status`.
- **`source_modality` semantically collided** with
  `canonical_us_lymph_node_v2.source_modality='US'` — needed to be
  renamed `source_note_type` (CHANGE I).
- **`evidence_span_hash` double-encoded** to 128 chars because
  `HEX(SHA256(...))` was wrapping DuckDB's already-hex-encoded SHA256
  output.

A dry-run would have surfaced **all** of these defects before `main.*`
mutated. Instead the agent had to author a remediation cascade
(Script 365b: Phases 0 → 1 → 3 → 4) just to reach the spec the
prior build was supposed to ship.

## How to apply

For any Tier-2 canonical build:

1. Write a sibling `<script>_dryrun.py` (e.g.
   `scripts/365b_dryrun.py`) that materialises the proposed canonicals
   into **session-scoped TEMP tables** named `_dryrun_*`. NO writes to
   `main.*`.
2. Run **all hard QA gates** against the TEMP tables — including
   cohort-parity (rollup == CPM rowcount), source attribution
   completeness, status enum, evidence-strength enum, view resolution.
3. Generate a **forensic PHI sample** at boundary cases (research_id +
   evidence_span_hash only, NEVER raw clinical text). Write to
   `phi_forensic/` (gitignored prefix).
4. Emit a **summary markdown report** to `scripts/output/<script>_dryrun_<ts>.md`
   and a **gates JSON** to `qa/qa_script_<script>_dryrun_<ts>.json`
   (gitignored).
5. **Post the summary in chat. STOP.** Wait for explicit "go" before
   the live-build rewrite.
6. **Capture the dry-run events row counts** — they become a hard QA
   guardrail (`events_rowcount_unchanged_<domain>`) on the live
   build. Any drift = something changed in source-extraction logic
   and needs investigation BEFORE accepting the rebuild
   (per pattern P5 in `project_script_365b_close_out.md`).

## Reference precedent

Script 365b dry-run: `scripts/365b_dryrun.py` →
`scripts/output/365b_dryrun_20260422T061914Z.md`. The dry-run caught
4 issues before `main.*` mutated:

- A SQL parser error in the anchor-consistency gate (CTE-as-derived-table
  syntax).
- The `evidence_span_hash` double-encoding bug.
- The negation-ladder zero-suspected/zero-indeterminate distribution
  (documented as a Tier-1 CF rather than papered over).
- The 23% null-anchor rate from the upstream `procedure_normalized`
  corruption (drove the Phase-1 hybrid-anchor decision).

All four were resolved before the Phase-1 commit landed in `main.*`.

## Related memory

- `project_script_365b_close_out.md` — complete cascade narrative
- `feedback_cpm_frozen_at_publication.md` — companion rule for
  cleanup cycles
