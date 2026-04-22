---
type: project
description: Script 365b close-out — 6 canonicals rebuilt with CHANGES A-N + Option-C cleanup; 3 local commits, no push
---

# Script 365b close-out

## Tables

- **6 canonicals** at `canonical_version='v1_0_script365_remediated'`
  (psh / pmh / meds × events / rollup):
  - `canonical_psh_events_v1`                    3,919 rows / 19 cols
  - `canonical_psh_patient_rollup_v1`           10,871 rows / 28 cols
  - `canonical_pmh_events_v1`                   12,444 rows / 19 cols
  - `canonical_pmh_patient_rollup_v1`           10,871 rows / 79 cols
  - `canonical_medications_events_v1`            7,501 rows / 19 cols
  - `canonical_medications_patient_rollup_v1`   10,871 rows / 28 cols
- **2 legacy entity-row sources DROPPED** from
  `thyroid_canonical_publication_v1_0.main`:
  - `note_entities_problem_list` (was 11,579 rows)
  - `note_entities_medications`  (was 7,501 rows)
- **`tier2.past_medical_hx_event_v1` DROPPED** (was 865 rows; the
  surviving tier2.* orphan flagged in the remediation handoff)
- **LN script renumbered**: `scripts/365_canonical_us_lymph_node_v2.py`
  → `scripts/364b_canonical_us_lymph_node_v2.py` to resolve the 365
  script-number collision; `canonical_us_lymph_node_v2` table comment
  patched to credit Script 364b.

## Commits (no push)

- `09e6f9f` — **Phase 0**: rename LN v2 + tier2 drop
- `5e3d22c` — **Phase 1**: rebuild 6 canonicals (CHANGES A-N + Logan overrides)
- `0a2ec27` — **Phase 3**: snapshot + DROP legacy entity-row sources
- *(this commit)* — **Phase 4**: memory + close-out

**Phase 2 SKIPPED** per Option-C — the literal-source readers (Scripts
212 + 215) target a different DB namespace and are historical one-shots,
so a CPM repoint would have overwritten 78 frozen publication col values
with current canonical state, breaking reproducibility for any analysis
already run against those cols.

## QA results

- **All 35 hard QA gates PASS** post-rebuild against `main.*`.
- **Events rowcounts preserved exactly** vs. dry-run (Logan's
  rowcount-unchanged guardrail): 3,919 / 12,444 / 7,501 (drift = 0).
- **Rollup parity**: all 3 rollups = 10,871 rows (CHANGE J:
  LEFT JOIN FROM canonical_patient_master).
- **anchor_source split** (HYBRID per Logan's Phase-1 override):
  77.0% strict (8,367 patients) / 23.0% first_surgery_fallback
  (2,504 patients) / 0% NULL. The 23% fallback is driven by upstream
  `procedure_normalized` corruption — see CF-A.
- **med_status distribution** (CHANGE H invert-default fix):
  80.68% unknown / 19.32% active / 0% historical. The 0% historical
  is an upstream evidence_span length limit (avg 11.8 chars carries no
  context cues), not a logic bug — see CF-B.

## Reusable patterns (5)

### P1 — Hybrid anchor with audit column

When a strict anchor (e.g., `procedure_normalized ILIKE '%thyroidect%'`)
returns >10% NULL because of upstream-source corruption, build a
HYBRID anchor `COALESCE(strict, fallback)` AND surface an
`anchor_source ∈ {strict, <fallback_label>, NULL}` audit column on
both events and rollup. Downstream consumers can filter to strict-only
when they need semantic precision; defaults preserve cohort coverage.

### P2 — Unknown-as-default for status-like classifiers

When a status-like classifier (e.g., `med_status`) has 3+ values
including a "catch-all" tier (active/historical/**unknown**), make the
catch-all the DEFAULT branch and require explicit marker matches for
the other tiers. Inverting "active is default" → "unknown is default
+ require markers" prevents silent classification of every row to the
default tier when source signals are missing. Add a QA gate asserting
the catch-all stays under 90% (or whatever loose-floor your data
supports).

### P3 — Uniform evidence_strength tier is acceptable when the source is structurally uniform

If a domain has only one source kind (e.g. PSH only has LLM extraction;
Meds only has structured pharmacy lists), it's expected and correct for
`evidence_strength` to roll up uniform across that domain (`probable`
for LLM-only PSH, `definitive` for pharmacy-list-only Meds). Document
the structural reason in the script header — DO NOT auto-tune the
heuristic to manufacture variance.

### P4 — Registry writes land in `manuscript_workspace`

`manuscript_workspace.detail_table_registry_v1` (NOT `main.*`) is the
authoritative registry. Filter column is `detail_table_name`. Write
pattern is **idempotent DELETE-first + INSERT** (Pattern 13). The
registry has 13 cols including `canonical_version` for traceability.

### P5 — `events_rowcount_unchanged` as a QA guardrail for layered changes

When CHANGES A-N or similar large refactors layer onto an existing
canonical, the events row counts should remain identical to the
prior build (the changes touch derived columns, status ladders,
phenotype rollups — NOT the underlying source-row inventory). Dry-run
the build, capture the events rowcounts, and turn them into a hard QA
gate (`events_rowcount_unchanged_<domain>` == expected) that fires on
the live rebuild. Any drift = something changed in source-extraction
logic and needs investigation BEFORE accepting the rebuild.

## Carry-forwards

### CF-A — `procedure_normalized` upstream corruption

`docs/tier1_cf_procedure_normalized_corruption_20260422.md`. Upstream
`canonical_operative_events_v1.procedure_normalized` collapses to only
3 distinct values (`total_thyroidectomy`, `hemithyroidectomy`,
`'other'`), with the `'other'` bucket polluted by pathology strings
(`procedure_raw='Papillary Thyroid Carcinoma'` style). Re-normalising
upstream with an expanded keyword set would raise the
`anchor_source='strict'` percentage above 77% and reduce reliance on
the `first_surgery_fallback` branch.

### CF-B — `med_status` historical=0 is an upstream evidence-span length limit

`note_entities_medications.evidence_span` averages 11.8 chars and is
just the bare med name (no surrounding context). Historical-phrase
markers like "was on", "previously took", "discontinued" cannot
match because the context isn't extracted. Future upstream
re-extraction with an N-character context window around the entity
would expose real historical meds.

### CF-C — Phase 2 skipped; 78 CPM cols frozen at prior publication

The literal-source-reader audit found 78 CPM cols sourced from the
2 legacy tables via Scripts 212 + 215 (lineage from a different DB
namespace — see `reference_thyroid_ete_fix_20260413_namespace.md`).
These cols remain frozen at their prior publication values; any
future "refresh" is a full publication re-cut (new
`canonical_version` on CPM), not a Tier-2 repoint.

## Artifacts in repo

- **Phase 3 close-out**: `docs/script_365b_phase3_close_out_20260422.md`
- **Tier-1 CF**: `docs/tier1_cf_procedure_normalized_corruption_20260422.md`
- **Build log**: `scripts/output/365_run_20260422T064230Z.log`
- **Decision JSON**: `scripts/output/365_decision_20260422T064230Z.json`
- **QA JSON**: `qa/qa_script_365_psh_pmh_meds.json` (35 gates +
  informational; tracked)
- **CPM audit MD** (gitignored — per-run artifact):
  `psh_pmh_meds_cpm_feeder_audit_20260422T064230Z.md`
- **Live registry rows**:
  `manuscript_workspace.detail_table_registry_v1` × 6 at
  `canonical_version='v1_0_script365_remediated'`
