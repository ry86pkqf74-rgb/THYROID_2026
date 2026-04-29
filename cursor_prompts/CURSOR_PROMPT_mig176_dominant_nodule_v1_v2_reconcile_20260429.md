# Cursor Prompt — mig_176 Dominant_Nodule v1/v2 Reconcile (1,065 mismatches)

**Lane:** 65 / mig_176
**Batch_id:** `mig_176_dominant_nodule_v1_v2_reconcile_20260429`
**Generated:** 2026-04-29
**Type:** Read-only profile + resolution rule proposal. **No data writes.** Logan ratifies before any apply.

---

## §0 Why this lane exists

`CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT` is open with **1,065 patients** showing mismatch between `dominant_nodule_size_cm` (v1) and `dominant_nodule_size_cm_v2` (v2). Without resolution, manuscript analytics on dominant nodule size silently bifurcate by which spine the analyst picks.

Cowork live 2026-04-29: 6 dominant_nodule cols on PM, all `verified`:

| Col | Dtype | Status | Notes |
|---|---|---|---|
| `dominant_nodule_size_cm` | DOUBLE | verified | v1 spine |
| `dominant_nodule_size_cm_v2` | DOUBLE | verified | v2 spine |
| `mri_has_dominant_nodule` | BOOLEAN | verified | MR-imaging-derived presence flag |
| `ops_dominant_nodule_bethesda` | VARCHAR | verified | OR pre-op Bethesda |
| `ops_dominant_nodule_location` | VARCHAR | verified | OR pre-op location |
| `ops_dominant_nodule_size_us` | VARCHAR | verified | OR pre-op US-derived size |

**This lane reconciles only the v1/v2 numeric size cols.** The 4 `ops_*` cols + `mri_*` are separate concepts (OR pre-op + MR sources), not v1/v2 of the same concept.

This lane:
1. Profiles the 1,065-mismatch shape (live-count by direction, magnitude, source patterns).
2. Proposes resolution rules (prefer v2 / prefer v1 / hybrid / case-by-case).
3. Surfaces a Logan-decision package.

## §1 Governance posture

- Read-only profile. No `query_rw`.
- Output: profile report + resolution rule proposal + commented probe SQL.
- Logan ratifies one resolution rule before mig_176b applies.

## §2 Required pre-flight probes

```sql
-- §2a Full mismatch shape
WITH dual AS (
  SELECT research_id,
         dominant_nodule_size_cm   AS v1_size,
         dominant_nodule_size_cm_v2 AS v2_size
  FROM main.canonical_patient_master
)
SELECT
  COUNT(*) FILTER (WHERE v1_size IS NOT NULL AND v2_size IS NOT NULL AND v1_size <> v2_size) AS n_mismatch,
  COUNT(*) FILTER (WHERE v1_size IS NOT NULL AND v2_size IS NULL) AS n_v1_only,
  COUNT(*) FILTER (WHERE v1_size IS NULL AND v2_size IS NOT NULL) AS n_v2_only,
  COUNT(*) FILTER (WHERE v1_size IS NOT NULL AND v2_size IS NOT NULL AND v1_size = v2_size) AS n_match,
  COUNT(*) FILTER (WHERE v1_size IS NULL AND v2_size IS NULL) AS n_both_null
FROM dual;
-- Expect: n_mismatch ≈ 1065 (per CF tag).

-- §2b Mismatch magnitude distribution
SELECT
  COUNT(*) FILTER (WHERE ABS(v1_size - v2_size) < 0.1) AS n_diff_lt_0_1cm,
  COUNT(*) FILTER (WHERE ABS(v1_size - v2_size) BETWEEN 0.1 AND 0.5) AS n_diff_0_1_to_0_5cm,
  COUNT(*) FILTER (WHERE ABS(v1_size - v2_size) BETWEEN 0.5 AND 1.0) AS n_diff_0_5_to_1cm,
  COUNT(*) FILTER (WHERE ABS(v1_size - v2_size) >= 1.0) AS n_diff_gte_1cm
FROM (
  SELECT dominant_nodule_size_cm AS v1_size, dominant_nodule_size_cm_v2 AS v2_size
  FROM main.canonical_patient_master
  WHERE dominant_nodule_size_cm IS NOT NULL AND dominant_nodule_size_cm_v2 IS NOT NULL
);

-- §2c Direction (v1 > v2 vs v2 > v1)
SELECT
  COUNT(*) FILTER (WHERE v1_size > v2_size) AS n_v1_larger,
  COUNT(*) FILTER (WHERE v2_size > v1_size) AS n_v2_larger,
  AVG(v1_size - v2_size) AS mean_diff
FROM (
  SELECT dominant_nodule_size_cm AS v1_size, dominant_nodule_size_cm_v2 AS v2_size
  FROM main.canonical_patient_master
  WHERE dominant_nodule_size_cm IS NOT NULL AND dominant_nodule_size_cm_v2 IS NOT NULL
);

-- §2d Source-table identification: trace the build for each col
-- Look at registry notes for both cols; identify what upstream tables/extractions they derive from
SELECT column_name, verification_method, batch_id, notes
FROM main.canonical_column_verification_registry_v1
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name IN ('dominant_nodule_size_cm','dominant_nodule_size_cm_v2');

-- §2e Cross-validate vs ops_dominant_nodule_size_us (independent OR pre-op signal)
SELECT
  COUNT(*) FILTER (WHERE v1_size = TRY_CAST(regexp_extract(ops_dominant_nodule_size_us, '^(\d+(?:\.\d+)?)', 1) AS DOUBLE)) AS n_v1_match_ops,
  COUNT(*) FILTER (WHERE v2_size = TRY_CAST(regexp_extract(ops_dominant_nodule_size_us, '^(\d+(?:\.\d+)?)', 1) AS DOUBLE)) AS n_v2_match_ops
FROM (
  SELECT dominant_nodule_size_cm AS v1_size, dominant_nodule_size_cm_v2 AS v2_size, ops_dominant_nodule_size_us
  FROM main.canonical_patient_master
  WHERE dominant_nodule_size_cm IS NOT NULL AND dominant_nodule_size_cm_v2 IS NOT NULL
);
-- This probe asks: when v1 and v2 disagree, which one matches the OR pre-op US note?
-- It's a tiebreaker signal.
```

## §3 Candidate resolution rules

Agent profiles + populates with live counts:

| Rule | Resolved size = | n_resolved (mismatch only) | Pro / Con |
|---|---|---:|---|
| **R1: prefer v2 always** | v2 if not null, else v1 | live-count | Simple; assumes v2 is newer/better |
| **R2: prefer v1 always** | v1 if not null, else v2 | live-count | Conservative; assumes v1 is established |
| **R3: max of v1/v2** | greatest non-null | live-count | Captures largest dimension; matches "dominant" semantics |
| **R4: avg of v1/v2** | mean if both present, else single | live-count | Smooth; clinically defensible |
| **R5: tiebreak by ops** | match whichever of v1/v2 agrees with `ops_*_size_us`; fall back to R1 if neither | live-count | Uses independent OR signal |
| **R6: keep both** | no resolution; manuscript chooses per-analytic | n/a | Defers decision; documentation only |

Agent recommends one with rationale.

## §4 Logan-decision package (must be in design doc)

The full **decision package** Logan needs:

1. Recommended rule (one of R1-R6).
2. Affected pt count under recommendation.
3. Distribution shift implied (mean / median / 95th percentile of resolved size before vs after).
4. Whether `dominant_nodule_size_cm_v2` should be deprecated post-resolution (rename to `_v2_legacy_raw`?) or kept.
5. Whether to add a `dominant_nodule_size_cm_resolution_rule` audit col.

## §5 Apply skeleton (for mig_176b later, NOT this lane)

```sql
-- mig_176b Section A: pre-snapshot dominant_nodule_size_cm + v2
-- mig_176b Section B: ALTER ADD COLUMN dominant_nodule_size_cm_resolved DOUBLE + dominant_nodule_size_cm_resolution_rule VARCHAR
-- mig_176b Section C: UPDATE per Logan-ratified rule
-- mig_176b Section D: registry resync
-- mig_176b Section E: post-state verify
```

DO NOT author this in mig_176 — wait for Logan's rule choice.

## §6 Required CFs

- `CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT` → STAYS OPEN until Logan ratifies; this lane provides decision package
- `CF-mig176-RECOMMENDED-RULE-<R1..R6>` (informational; agent's recommendation)

## §7 Files + Git workflow

- `qc_framework_v1/reports/mig_176_dominant_nodule_v1_v2_reconcile_20260429.md`
- `qc_framework_v1/migrations/176_dominant_nodule_reconcile_probes_20260429.sql`
- Commit: `qc: mig_176 dominant_nodule v1/v2 reconcile (read-only profile + resolution proposal)`
- Push.

## §8 Out of scope

- Do NOT apply any UPDATE.
- Do NOT touch the 4 `ops_*` / `mri_*` dominant_nodule cols (separate concepts).
- Do NOT propose changes to upstream extraction (that's a separate build lane).
- Do NOT modify dual-spine architecture for any other v1/v2 PM col pair (separate analyses).

## §9 Apply governance

Read-only lane. Agent ships profile + decision package only.

Per AGENTS governance: agent ships profile only. **No `query_rw` from agent.**
