# Cursor Prompt — mig_163 ANY-RECURRENCE Investigation (read-only profile)

**Lane:** 51 / mig_163
**Batch_id:** `mig_163_any_recurrence_investigation_20260429`
**Generated:** 2026-04-29 (late evening) — sourced from COWORK_HANDOFF_PROMPT_2026-04-29_v5.md §9
**Type:** Read-only investigation. **Output is SQL + a Markdown report**. No registry writes; no UPDATE/ALTER on `main.*`. Logan picks the clinical definition (STRICT / WIDE / HYBRID) before any mig_163b apply step is authored.

---

## §0 Governance — AGENTS doctrine

- **Read-only probes** against MotherDuck `thyroid_canonical_publication_v1_0` (and `"Thyroid 2026 UPdated".archive_pub_v1_0` for archive snapshot reads only).
- **No `query_rw`.** All output is SQL files + a Markdown report; Cowork (the orchestrator) and Logan ratify before any apply.
- **No table mutations**, no PM data writes, no registry writes — even the `notes` field is off-limits in this lane. Apply lane is mig_163b after Logan's clinical definition is locked.
- **Pre-snapshots:** none required (read-only).

## §1 The finding (background — already verified by Cowork)

PM `any_recurrence_flag` (ARF) is supposed to be a cross-domain "any recurrence anywhere" boolean. Cowork independent reconcile (2026-04-29) found this 2x2 vs `canonical_recurrence_v1.recurrence_confirmed`:

| Cell | Count | Meaning |
|---|---|---|
| ARF=TRUE / canonical=TRUE | 165 | Both flag (consistent) |
| ARF=TRUE / canonical=FALSE | 219 | PM-only (envelope wider than canonical_recurrence_v1) |
| ARF=FALSE / canonical=TRUE | **349** | **canon-only — derivation gap** |
| ARF=FALSE / canonical=FALSE | 10,138 | No recurrence either side |

Of the 349 canon-only: 11 have `biochemical_recurrence_flag=TRUE`, 149 have `structural_recurrence_flag=TRUE`, 149 have `distant_mets_proxy=TRUE`, 345 are `is_malignant=TRUE`. Split by canonical recurrence definition: 246 surgical_pathology / structural_confirmed (largest), 53 fna_bethesda_vi_malignant / fna_confirmed, ~50 other.

Naive `ARF = bioch_flag OR struct_flag OR distant_proxy OR canonical_confirmed` would flip **1,805 patients** TRUE (jump 384 → 2,187) — over-correction because `structural_recurrence_flag` and `distant_mets_proxy` use a wider envelope (1,818 each).

CF reference: `CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT`.

## §2 What the lane must produce

A single Markdown report at `qc_framework_v1/reports/mig_163_any_recurrence_investigation_20260429.md` containing the answers to §3, plus a **decision package** for Logan listing the three plausible definitions (STRICT / WIDE / HYBRID) with patient counts, malignancy splits, and clinical implications for each. No SQL apply file in this lane — apply waits on Logan's pick.

## §3 Required probes (verbatim — agent fills in counts and includes them in the report)

### §3.1 Profile the 1,818 `structural_recurrence_flag=TRUE` source distribution

```sql
WITH crr_path AS (SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
                  FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final='path_proven'),
     crr_imaging AS (SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
                     FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final='imaging_only_unconfirmed'),
     cr_conf AS (SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
                 FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed=TRUE),
     pm AS (SELECT CAST(research_id AS VARCHAR) AS rid, structural_recurrence_flag
            FROM main.canonical_patient_master)
SELECT
  SUM(CASE WHEN pm.structural_recurrence_flag THEN 1 ELSE 0 END) AS struct_t,
  SUM(CASE WHEN pm.structural_recurrence_flag AND cr_conf.rid IS NOT NULL THEN 1 ELSE 0 END) AS struct_in_cr_conf,
  SUM(CASE WHEN pm.structural_recurrence_flag AND crr_path.rid IS NOT NULL THEN 1 ELSE 0 END) AS struct_in_crr_path,
  SUM(CASE WHEN pm.structural_recurrence_flag AND crr_imaging.rid IS NOT NULL THEN 1 ELSE 0 END) AS struct_in_crr_imaging,
  SUM(CASE WHEN pm.structural_recurrence_flag
           AND cr_conf.rid IS NULL AND crr_path.rid IS NULL AND crr_imaging.rid IS NULL THEN 1 ELSE 0 END) AS struct_no_canonical_source
FROM pm
LEFT JOIN cr_conf ON cr_conf.rid = pm.rid
LEFT JOIN crr_path ON crr_path.rid = pm.rid
LEFT JOIN crr_imaging ON crr_imaging.rid = pm.rid;
```

Then repeat for `distant_mets_proxy` and `biochemical_recurrence_flag`. Report in a 3×5 matrix.

### §3.2 Probe the `note_entities_llm_recurrence` Tier-1 layer

For the `struct_no_canonical_source` patients, sample 20 rids and check whether they have any matching `note_entities_llm_recurrence` rows; report what entity types / mention text are driving the PM flag.

### §3.3 The 349 canon-only patients — clinical context

For the 349 ARF=FALSE / canonical=TRUE patients, profile by canonical `recurrence_type` (already partially done in §1 background). Then reverse-trace 10 random rids: for each, dump the canonical_recurrence_v1 row, the canonical_recurrence_resolved_v1 row, and the relevant PM proxy cols (biochemical_*, structural_*, distant_*). Hypothesis to test: are these patients with confirmed recurrence whose PM derivation never received the canonical signal because of a builder bug, or are they patients whose recurrence is in a definitional bucket (e.g., FNA-confirmed lateral neck) that is intentionally outside ARF's clinical scope?

### §3.4 Three-option counts table

For each of the three candidate definitions, report total ARF=TRUE patient count, malignant-only count, agreement % vs current PM ARF, and (most importantly) **which 219 PM-only patients each option drops** and **which 349 canon-only patients each option adds**:

- (a) **STRICT**: `ARF := canonical_recurrence_v1.recurrence_confirmed=TRUE` → 514 pts (drops 219, adds 349)
- (b) **WIDE**: `ARF := bioch_flag OR struct_flag OR distant_proxy OR canonical_confirmed` → 2,187 pts (adds 1,805)
- (c) **HYBRID**: `ARF := canonical_recurrence_v1.recurrence_confirmed=TRUE OR canonical_recurrence_resolved_v1.recurrence_status_final='path_proven'` → estimate

Add a fourth option only if §3.1–§3.3 surface a fifth pattern Cowork didn't anticipate.

### §3.5 Sanity gates

- Cohort parity: PM = 10,871 rows / 10,871 distinct research_id (verify and report).
- Confirm `canonical_recurrence_v1.recurrence_confirmed=TRUE` count equals 514.
- Confirm the 2x2 in §1 reproduces (165 / 219 / 349 / 10,138).

## §4 Report structure (`qc_framework_v1/reports/mig_163_any_recurrence_investigation_20260429.md`)

1. Executive summary (3 bullets)
2. The 2×2 reconcile + 349 canon-only profile
3. §3.1 source distribution matrix (3 PM proxy flags × 5 source columns)
4. §3.2 Tier-1 LLM probe results (sample of 20 rids)
5. §3.3 reverse-trace of 10 canon-only rids
6. §3.4 three-option counts table
7. **Decision request for Logan** — explicit framing of the clinical definition trade-off (manuscript implication: STRICT under-counts, WIDE over-counts, HYBRID balances). Recommend the option the data most supports, but defer the call.
8. Open carry-forwards (anything new the investigation surfaced)

## §5 Git workflow

- New file: `qc_framework_v1/reports/mig_163_any_recurrence_investigation_20260429.md`
- (Optional) save raw probe SQL to `qc_framework_v1/migrations/163_any_recurrence_investigation_probes_20260429.sql` — read-only / commented-only.
- Commit: `qc: mig_163 ANY-RECURRENCE investigation report (read-only profile)`
- Push to `origin/main`.

## §6 Out of scope (do NOT do in this lane)

- Do **not** apply any UPDATE on PM.any_recurrence_flag.
- Do **not** add notes or CFs to the registry.
- Do **not** define a new SSOT enum.
- Do **not** assume the answer; if you have a strong opinion, voice it as a recommendation to Logan in §7 of the report, but do not write the apply SQL.
