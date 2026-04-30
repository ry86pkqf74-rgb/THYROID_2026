# Cursor Prompt — mig_183 PM vessel_count (1 remaining not_started col) verify + apply

**Date:** 2026-04-30
**Lane:** mig_183 / pm_vessel_count_last_not_started
**Batch (proposed):** `mig_183_pm_vessel_count_verify_apply_20260430`
**Predecessor:** mig_181 (CLOSED at `ff1af15` — 15 syn_*_size cols flipped to verified; PM not_started 16 → 1)
**Posture:** Read-only audit + SQL authoring + commit + push. **DO NOT execute against MotherDuck.** Cowork applies via Path C.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** `main.canonical_column_verification_registry_v1` + `main.canonical_table_signoff_registry_v1` + `manuscript_workspace.cpm_reconciliation_provenance_v1` (registry-only)

---

## Mission

After mig_181, **PM has exactly 1 not_started col** remaining: `vessel_count`. mig_183 closes it. This is the last gate before `mig_162` PM finalization can run.

**Live MD probed by Cowork 2026-04-29 (post-mig_181):**
- 1 col at `not_started`: `vessel_count` (DOUBLE, ordinal_position=965, batch_id=NULL, verification_method=NULL, verified_by=NULL, notes=NULL)
- Population: 46 / 10,871 nonnull (0.42%); 6 distinct values; min=1, max=6, avg≈2.04
- Distribution: NULL=10,825 / 1=20 / 2=14 / 3=7 / 4=2 / 6=2 / 5=1
- Likely upstream/parallel cols on `canonical_patient_master`: `vasc_vessel_count_v13`, `vascular_vessel_count`, `vi_vessels_max` (each DOUBLE, each 46 nonnull on stable_true VI per mig_177c scope) and possibly `nsqip_vessel_sealant`
- All four DOUBLE cols are present in archive snapshots `cpm_pre_*_20260423_*` — so they pre-date the publication build

---

## Required scope

### §1 Discover what `vessel_count` actually is (lineage)

Read PM build script (likely `scripts/132_*` or similar) and grep for `vessel_count` in `scripts/`, `qc_framework_v1/migrations/*.sql`, and `cursor_prompts/*.md` to find:
- Which lane originally created it
- What its derivation rule is (alias of `vasc_vessel_count_v13`? `vi_vessels_max`? Independent parse from path_synoptics?)
- Whether it's a Type-A presence flag or a real measurement

```sql
-- Check pairwise correspondence with the 3 parallel VI cols on the 46 nonnull patients
SELECT
  vessel_count IS NOT NULL AS vc_nn,
  vasc_vessel_count_v13 IS NOT NULL AS v13_nn,
  vascular_vessel_count IS NOT NULL AS vvc_nn,
  vi_vessels_max IS NOT NULL AS vmax_nn,
  COUNT(*) AS n_pts
FROM main.canonical_patient_master
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC;

-- Where all 4 are nonnull, do they agree?
SELECT
  COUNT(*) AS n_all_nn,
  COUNT(*) FILTER (WHERE vessel_count = vasc_vessel_count_v13) AS n_match_v13,
  COUNT(*) FILTER (WHERE vessel_count = vascular_vessel_count) AS n_match_vvc,
  COUNT(*) FILTER (WHERE vessel_count = vi_vessels_max) AS n_match_vmax
FROM main.canonical_patient_master
WHERE vessel_count IS NOT NULL
  AND vasc_vessel_count_v13 IS NOT NULL
  AND vascular_vessel_count IS NOT NULL
  AND vi_vessels_max IS NOT NULL;

-- Spot-check: 5 random rids with vessel_count nonnull
SELECT research_id, vessel_count, vasc_vessel_count_v13, vascular_vessel_count, vi_vessels_max,
       vi_any_present_path, vasc_grade
FROM main.canonical_patient_master
WHERE vessel_count IS NOT NULL
ORDER BY research_id
LIMIT 5;
```

### §2 Cohort-uniformity classification

Given the 46/10,825 split with 6 distinct values, this is neither Type-A near-uniform-TRUE nor Type-B placeholder. It's a sparse **multi-valued integer measurement** — same shape as `vasc_vessel_count_v13`. Classify accordingly.

### §3 Decide verified vs na

- If `vessel_count` is an EXACT alias of one of {vasc_vessel_count_v13, vascular_vessel_count, vi_vessels_max}: flip to **verified** with `verification_method = 'derivation_vs_<alias_col>'`.
- If `vessel_count` was a placeholder later superseded by `vasc_vessel_count_v13`: reclass to **na** with `helper_<vessel_count>_pending_real_extraction` or `superseded_by_vasc_vessel_count_v13`.
- If independent and authoritative: flip to **verified** with `verification_method = 'derivation_vs_<source>'` after locating source.

### §4 Author apply SQL

`qc_framework_v1/migrations/183_pm_vessel_count_verify_apply_20260430.sql`:

- §0 pre-flight invariants (CPM 10,871/10,871; 1 not_started)
- §A pre-snapshot 1 registry row to `archive_pub_v1_0.canonical_column_verification_registry_pre_mig183_20260430`
- §B Path-C stamp (verified_by=Logan Glosser; batch_id=mig_183_*; verification_method=<chosen>; verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP); notes appendix)
- §C status flip (verified OR na, depending on §3)
- §E table-level signoff resync — should compute n_verified=1591, n_not_started=0, signoff_migration=mig_183. **If chosen status='verified': table_status flips to 'verified'. If chosen 'na': table_status flips to 'verified' too (1,591v + 25na = 1,615 total, 0 not_started 0 failed).** Either way PM becomes table_status='verified'.
- §F cpm_reconciliation_provenance_v1 row

### §5 Audit/report

`qc_framework_v1/reports/mig_183_vessel_count_audit_20260430.md`:
- §1 lineage discovery results
- §2 pairwise correspondence with the 3 parallel VI cols
- §3 cohort-uniformity classification
- §4 chosen disposition (verified vs na) + rationale
- §5 expected post-state (PM 1,591v / 24-25na / 0not_started → table_status='verified' eligible for mig_162)

### §6 Optional: surface readiness for mig_162

Once mig_183 lands, PM has 0 not_started. Note in §5 that mig_162 (PM finalization + lakehouse coverage report) is the next-priority lane — Cowork will queue it in the apply queue.

---

## Governance reminders

- Read-only audit + SQL authoring only. Cowork applies via Path C.
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits.
- Surgical git add only.
- No `BEGIN TRANSACTION;`/`COMMIT;`.

---

## Deliverables

1. `qc_framework_v1/migrations/183_pm_vessel_count_verify_apply_20260430.sql`
2. `qc_framework_v1/reports/mig_183_vessel_count_audit_20260430.md`

Commit message: `qc: mig_183 PM vessel_count last not_started col verify + apply authoring`

---

End of prompt.
