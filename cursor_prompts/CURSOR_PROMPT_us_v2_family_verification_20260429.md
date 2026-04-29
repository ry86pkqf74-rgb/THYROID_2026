# Cursor Agent Task — US v2 Imaging Family Verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (3 tables, 23-53 not_started cols each)
**Run order:** Lane 10 (run last — biggest scope)

---

## 1. Goal

Verify the **3 US v2 imaging canonicals** under Protocol v2 in one batch:

| Table | Rows | Patients | Cols total | not_started |
|---|---:|---:|---:|---:|
| canonical_us_nodule_v2 | 37,579 | 6,523 | 57 | 53 |
| canonical_us_thyroid_gland_v2 | 13,578 | 10,859 | 32 | 28 |
| canonical_us_lymph_node_v2 | 6,801 | 4,077 | 29 | 23 |

This is the **Tier 1 source** ultrasound family. Larger than other Tier 2 work because of the per-nodule / per-gland measurements + sonographic features.

---

## 2. Domain context

Per `project_exam_id_portability.md` memory:
- US v2 build found cunc_v1's us_exam_id hash didn't match new gland/LN v2 hashes
- exam_master fell back to (research_id, exam_date) join
- "Lock exam_id recipe per modality before building CT/PET/MR/nucmed LN tables"

Implication: us_exam_id may be inconsistent across the 3 v2 tables. Test this during verification — if exam_id doesn't link cleanly, document as a known CF (carry-over from build). Don't block sign-off on it.

Find build SQL:
```
grep -rn "canonical_us_nodule_v2\|canonical_us_thyroid_gland_v2\|canonical_us_lymph_node_v2" scripts qc_framework_v1 | head
```

Likely scripts in the 3xx range (probably 350-380).

---

## 3. Date-type CF candidates — flag during verification

US imaging tables likely have `exam_date` and possibly `report_date` cols. Per Logan's `feedback_clinical_dates_calendar_only.md`, these are clinical event dates and MUST be DATE.

- If you find TIMESTAMP or VARCHAR clinical date cols → flag CF-mig<N>-US-DATE-RETYPE for future cleanup pass
- Don't block sign-off on type repair

---

## 4. Methodology — per-table within the batch

For EACH of the 3 tables, apply the most appropriate established pattern:

### 4a. Probe lineage first
Read the build SQL. Determine if:
- Deterministic SELECT from single upstream → extraction-faithfulness pattern (mig_102/110)
- UNION/MERGE of multiple sources → multi-source pattern (mig_107)
- Aggregation/derivation from multiple raw imaging tables → derivation re-derivation (mig_104/106)

### 4b. Probe natural keys + uniqueness
- us_nodule_v2: probably (research_id, us_exam_id, nodule_ix) or similar
- us_thyroid_gland_v2: probably (research_id, us_exam_id) — one row per gland per exam
- us_lymph_node_v2: probably (research_id, us_exam_id, ln_ix)
Test COUNT(*) vs COUNT(DISTINCT key) on each.

### 4c. Per-col verification by category
- IDs/linkage: research_id, us_exam_id, ix cols → na_provenance
- Dates: clinical event date cols → DATE type CF if violated, otherwise extraction-faithfulness
- Measurements: nodule_size_cm, gland_volume_ml, etc. — verify ranges + units consistent
- Sonographic features: TIRADS classification, composition, echogenicity, etc. — verify enums
- Linkage cols (linked_*): probably na_provenance per `feedback_no_crossdomain_linkage_ids.md`

### 4d. Cross-validation against verified canonicals
- canonical_fna_events_v1 (verified mig_78→96) has nodule references — check that us_nodule_v2.nodule_id values that link to FNA actually appear in fna_events
- If cross-validation surfaces orphan refs → CF-mig<N>-US-FNA-ORPHAN

### 4e. Sign-off SQL
`qc_framework_v1/migrations/<N>_us_v2_family_signoff.sql`. 3 sub-blocks (one per US v2 table) + final 3 table_signoff updates.

---

## 5. Acceptance gates

For each of 3 tables:
- All not_started cols flipped to verified or na
- table_status='verified'
- Natural key uniqueness confirmed
- Measurement units consistent within each col
- Sonographic feature vocab clean

Cross-table:
- Cohort parity: us_thyroid_gland_v2 has 10,859 patients ≈ canonical_patient_master 10,871 (1 row per patient with any US — expected). us_nodule_v2 6,523 patients (only patients with US nodules — events scope).
- Document any us_exam_id inconsistency findings as CFs

---

## 6. Don't touch (active parallel lanes)

- `canonical_ete_subgrade_events_v1` / `canonical_ete_subgrade_patient_rollup_v1` — Cowork's lane
- Any table touched by sibling Cursor lanes 8 + 9 (labs, molecular)
- `canonical_us_*_v1` (older v1 versions) — leave for separate verification round

---

## 7. Reference reading

Required:
- `project_exam_id_portability.md` — exam_id consistency context
- `feedback_clinical_dates_calendar_only.md`
- `feedback_no_crossdomain_linkage_ids.md`
- `feedback_motherduck_direct_check.md`
- `feedback_surgical_git_add.md`

Repo (templates):
- `qc_framework_v1/migrations/102_parathyroid_events_table_signoff.sql` (extraction-faithfulness)
- `qc_framework_v1/migrations/110_pathology_clinical_events_table_signoff.sql` (UNNEST variant if applicable)
- `qc_framework_v1/migrations/107_pmh_events_table_signoff.sql` (multi-source if applicable)

---

## 8. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing all 3 US v2 tables
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP

---

## 9. If something unexpected surfaces

- us_exam_id inconsistency > 10% → STOP, the "exam_id portability trap" memory may need updating; ask Logan
- TIRADS or composition vocab has unexpected raw strings → flag as CF; do not auto-normalize
- Cross-validation against fna_events surfaces > 50 orphan refs → STOP, may indicate version mismatch

---

End of prompt. Lane 10 of new 3-prompt batch. Closes 3 US v2 imaging canonicals (Tier 1 source).
