# Revision packet — submission v1

## What changed versus prior drafts

| Prior artifact | Change in v1 package |
|----------------|----------------------|
| `manuscript_full_draft.md` | Expanded to full IMRAD as `manuscript_submission_v1.md` with outcomes tied to CSVs. |
| `claims_ledger.csv` | Superseded by **`CLAIM_SOURCE_LEDGER.md`** (row-level traceability). |
| `figure_legends.md` | Superseded by **`figure_legends_v1.md`** (documents absence of figure binaries). |
| No combined audit | Added **`MANUSCRIPT_STATE_AUDIT.md`**, **`MANUSCRIPT_GAP_LIST.md`**. |

---

## Conflicts resolved (source hierarchy)

1. **Primary N:** **558** (`patient_level_dataset.csv`, `validation_report.md`, `analysis_manifest.json`) — **overrides** any informal prose elsewhere.
2. **Broad N:** **635** (`patient_level_dataset_broad_nodal_exclusion.csv`, manifest).
3. **Pathology sensitivity N:** **0** (`cohort_build_log.md`, manifest) — **overrides** any mention of a completed pathology-parallel results section.
4. **Completion:** **0/238** (`table7_completion_thyroidectomy.csv`) — **overrides** casual language implying common completion without defining flags.
5. **`cohort_flow.csv` oddities (zeros):** Interpret per **`MANUSCRIPT_STATE_AUDIT.md`**; do **not** treat inconsistent intermediate rows as separate analytic cohorts without pipeline reconciliation.

---

## NEEDS REFERENCE CHECK

No bibliography file exists in this folder. The following placeholders **must** be replaced with verified citations:

| Tag | Topic | Action |
|-----|-------|--------|
| `[REF:NEEDS_GUIDELINE]` | ATA / guideline context for lobectomy vs total in intermediate nodules | Replace with current guideline citation + accessed date if online. |
| `[REF:NEEDS_EPIDEMIOLOGY]` | Population patterns of extent / epidemiology | Replace with peer-reviewed source appropriate to claim strength. |
| STROBE statement (if cited) | STROBE checklist paper | Add formal reference if journal asks for reporting guideline citation. |
| Statsmodels / scipy | Software citation | Add if journal requires software citations (version pinned in author environment). |

**Do not fabricate:** authors, titles, volumes, pages, or DOIs.

---

## Reproducibility — what was verified this pass

- **Static CSV checks:** Row count **558** for `patient_level_dataset.csv`; **238** lobectomy / **320** total; **635** broad cohort with **375** total; **20** preoperative molecular tests; symmetric ID validation **0** mismatch in `validation_report.md` (source file).
- **`study_pipeline.py`:** Inspection confirms **SELECT**-only MotherDuck use for cohort build in code paths reviewed; **local** CSV writes in `run()`. **This markdown task did not execute `study_pipeline.py`.**
- **MotherDuck write test:** **Not performed** and **not required** for static manuscript package.

**If a future refresh is run:** record new `analysis_manifest.json` `run_utc` and `git_sha`; regenerate all dependent CSVs; update `CLAIM_SOURCE_LEDGER.md` if any number shifts.

### Read-only MotherDuck spot-check (performed)

Using `MotherDuckClient.connect_ro_share()` (prod) with qualified table `thyroid_share.operative_episode_detail_v2`, count of rows matching cohort `research_id` list with `procedure_normalized IN ('hemithyroidectomy','total_thyroidectomy')` was **559**, matching `validation_report.md` (`operative_rows_over_cohort_ids` ratio **1.0018** for **558** patients). **No writes** were executed.

---

## Files created/updated in v1 package (checklist)

- `MANUSCRIPT_STATE_AUDIT.md`
- `CLAIM_SOURCE_LEDGER.md`
- `MANUSCRIPT_GAP_LIST.md`
- `manuscript_submission_v1.md`
- `abstract_structured_v1.md`
- `figure_legends_v1.md`
- `supplement_methods_v1.md`
- `strobe_checklist_v1.md`
- `cover_letter_v1.md`
- `journal_fit_matrix_v1.md`
- `reviewer_attack_sheet_v1.md`
- `revision_packet_v1.md` (this file)
- `README.md` (planned update)

---

## Suggested revision-order (if desk-rejected or R&R)

1. Add **flow diagram** reconciled with pipeline.  
2. Add **ethics / IRB** and **funding / COI**.  
3. Build **figure** exports if journal requires.  
4. Complete **references**; remove placeholders.  
5. Optional: **missing-data sensitivity** (Bethesda / FNA-linked subset).  
