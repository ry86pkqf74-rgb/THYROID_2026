# Revision packet — submission v1 (updated 2026-03-26)

## What changed versus prior drafts

| Prior artifact | Change |
|----------------|--------|
| `manuscript_full_draft.md` | Expanded to full IMRAD as `manuscript_submission_v1.md` with outcomes tied to CSVs. |
| `claims_ledger.csv` | Superseded by **`CLAIM_SOURCE_LEDGER.md`**. |
| `figure_legends.md` | Superseded by **`figure_legends_v1.md`**. |
| No combined audit | Added **`MANUSCRIPT_STATE_AUDIT.md`**, **`MANUSCRIPT_GAP_LIST.md`**. |
| “No figures in folder” | **Obviated:** seven `fig_*.png` exports exist and track in git; **main text** uses **Figure 1** (`fig_cohort_flow.png`) and **Figure 2** (`fig_forest_total_vs_lobectomy.png`) per `figure_legends_v1.md`. |

---

## Conflicts resolved (source hierarchy)

1. **Primary N:** **558** (`patient_level_dataset.csv`, `validation_report.md`, `analysis_manifest.json`) — **overrides** any informal prose elsewhere.
2. **Broad N:** **635** (`patient_level_dataset_broad_nodal_exclusion.csv`, manifest).
3. **Pathology sensitivity N:** **0** (`cohort_build_log.md`, manifest) — **overrides** any mention of a completed pathology-parallel results section.
4. **Completion:** **0/238** on **OED pipeline**; **25/238 path-synoptic definite** (`table7_completion_thyroidectomy.csv`, `completion_audit_outputs/`) — always **name the definition** when comparing to literature.
5. **`cohort_flow.csv` oddities (zeros):** Interpret per **`MANUSCRIPT_STATE_AUDIT.md`**; **Figure 1** visualizes the same pipeline steps with zeros explicit.

---

## References — `references_working_20260326.md`

- In-text Vancouver citations **1–10** appear at the end of `manuscript_submission_v1.md`.
- Entries **1–6** are **verified** (Ringel *Thyroid* 2025 **without** the incorrect DOI; Worrall *Endocr Oncol.*; full STROBE author line; Dhir *Surgery* 2018; Wang *World J Surg Oncol*; Montgomery *Surgery* 2023).
- Entries **7–10** are **verified 2026-03-27** (Kiss *BMC Endocr Disord*; Conroy *Surgery* 2022; Loderer *Gland Surg* 2023; Hao *Gland Surg* 2025) — apply journal reference style at submit time.
- **Removed from active manuscript list:** Kim MH (former ref 4); placeholder completion systematic review (former ref 12). See `SCHOLAR_GPT_REFERENCE_RECONCILIATION_20260326.md`.
- Optional background list items (e.g., Barbaro narrative review, Xu *Sci Rep*) remain under **Not used** unless authors expand Discussion.

**Do not fabricate** bibliographic fields.

---

## Reproducibility — what was verified this pass

- **Static CSV checks:** Row count **558** for `patient_level_dataset.csv`; **238** lobectomy / **320** total; **635** broad cohort with **375** total; **20** preoperative molecular tests; symmetric ID validation **0** mismatch in `validation_report.md` (source file).
- **Figure linkage:** `fig_cohort_flow.png` and `fig_forest_total_vs_lobectomy.png` are cited in `manuscript_submission_v1.md` and described in `figure_legends_v1.md`.
- **`study_pipeline.py`:** Inspection (prior notes) confirms **SELECT**-only MotherDuck use for cohort build in reviewed paths; **local** CSV writes in `run()`. **This authoring pass did not execute `study_pipeline.py`.**

**If a future refresh is run:** record new `analysis_manifest.json` `run_utc` and `git_sha`; regenerate all dependent CSVs and figures; update `CLAIM_SOURCE_LEDGER.md` if any number shifts.

### Read-only MotherDuck spot-check (historical note in prior package)

Using `MotherDuckClient.connect_ro_share()` (prod) with qualified table `thyroid_share.operative_episode_detail_v2`, count of rows matching cohort `research_id` list with `procedure_normalized IN ('hemithyroidectomy','total_thyroidectomy')` was **559**, matching `validation_report.md` (`operative_rows_over_cohort_ids` ratio **1.0018** for **558** patients). **No writes** were executed.

---

## Files in manuscript package (checklist)

- `MANUSCRIPT_STATE_AUDIT.md`
- `CLAIM_SOURCE_LEDGER.md`
- `MANUSCRIPT_GAP_LIST.md`
- `manuscript_submission_v1.md`
- `abstract_structured_v1.md`
- `figure_legends_v1.md`
- `references_working_20260326.md`
- `AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`
- `AUTHOR_INPUTS_REQUIRED_20260326.md`
- `supplement_methods_v1.md`
- `strobe_checklist_v1.md`
- `cover_letter_v1.md`
- `journal_fit_matrix_v1.md`
- `reviewer_attack_sheet_v1.md`
- `revision_packet_v1.md` (this file)
- `README.md`
- External context memos: `external_context_*_20260326.md`

---

## Suggested revision-order (if desk-rejected or R&R)

1. Apply target journal reference style to **`references_working_20260326.md` items 1–10** (sources verified; formatting may differ by journal).  
2. Production **Figure 1** relabeling if editors require CONSORT-style layout.  
3. Add **ethics / IRB** and **funding / COI** (see `AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`).  
4. Optional **extended-model** supplemental forest figure from `logistic_primary_extended.csv`.  
5. Optional **missing-data sensitivity** (Bethesda / FNA-linked complete-case subset).  
