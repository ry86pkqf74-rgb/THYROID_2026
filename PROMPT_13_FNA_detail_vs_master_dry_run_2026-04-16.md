# Prompt 13 Dry-Run: FNA Episode Detail ↔ Master Rollup Cross-Validation

**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck, `main` schema)
**Detail table:** `fna_episode_master_v2` — 8,119 rows / 5,266 patients / 17 columns
**Master rollup:** `canonical_patient_master` — 10,871 rows (one per patient)
**Run date:** 2026-04-16 (read-only)
**Repo reference:** https://github.com/ry86pkqf74-rgb/THYROID_2026

Baseline row counts reconcile exactly with the preamble in the dry-run prompt pack: 8,119 / 5,266 / 10,871. Every `fna_episode_id` is unique, and neither table has NULL `research_id`. One structural note before the findings: `fna_episode_master_v2.research_id` is stored as `INTEGER` while `canonical_patient_master.research_id` is `VARCHAR`. Joins therefore require an explicit cast; all queries below cast the detail side to `VARCHAR`. Aligning the dtype across the fleet is a small but worthwhile cleanup for v1_1.

---

## 1. Episode-count concordance

Derivation: `COUNT(*)` per `research_id` in `fna_episode_master_v2`, FULL OUTER JOIN to `canonical_patient_master` on `research_id`, compare to `cpm.n_fna_episodes`.

| bucket | patients |
|---|---:|
| detail = master | 5,843 |
| &nbsp;&nbsp;&nbsp;of which both zero | 5,605 |
| &nbsp;&nbsp;&nbsp;of which both non-zero (all `prm_fna_n_sources = 1`) | 235 |
| &nbsp;&nbsp;&nbsp;of which both = 12 (coincidence at ceiling) | 3 |
| detail > master (master undercount) | 19 |
| detail < master (master overcount) | 5,009 |
| detail only (master is NULL despite detail rows) | 17 |
| master only (master > 0 with no detail rows) | 0 |
| total CPM patients | 10,871 |

The large `detail < master` bucket is not a rare-event discrepancy — it is systematic. Grouping the diff by `prm_fna_n_sources`:

| diff (master − detail) | prm_fna_n_sources | patients | min/max detail | min/max master |
|---:|---:|---:|---|---|
| +10 | 2 | 4,446 | 1–2 | 11–12 |
| +10 | 1 | 36 | 1–2 | 11–12 |
| +9 | 2 | 331 | 3 | 12 |
| +8 | 2 | 116 | 4 | 12 |
| +7 | 2 | 44 | 5 | 12 |
| +6 | 2 | 20 | 6 | 12 |
| +5 | 2 | 7 | 7 | 12 |
| +4 | 2 | 5 | 8 | 12 |
| +3 | 2 | 1 | 9 | 12 |
| +1 | 2 | 2 | 11 | 12 |
| 0 | 2 | 3 | 12 | 12 |
| −1 | null (missing master) | 17 | 1 | 0 |
| −1 | 1 | 2 | 2 | 1 |

The pattern is unmistakable: for **every** patient whose FNA data originates from two sources (`fna_cytology` **and** `fna_episode_master_v2`), `n_fna_episodes` is clamped to `11` when the detail count is 1 and `12` when the detail count is anywhere from 2 to 12. The “10” offset at `detail=1` and the flat ceiling of `12` for `detail ≥ 2` is consistent with a string-concatenation-to-integer bug (two digits like `"1"||"1"=11`, `"1"||"2"=12`, with the second digit saturated at `2` by something like `LEAST(detail_count, 2)`). Whatever the exact etiology, **`canonical_patient_master.n_fna_episodes` is currently unusable as an episode count for any patient with more than one FNA source.** Five thousand twenty-eight patients (46 %) are affected. The 235 patients whose FNA signal comes only from `fna_episode_master_v2` roll up correctly.

## 2. Bethesda concordance

Detail-side derivation: `MAX(bethesda_category)` per `research_id`. Compared against both `cpm.bethesda_final` and `cpm.worst_bethesda_num`.

- 5,249 CPM patients have a non-null `bethesda_final`; 5,250 patients have at least one non-null `bethesda_category` in the detail table (nearly identical coverage — encouraging).
- **0 patients** have `bethesda_final` non-null while the detail table has zero FNA rows. The “Bethesda-without-evidence” failure mode the prompt was probing does not exist.
- **1 patient** has a Bethesda in detail but `bethesda_final IS NULL` in master — `research_id = 1198` (detail max = 2, `n_fna_episodes` also NULL). This is the same kind of silent rollup drop as the 17 orphans in §1.
- Exact match between `bethesda_final` and `MAX(bethesda_category)` for 4,454 patients (84.9 % of the 5,249 with any master value).
- Match against `worst_bethesda_num` for 4,565 — marginally better — suggesting `bethesda_final` is being set to a worst-across-sources value rather than a strict "last FNA" value.
- **795 discordant patients.** Top patterns (master → detail-max):

| master `bethesda_final` | detail MAX | patients |
|:---:|:---:|---:|
| 4 → 3 | 4 | 192 |
| 6 → 4 | 6 | 120 |
| 6 → 3 | 6 | 93 |
| 6 → 2 | 6 | 84 |
| 6 → 5 | 6 | 58 |
| 2 → 1 | 2 | 24 |

Every discordance has `master ≥ detail`. The most common pattern (`master=6, detail<6`) is consistent with cohort patients whose cytology never reached Bethesda VI but whose surgical pathology confirmed malignancy; `bethesda_final` appears to be hydrated from a pathology fallback rather than pure cytology. If the documented intent is “final cytology Bethesda,” the column is mislabeled; if the intent is “worst FNA/path grade,” then it duplicates `worst_bethesda_num`. Either way, the semantics of `bethesda_final` deserve a data-dictionary clarification.

## 3. Orphan / referential check

| check | count |
|---|---:|
| `research_id` in detail but NOT in master | 0 |
| `cpm.n_fna_episodes > 0` AND no detail rows exist | 0 |
| `cpm.n_fna_episodes IS NULL` AND detail rows exist | **17** |

No detail-side orphans exist — every FNA row maps to a CPM patient — but 17 CPM patients who have FNA detail rows carry a NULL (not zero) in `n_fna_episodes`. These are the same 17 patients flagged as “detail only” in §1. They are a rollup silence bug, not a join-key problem.

## 4. Date integrity

`date_status` domain is `exact_source_date` (6,449) or `unresolved_date` (1,670). The prompt expected a value named `'resolved'`, which does not appear in the data; the operational equivalent is `exact_source_date`.

| bucket | count |
|---|---:|
| `resolved_fna_date` IS NULL | 1,670 (20.6 %) |
| `date_status <> 'exact_source_date'` | 1,670 |
| `resolved_fna_date < 1995-01-01` | 25 |
| `resolved_fna_date > 2026-12-31` | 1 |
| patients: earliest FNA > 5 years before `first_surgery_date` | 374 |
| &nbsp;&nbsp;&nbsp;of which attributable to pre-1995 bad dates | 24 |
| patients: earliest FNA date after `first_surgery_date` | 171 |
| patients: latest FNA date after `first_surgery_date` | 222 |

The 25 pre-1995 rows all come from `source_table = 'fna_history'` and all look like two-digit years parsed as four-digit — sample dates include `0006-04-15`, `0015-06-22`, `0021-05-14`, `0106-12-22`, `0202-04-18`, `0000-09-19`. The single post-2026 row is `2029-11-17` (also `fna_history`). These are a `fna_history` ingestion bug, not real outliers. After excluding them, 350 patients still have an earliest FNA more than 5 years before first surgery; those are clinically plausible surveillance biopsies, not an integrity concern. Post-surgery FNAs (171–222 patients) are also expected — they represent recurrence-monitoring biopsies — but they do raise the question of whether a `fna_timing_vs_surgery` flag (preop / intraop / postop) should be added to the detail schema to make downstream filtering unambiguous.

## 5. Linkage coverage and the Bethesda III/IV molecular gap

| FK column | non-NULL FNA rows | % |
|---|---:|---:|
| `linked_surgery_episode_id`   | 5,886 | **72.5 %** |
| `linked_imaging_nodule_id`    | 1,598 | **19.7 %** |
| `linked_molecular_episode_id` | 0 | **0.0 %** |

`linked_molecular_episode_id` is populated for zero of 8,119 rows. The column exists but is never written. This is the single most consequential data-integration gap in `fna_episode_master_v2`.

Focusing on the indeterminate FNAs where molecular testing is clinically indicated (Bethesda III or IV): **1,920 FNA rows** (1,279 Bethesda III + 641 Bethesda IV) across **1,685 patients** — and 100 % of them have a NULL molecular link. Crucially, the underlying molecular episodes **do exist**: of those 1,685 patients, **1,613 (95.7 %)** have at least one row in `molecular_test_episode_v2`; only 72 patients (4.3 %) have no molecular testing anywhere in the database. The data is there; the FK is simply not being hydrated. This is a high-leverage fix — the `imaging_fna_linkage_mm_v1` pipeline described in the repo (script 129) already demonstrates the (research_id, date-window) linkage pattern and can be adapted directly to emit `linked_molecular_episode_id` against `molecular_test_episode_v2.molecular_episode_id` on a ±90-day window.

`linked_imaging_nodule_id` at 19.7 % is also low and overlaps with the same concern — the repo’s `imaging_fna_linkage_v3` table is a richer linkage surface than what’s landed in the detail table column, so hydrating the FK from that table is an obvious next step.

---

## Top 5 FNA detail-vs-master integrity concerns (ranked)

1. **`canonical_patient_master.n_fna_episodes` is a broken rollup for multi-source patients (5,028 / 10,871, 46 %).** Values are clamped to 11 for detail=1 and saturate at 12 for any detail ≥ 2. Suspected cause: digit-concatenation in the rollup SQL. Recommended fix: replace the current expression with `SELECT COUNT(*) FROM fna_episode_master_v2 GROUP BY research_id` (deduped on `fna_episode_id`) and join into the master as a clean left-joined scalar. Add a `n_fna_cytology_only_records` column if the intent was to preserve the cytology-source count separately. Re-derive `bethesda_source`/`prm_fna_source_tables` from the same CTE so they stay in sync.
2. **`linked_molecular_episode_id` is 0 % populated across all 8,119 FNA rows.** For the 1,920 indeterminate FNAs (Bethesda III/IV), 95.7 % of their patients have a molecular episode available in `molecular_test_episode_v2` — so the join is recoverable and high-value. Build a deterministic linker on `(research_id, fna_date ± 90 days)` modeled on the existing `imaging_fna_linkage_mm_v1` pipeline, and expose both the nearest-match `molecular_episode_id` and a confidence/score column.
3. **`fna_history` two-digit-year parsing error contaminates 25 pre-1995 dates and at least 1 post-2026 date.** Dates like `0015-06-22` should be `2015-06-22`. Fix in the `fna_history` source ingester, back-populate `resolved_fna_date` for the affected `fna_episode_id`s, and add a release-mode check (per the repo’s script 119 pattern) that fails closed on `resolved_fna_date < 1995-01-01 OR > CURRENT_DATE`.
4. **17 patients (+1 Bethesda-only case = 18) are silently dropped from the FNA rollup.** They have detail rows but `n_fna_episodes IS NULL` (and in one case `bethesda_final` is also NULL). Because the orphan direction is always detail-present/master-null, this is almost certainly an INNER-JOIN in the rollup SQL where a LEFT JOIN is intended. The fix is one-line and should be paired with a regression test that asserts `COUNT(DISTINCT research_id) FROM detail ⊆ COUNT(DISTINCT research_id) FROM master`.
5. **`bethesda_final` semantics are ambiguous and disagree with `MAX(bethesda_category)` for 795 patients.** Discordances are exclusively in the direction `master ≥ detail`, which indicates `bethesda_final` is pulling from pathology/worst-across-sources rather than cytology-only. This duplicates `worst_bethesda_num` (which actually matches detail MAX slightly better). Either rename the column to reflect its "worst observed" semantics, or redefine it as the strict final-cytology value (MAX `bethesda_category` from `fna_episode_master_v2` only). Document the choice in `data_dictionary_v240` / `data_dictionary.md` and add a comment to the table.

### Secondary suggestions (for v1_1)

- Align `research_id` dtype to `VARCHAR` across all detail tables — currently `fna_episode_master_v2.research_id` is `INTEGER` while the master is `VARCHAR`. A consistent dtype removes the need for defensive casting in every downstream view.
- Add a `fna_timing_vs_surgery` derived column on `fna_episode_master_v2` with values {preop, intraop, postop, no_surgery}; this would retire the need for ad-hoc date math in manuscript views and would also make the 171 post-surgery FNAs self-documenting.
- Populate `date_status = 'resolved'` (or rename it to match the existing domain `exact_source_date`) — the prompt pack assumes a value that does not appear in the data, and other validation prompts in the pack will trip on the same mismatch.
- For `prm_first_fna_days_from_surg` / `prm_last_fna_days_from_surg` in CPM, add a release-mode check that they reconcile with `MIN/MAX(resolved_fna_date) - first_surgery_date` from the detail table (not run here; recommended for the next dry-run pass).
