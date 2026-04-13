# Residual NULL `bethesda_category` — source trace (23 episodes)

**Context:** After `152` (cytology join) and `153` (path digit + `2*` raw), **23** `fna_episode_master_v2` rows remain with `bethesda_category IS NULL`. This note traces each case to **`FNAs 12_5_2025.xlsx`**, **`fna_history`**, and **`fna_cytology`** on MotherDuck and states what would be required to reach **100% numeric Bethesda** without guessing.

## Summary

| Bucket | Count | Root cause |
|--------|------:|------------|
| **A — Ordinal FNA in Excel & cytology empty** | 14 | Wide workbook has no Bethesda cell and no path narrative for the matching FNA ordinal; `fna_cytology` has no row or NULL `category_num` for that `(research_id, fna_index)`. |
| **B — Episode 4 with no 4th cytology row** | 8 | `fna_episode_id = 4` (fourth chronological FNA in `fna_history`) but **`fna_cytology` has zero rows with `fna_index = 4`** for these patients. Bethesda text often exists for FNA 1–3 in the same workbook, **not** in the FNA#4 path/history columns. |
| **C — Stub / sparse cytology row** | 1 | `10637` / `fna_index = 4`: row exists in `fna_cytology` but **`path_text` and `category_num` are NULL** (placeholder). |
| **D — Path field is not Bethesda** | 1 | `8330` / `fna_episode_id = 1`: `pathology_diagnosis` holds a **patient name**, not a class; unscorable without chart review. |

## Episodes with `fna_episode_id = 4` (8 rows)

| research_id | Excel FNA4 path/history (ordinal columns) | `fna_cytology` `fna_index = 4` | Notes |
|-------------|-------------------------------------------|--------------------------------|-------|
| 11254 | Specimen/gross lines only — **no** “Bethesda Category …” | **0 rows** | FNA 1–3 in same sheet have Bethesda V / III / II; those populate other episodes, not episode 4. |
| 11739 | “Fine needle aspirate, thyroid - Left” only | **0 rows** | Same pattern: prior FNAs have narrative; 4th column is specimen-only. |
| 11895 | Specimen dimensions only | **0 rows** | |
| 11896 | “Left Thyroid #1, FNA” style gross | **0 rows** | |
| 11902 | “3.8cm LLP solid” specimen line | **0 rows** | FNA 1–3 cells explicitly say “Bethesda Category II”; **FNA4 column does not**. |
| 11958 | “Fine needle aspirate, thyroid - Left” only | **0 rows** | |
| 10637 | (see C) | **1 row** — `category_num` NULL, `path_text` NULL | Cytology has **`fna_index` 1, 2, 4** but **no `fna_index` 3**; episode 3 vs index alignment differs from cytology numbering. Third chronological episode may not equal `fna_index` 3 in cytology. |

**Implication:** Resolving **episode 4** requires either (1) **ingesting** a 4th cytology report into `fna_cytology` with `category_num` or parseable text, (2) **date-based** linkage from a long-format cytology feed, or (3) **manual** adjudication. Pulling Bethesda from FNA1–3 text for episode 4 would be **wrong** (different specimens).

## Single-FNA / other episodes (14 + 1 = 15 rows)

For `fna_episode_id = 1` (and `10637` / ep 3), spot checks on **`FNAs 12_5_2025.xlsx`** show **`FNA1_path_extended`** and **`Bethesda*`** often **empty** for these `research_id`s —consistent with “row present but no structured outcome in the wide sheet.” `fna_cytology` also has **no** usable `category_num` for the matching index.

`8330` is the **pathology_present_bethesda_unparsed** case: structured path column contains a **name**, not a cytopath class.

## What would be needed to get **all** resolved

1. **Structured cytology extension**  
   - Ingest **4th+** FNAs into `fna_cytology` with correct `fna_index` (or switch to **date-keyed** linkage to `fna_episode_master_v2`).  
   - Backfill or delete **empty stub** rows so `fna_index` aligns with episode chronology.

2. **Deterministic parsers on cytology path text** (new script scope)  
   - Apply the same conservative patterns as `153` to `fna_cytology.path_text` where `category_num` IS NULL and text contains `Bethesda Category [I–VI|1–6]`.  
   - Requires QA on multi-site path formats (Emory vs EUH vs legacy).

3. **Manual adjudication**  
   - For the remainder, a small CSV worklist: `research_id`, `fna_episode_id`, `resolved_bethesda_num`, `source` (chart / addendum), `reviewer`, `date` — then a controlled `UPDATE` to `fna_episode_master_v2`.

4. **Explicit “unscorable” (optional)**  
   - If the goal is analytic flags rather than digits: add `bethesda_category_derived` vs `bethesda_category_final` and allow NULL with **`bethesda_unscorable_reason`** populated (your view already supports narrative completeness).

## Bottom line

- **No additional fully deterministic fills** are available from **`FNAs 12_5_2025.xlsx` ordinal cells alone** for these 23 episodes without **cross-table date alignment** or **new cytology rows**.  
- The **8× episode-4** cases are blocked by **missing `fna_index = 4` in `fna_cytology`** (and empty FNA4 Bethesda/path in the wide sheet).  
- **`8330`** needs **human correction** of misfiled path text or chart-sourced Bethesda.

## Update — script 154 (path_text parse + worklist)

`scripts/154_fna_cytology_bethesda_from_path_text.py` applies the same **deterministic** “Bethesda Category … / cytopathology category …” rules to **`fna_cytology.path_text`** (deduped per `research_id`, `fna_index`), then re-runs the script **152**-style join into `fna_episode_master_v2`, and writes a timestamped **`exports/fna_bethesda_residual_worklist_*/`** CSV + manifest for manual review. On the current MotherDuck snapshot, **Phase A updated 0** cytology rows (no `category_num` NULL row had extractable Bethesda text—consistent with §Summary). **23** episodes remain on the worklist until ingest or adjudication.

---

_Generated 2026-04-13; sources: MotherDuck `fna_episode_master_v2`, `fna_history`, `fna_cytology`, `raw/FNAs 12_5_2025.xlsx`._
