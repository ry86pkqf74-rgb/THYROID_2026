# CPM Phase 2 — Prompt 19 closeout (`20260514`)

**Scope:** Rebuild ASM204 on parity-complete BQ feeders; advance assembly scratches through the Phase 1 DAG; reconcile cumulative output vs `pub_archive.canonical_patient_master_base_archived_20260514`.

**Writes:** Only `thyroid-canonical-pub-2026.pub_workspace` scratch tables (`CREATE OR REPLACE`). **No** writes to `pub_canonical.*` or `pub_archive.*`.

**BigQuery location:** `us-central1` (dataset default).

---

## 1. DAG vs prompt wording (`ASM216`)

- Canonical DAG (`studies/cpm_bq_native_rebuild_phase1_dag_20260514.json`) chain after ASM204:
  **`ASM205 → ASM207 → ASM208 → ASM211 → ASM212 → ASM214 → ASM215 → ASM217`**
- There is **no** **`ASM206` / `ASM209` / `ASM210` / `ASM213` / `ASM216`** node in that DAG JSON.
- **`ASM217`** is the parity step for **`canonical_patient_master_base_archived_20260514`** (1,663 columns).
- Interpret “`ASM205 → ASM216`” as **MotherDuck script numbering** colloquially, or typo for **`ASM217`** — the archived checkpoint is **`ASM217`-equivalent**, not `ASM216`.

---

## 2. Completed scratch builds

| Stage | Scratch table | Rows | Distinct `research_id` | Output columns |
|-------|---------------|------|------------------------|----------------|
| **ASM204** | `pub_workspace.cpm_stage_asm204_20260514` | 10,871 | 10,871 | 96 |
| **ASM205** | `pub_workspace.cpm_stage_asm205_20260514` | 10,871 | 10,871 | 125 |

**SQL artefacts**

- ASM204 (existing): `studies/cpm_stage_asm204_20260514.sql` — **rebuilt** (`Replaced … cpm_stage_asm204_20260514`).
- ASM205 (new): `studies/cpm_stage_asm205_20260514.sql` — port of **`scripts/205_canonical_master_assembly.py` / `MASTER_SQL`** with BigQuery qualifiers and feeder/schema bridges below.

---

## 3. ASM204 recurrence fields (parity-complete feeders)

Validated on rebuilt ASM204 (`canonical_recurrence_v1` joins no longer circular / NULL stubs):

| Metric | Value |
|--------|------|
| `recurrence_histology` non-empty | **440** |
| `recurrence_evidence_source` non-empty | **1,287** |
| `recurrence_confirmed = TRUE` | **514** |

(Counts can exceed `recurrence_confirmed` where evidence text exists under non-confirmed tiers — feeder semantics.)

---

## 4. Column delta vs prior stage / plan snapshot

### ASM205 vs ASM204 (+29 cols)

ASM205 is a **pure superset** of ASM204 by name (`EXCEPT DISTINCT` ASM204 − ASM205 = empty). Columns added vs ASM204:

`bethesda_2010`, `bethesda_2015`, `bethesda_2023`, `fna_path_concordance_category`, `fna_path_concordant`, `fna_path_outcome`, `imaging_ln_abnormal`, `ln_mets_*` (`atc`,`ene_count`,`ftc`,`hurthle`,`micrometastasis`,`mtc`,`pdtc`,`ptc`), `n_fna_cytology_records`, `n_us_with_ln_assessment`, `tirads_best_combined`, `tirads_nodules_scored_combined`, `tirads_worst_combined`, `tp_central_examined`, `tp_central_positive_total`, `tp_ln_*` (central positive / ene / examined / largest deposit / lateral positive / levels / positive).

### Plan alignment

| Reference | Columns |
|-----------|---------|
| PAR / S2 memo (`patient_analysis_resolved_v1`) | 146 |
| ASM204 memo (`studies/cpm_stage_asm204_validation_20260514.md`) | **96** (unchanged) |
| ASM205 (MD analogue: widen to **125**‑wide `canonical_patient_master_v1` spine pre‑207) | **125** ✅ |
| **`canonical_patient_master_base_archived_20260514`** | **1,663** |

---

## 5. Blockers — ASM207 … ASM217 (not executed)

**Missing BigQuery analogue of MotherDuck `patient_refined_master_clinical_v12` (PRM v12)**

- `scripts/207_canonical_master_expansion.py` **`EXPANSION_SQL`** relies on **`prm`** = dedup PRM (**129** projected `prm.*` fields across blocks E/U/V/etc.).
- **No** `patient_refined_master_clinical_v12` table under `pub_canonical` or `pub_workspace` (confirmed via `INFORMATION_SCHEMA` scans).
- ASM205 bridged **minimal** PRM pathology:
  - `fna_path_outcome` ← `patient_analysis_resolved_v1`
  - `fna_path_concordance_*` ← **`NULL`** placeholders (**feeder gap** vs MD 205 semantics).

Additional dependency notes surfaced while scoping downstream ports:

- **`pub_workspace.ln_master_rollup_v1`** exists (**4,273 rows**) — LN rollup grain is **not** 10,871; script **208** expects left‑join rollup semantics (fine) but depends on ASM207 wide row first.
- **Live** `pub_canonical.canonical_patient_master` (2,314 cols) ≠ base archive ladder grain; **cannot** substitute as an assembly‑honest `prm` without circularity risk.

**Unblock options (Prompt 20+ prerequisites)**

1. **Hydrate PRM v12 into `pub_workspace`** (parquet‑load / native BQ DDL) keyed by `research_id` STRING, aligned to MD freeze.
2. **Or** refactor `prm` reads in staged SQL to authoritative BQ silvers (`MIG_*` / reconstructed rollups), with provenance parity vs MD.

Until PRM resolves, **`CREATE OR REPLACE` CTAS chains for ASM207+ are not mechanically faithful** — materializing blanks would inflate scratch width without matching assembly semantics.

---

## 6. Parity harness vs `canonical_patient_master_base_archived_20260514` (informational)

| Check | Result |
|-------|--------|
| **`INFORMATION_SCHEMA` column‑name overlap (ASM205 ∩ base)** | **116** shared names |
| **Columns present only on base (`base − ASM205`)** | **1,547** (expected until ASM217) |
| **Sample row join** `ASM205 ⋈ base` ON `research_id` (INT64 bridge) **`recurrence_confirmed`** | **0 mismatches** (STRING cast compare) |
| **Sample `recurrence_histology`** | **187** mismatches — exemplar rows show **verbatim histology phrases** (`follicular carcinoma`, `metastatic thyroid carcinoma`) on ASM205 feeder vs **`ftc`** style codes on archived base (**encoding / derivation delta**, not join grain failure) |

**Full 1,663‑column row hash parity** vs base was **not** run — intermediate assembly incomplete by design pending §5.

---

## 7. Acceptance matrix (Prompt 19)

| Criterion | Status |
|-----------|--------|
| ASM204 rebuilt; recurrence columns sourced from **`canonical_recurrence_v1`** (no `CAST(NULL)` stubs), 10,871 grains | ✅ |
| ASM205‑style scratch onward per DAG | ⚠️ **ASM205 ✅** · **ASM207‑ASM217 🔴 blocked** (PRM absent) |
| Cumulative ASM output **FULL** parity `base_archived` (cols + row values) | 🔴 **not reached** |
| **`pub_canonical` / `pub_archive` untouched** by agent | ✅ |

**Next prompt (promotion ladder)** should wait until **`cpm_stage_asm217_<date>`** materializes parity‑clean vs `canonical_patient_master_base_archived_20260514`.
