# EXT2-4 manuscript package — v3 superseded by v4 (append-only)

## Cohort-definition change (never-delete audit)

| Item | v3 (superseded imaging-index cohort) | v4 (current) |
|---|---|---|
| Inclusion rule | Patient-grain `imaging_nodule_size_cm` restricted 2.0–4.0 cm (~n=400 primary descriptive row) | Surgical `n=8,368` ∩ **any** pre-operative US nodule with `canonical_us_nodule_v2.size_cm_max` ∈ [2.0, 4.0] on an exam with `exam_date` ≤ surgery day |
| Primary `n` | ~400 | **765** (verified BigQuery 2026-05-13) |
| STRICT nodal exclusions | Same family of rules | **654** patients (CT/MRI suspicious LN + Bethesda‑VI LN-directed FNA); *not* the ~620 illustrative bound in early memos |
| Decision memo | — | `cohort_reconciliation_v1_vs_v3.md` §DECISION, dated **2026-05-14** (Logan) |
| DFL pre-edit (Data Feedback Log) | — | `DFL-20260513-EXT2-4-V4-COHORT-PRELOG` (`recwKfs4ZB9fZQmrC`) — `change_type=migration` carrying “major revision equivalent” summary |

## Headline analytic shifts (Tables 1–4 CSV SSOT)

Rounded to published integers; full Wilson strings live in `tables/table3_v4_*.csv`.

| Metric | v3 (typical post–mig_325 headline) | v4 |
|---|---|---|
| Bethesda III+IV inside analytic layer | higher share of full surgical Bethesda pool | **155** / 765 primary |
| Afirma B3+B4 STRICT 2×2 `n` | ~90 | **13** |
| ThyroSeq B3+B4 STRICT 2×2 `n` | ~222 | **71** |
| Descriptive index-nodule 2–4 cm strata (still in Table 3 for v3 comparison only) | various | Afirma **`n=5`**, ThyroSeq **`n=30`** (imaging-index strata, *not* cohort gate) |
| Table 4 malignant denominators (path-proven recurrence) | superseded CSVs in `tables/superseded/` | Afirma malig **24**, TS **57**, other/historical **317**, untested **5** |

## Files archived here

- `manuscript_v3_draft.docx`
- `manuscript_v3_package_20260509.zip`
- `figures/` — snapshots of v2/v3-era figure PNG/PDF names (non-`*_v4` assets).
- **`manuscript_v3_package_20260509/`** — full unpacked package directory mirrored under this folder for diff-friendly audit.

## Repo references (not moved)

- Builders renamed at study root: `build_elicit_expansion_v3_archived.py`, `build_table3_v2_actual_call_v3_archived.py`, `build_figures_v2_v3_archived.py`.
- Active v4 builders: `build_*_v4.py`, `build_manuscript_docx_v4.js`, `ext2_4_v4_derive_tables.py`.

## MFL (post-rebuild)

**`MFL-20260513-EXT2-4-V4-COHORT-REBUILD`** (`recylT6gWb9raAiOr`) — linked manuscript **EXT2‑4** (`rec1GJyrmKdKxjlaY`); `change_type=major_revision`.

## thyroid-integration skill

Bumped **2.4.0** with verified-state pre-check (`cohort_v4_pts` distinct count **765**; `v4_strict` **654**) — see `.cowork/skills/thyroid-integration/references/CHANGELOG.md`.
