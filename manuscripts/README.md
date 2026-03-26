# Manuscripts (by paper / generation)

Human-authored drafts, revision packets, reference PDFs, and related assets live here so **`studies/`** stays focused on **executable pipelines** (scripts, cohort reproducibility, generated tables driven by `scripts/33_manuscript_tables.py` → `studies/manuscript_tables/`).

## Layout

| Folder | Contents |
|--------|----------|
| **`ete_ajcc8_202603/`** | ETE / AJCC 8th manuscript support: `MANUSCRIPT_REVISION_PACKET_20260326.md`, `revision_rerun_20260326/` (PSM sensitivity on exports). Analytic code remains in [`../studies/proposal2_ete_staging/`](../studies/proposal2_ete_staging/). |
| **`pool_malignancy_202603/`** | Legacy pooled cohort drafts (`manuscript_v1.md`, H1/H2 hypothesis manuscripts) and **`figures/`** for script 34 outputs. |
| **`elicit_reference_reports/`** | Elicit literature reports (PDFs) used while scoping topics. |
| **`lobectomy_molecular_202603/`** | Breadcrumb to the IMRAD draft in `studies/lobectomy_molecular_202603/` (analysis stays in `studies/`). |

## Naming convention

Use **`{short_topic}_{YYYYMM}/`** for new paper generations (e.g. `lobectomy_molecular_202603/` can be added when the IMRAD moves out of `studies/` only).

## Pipeline outputs (not moved)

- **`studies/manuscript_tables/`** — targets of `scripts/33_manuscript_tables.py` (do not relocate without updating that script).
- **`exports/manuscript_tables/`** — generated LaTeX/MD from other manuscript scripts.

## Redirect

The old path `studies/manuscript_draft/` is retired for drafts; see [`../studies/manuscript_draft/README.md`](../studies/manuscript_draft/README.md).
