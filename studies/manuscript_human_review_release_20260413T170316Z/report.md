# Manuscript human-review & governance release — live run (2026-04-13)

**Generated (UTC):** 2026-04-13T17:03–17:04Z  
**MotherDuck catalog:** `Thyroid 2026` (fail-closed `--md`; RW token via `motherduck_client.get_token()` / repo `motherduck.local.toml` — not logged).

## Purpose

Move operator documentation and automation from “technically green” toward **manuscript-governance readiness**: distinguish structural/validator PASS from **named human review**, export safe reviewer worklists (no raw note text), and capture a fresh **`119 --release-mode`** evidence pack **without** fabricating human decisions.

## 1. Manual review queue (`qa.manual_review_queue`)

| Metric | Value |
|--------|------:|
| Total rows | 5,622 |
| Pending (`verification_status` NULL) | 0 |
| Structurally reviewed (non-NULL status) | 5,622 |

**Manuscript-quality tiers** (from `exports/review_queue_triage_20260413_170316/counts_manuscript_quality_tiers.csv` — gitignored export, mirrored by this report):

| Tier | n_rows | Note |
|------|-------:|------|
| `C_automation_tier_policy_only` | 5,620 | `auto_accepted_*` — **not** human sign-off |
| `E_reviewed_status_without_reviewer_timestamp` | 2 | Non-pending but missing `reviewer` + `reviewed_at` |

**Three-bucket governance sign-off** (`counts_mrq_three_bucket_signoff.csv`):

| Bucket | n_rows |
|--------|-------:|
| `automation_tier_or_incomplete_non_human` | 5,622 |
| `true_human_reviewed` | **0** |
| `unresolved_pending` | 0 |
| `synthetic_automation_only` | 0 |

**Interpretation:** Do **not** treat `auto_accepted_*` or tier-policy automation as **human-reviewed** manuscript sign-off. The conservative “true human-reviewed” bucket remains **empty** on this live snapshot.

## 2. Specimen / genomic review burden

| Source | Metric | Value |
|--------|--------|--------|
| `qa.v_diag_specimen_review_burden_v1` | `specimen_genomic_link_review` + `open` | **10,155** rows |
| `qa.specimen_genomic_link_review_v1` (detail export) | rows in `151` “all” CSV | **10,155** |
| Batched worklist CSVs (`151`) | files | 4 |

Safe export: [`scripts/151_specimen_genomic_review_queue_export.py`](../../scripts/151_specimen_genomic_review_queue_export.py) — truncates long text fields; **no note bodies** (not present on these objects).

## 3. Imported reviewer decisions (this session)

**None.** No new `promotion_review_decisions.csv` append and no `126 --hydrate-mrq-from` run were executed: there is **no new** operator-delivered, row-level **human-reviewed** MRQ CSV or decision batch in-repo for this cycle.

**Compatible path for a future operator run** (preserves additive decisions + provenance; destructive MRQ replace only when intentionally hydrating):

1. `scripts/126_final_master_release.py --md --release-date <tag> --decisions-csv <path> --decision-batch-id <batch> --hydrate-mrq-from <reviewed_gate_dir>`  
2. Or `scripts/114_qa_schema_setup.py --md --hydrate-from <gate_dir>` for MRQ-only hydrate.  
3. Re-run `scripts/119_md_formalization_validate.py --md --release-mode`.

See [`docs/review_queue_triage_export.md`](../../docs/review_queue_triage_export.md) and [`docs/publication_governance_gate.md`](../../docs/publication_governance_gate.md).

## 4. `119` formalization validation — release mode (effect before/after)

**No database mutations** were performed in this session before validation, so **before = after** for cloud state.

| Summary | Value |
|---------|--------|
| Verdict | **PASS WITH WARNINGS** |
| PASS / WARN / FAIL | 35 / 4 / 0 |
| Artifacts | [`119_release_validation/validation_report.md`](119_release_validation/validation_report.md) |

**Notable WARNs (unchanged structural posture):**

- Canonical `note_extraction_runs`: local parquet absent — parity not checked.
- Molecular dictionary / panel_version warnings (expected for some panels).
- **Specimen-adjacent review burden:** `genomic_link_review` open/pending = **10,155**; `specimen_merge_review` open/pending = **1**.

**Script 126** was **not** re-run: requires real decision inputs + reviewed gate directory when used for publication; see §3.

## 5. Institutional non-Tg labs

No new analyst lab CSV was verified in this task. **127** remains the ingest path for institutional chemistry-style waves; residual limits are **coverage / analytes / sparsity**, not a missing append mechanism — see [`studies/20260411_final_master_release/EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md) and README lab section.

## 6. Exact next blocker (manuscript governance)

1. **MRQ:** Achieve **non-empty** `true_human_reviewed` bucket (or explicit publication policy) by supplying **named reviewer + `reviewed_at`** on substantive rows, and/or resolve the **2** rows in `E_reviewed_status_without_reviewer_timestamp` with valid reviewer provenance.  
2. **Specimen–genomic queue:** Burn down **10,155** open `qa.specimen_genomic_link_review_v1` items (or document waiver scope) — `119` continues to **WARN** on this burden while structural checks pass.  
3. **Decisions:** Append **row-scoped** `qa.promotion_review_decisions` with real `decision_batch_id` **only** when decisions are genuinely adjudicated (not tier-policy summaries alone).

## 7. Artifact index (committed)

| Path | Role |
|------|------|
| This report | Governance snapshot |
| `119_release_validation/` | Full `119 --release-mode` report + stdout capture |
| `119_stdout.txt` | CLI transcript |

**Gitignored (local operator exports):** `exports/review_queue_triage_20260413_170316/`, `exports/specimen_genomic_review_20260413_170321/`.
