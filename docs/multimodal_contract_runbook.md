# THYROID_2026 — Multimodal contract operator runbook (scripts 128 / 129)

This runbook is for operators building the **multimodal contract v1** layer: star-schema tables under `mm_contract_dev` (default on MotherDuck) plus imaging ↔ FNA linkage. It assumes repo root `THYROID_2026/`, `.venv` active, and credentials in env or `.streamlit/secrets.toml` per `motherduck_client.get_token()`.

## Session attribution (MotherDuck)

Set these for query-history correlation (see also [`docs/motherduck_operator_runbook.md`](motherduck_operator_runbook.md)):

```bash
export MOTHERDUCK_SESSION_HINT=THYROID_2026
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_molecular/manual_mm_contract;kind=contract"
```

Scripts **128** and **129** call `setdefault` on `MOTHERDUCK_SESSION_HINT=THYROID_2026` and a script-specific user-agent when `--md` is used, so manual runs still pick up the common session hint if you omit it.

## Boundaries: script 129 vs script 128

| Layer | Responsibility |
|--------|----------------|
| **129** (`129_imaging_fna_linkage_mm_v1.py`) | Implements **pairing logic**: candidate wide table, `imaging_fna_linkage_mm_v1`, `review_queue_imaging_fna_mm_v1`, `val_imaging_fna_linkage_audit_v1`. Can target **main** or a contract schema via `--contract-schema` / `MM_IFNA_OUTPUT_SCHEMA`. |
| **128** (`128_multimodal_contract_mm_v1.py`) | **Imports 129’s SQL** and runs it **inside the contract schema**, then builds `link_imaging_fna_mm_v1` (join to `fact_imaging_mm_v1` / `fact_fna_mm_v1`), and `val_imaging_fna_contract_blockers_mm_v1` (mirror of the review queue for strict gating). Also builds all other contract facts/dims and multimodal `val_*` tables. |

**Risks if run out of order or in the wrong schema**

- Running **128** without 129’s inputs (`imaging_nodule_master_v1`, `fna_episode_master_v2`, etc.) will fail (fail-closed) unless `--allow-bootstrap-dev` is used (dev only).
- Running **129** to **main** and **128** to `mm_contract_dev` without aligning `--contract-schema` on 129 leaves 128 rebuilding linkage inside the schema; stale **main** copies of `imaging_fna_linkage_mm_v1` may confuse ad-hoc queries. **Production convention:** run 129 with `--contract-schema mm_contract_dev` before 128 on MotherDuck (see [`.github/workflows/motherduck_episode_pipeline.yml`](../.github/workflows/motherduck_episode_pipeline.yml)).

## Commands

### Local file DB (developer)

```bash
.venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py
```

Dev-only stubs (missing upstream tables):

```bash
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py --allow-bootstrap-dev
```

### MotherDuck (writes `mm_contract_dev` by default for 128)

```bash
export MM_IFNA_OUTPUT_SCHEMA=mm_contract_dev
.venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py --md --sa \
  --contract-schema mm_contract_dev
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py --md --sa
```

### Strict release (CI / promotion)

Use **`--strict-release`** on **both** scripts so upstream gaps cannot silently relax temporal rules:

```bash
.venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py --md --sa \
  --contract-schema mm_contract_dev --strict-release
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py --md --sa --strict-release \
  --emit-ci-artifact artifacts/multimodal_release_gate.json
```

Artifacts:

- `129`: `--emit-ci-artifact PATH` → `imaging_fna_linkage_mm_v1_gate_v1` JSON (status, counts, `review_queue_by_reason`).
- `128`: `--emit-ci-artifact PATH` → `multimodal_release_gate_v1` JSON (row counts + `release_validation_metrics` + strict summary).
- **Review queue deltas:** re-run 128 with `--prior-gate-artifact path/to/prior_multimodal_release_gate.json` to populate `review_queue_deltas` (compares `review_queue_by_reason` to the prior artifact).

## Interpreting outputs

After 128, the export JSON and stdout include **`release_validation_metrics`**:

| Field | Meaning |
|--------|---------|
| `blocking_validation_row_counts` | Row counts for each **strict-blocking** `val_*` table (release requires all zero). |
| `imaging_fna_link_flags` | Counts of ambiguous / multi-FNA / discordant-side flags on `link_imaging_fna_mm_v1`. |
| `ambiguous_multimodal_by_domain` | Rows in `val_ambiguous_multimodal_linkage_mm_v1` by domain (e.g. surgery_pathology, preop_surgery). |
| `laterality_mismatch_by_issue_type` | Breakdown of `val_side_lobe_mismatch_mm_v1`. |
| `temporal_violation_by_issue_type` | Breakdown of `val_preop_temporal_order_mm_v1`. |
| `node_invariant_by_violation_type` | Breakdown of `val_nodes_invariant_mm_v1`. |
| `review_queue_by_reason` | Imaging–FNA review queue rows by `review_reason`. |
| `imaging_fna_audit` | One-row snapshot from `val_imaging_fna_linkage_audit_v1`. |

**Blocking vs investigative**

- **Strict gate (blocking):** empty `val_contract_required_join_keys_mm_v1`, `val_nodes_invariant_mm_v1`, `val_multitumor_expansion_mm_v1`, `val_side_lobe_mismatch_mm_v1`, `val_preop_temporal_order_mm_v1`, `val_ambiguous_multimodal_linkage_mm_v1`, `val_imaging_fna_contract_blockers_mm_v1`, and **no** dev bootstrap. Full list: [`docs/multimodal_release_gate.md`](multimodal_release_gate.md).
- **Non-blocking investigation:** high counts in ambiguous linkage or review queues still warrant triage before claiming clinical analyses are “clean.”

## Related

- CI: offline tests `tests/test_multimodal_contract_mm_v1.py`, `tests/test_imaging_fna_linkage_mm_v1.py`; MotherDuck strict job (manual): `.github/workflows/ci.yml` → `multimodal-md-contract-gate`.
- Episode pipeline (optional multimodal): `.github/workflows/motherduck_episode_pipeline.yml`.
