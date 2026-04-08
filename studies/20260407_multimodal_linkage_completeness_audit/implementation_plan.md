# Implementation plan — shortest repo-native path to a real deterministic chain

**Goal:** Use **existing** deterministic keys (`research_id`, episode IDs, `score_rank = 1` v3 linkages, primary `imaging_fna_linkage_mm_v1`) — **no new patient matching**.

## Path A — Study-grade nodule grain (lowest new surface area)

1. Ensure on target catalog: `imaging_nodule_master_v1`, `fna_episode_master_v2`, `molecular_test_episode_v2`, `tumor_episode_master_v2`, `preop_surgery_linkage_v3`, `fna_molecular_linkage_v3`, `surgery_pathology_linkage_v3`.
2. Run **49** (and any prerequisite episode scripts **22+**) if v3 tables missing or stale.
3. Run **129** with **`--contract-schema mm_contract_dev`** (MotherDuck) or local default so `imaging_fna_linkage_mm_v1` aligns with **128** (per runbook).
4. Execute **`utils/canonical_nodule_linkage.canonical_nodule_linkage_sql()`** as a VIEW or export:
   - **149** `scripts/149_md_canonical_nodule_linkage_study.py --md` (read-heavy) with optional `--materialize-view` on **dev/qa** first; prod only with **`--confirm-prod-view`**.
5. Add release policy: treat **`manual_review_needed_flag`** and linkage tier columns as **eligibility**, not as fuzzy matching.

## Path B — Multimodal star schema + analysts (contract v1)

1. Same upstream prerequisites as Path A.
2. **129 → 128** on MotherDuck: `docs/multimodal_contract_runbook.md` (strict: **`--strict-release`**).
3. Consume **`mm_contract_dev.link_imaging_fna_mm_v1`**, **`fact_genetics_mm_v1`**, **`link_surgery_path_mm_v1`**, **`link_surgery_context_mm_v1`** — join in **analyst SQL** for dashboards; accept **multiple rows** / **multiple tables** instead of one wide chain.

## Path C — Specimen + genomics (downstream of surgery/molecular)

1. Deploy **109** encounter QC + **138** + **140** per `docs/motherduck_database_contract_v1.md` for **specimen/FHIR** consumers.
2. Join specimen rows to molecular episodes via **`specimen_genomic_assay_v1`** — **specimen grain**, not nodule grain.

## Recommended sequencing

- **Validate upstream presence** with `information_schema` + row counts (read-only).
- **Path A** for explicit **nodule-level** lineage JSON / one row per nodule.
- **Path B** for **CI/gated** multimodal contract and surrogate IDs.

## Explicit non-goals (avoid greenfield)

- Do not invent new MRN-based patient matching — repo policy routes ambiguity to review queues.
- Do not replace `imaging_nodule_master_v1` with NLP-only US until structured ingest exists (`docs/imaging_layer_v3_design.md`).
