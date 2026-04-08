# Missing contract items (vs a signed “full nodule chain” release)

Items below are **gaps relative to** a hypothetical requirement: *one promoted `main.*` object that is the authoritative, row-level, nodule-grain deterministic chain from US to final pathology, with release validation in **119**/**148**.*

## Not in core DB contract as a single `main` wide table

1. **`main.canonical_nodule_chain_v1` (table)** — **does not exist** in `docs/motherduck_database_contract_v1.md`. Closest **code** is `utils/canonical_nodule_linkage.canonical_nodule_linkage_sql()` + optional **`main.canonical_nodule_linkage_study_v1`** VIEW via **149** (study / explicit prod opt-in).

2. **`link_fna_genetics_mm_v1` / `link_imaging_pathology_mm_v1`** — **128** exposes **`link_imaging_fna_mm_v1`** and **`link_surgery_path_mm_v1`** and episode-context aggregates, **not** a single surrogate-key chain table spanning imaging→molecular→tumor in one row.

## Multimodal contract location

3. **Default multimodal materialization** is **`mm_contract_dev`**, not **`main`** — per **128** docstring and `docs/multimodal_contract_v1.md`. Promotion to `main` is **exception** path (env override), not the documented default.

## Script 129 scope

4. **129** stops at **imaging ↔ FNA** (+ QA/audit). Molecular and pathology continuation require **v3 linkages** + episode tables (and/or **128** facts / **149** SQL).

## Operational / data prerequisites

5. **Upstream v3 linkage tables** must exist and be populated on the target catalog (**49** + repairs). Local audit DB lacked them — **environment risk**.

6. **`imaging_nodule_master_v1`** must exist with dated rows for the chain — local audit DB **lacked** the table entirely.

## Strict-release cleanliness

7. **`val_*_mm_v1` blocker tables** — repo audit artifacts indicate **non-zero** rows on some MotherDuck envs (`reports/motherduck_read_only_audit.md`). Until empty under **128 --strict-release**, multimodal gate is **not** clean.

## Specimen layer vs nodule chain

8. **`specimen_*` / FHIR** — contract-complete for **specimen identity + analytic export**, **orthogonal grain** to **nodule-level** TI-RADS rows; does not replace nodule chain logic.

## Molecular results layer

9. **131/132** add **assay/variant** normalization — **complementary** to `molecular_test_episode_v2`, not a replacement for **fna_molecular_linkage_v3** joins in the nodule chain.
