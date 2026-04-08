## Multimodal contract — repo facts

### Script **129** (`129_imaging_fna_linkage_mm_v1.py`)

- **Scope:** **Imaging ↔ FNA** linkage: `imaging_fna_linkage_mm_v1`, audit, and review queue.
- **Inputs:** Canonical imaging from **`imaging_nodule_master_v1`** (not legacy empty `imaging_nodule_long_v2`).
- **Not in scope:** A single deterministic graph tying **TIRADS → Bethesda → molecular_test → pathology** into one **`main.*`** promoted “chain table.” Those domains exist separately (canonical facts, molecular contract views, episode tables).

### Script **128** (`128_multimodal_contract_mm_v1.py`)

- **Scope:** Star-schema style **multimodal contract v1** tables + **validation views** (`val_*_mm_v1`).
- **Default MotherDuck schema:** **`mm_contract_dev`** (override via `MM_CONTRACT_SCHEMA` / flags; MD override gated).
- **Relationship to 129:** Runbook warns to align **`--contract-schema`** on **129** before **128** so linkage and contract live in the same schema.

### Release surface

- **`mm_contract_dev`** is the **documented default** for MM validation and CI isolation (see `docs/multimodal_contract_runbook.md`, `tests/test_multimodal_contract_mm_v1.py`).
- **`148_thyroid2026_release_gate.py`** considers **`mm_contract_dev`** and **`main`** for multimodal validators — promotion to **`main`** is **not** assumed by default in operator docs.

### Hypothesis conclusion

**Confirmed:** multimodal work is **partial** — imaging↔FNA plus MM star schema/validators — **without** a promoted end-to-end **US/TIRADS → FNA/Bethesda → molecular → surgery/pathology** chain as one **production `main.*`** contract object.
