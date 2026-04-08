# Multimodal linkage completeness audit (2026-04-07)

**Question:** Does the repo provide a **production-usable, deterministic** chain from ultrasound / TI-RADS → FNA / Bethesda → molecular testing → surgery / synoptic / final pathology?

**Verdict:** **No** — not as a **single promoted nodule-level object in `main.*`** that is part of the core MotherDuck publication contract. The repo provides **deterministic building blocks** (episode tables, v3 scored linkages, script **129** imaging↔FNA, script **128** multimodal star schema in **`mm_contract_dev`**, optional **`utils/canonical_nodule_linkage`** + script **149** study VIEW), plus **separate** specimen / FHIR / molecular layers in **`main`** when deployed.

**Evidence sources:** repository code and docs (see `surface_inventory.md`), pytest run (`validation_summary.md`), optional local read-only catalog probe (`commands_run.md`). **MotherDuck prod** was not queried in this run (no `.streamlit/secrets.toml` in workspace; token expected via env or gitignored TOML per `motherduck_client.py`).

**Outputs in this folder:**

| File | Purpose |
|------|---------|
| `surface_inventory.md` | Scripts, modules, and catalog objects referenced by the chain |
| `canonical_chain_matrix.md` | Step-by-step chain with grain, keys, promotion flags |
| `promoted_vs_dev_only_matrix.md` | `main` vs `qa` vs `mm_contract_dev` vs local/historical |
| `missing_contract_items.md` | Gaps vs a signed-off release contract |
| `implementation_plan.md` | Shortest repo-native path to a usable end-to-end chain |
| `validation_summary.md` | Tests and inspection results |
| `commands_run.md` | Exact commands executed |

**Operator alignment:** Multimodal runbook — `docs/multimodal_contract_runbook.md`; DB contract — `docs/motherduck_database_contract_v1.md`; release gate — `scripts/148_thyroid2026_release_gate.py` (multimodal blockers in `mm_contract_dev` or `main`).
