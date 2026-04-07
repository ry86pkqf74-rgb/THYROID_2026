# THYROID_2026 — agent prompt: clear multimodal HOLDs (129 / 128)

Copy everything inside the **AGENT PROMPT** fence into a new chat or task.  
Use after [`reports/local_dry_run_report.md`](local_dry_run_report.md) reported **HOLD** on local `thyroid_master.duckdb`.

---

## AGENT PROMPT (copy from here)

```text
You are working in the THYROID_2026 repo. Goal: resolve HOLDs on multimodal pipeline
scripts 129 (imaging_fna_linkage_mm_v1) and 128 (multimodal_contract_mm_v1).

Context (from prior dry run):
- Local file DB `thyroid_master.duckdb` was missing: fna_episode_master_v2,
  imaging_nodule_master_v1, operative_episode_detail_v2, etc.
- mm_contract_upstream.CORE_TABLES must ALL exist before any bootstrap; they are never stubbed:
  operative_episode_detail_v2, tumor_episode_master_v2, molecular_test_episode_v2,
  imaging_nodule_master_v1.
- --allow-bootstrap-dev on 128 only stubs *non-core* upstreams after CORE_TABLES exist.

Do this in order:

1) MotherDuck path (preferred when cloud catalog is complete)
   - Ensure MOTHERDUCK_TOKEN or MD_SA_TOKEN (or .streamlit/secrets.toml) is available; never log tokens.
   - Read-only inventory: run or adapt scripts/129_motherduck_readonly_audit.py --md if present,
     or connect with connect_md_fail_closed and verify:
     EXISTS + non-zero (or expected) row counts for CORE_TABLES and fna_episode_master_v2.
   - If fna rows missing on MD: consider scripts/motherduck_seed_fna_episode_master_v2.py (see motherduck/README.md),
     then scripts/129_imaging_fna_linkage_mm_v1.py --md, then scripts/128_multimodal_contract_mm_v1.py --md
     (add --strict-release only for release gates; use --allow-bootstrap-dev never to bypass missing CORE_TABLES).
   - Capture motherduck/exports/imaging_fna_linkage_mm_v1_audit.json status after 129.

2) Local file DB path (no MotherDuck, or to mirror cloud)
   - Build canonical episodes: scripts/22_canonical_episodes_v2.py (loads processed/*.parquet;
     use --md only when targeting MotherDuck).
   - Build imaging nodule master: scripts/50_multinodule_imaging.py (creates imaging_nodule_master_v1).
   - Optionally enhanced linkage: scripts/49_enhanced_linkage_v3.py if the runbook requires v3 link tables before 128.
   - Then scripts/129_imaging_fna_linkage_mm_v1.py (no --md), then scripts/128_multimodal_contract_mm_v1.py.
   - If any step fails, print the exact RuntimeError and which table is missing; cross-check CORE_TABLES + UPSTREAM_KEYS
     in scripts/mm_contract_upstream.py.

3) Deliverable
   - Short markdown or reply: PASS (commands + row counts) or BLOCKED (missing table / token / schema)
     with the next single command to try.

Constraints: follow repo flags; do not invent new CLIs; no PHI in logs; user may forbid MotherDuck writes—ask if unclear.
```

---

## Reference (human / maintainer)

| Requirement | Where |
|-------------|--------|
| **CORE_TABLES** (must exist on catalog before 128 bootstrap) | `scripts/mm_contract_upstream.py` (`CORE_TABLES`, `ensure_upstream_sources`) |
| **129** inputs | `imaging_nodule_master_v1`, `fna_episode_master_v2` (see `scripts/129_imaging_fna_linkage_mm_v1.py`) |
| **FNA seed on MD** (reduced fidelity) | `scripts/motherduck_seed_fna_episode_master_v2.py` + `motherduck/README.md` |
| **Local `imaging_nodule_master_v1`** | `scripts/50_multinodule_imaging.py` |
| **Canonical v2 episode base** | `scripts/22_canonical_episodes_v2.py` |
| **CI / offline tests** | `tests/test_multimodal_contract_mm_v1.py`, `tests/test_imaging_fna_linkage_mm_v1.py` |
| **Workflow (129 → 128 on MD)** | `.github/workflows/motherduck_episode_pipeline.yml` |

**Important:** `--allow-bootstrap-dev` does **not** create **operative_episode_detail_v2** or **imaging_nodule_master_v1**. Those must come from real pipeline outputs (local scripts above or MotherDuck `main`).

---

## Quick command cheatsheet

**MotherDuck (read/write per your policy):**

```bash
cd /path/to/THYROID_2026
.venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py --md
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py --md
```

**Local `thyroid_master.duckdb` (typical build order):**

```bash
cd /path/to/THYROID_2026
.venv/bin/python scripts/22_canonical_episodes_v2.py
.venv/bin/python scripts/50_multinodule_imaging.py
.venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py
.venv/bin/python scripts/128_multimodal_contract_mm_v1.py
```

(Insert `49_enhanced_linkage_v3.py` when your release runbook requires those tables.)
