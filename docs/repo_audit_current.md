# THYROID_2026 — current-state repo audit (2026-04-07)

**Method:** Static review of named files + workflows. No live MotherDuck queries in the audit shell (env vars for `MD_SA_TOKEN` / `MOTHERDUCK_TOKEN` / `motherduck_token` were **MISSING** there). **Local dev:** the project keeps MotherDuck credentials in **gitignored** `.streamlit/secrets.toml`; `motherduck_client.get_token()` reads `MOTHERDUCK_TOKEN` / `MD_SA_TOKEN` from that file when env is empty (see `motherduck_client.py` Streamlit secrets block — lines ~189–199 in the audited revision). Run `--md` scripts from repo root so that path resolves.  
/git: `main` @ `2d18dd2aa668b0211c69de9792084747f365d84a`, `origin/main`.

---

## 1. Instruction / policy surfaces

| Artifact | Role |
|----------|------|
| `AGENTS.md` | Long-form agent memory: PHI, provenance, append-only tables, commit scope, DuckDB/MotherDuck notes. |
| `README.md` | Product-facing posture: MotherDuck formalization, release gates (`119 --release-mode`), MRQ/lab blockers, registry SSOT. |
| `pyproject.toml` | Mypy scoped to listed packages (not full repo); excludes `studies/`, `exports/`, etc. |
| `requirements.txt` / `requirements-dev.txt` | Runtime vs dev deps (read headers in repo for pins). |
| `.github/copilot-instructions.md` | Copilot: tool checklist, cost/confirm gates, project context. |
| `.github/agents/*.agent.md` | Referenced in copilot instructions (not re-loaded in full for this audit). |

---

## 2. Tests and CI

- **Blocking offline suite** (`ci.yml` job `llm-extraction-gold`): pytest on `tests/test_llm_extraction_regression.py`, `test_fact_provenance_contract.py`, `test_registry_and_md_connect.py`, `test_fleet_registry_parity.py`, `test_120_review_queue_triage.py` (lines 147–154).
- **Multimodal offline** (`multimodal-tests`): pytest including `test_multimodal_contract_mm_v1.py`, `test_imaging_fna_linkage_mm_v1.py`, specimen/FHIR tests (lines 176–181).
- **MotherDuck episode + multimodal (128/129):** separate workflow `motherduck_episode_pipeline.yml`, **manual** `workflow_dispatch` only (lines 6–7, 101–120). Not triggered by `ci.yml` path filters alone.

---

## 3. V2 extraction vs “generic llm” collapse

**Verdict from code:** v2 domain runs **do not** persist under a single generic `entity_domain` in the runner; each row is stamped with the registry domain name.

**Evidence:**

1. `run_llm_for_domain` docstring: results are “keyed to *domain_name* — not `llm`” (`llm_extraction/run_extraction.py` lines 286–288).
2. Per-row: `rec["entity_domain"] = domain_name` (`run_extraction.py` lines 322–325).
3. Per-domain parquet stems from registry: v2 example `imaging` → `note_entities_llm_imaging` (`config/extraction_domain_registry.yaml` lines 138–140).

**Caveat — class-level default:** `LLMExtractor.entity_domain = "llm"` (`llm_extraction/extract_llm.py` line 68). Any code path that stamped rows **without** the runner’s override could still emit `llm`; the audited v2 path explicitly overrides.

**Caveat — merged audit artifact:** `note_entities_llm.parquet` is optional and described as audit-only (`run_extraction.py` docstring lines 31–32, `--merge-audit` help lines 524–529).

---

## 4. Validator (111) — merged-only vs per-domain

**Verdict:** Supports **both**; default legacy path is **discouraged** when merged file absent.

**Evidence:**

- Modes documented: `--input` (legacy), `--domain`, `--all-llm-domains` (`scripts/111_llm_extraction_validation.py` lines 136–152, 156–187).
- If no flags and `note_entities_llm.parquet` **missing**, **SystemExit(1)** with message to use `--all-llm-domains` or `--domain` (lines 1651–1660).
- If merged file **exists**, warns and falls back (lines 1662–1667).

So “coverage” is **operator-chosen**: true per-domain validation requires explicit `--domain` or `--all-llm-domains`; legacy merged file validates one combined parquet only.

---

## 5. Shared MotherDuck helpers vs bypasses (audited scripts)

| Script | MotherDuck path | Local / other |
|--------|------------------|---------------|
| `02b_register_notes_entities.py` | `connect_md_or_file` | — |
| `103_fact_lineage_materialize.py` | `connect_md_or_file` (later in file) | `duckdb.connect(DB_PATH, read_only=True)` for episode parquet fallback loader (lines 232–234) |
| `113_tg_lab_ingestion.py` | `connect_md_or_file` when `--md` | — |
| `128_multimodal_contract_mm_v1.py` | `connect_md_or_file` when `--md` | `duckdb.connect(DB_PATH)` without MD |
| `129_imaging_fna_linkage_mm_v1.py` | `connect_md_or_file` when `--md` | `duckdb.connect(DB_PATH)` without MD |
| `09b_fabric_upload_notes_entities.py` | N/A | Azure + pandas; not DuckDB MotherDuck |

**Repo-wide note:** Many other `scripts/*.py` use raw `duckdb.connect` for **local** workflows; the audit scope listed only the user’s script set. Representative grep shows `duckdb.connect` in dozens of scripts (local-first pattern).

---

## 6. dev / qa / prod MotherDuck targets

**Verdict:** **Different default database names** per environment in `config/motherduck_environments.yml` (lines 12–18):

- dev: `Thyroid 2026 Molecular Dev 20260407`
- qa: `Thyroid 2026 Molecular QA 20260407`
- prod: `Thyroid 2026`

**Override:** `motherduck_client.resolve_database_for_env` uses `MOTHERDUCK_DATABASE` / `MOTHERDUCK_DB` when set (`motherduck_client.py` lines 138–149).

**Operational implication:** Clones/sandboxes reduce blast radius; promotion must be explicit. Accidental cross-env writes are mitigated **only if** teams do not point all automation at the same override.

---

## 7. Token resolution order (documented)

`utils/md_connect.py` delegates to `motherduck_client.get_token` / `MotherDuckClient` (lines 34–48, 98–107).

`motherduck_client.get_token` documents:

- `prefer_service_account=True`: `MD_SA_TOKEN` → `MOTHERDUCK_TOKEN` / `motherduck_token` → secrets file (lines 155–166 docstring; implementation lines 168–177).
- `prefer_service_account=False`: personal token first, then SA (lines 160–163; implementation lines 178–184).

---

## 8. Multimodal 128/129 and CI/release

- **Automated tests:** `ci.yml` runs pytest multimodal tests on push/PR (lines 157–181).
- **MotherDuck materialization:** `motherduck_episode_pipeline.yml` runs 129 then 128 when `multimodal_enabled` (default true), with schema isolation vs promotion controlled by inputs (lines 8–24, 77–120).

**Gap:** On-push CI does **not** invoke 128/129 against MotherDuck; cloud path relies on **manual** workflow or other ops.

---

## 9. README-stated release blockers (narrative, not re-proven live)

`README.md` lines 3–19 state (paraphrased with pointers):

- Synthetic MRQ verification dominates; human-reviewed CSV hydrate path required for manuscript sign-off.
- Non-Tg institutional lab wave pending.
- Latest `119 --release-mode` run failed specimen/FHIR QA diagnostics per signoff memos (see linked `studies/20260407_publication_signoff_live/`).

This audit does **not** re-run `119` or query MRQ without MotherDuck tokens.

---

## 10. Findings rank (P0 / P1 / P2)

See `docs/release_readiness_gap_list.md` for ranked list and recommended order.

---

## Files inspected

Same list as `docs/architecture_map.md` plus: `pyproject.toml`, `requirements.txt` (path only), `tests/` (via CI YAML references only).

## Files changed (this audit)

`docs/repo_audit_current.md`, `docs/release_readiness_gap_list.md`, `docs/architecture_map.md`.
