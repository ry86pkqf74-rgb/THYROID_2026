# THYROID_2026 — Adversarial promotion review (2026-04-07)

**Assumption:** CI green and docs read “ready” can still hide promotion risk. This note is a severity-ranked pass over extraction, validators, MotherDuck wiring, multimodal release semantics, and operational gates.

**MotherDuck session:** operators should keep `MOTHERDUCK_SESSION_HINT=THYROID_2026` (scripts 128/129/148/142 setdefault this when `--md`).

**Promotion verdict (today):** **Would not allow unconditional promotion.** Use script **148** as the arbiter: it exits **2** on **HOLD** and **1** on **FAIL**; treat **0** as the only automated “all clear,” and still require human sign-off on data policy (backlogs below).

---

## Severity-ranked findings

### P0 — Blockers (fix before treating cloud as publication-safe)

| ID | Topic | Evidence | Minimal fix |
|----|--------|----------|-------------|
| P0-1 | **Manual review / Tg queues do not fail the unified release gate** — large backlogs only yield **HOLD** (exit 2), not **FAIL**, so policy must explicitly forbid shipping while HOLD persists. | `scripts/148_thyroid2026_release_gate.py` (`Severity.HOLD` for `qa.manual_review_queue` pending rows, `tg_lab_review_queue_v1` non-empty); rollup exits 2 on any HOLD (`L737-L741`, `L822-L828`). | **Process:** require `148` **PASS** (zero HOLD) for production promotion, *or* change policy-driven checks to **FAIL** when `pending > N` / queue age SLA breached. |
| P0-2 | **`MOTHERDUCK_DATABASE` / `MOTHERDUCK_DB` collapses dev/qa/prod to one catalog** — `motherduck_client.resolve_database_for_env` lets a single override bypass separated DB names in `config/motherduck_environments.yml`, increasing **schema collision** and accidental cross-env writes. | `motherduck_client.py` `resolve_database_for_env` (`L138-L149`). | **CI/pre-push:** forbid `MOTHERDUCK_DATABASE` in release workflows unless explicitly tagged; **operators:** use distinct catalogs per env without override. |
| P0-3 | **Per-domain LLM path still pulls the generic `"llm"` extractor bucket** — targeted v1 runs merge regex domain extractors with **all** rows from `entity_domain == "llm"`, not only rows tagged for that domain. Risk: **cross-domain bleed** into domain parquets and dilution of per-prompt discipline. | `llm_extraction/run_extraction.py` `L732-L734` (`extractors.extend(... entity_domain == "llm")`). | **Code:** when `--target <domain>` is set, drop the unconditional `llm` bucket **or** filter LLM outputs with `entity_domain == target_domain` / registry mapping before write. |
| P0-4 | **LLM prompt loader uses only the first registry prompt and swallows errors** — `prompt_for_domain` returns a single `PromptSpec`; multi-prompt domains are under-served; failed registry IO falls through to **lab_date** / default prompts → **silent generic extraction**. | `llm_extraction/extract_llm.py` `_load_system_prompt` / `_prompt_version` (`L160-L176`, `L167-L168` bare `except Exception: pass`); registry API `registry.py` documents multi-prompt (`prompts_for_domain`). | **Code:** use `prompts_for_domain` where multiple prompts exist; **fail closed** (raise or log+FATAL) when domain prompt missing for v2 domains. |

### P1 — High (likely ship-stop in strict multimodal / formalization paths)

| ID | Topic | Evidence | Minimal fix |
|----|--------|----------|-------------|
| P1-1 | **Multimodal “fail closed” depends on `--strict-release` and artifact tables** — without strict mode, MotherDuck can return **blocked** payloads for missing upstreams instead of raising; release gate only treats **non-empty** `val_*_mm_v1` blocker tables as **FAIL** (`148` `L640-L688`). | `scripts/129_imaging_fna_linkage_mm_v1.py` MotherDuck branch `blocked_missing_fna_episode_master_v2` (`L477-L507`) vs strict (`L462-L476`, `L702-L705`); `docs/multimodal_release_gate.md`. | **Automation:** always run **129→128** with `--strict-release` on promotion paths; **148** already encodes empty blocker tables as FAIL when tables exist. |
| P1-2 | **Imaging→FNA integration still has structural coupling gaps** — linkage depends on `imaging_nodule_master_v1` + `fna_episode_master_v2` + (strict) `tumor_episode_master_v2`; empty or misaligned schemas → blocked or weak linkage; historical notes document `imaging_fna_linkage_v3` **0 rows** until upstream imaging populated (AGENTS / post_maturation gaps). | `scripts/129_imaging_fna_linkage_mm_v1.py` prechecks (`L460-L476`, `L477-L507`); AGENTS.md imaging→FNA backlog notes. | **Data:** materialize upstream masters; **ops:** run 129 with `--contract-schema mm_contract_dev` aligned with 128 (see `docs/multimodal_contract_runbook.md`). |
| P1-3 | **Validator entry points still allow legacy merged parquet mode** — `111` errors if merged file missing but still supports `--input` path to **monolithic** `note_entities_llm.parquet`, which can mask per-domain QA. | `scripts/111_llm_extraction_validation.py` `L1778-L1801`. | **Policy:** require `--domain` or `--all-llm-domains` in CI; reject merged input for v2 promotion. |

### P2 — Medium (operational / correctness debt)

| ID | Topic | Evidence | Minimal fix |
|----|--------|----------|-------------|
| P2-1 | **Cross-wave Tg/TgAb dedup keys omit assay context** — Phase C dedup uses `research_id`, `test_name`, `specimen_collect_dt`, `result` only; different methods/units same display string could collapse incorrectly. | `scripts/113_tg_lab_ingestion.py` `phase_c_dedup` (`L197-L201`). | **Augment key** with `assay_method` or normalized unit hash where populated; route conflicts to review queue (script already has combo / cross-wave phases — extend tests). |
| P2-2 | **v1/v2 table shadowing** — Streamlit and SQL patterns probe `table` then `md_` prefix; dual materialization can hide which layer consumers read. | AGENTS.md Streamlit fallback notes; `docs/multimodal_contract_runbook.md` schema alignment. | **Convention:** document authoritative schema per environment; add lint or health view listing “active” table per entity. |
| P2-3 | **Tests are mostly offline** — multimodal tests use `:memory:` DuckDB (`tests/test_multimodal_contract_mm_v1.py`, `test_imaging_fna_linkage_mm_v1.py`); they **cannot** catch MotherDuck-specific attach, share-path, or workload issues. | Test file grep patterns throughout `tests/`. | Keep **manual** `multimodal-md-contract-gate` workflow; extend smoke test that calls `connect_md_fail_closed` with a read-only query when secrets present. |

### P3 — Low (hardening; one fixed in this pass)

| ID | Topic | Evidence | Minimal fix |
|----|--------|----------|-------------|
| P3-1 | **G8 MotherDuck parity used ad-hoc `duckdb.connect(md:…?token=)`** — bypassed `MotherDuckClient`, default DB string, session hint, and attach verification vs shared helpers. **Fixed:** `check_md_parity` now uses `MotherDuckClient.for_env`, `THYROID_2026` session default, and PRAGMA verification (see `scripts/112_v2_domain_promotion_gate.py`). | Prior: `112` `check_md_parity` raw URI (`~L982-L985` before change). | **Done** in repo: align with `utils/md_connect` semantics. |

---

## Tests vs operational gaps

- **Passing pytest ≠ safe promotion:** multimodal and linkage tests do not run against live MotherDuck; **148** / **147** reports are the contract for cloud.
- **Registry fleet parity** is enforced in **148** (`_check_registry_integrity`); if removed from CI, domain/prompt drift can return silently.

---

## Explicit promotion statement

**Would I allow promotion today?** **No** for a standalone “green CI → ship” decision: **P0-1** (HOLD-class review backlogs), **P0-2** (catalog override risk), **P0-3 / P0-4** (LLM domain collapse + prompt fallback), and **P1** multimodal/imaging prerequisites must be resolved or explicitly accepted in writing. After **148** returns **PASS** (exit 0), **strict** multimodal rerun is clean, and ops confirm **no** accidental `MOTHERDUCK_DATABASE` aliasing, promotion becomes defensible.

---

## Change log (repo)

- Hardened **112** MotherDuck G8 parity to use `MotherDuckClient`, `MOTHERDUCK_SESSION_HINT=THYROID_2026`, and attach verification consistent with shared connection helpers.
