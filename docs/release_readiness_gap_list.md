# Release readiness — gap list (ranked)

**Audit date:** 2026-04-07 · **Repo:** `main` @ `2d18dd2aa668b0211c69de9792084747f365d84a`  
**Evidence mode:** Code + README + workflow YAML. Live MotherDuck / MRQ counts **not** verified (no RW token in audit session).

---

## P0 — blocks trustworthy “signed” MotherDuck release / manuscript posture

| ID | Gap | Evidence | Recommended fix order |
|----|-----|----------|------------------------|
| P0-1 | **Release-mode validation & specimen/FHIR QA** called out as failing in README narrative | `README.md` lines 7–18 (119 `--release-mode`, specimen/FHIR diagnostics); requires rerun with secrets | 1) Rerun `scripts/119_md_formalization_validate.py --md --release-mode` in gated CI or controlled runner; 2) triage `142`/specimen QA outputs per repo docs |
| P0-2 | **Manual review queue** not manuscript-approved (synthetic verification) | `README.md` lines 27–28 | 1) Human review CSV path + hydrate (`docs/review_queue_triage_export.md`, script 120 referenced in README); 2) align `verification_status` semantics with ops |
| P0-3 | **LLM prompt coverage** uses **first prompt only** in `LLMExtractor` for a domain | `llm_extraction/extract_llm.py` lines 160–166 (`prompt_for_domain`) vs registry multi-prompt domains (`config/extraction_domain_registry.yaml` e.g. `genetics` lines 31–35) | 1) For multi-prompt domains, either call `prompts_for_domain` and iterate, or split registry into single-prompt domains; 2) add contract test |

---

## P1 — pipeline / CI / ops gaps (high impact, not always hard-stop)

| ID | Gap | Evidence | Recommended fix order |
|----|-----|----------|------------------------|
| P1-1 | **Multimodal 128/129** not on push CI — only manual `motherduck_episode_pipeline` + offline pytest | `.github/workflows/ci.yml` multimodal job = pytest only (lines 157–181); `motherduck_episode_pipeline.yml` is `workflow_dispatch` (lines 6–7) | 1) Decide: scheduled job, post-promotion hook, or accept manual; 2) document SOP in runbook |
| P1-2 | **Script 111** legacy docstring still centers merged `note_entities_llm` while runtime fail-closed nudges per-domain | `scripts/111_llm_extraction_validation.py` lines 5–6 vs 1651–1660 | 1) Update module docstring to match v2-first UX; 2) optional: default `--all-llm-domains` in CI smoke |
| P1-3 | **Environment drift risk** if `MOTHERDUCK_DATABASE` override aligns all envs to one DB unintentionally | `motherduck_client.py` lines 138–149; `config/motherduck_environments.yml` separate DBs | 1) Enforce per-env secrets/vars in CI; 2) audit automation templates |

---

## P2 — tech debt / clarity

| ID | Gap | Evidence | Recommended fix order |
|----|-----|----------|------------------------|
| P2-1 | **`LLMExtractor.entity_domain = "llm"`** can confuse readers despite runtime override | `llm_extraction/extract_llm.py` line 68; contrast `run_extraction.py` lines 322–325 | Docstring cross-link or rename internal field |
| P2-2 | **103** uses ad hoc `duckdb.connect` for local episode fallback (acceptable but mixed pattern) | `scripts/103_fact_lineage_materialize.py` lines 232–234 | Optional: wrap in small helper for consistency |
| P2-3 | **Non-Tg labs** described as pending institutional wave | `README.md` line 29; AGENTS.md script 113 notes | Track as data acquisition dependency |

---

## Quantified blockers

Live **counts** for MRQ rows, review queues, lab reconciliation, and `119` diagnostics require MotherDuck access. This document records **structural** gaps only.

**From README (narrative baselines, not re-verified):**

- MRQ dominated by synthetic verification (`README.md` line 28).
- Non-Tg lab pull pending (`README.md` line 29).
- V2 domains / inventory: README references **31** parent domains and **23** promoted v2 (`README.md` lines 17–18) — confirm with `studies/20260406_domain_inventory_current/` regenerator when releasing.

---

## Fail-closed gaps (behavioral)

- `utils/md_connect.connect_md_or_file(..., fail_closed=True)` exits if MD not reachable (`utils/md_connect.py` lines 115–136, 142–154).
- Script 111 exits if legacy merged parquet missing and no explicit mode (`scripts/111_llm_extraction_validation.py` lines 1651–1660).
- Multimodal workflow supports `--strict-release` for 128 via `multimodal_strict_release` input (`motherduck_episode_pipeline.yml` lines 13–16, 117–118).

---

## Files changed

`docs/release_readiness_gap_list.md` (this file), plus companion audit docs.
