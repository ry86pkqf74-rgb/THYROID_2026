## Stale or superseded checked-in artifacts (can mislead agents)

| Artifact | Issue | Current truth source |
|----------|--------|----------------------|
| `studies/20260407_publication_signoff_live/validation_report.md` | **Historical BLOCKED** (`broken_fhir_refs=10139`) | Superseded by folder README + [`../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`](../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md) + **fresh** [`119_release_mode_rerun/validation_report.md`](119_release_mode_rerun/validation_report.md) |
| `studies/20260407_publication_signoff_live/live_audit_memo.md` §3 first bullet | Reports **BLOCKED** for first `119` capture | Same supersession; later automation **PASS** on specimen/FHIR diagnostics |
| `studies/20260407_publication_signoff_live/lab_coverage_memo.md` | States **no** `final_institutional*` wave | **Stale** — live `longitudinal_lab_canonical_v1` has **`final_institutional_20260407`** (verified 2026-04-08) |
| `studies/20260407_publication_signoff_live/mrq_reconciliation_memo.md` | Synthetic MRQ distribution **5,620 / 5,622** | **Likely stale** vs live — fresh `119 --release-mode` reports **no** synthetic-placeholder statuses and **11,244** reviewed rows |
| `studies/20260407_publication_signoff_live/final_verdict_memo.md` | Mixed **historical** and **updated** banners | Use **banner + README**; executive body still useful for audit trail |
| `studies/20260407_repo_delta_gap_audit/CURRENT_MOTHERDUCK_REPO_STATE.md` (from `144`) | Boilerplate line: “blocked by synthetic MRQ” | **May lag** generator template vs live; **this audit** updates governance row from fresh `119` |
| `data_dictionary.md` | Pre–MotherDuck-formalization local focus | **`docs/motherduck_database_contract_v1.md`** + live schema for cloud |
| `docs/imaging_layer_v3_design.md` | **Future v3** design; `imaging_fna_linkage_v3` called out as **0 rows** historically | **129** addresses imaging↔FNA via **`imaging_nodule_master_v1`** + MM linkage table — design doc still valid as **roadmap**, not “current implemented v3” |

## Still reliable

- `docs/motherduck_sandbox_clone_runbook.md` — DuckLake vs native snapshot semantics (aligned with `130 inspect`).
- `docs/release_runbook.md` — prepromote backup pattern.
- Top-level `README.md` (April 2026) — explicitly distinguishes automation vs governance vs source-limited gaps.
