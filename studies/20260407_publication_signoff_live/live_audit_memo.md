# Live audit memo — MotherDuck publication signoff

**Update (2026-04-14):** `MD_INFORMATION_SCHEMA.QUERY_HISTORY` may appear **accessible** in a coarse introspection snapshot while **filtered exports** still fail with `MDExternalException` for non–organization-admin identities — see [`../../docs/motherduck_database_contract_v1.md`](../../docs/motherduck_database_contract_v1.md) and [`../live_state_refresh_20260408_074310/LIVE_STATE_REFRESH.md`](../live_state_refresh_20260408_074310/LIVE_STATE_REFRESH.md). Row counts below are **2026-04-07 capture**; prod `longitudinal_lab_canonical_v1` has since grown (e.g. **77,960** rows in [`../live_state_refresh_20260408_074310/env_row_counts_probe.csv`](../live_state_refresh_20260408_074310/env_row_counts_probe.csv)) — cite refresh CSVs for current N.

**Supersession:** Section §3 below reflects the **first** `119` capture in this folder (**BLOCKED**, `broken_fhir_refs=10139`). **Later same-day** evidence: [`../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`](../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md) (**PASS WITH WARNINGS**, `broken_fhir_refs=0`).

**Captured:** 2026-04-07 (UTC-aligned with validation report timestamp in-folder)  
**Database (attach target):** `Thyroid 2026` (`md:Thyroid 2026`)

## 1) Database inventory

| Field | Live value |
|--------|------------|
| Current database name | Thyroid 2026 |
| Type (MD_INFORMATION_SCHEMA.DATABASES) | DUCKLAKE |
| Transient flag | False (catalog sample row) |
| Historical snapshot retention | 7 days (sample row) |
| `release_*` schemas | 6 present: `release_20260406`, `release_20260407`, `release_20260407_final`, `release_20260407_final2`, `release_20260408`, `release_20260409` |
| Latest `qa.release_manifest` tag | **20260409** (timestamp 2026-04-07 02:05:07 per `119` report) |

### MD_INFORMATION_SCHEMA access (this token)

| View | Accessible |
|------|------------|
| DATABASES | Yes |
| DATABASE_SNAPSHOTS | Yes |
| QUERY_HISTORY | Yes |
| RECENT_QUERIES | Yes |

Full sample output: [`md_introspection_snapshot.md`](md_introspection_snapshot.md).

**DuckLake note:** Snapshot semantics differ from native-only assumptions; use MotherDuck UI / org runbook for point-in-time evidence beyond `release_*` schema copies.

## 2) Analyst / canonical row counts (live)

| Object | Rows |
|--------|-----:|
| `main.canonical_extracted_fact_long_v2` | 123,577 |
| `main.canonical_fact_quarantine_v2` | 199 |
| `main.note_extraction_runs` | 3 |
| `main.longitudinal_lab_canonical_v1` | 76,971 |
| `main.longitudinal_lab_deduped_v` | 55,210 |
| `main.master_fact_long_verified_v1` | 123,577 |
| `main.master_patient_rollup_verified_v1` | 5,574 |
| `main.master_source_lineage_v1` | 123,577 |

Specimen/FHIR: `119` reports 10 objects present; **in-folder** first capture had QA diagnostics **FAIL** (see §3); **later** run **WARN** only (`broken_fhir_refs=0`).

## 3) Release-mode automation (`119 --release-mode`)

- **Verdict (in-folder snapshot, `2026-04-07T10:33:51Z`):** **BLOCKED** — 25 PASS / 1 WARN / 1 FAIL  
- **FAIL (that snapshot):** Specimen/FHIR QA diagnostics (`broken_fhir_refs=10139`; plus `high_tier_null_spec=14` in detail string).  
- **WARN:** Specimen-adjacent review burden — `genomic_link_review` open/pending ≈ 9,952.

- **Verdict (later same-day rerun — use for current automation):** **PASS WITH WARNINGS** — 25 PASS / 2 WARN / 0 FAIL; **`broken_fhir_refs=0`**. Artifact: [`../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`](../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md).

Historical artifact (unchanged body): [`validation_report.md`](validation_report.md).

## 4) Traceability (presentation layer)

`main.master_fact_long_verified_v1` includes (among others): `research_id`, `source_domain`, `source_object_id`, `extraction_run_id`, `reviewer_status`, `release_tag`. (`note_row_id` not asserted here; lineage uses `source_object_id` per contract.)

## 5) Branch decision (plan §6)

- **Not branch C** — do not run `126_final_master_release.py` for publication until gates clear.
- Dual blockers documented under:
  - [`../20260407_publication_blocker_assessment/README.md`](../20260407_publication_blocker_assessment/README.md) (governance / synthetic MRQ)
  - [`../20260407_lab_blocker_assessment/README.md`](../20260407_lab_blocker_assessment/README.md) (non-Tg lab coverage)

No raw clinical note text stored in this folder.
