# Release / publication gap list (2026-04-07)

Items below are **gaps between automation, orchestration, and manuscript sign-off**, backed by concrete repo references (no speculation).

## 1. Release-mode MRQ does not reject “synthetic” verification labels

**Evidence:** `scripts/119_md_formalization_validate.py` counts “reviewed” rows as `WHERE verification_status IS NOT NULL` and fails release mode only when `pending > 0` (`pending = total - reviewed`). Any non-NULL string—including an automation-only or synthetic label—clears the pending bucket.

```436:460:scripts/119_md_formalization_validate.py
def check_review_queue(
    con: duckdb.DuckDBPyConnection,
    results: ValidationResult,
    strict: bool = False,
) -> None:
    """Check 5: Review queue population.

    In release mode, any pending (unreviewed) promotable rows cause FAIL.
...
        total = con.execute("SELECT COUNT(*) FROM qa.manual_review_queue").fetchone()[0]
        reviewed = con.execute(
            "SELECT COUNT(*) FROM qa.manual_review_queue "
            "WHERE verification_status IS NOT NULL"
        ).fetchone()[0]
        pending = total - reviewed

        if strict and pending > 0:
            results.add("Review queue", "FAIL",
...
```

**Why this explains the live publication block:** `README.md` states manuscript sign-off is blocked when MRQ is dominated by **synthetic** verification—an operator/governance criterion `119` does not encode.

## 2. Final-master orchestrator explicitly excludes specimen/FHIR from snapshot/bundle

**Evidence:** `scripts/126_final_master_release.py` docstring steps `115`/`118` as **manuscript analytic subset only**, not specimen/FHIR tables.

```8:18:scripts/126_final_master_release.py
This orchestrator:
  1. Ensures qa schema DDL (114) including promotion_review_decisions extensions
...
  6. Creates release_YYYYMMDD snapshot (115 --final-master) and parquet bundle (118 --final-master);
     both steps copy/export the manuscript analytic subset only — not specimen/FHIR (see
     docs/specimen_fhir_contract_review.md)
  7. Runs formalization validator in --release-mode (119)
```

**Gap:** Operators who expect final-master path to **materialize or package** `specimen_*` / `fhir_*` will not get that from `126` alone; `138` (and optionally `143`) are separate steps. Release-mode validation *does* include specimen/FHIR checks when anchor tables exist (`119` Check 13).

## 3. Molecular workflow `137` does not invoke specimen/FHIR materialization

**Evidence:** `scripts/137_md_molecular_release_workflow.py` chains `130`, `136`, `119` (QA), `124` (prod), `136` reader—no call to `138`/`143`.

```1:11:scripts/137_md_molecular_release_workflow.py
"""Production-safe molecular release workflow — MotherDuck orchestration.

Wires existing scripts in promotion order:

  130 prepromote-backup  — DuckLake-safe rollback clone (deterministic name)
  130 snapshot           — optional named snapshot (native catalog only; skipped for DUCKLAKE)
  136 writer             — CREATE SNAPSHOT OF prod for read-scaling visibility
  119 --release-mode     — formal QA validation (qa catalog)
  124 --final-release    — live prod release audit (116→…→119)
  136 reader             — REFRESH DATABASE for share-backed dashboards
```

**Gap:** Specimen/FHIR layer freshness depends on whether `124` + `119` runs occur **after** `138` has been applied on the same catalog; `137` does not enforce `138` in the chain.

## 4. Live release audit `124` pipeline vs specimen materialization

**Evidence:** `scripts/124_md_live_release_audit.py` module docstring lists steps through `119`; no step `138` or `143`.

```8:16:scripts/124_md_live_release_audit.py
Steps executed (in order):
  1. Preflight     — MD attachment, md_information_schema.databases, retention check
  2. Stage refresh — 116_md_stage_loader.py --md  →  v2_stage + load_inventory
  3. Promotion gate — 112_v2_domain_promotion_gate.py --motherduck-check
  4. Canonical + QA — 103 --md, 114 --md (hydrate), 117 --md (episode + molecular contract views)
  4b. Molecular lineage — 132_molecular_fact_lineage_views.py --execute --md (unified facts)
  5. Presentation views — 125_master_verified_views.py --md
  6. Release       — 115_release_snapshot.py --md, 118_parquet_release_bundle.py --md
  7. Validation    — 119_md_formalization_validate.py --md --release-mode
```

**Gap:** If `119 --release-mode` is expected to gate specimen/FHIR (`check_specimen_fhir_layer`), operators must run `138` / `143` **before** step 7 (or accept skips when anchor tables are absent—see `119` implementation).

## 5. Non-publication escape hatch in `126` can populate synthetic MRQ fields

**Evidence:** `--synthetic-fill-mrq-verification` copies gate CSVs and fills blank `verification_status` with a supplied status, stamping `reviewer` when applicable.

```113:120:scripts/126_final_master_release.py
    p.add_argument(
        "--synthetic-fill-mrq-verification",
        metavar="STATUS",
        default=None,
        help="NON-PUBLICATION: copy --hydrate-mrq-from into the study folder, set blank "
        "verification_status to STATUS, then hydrate from that copy. Real releases require "
        "human-reviewed CSVs without this flag.",
    )
```

Together with item (1), this makes automated **PASS** compatible with non–human-reviewed rows if every row ends up non-NULL.

## 6. Information-schema naming drift (mitigated in `124` during this audit)

**Evidence:** Live prod attach: `md_information_schema.snapshots` and `query_log` absent; `MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS` and `query_history` present *on the probed catalog*.

**Change:** `scripts/124_md_live_release_audit.py` updated preflight and `release_validation_strict.json` capture to match. Docs that still mention only `snapshots` / `query_log` should be read alongside `docs/motherduck_database_contract_v1.md` § “MD_INFORMATION_SCHEMA” and the updated script behavior.

## 7. Checked-in study snapshots can contradict “live” truth

**Evidence:** `README.md` warns that multiple same-day study folders can disagree and to use the latest committed `119 --release-mode` under `studies/20260407_live_truth_and_lineage_contract_audit/` as the “current live-audit package.”

**Gap:** Any older PASS under `studies/` remains in git and can be cited out of context unless paired with timestamps and reruns.

---

*No MotherDuck DDL/DML was executed to produce this list beyond read-only introspection for the naming probe documented in `repo_update_audit_20260407.md`.*
