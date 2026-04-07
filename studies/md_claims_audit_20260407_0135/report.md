# MotherDuck live audit vs README claims

**Audit time (local):** 2026-04-07 (folder stamp `20260407_0135`)  
**Connection:** `utils.md_connect.connect_md_or_file(Path("thyroid_master.duckdb"), md=True, fail_closed=True)` — env-token auth only (no token values logged here; see `connection_log.txt` for attach diagnostics).  
**Scope:** Read-only `SELECT` / `COPY ... TO` CSV extracts only. No DDL/DML on MotherDuck.

## Live measurements (summary)

| Check | Live result |
|--------|-------------|
| `current_database()` | `Thyroid 2026` |
| Distinct `release_%` schemas | `release_20260406`, `release_20260407`, `release_20260407_final`, `release_20260407_final2`, `release_20260408`, `release_20260409` (6) |
| `qa.release_manifest` latest `release_tag` | `20260409` (`created_at` 2026-04-07 02:05:07 UTC) |
| `qa.manual_review_queue` | **5,622** rows; **0** with `verification_status IS NULL`; **5,622** reviewed |
| `v2_stage.load_inventory` | **180** rows; **0** with `NOT COALESCE(row_match, FALSE)` |
| Object counts (`information_schema.tables`) | `v2_stage`: 76 BASE TABLE; `main`: 171 BASE TABLE, 33 VIEW; `qa`: 14 BASE TABLE, 8 VIEW |

CSV extracts: `01_current_database.csv` … `07_table_counts_by_schema.csv` in this folder.

## README claim verdicts

Claims are taken from [`README.md`](../../README.md) status table and “Current repo status” (formalization, release-mode validator, manual review gate).

### 1. Formalized MotherDuck structure

**README:** MotherDuck structure is **Formalized** — `v2_stage` ↔ `main` parity for **23** promoted `canonical_output` domains; multiple `release_*` snapshots; latest tag in checked-in validation **20260409**.

| Sub-claim | Verdict | Evidence |
|-----------|---------|----------|
| Multiple `release_*` snapshots & governance schemas (`v2_stage`, `main`, `qa`) present | **PASS** | `02_all_schemas.csv`, `03_release_schemas.csv`, `07_table_counts_by_schema.csv` |
| Latest release tag **20260409** consistent with README / checked-in validation | **PASS** | `04_release_manifest_latest.csv` — top row `release_tag=20260409` |
| `v2_stage.load_inventory` healthy (no row-count mismatches) | **PASS** | `06_load_inventory_totals.csv` — `mismatch_count=0` |
| Row parity for **23** promoted v2 domains (`v2_stage` ↔ `main`) | **PARTIAL** | Not re-run in this audit; inventory flag `row_match` is all true, which supports loader health but does not replace per-domain parity checks (see `119_md_formalization_validate.py`) |

**Overall:** **PARTIAL** — snapshot/manifest/latest-tag and inventory mismatch checks **PASS**; explicit **23-domain parity** not reproduced here.

---

### 2. Release snapshot presence (`release_*`, `qa.release_manifest`)

**README / validator:** Immutable `release_YYYYMMDD` snapshots and `qa.release_manifest` are part of the formalization path and release-mode checks.

| Sub-claim | Verdict | Evidence |
|-----------|---------|----------|
| At least one `release_%` schema | **PASS** | Six distinct release schemas (see summary) |
| `qa.release_manifest` populated with recent rows | **PASS** | `04_release_manifest_latest.csv` — multiple tags through `20260409` |

**Overall:** **PASS**

---

### 3. Pending manual review queue (`qa.manual_review_queue`)

**README:** `--release-mode` fails if any queue row has **NULL** `verification_status` (pending human review). The status table points readers to the validation report for counts “at last run.”

| Sub-claim | Verdict | Evidence |
|-----------|---------|----------|
| **Pending** count (`verification_status IS NULL`) = **0** | **PASS** | `05_manual_review_queue_counts.csv` — `pending_null_status=0` |
| Queue **total** still matches checked-in validation artifact (16,866 total on 2026-04-07) | **PARTIAL** | Live **5,622** rows — README does not fix a total, but the cited validation report is no longer representative of live queue **volume** |

**Overall:** **PASS** for the README’s **pending-count / release-gate** claim; **PARTIAL** only if you treat the linked validation artifact as implying a frozen **total** queue size.

---

## Consolidated summary (three README themes)

| README claim | Verdict |
|--------------|---------|
| Formalized MotherDuck structure (`v2_stage`/`main`/`qa`, 23-domain story, snapshots, latest tag) | **PARTIAL** |
| Release snapshot presence (`release_*`, `qa.release_manifest`) | **PASS** |
| Pending manual-review queue (must be zero pending for release gate) | **PASS** |

## Artifacts

- `run_audit.py` — reproducible read-only runner (re-execute from repo root).
- `connection_log.txt` — MotherDuck attach lines (no secrets).
- `01_current_database.csv` … `07_table_counts_by_schema.csv` — query exports.
