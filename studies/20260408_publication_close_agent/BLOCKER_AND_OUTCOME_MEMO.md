# Publication-close attempt — outcome memo (2026-04-08)

## Pass / fail (operator)

| Gate | Result |
|------|--------|
| Local prerequisites (lab CSV + MRQ hydrate) | **Met** (see below) |
| `126` publication-close (115/118/embedded 119 evidence pack) | **FAIL** — step **115** aborted: `release_20260411` schema already exists |
| `119 --md --release-mode` (standalone) | **PASS** (33 then 35 PASS / WARN only; **0 FAIL**; Check **13** clean) |
| `124 --md` (standard audit) | **FAIL** — step **115** aborted: `release_20260408` schema already exists |
| Specimen / FHIR (Check 13) | **PASS** on both `119` runs (diagnostics clean; WARN on genomic review burden only) |

**Verdict:** **Do not claim “publication-close complete.”** Scripted orchestrators did not finish snapshot/bundle steps; live catalog was **mutated** (MRQ hydrate, labs, **103** rebuild) — see risks.

## Prerequisites (task 1–2)

1. **Final institutional non-Tg lab CSV**  
   - **Path:** `exports/incoming/final_institutional_chemistry_20260407.csv` (**present**; typical gitignored incoming dir).  
   - **127** idempotent replace for wave `final_institutional_20260407`: **989** rows prepared; post-replace **longitudinal_lab_canonical_v1** remained **77,960** (wave replaced in place per script logic).

2. **Human-reviewed MRQ hydrate folder**  
   - **Path used:** `studies/20260407_tier_policy_review_gate/` (tracked in git; **not** gitignored).  
   - `manual_review_queue.csv`: **5,622** rows; **0** blank `verification_status`; **0** blocked synthetic placeholders. Statuses are predominantly **`auto_accepted_*`** tier-policy outcomes plus **2** `confirmed_correct` — **allowed** by `docs/publication_governance_gate.md` for release-mode **5b**, but **not** the same as row-by-row human adjudication for every entity.

## Commands executed (exact)

```bash
cd "/Users/ros/THyroid 2026"
unset LOCAL_DB_PATH
export MOTHERDUCK_CUSTOM_USER_AGENT='THYROID_2026_publication_close/1.0'
export MOTHERDUCK_SESSION_HINT='publication_close_20260408_agent'

# Token: resolved via repo-root motherduck.local.toml (never logged).

# 126 dry-run
THYROID_2026/.venv/bin/python scripts/126_final_master_release.py --md --dry-run --release-date 20260411 \
  --hydrate-mrq-from studies/20260407_tier_policy_review_gate \
  --decisions-csv studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv \
  --lab-csv exports/incoming/final_institutional_chemistry_20260407.csv \
  --ingestion-wave final_institutional_20260407

# 126 real (partial) — skip local parquet regression on 103/117 per script guidance
THYROID_2026/.venv/bin/python scripts/126_final_master_release.py --md --release-date 20260411 \
  --hydrate-mrq-from studies/20260407_tier_policy_review_gate \
  --decisions-csv studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv \
  --lab-csv exports/incoming/final_institutional_chemistry_20260407.csv \
  --ingestion-wave final_institutional_20260407 \
  --skip-103 --skip-117
# → Failed at 115: release_20260411 already exists.

# 119 release-mode (post-126 partial)
THYROID_2026/.venv/bin/python scripts/119_md_formalization_validate.py --md --release-mode \
  --output-dir studies/20260408_publication_close_119

# 124 standard (partial) — skipped 116/112 to avoid a fresh gate MRQ overwrite in this session
THYROID_2026/.venv/bin/python scripts/124_md_live_release_audit.py --md --skip-stage --skip-gate \
  --output-dir studies/20260408_publication_close_124
# → Failed at 115: release_20260408 already exists.

# 119 release-mode (post-124 partial / post-103)
THYROID_2026/.venv/bin/python scripts/119_md_formalization_validate.py --md --release-mode \
  --output-dir studies/20260408_publication_close_119_post124
```

## Evidence paths (repo)

| Artifact | Path |
|----------|------|
| MotherDuck introspection + counts | `studies/20260408_publication_close_agent/motherduck_evidence_snapshot.md` |
| `119` reports | `studies/20260408_publication_close_119/validation_report.md`, `studies/20260408_publication_close_119_post124/validation_report.md` |
| `126` subprocess logs (partial) | `studies/20260411_final_master_release/logs/` (`114`, `127`, `125`, failed `115`) |
| `124` audit dir (partial) | `studies/20260408_publication_close_124/` (logs through failed `115`) |

## Material risks introduced

1. **`103` inside `124` rebuilt `main.canonical_extracted_fact_long_v2` from local v2 parquets to 55,500 rows** (was **20,188** immediately before `124`). **`master_*_verified_v1`** now reflects **55,500** facts.  
2. **Manifest lag:** `qa.release_manifest` latest row (**20260408r4**) still documents **20,188** v2 facts in `row_counts`; **live main** no longer matches until operators run a **new** `115` + manifest row with a **fresh unused** `--tag`.  
3. **`126` appended** one row to `qa.promotion_review_decisions` with batch **20260411** (total **6** rows with batch ids after backfill).  
4. **`qa.manual_review_queue` was cleared and rehydrated** from tier-policy gate (**5,622** rows).

## Recommended next operator steps

1. Pick an **unused** `release_YYYYMMDD` / `release_YYYYMMDDrx` tag and run **`115` / `118`** (or a full **`126`** with that tag) so **`qa.release_manifest`** matches live promotion intent.  
2. **Avoid** chaining **`124` after a `126 --skip-103`** unless you intend **`103`’s expanded parquet truth** on MotherDuck.  
3. If manuscript governance requires **per-row human** MRQ (not tier **`auto_accepted_*`** only), obtain the appropriate CSV + hydrate folder before sign-off language.

## Release-ready claim

**No.** Conditions not all satisfied:

- Orchestrated **publication-close** (`126` / `124` through release snapshot) **did not complete**.  
- **Governance:** MRQ is **policy-compliant** for automation (no synthetic placeholder) but **not** demonstrated as fully human-line-reviewed.  
- **Manifest / `release_*`** alignment with **55,500** fact spine **pending** new snapshot.

MotherDuck token was loaded from **`motherduck.local.toml`**; value never printed.
