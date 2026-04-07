# Lineage completeness — `master_source_lineage_v1`

Source: live MotherDuck after lab refresh and presentation-layer rebuild.

| Metric | Count | Notes |
|--------|------:|-------|
| Total rows | 123,577 | One per fact in presentation layer |
| `extraction_run_id` null | **0** | Contract check 10 PASS — all facts resolve to a run |
| `source_object_id` blank | **0** | Note-row identifiers present |
| `reviewer_status` null | **104,606** | Expected for most facts: queue join is **per (research_id, domain)** (latest queue row), not per fact |

## Reviewer coverage (distribution on `master_fact_long_verified_v1`)

| `reviewer_status` | Facts |
|-------------------|------:|
| `NULL` | 104,606 |
| `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` | 18,963 |
| `confirmed_correct` | 8 |

**Interpretation:** `NULL` reviewer status does **not** mean extraction is unprovenanced — extraction run, source domain, and `note_row_id` remain on the fact lineage. It means no human discordance-resolution row in `qa.manual_review_queue` applies to that fact’s `(research_id, fact_domain)` key. Manuscripts must not imply per-fact human verification where status is null unless a separate audit says so.

For frozen sign-off tables, use schema **`release_20260407_final2`**; each table includes explicit `release_tag`.

## `release_tag` in live views vs manifest

`main.master_*_verified_v1` views derive display `release_tag` from `qa.release_manifest` using numeric ordering (`TRY_CAST(release_tag AS BIGINT)`). Non-numeric tags sort after nulls; the **latest numeric** tag currently dominates (e.g. `20260409`). For manuscript tables, **prefer the release schema** (`release_20260407_final2.*`) where `release_tag` is pinned to `20260407_final2`.
