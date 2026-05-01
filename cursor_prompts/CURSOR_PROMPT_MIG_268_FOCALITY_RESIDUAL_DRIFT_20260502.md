# Cursor Composer Dispatch — mig_268: Residual focality drift cleanup (2 long-tail values from mig_261)

**Generated:** 2026-05-02 by Cowork.
**Lane:** mig_268 — mig_261 normalized `path_synoptics.tumor_focality` via `LOWER+TRIM+REPLACE(chr(10),'')`. 2 residual long-tail values remain ("Multifocal", "Unifocal" — the original case-uppercase values; or "unifocal*" / "unifocal␣"). Tiny scope; mechanical fix.
**Recommended agent:** **Cursor Composer** — single UPDATE.
**Estimated runtime:** 10 min
**Triggered by:** mig_261 verification post-apply (Cowork MCP probe found 2 residual rows).
**Severity:** LOW (N=2). Cleanup for completeness.

---

## §0 — First message to paste into Cursor Composer

> mig_268 dispatch. Tiny cleanup of 2 residual focality values mig_261's mechanical map missed. Probe + fix + signoff. ~5 SQL statements total.

---

## §1 — What's left

```sql
-- These 2 rows remain after mig_261:
SELECT tumor_focality, COUNT(*) AS n FROM main.path_synoptics
WHERE tumor_focality IN ('Multifocal','Unifocal','unifocal*','unifocal ','multifocal ','multifocal\n')
GROUP BY 1 ORDER BY n DESC;
-- Probe identified 2 rows total
```

The mig_261 mapping logic was `LOWER(TRIM(REPLACE(value, chr(10), '')))`. That collapsed most variants but missed:
- `unifocal*` (the asterisk wasn't stripped)
- `Multifocal`, `Unifocal` (mig_261 ran in the right column but the rows had fallback logic that preserved one variant)

This mig handles both with explicit pattern strip.

## §2 — Apply

```sql
-- 2a. Pre-snapshot (tiny)
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.path_synoptics_pre_mig268_20260502 AS
SELECT research_id, tumor_focality
FROM main.path_synoptics
WHERE tumor_focality IN ('Multifocal','Unifocal','unifocal*','unifocal ','multifocal ','multifocal\n');

-- 2b. Apply: strip asterisk + LOWER+TRIM
UPDATE main.path_synoptics
SET tumor_focality = LOWER(TRIM(REPLACE(REPLACE(tumor_focality, '*', ''), CHR(10), '')))
WHERE tumor_focality IN ('Multifocal','Unifocal','unifocal*','unifocal ','multifocal ','multifocal\n');

-- 2c. Verify
SELECT tumor_focality, COUNT(*) FROM main.path_synoptics
WHERE tumor_focality IN ('Multifocal','Unifocal','unifocal*','unifocal ','multifocal ','multifocal\n')
GROUP BY 1;
-- Expect: 0 rows

-- 2d. Confirm normalized counts increased correctly
SELECT tumor_focality, COUNT(*) AS n FROM main.path_synoptics
WHERE tumor_focality IS NOT NULL
GROUP BY 1 ORDER BY n DESC LIMIT 5;
-- Expect: unifocal ≈ 2,582-2,583 / multifocal ≈ 1,411

-- 2e. Signoff
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_268', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
  'Cleared 2 residual focality drift values from mig_261 long tail (asterisk + case + whitespace).');
```

## §3 — Carry-forwards
- CF-mig261-FOCALITY-RESIDUAL → CLOSED

## §4 — Surgical git add
```
qc_framework_v1/migrations/268_focality_residual_20260502.sql
scripts/output/mig_268_apply_log.txt
```
