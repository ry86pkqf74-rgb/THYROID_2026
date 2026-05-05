# Cursor prompt — mig_320: ThyroSeq parser routing + variant-block extraction fix

**Agent:** cursor_composer
**Estimated time:** 1.5–3 hours (parser audit + fix + reparse + rebuild)
**Cost:** $0 (re-uses persisted raw report text; no new lab work)
**Priority:** **P0** — blocks M083 publication; the current "ThyroSeq under-calls BRAF" headline is a parser artifact and would be a retraction-class error if shipped
**Closes:** new `CF-M083-PARSER-BUG`

## Problem (Cowork-discovered)

The 99-patient ThyroSeq false-negative pattern in `cohort_m083_braf_dual_platform_discordance_v1` is **not a biological finding**. It's a parser pipeline gap. Among the 99 affected ThyroSeq records:

| parser | parse_status | gene_mutations_status | n | n with braf_flag=true |
|---|---|---|---|---|
| `thyroseq` | `no_detailed_block` | Positive | 69 | 0 |
| **`afirma` (wrong!)** | `ok` | Positive | 25 | 0 |
| `afirma` | `partial` | Negative | 3 | 0 |
| `afirma` | `partial` | Positive | 2 | 0 |

**Two distinct defects:**

1. **Parser routing bug (n=30):** records where `platform='ThyroSeq'` but `parser='afirma'`. Wrong parser was applied. The Afirma parser doesn't extract variants, so `braf_flag` defaulted to false.

2. **Variant-block skip (n=69):** records where `parser='thyroseq'` ran and successfully captured `gene_mutations_status='Positive'`, but the variant-detail block was never extracted, so `braf_flag` and `braf_variant` defaulted to false/NULL.

Cross-check: `readonly_share.molecular_variant_long` has **zero BRAF records** for any of the 99 affected patients. The variant-long pipeline never received variant rows from the ThyroSeq parser for these reports.

The raw report text is preserved in `canonical_molecular_genetics_v2.report_text_ref` → `enrichment.pathology_raw` (1,169-bytes-ish per patient). Re-parsing is local-only and free.

See `studies/m083_braf_discordance/CRITICAL_PARSER_BUG_FINDING_20260505.md` for full audit.

## Recipe

### Step 1 — Diagnose the routing defect

```bash
cd /Users/loganglosser/THYROID_2026
grep -rn "thyroseq.*parse\|afirma_parser\|select_parser\|parser =" \
  scripts/ pipelines/ 2>/dev/null | grep -iE "parser|route" | head -40
```

Locate the script that decides which parser to invoke per record. Likely candidates:
- `scripts/molecular_ingestion_*.py`
- `pipelines/canonical_molecular_genetics_*.py`

Find the if/elif branch that selects parser based on `platform`. Add an assertion that `parser==platform` for every record that gets a non-error parse. Failing that assertion should hard-stop the pipeline (not log-and-continue).

### Step 2 — Fix the variant-block extractor

The ThyroSeq report format (per the synthetic test file `studies/20260407_molecular_live_activation/inputs/thyroseq_governed_dev_7508.xlsx`) uses lines like:

```
Thyroseq Mutation: BRAF V600E (positive, AF 12%)
```

Or in real reports the variant block is typically:

```
Detected Mutations:
  BRAF V600E (positive, AF 12%)
  TERT C228T (positive, AF 8%)
```

The current parser hits `parse_status='no_detailed_block'` when it doesn't find its expected block delimiter. Add fallback regex extraction:

```python
import re

# Conservative extractor: scan the entire report text for known gene-variant patterns
GENE_PATTERN = re.compile(
    r"(?P<gene>BRAF|TERT|RAS|HRAS|KRAS|NRAS|RET|NTRK[123]|EIF1AX|TP53|PAX8)"
    r"\s+(?P<protein>[A-Z]\d+[A-Z]|V600E|c\.\d+[A-Z]>[A-Z])"
    r"(?:\s*\((?P<status>positive|negative|wild[\s-]?type)"
    r"(?:,\s*AF\s*(?P<af>\d+(?:\.\d+)?)\s*%)?\))?",
    re.IGNORECASE,
)

def extract_variants_from_text(report_text: str) -> list[dict]:
    matches = GENE_PATTERN.finditer(report_text)
    return [m.groupdict() for m in matches]
```

This is a **conservative fallback** — it triggers only when the structured-block parser fails. The structured parser remains the primary; this catches the cases where the report format deviates.

### Step 3 — Reparse the affected records

```python
from utils.md_connect import connect_md_fail_closed

md = connect_md_fail_closed(REPO_ROOT / "thyroid_master.duckdb")
md.execute("USE thyroid_canonical_publication_v1_0")

# Pull all ThyroSeq records with parse_status in ('no_detailed_block', 'partial') OR with parser != platform
rows = md.execute("""
  SELECT cmg.molecular_episode_id, cmg.research_id, cmg.platform, cmg.parser,
         cmg.parse_status, cmg.report_text_ref, cmg.gene_mutations_status
  FROM main.canonical_molecular_genetics_v2 cmg
  WHERE cmg.platform = 'ThyroSeq'
    AND (cmg.parse_status IN ('no_detailed_block', 'partial')
         OR cmg.parser != 'thyroseq')
""").fetchall()

# Apply the fixed parser to each, build a v2 result table
# Update braf_flag, braf_variant, gene_mutations_variants
```

Materialize fixes in a side table (`canonical_molecular_genetics_v2_braf_repair`), do a careful diff against the live values, then `UPDATE canonical_molecular_genetics_v2` once Logan reviews.

### Step 4 — Rebuild downstream artifacts

```sql
-- Rebuild molecular_variant_long entries for the repaired records
-- (cursor: implement carefully, preserving existing non-ThyroSeq rows)

-- Rebuild cohort_m083_braf_dual_platform_discordance_v1
-- (cursor: re-apply mig_319 view DDL; row count should remain 167)

-- Refresh studies/m083_braf_discordance/discordance_characterization.csv
.venv/bin/python studies/m083_braf_discordance/m083_analysis.py
```

### Step 5 — Validation gates

```sql
-- Gate 1: routing parity restored
SELECT COUNT(*) AS n_mismatched
FROM main.canonical_molecular_genetics_v2
WHERE platform='ThyroSeq' AND parser != 'thyroseq';
-- Acceptance: 0
```

```sql
-- Gate 2: parse-status improvement on the 99 affected patients
WITH affected AS (
  SELECT research_id
  FROM manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1
  WHERE afirma_braf='positive' AND thyroseq_braf='negative' AND path_braf_status='positive'
)
SELECT
  COUNT(DISTINCT a.research_id) AS n_affected,
  COUNT(DISTINCT CASE WHEN cmg.parse_status='ok' THEN a.research_id END) AS n_now_ok,
  COUNT(DISTINCT CASE WHEN cmg.braf_flag THEN a.research_id END) AS n_now_braf_pos,
  COUNT(DISTINCT CASE WHEN cmg.braf_variant LIKE '%V600E%' THEN a.research_id END) AS n_now_v600e
FROM affected a
JOIN main.canonical_molecular_genetics_v2 cmg
  ON CAST(cmg.research_id AS VARCHAR) = a.research_id
WHERE cmg.platform='ThyroSeq';
-- Acceptance: n_now_ok ≥ 90 (most records reparsed cleanly)
-- Acceptance: n_now_braf_pos in [60, 99] — the post-fix true ThyroSeq BRAF+ count
-- The fraction n_now_braf_pos / 99 is the publication-relevant metric
```

```sql
-- Gate 3: molecular_variant_long now has BRAF rows
SELECT
  COUNT(*) AS n_braf_records,
  COUNT(DISTINCT mvl.research_id) AS n_distinct_pts
FROM readonly_share.molecular_variant_long mvl
JOIN (SELECT CAST(research_id AS INTEGER) AS rid
      FROM manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1
      WHERE afirma_braf='positive' AND thyroseq_braf='negative'
        AND path_braf_status='positive') a
  ON mvl.research_id = a.rid
WHERE LOWER(mvl.gene_symbol) = 'braf';
-- Acceptance: n_braf_records ≥ 60 (roughly matches Gate 2 result)
```

### Step 6 — Refresh the M083 cohort + audit

After the canonical fix lands:

```sql
-- Re-execute mig_319 view DDL (the recipe from CURSOR_PROMPT_MIG_319_M083_BRAF_DUAL_PLATFORM_BUILD_20260505.md)
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1 AS ...

-- Re-run discordance_characterization
.venv/bin/python studies/m083_braf_discordance/m083_analysis.py
```

Generate a fresh `discordance_2x2.csv` with post-fix counts. The new cell counts will replace the old ones (30 / 1 / 99 / 30) and become the actual M083 publication numbers.

### Step 7 — Signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT 'mig_320', CURRENT_TIMESTAMP, 'cursor_composer_mig320',
  'mig_320: ThyroSeq parser routing + variant-block extraction fix. Pre-fix: 99 affected patients had braf_flag=false despite gene_mutations_status=Positive on 96/99; 30 records had wrong parser routing (afirma instead of thyroseq); 69 records had parse_status=no_detailed_block. Post-fix: <X>/99 now have braf_flag=true; <Y>/99 have braf_variant=V600E; routing parity restored. molecular_variant_long now has <Z> BRAF records for the 99 affected patients. mig_319 cohort_m083 view rebuilt with corrected counts: TS+/Af+ <new>, TS-/Af+ <new>, etc. Closes CF-M083-PARSER-BUG; M083 publication unblocked.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id='mig_320');
```

## Out of scope

- Do NOT modify the M083 manuscript prose — the parser fix is upstream; manuscript pass is Cowork's after the cohort numbers stabilize.
- Do NOT modify Afirma parsing logic — only ThyroSeq routing + variant-block extraction.
- Do NOT touch `readonly_share.*` directly — fixes flow through `canonical_molecular_genetics_v2` and the variant-long materializer.

## When done, ping Cowork

One-line message: `mig_320 complete; n_now_braf_pos=NN/99; n_now_v600e=MM/99; cohort_m083 rebuilt`. Cowork will then:
1. Re-verify cohort_m083 acceptance gates
2. Re-write `MIG_319_VERIFICATION_AND_HEADLINE_FINDING_20260505.md` with the corrected interpretation
3. Decide whether the fixed M083 finding still merits publication, or whether the corrected ThyroSeq BRAF+ count converges with Afirma's (in which case M083 has no headline left)
