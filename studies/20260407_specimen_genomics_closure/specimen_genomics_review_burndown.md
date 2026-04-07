# Specimen-adjacent genomic review — burndown and policy framing

**Catalog:** MotherDuck `Thyroid 2026` (prod), introspection 2026-04-07 UTC.  
**Queue:** `qa.specimen_genomic_link_review_v1` / rollup `qa.v_diag_specimen_review_burden_v1` (`queue_key = specimen_genomic_link_review`).

## 1. Reproduced warnings

| Signal | Value |
|--------|------:|
| Open/p-ending genomic link review rows | **9,966** |
| Specimen merge review (open/pending) | **1** |
| `main.molecular_results` rows | **0** (governed ThyroSeq/Afirma layer not loaded) |

## 2. Breakdown (open/pending genomic link review)

### Reason code (`reason_codes` — top)

| reason_codes | rows |
|--------------|-----:|
| `NO_FNA_MOLECULAR_LINK` | 9,445 |
| `NO_PREOP_SURGERY_LINK` | 456 |
| `plausible_review` | 42 |
| `MULTIPLE_SPECIMEN_AMBIGUOUS` | 16 |
| `unresolved_review` | 5 |
| `MULTIPLE_SPECIMEN_AMBIGUOUS\|PATHOLOGY_LINK_NON_STRONG` | 2 |

### Linkage tier (`linkage_confidence_tier` on rows that are ambiguous / in review path)

| tier | rows |
|------|-----:|
| `unresolved_review` | 9,908 |
| `plausible_review` | 58 |

### Assay / platform (joined to `main.specimen_genomic_assay_v1`)

| platform | rows in review |
|----------|---------------:|
| Other | 9,197 |
| ThyroSeq | 421 |
| Afirma | 348 |

### Source path (`source_table` on review queue)

| source_table | rows |
|--------------|-----:|
| `molecular_test_episode_v2` | 9,966 |

### Binding confidence tier (full `specimen_genomic_assay_v1` — not only queue)

| binding_confidence_tier | n |
|-------------------------|--:|
| `D_unlinked` | 9,901 |
| `A_exact_high` | 160 |
| `C_review` | 35 |
| `B_specimen_only` | 30 |

Rows with `review_flag = FALSE`: **160** (these do not require genomic link review; they are the small “clean” slice).

## 3. Is deterministic linkage possible today?

**Mostly no** on this catalog without upstream deploy fixes:

- `fna_molecular_linkage_v3` is built from FNA dates ↔ **non-null** `molecular_test_episode_v2.test_date_native` (script **49**).
- On prod, **9,280 / 10,126** episodes have **`test_date_native` NULL**.
- `main.molecular_testing` is **missing** on MotherDuck, so script **22** cannot reconstruct dates from the authoritative structured extract.
- Result: only **2** distinct `molecular_episode_id` values participate in FNA–molecular candidates; **9,445** review rows are **`NO_FNA_MOLECULAR_LINK`** — consistent with a **build/deploy omission**, not ambiguous biology.

Deterministic rules already in the repo (**49**, **140**) *would* attach more rows **after** `molecular_testing` is present and `molecular_test_episode_v2` is rebuilt with real dates (then re-run **49** and **140**).

## 4. Policy framing

| Verdict | Rationale |
|---------|-----------|
| **Not “normal manual review” at this scale** | ~99% of rows are a single mechanical reason (`NO_FNA_MOLECULAR_LINK`) driven by missing spine + null dates. |
| **Too large for signoff policy that treats WARN as acceptable** | Queue size blocks credible human triage until linkage spine is repaired. |
| **Primary cause: build/deploy omission** | Missing `molecular_testing`, empty `molecular_results`, and null episode dates — not irreducible ambiguity. |

**After spine repair**, remaining **`NO_PREOP_SURGERY_LINK`**, **multi-specimen**, and **pathology-tier** rows may be legitimately manual under current rules (no fuzzy auto-merge).
