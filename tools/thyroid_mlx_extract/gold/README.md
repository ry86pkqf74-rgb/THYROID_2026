# Gold subsets

Each task gets a gold CSV: `<task>_gold.csv`. Schema (4 columns):

| Column | Description |
|---|---|
| `source_pk` | Primary key of the source row (matches `runs/<task>/source.jsonl`'s `source_pk`) |
| `field_path` | Dotted path into the schema, e.g. `tumors[0].ete_grade` or `risk_call.band` |
| `gold_value` | The correct value for that field (string-coerced; nulls allowed) |
| `gold_evidence_substring` | Exact substring of the source text supporting the value (used for hallucination QC) |

## How to build a gold set

1. Run `thyroid-mlx pull <task> --limit 200` to dump a representative sample.
2. Open `runs/<task>/source.jsonl`, manually annotate each row using the Pydantic schema in `src/thyroid_mlx_extract/schemas/<task>.py` as your guide.
3. Stratify selection: easy templated cases, ambiguous cases, edge cases. Aim for ~25% edge.
4. For high-stakes fields (ETE grade, cause of death, complications), get a second annotator and resolve disagreements before locking the gold.
5. Save as `gold/<task>_gold.csv` and commit.
6. Run `thyroid-mlx eval <task> --gold gold/<task>_gold.csv` to compare candidate models.

## Locked-set discipline

Once a gold CSV is locked it becomes a regression test. **Do not edit it casually** when a model gets something wrong — fix the model or the prompt instead. Edit gold only when you discover the *gold itself* is wrong, and document the edit in the commit message.

## Templates per task

Empty template CSVs are provided as `<task>_gold_template.csv` — copy, fill in, rename without `_template`.
