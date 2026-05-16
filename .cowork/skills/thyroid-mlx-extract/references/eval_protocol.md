# Eval protocol — gold sets and regression discipline

## Building a gold set

For each task, build a gold subset before any corpus run:

1. **Sample size**: 50–200 cases. Below 50, F1 estimates are noisy; above 200, returns diminish.
2. **Stratification**:
   - 50% easy templated cases (typical lab report, typical synoptic)
   - 25% ambiguous cases (where 2 plausible interpretations exist)
   - 25% edge cases (truncated reports, mixed languages, scanned-OCR artifacts, rare findings)
3. **Annotation**: One annotator per case for low-stakes fields; two annotators with adjudication for high-stakes fields (ETE grade, cause of death, complication type, molecular risk).
4. **Format**: CSV with 4 columns (`source_pk`, `field_path`, `gold_value`, `gold_evidence_substring`).
5. **Lock**: once committed to `gold/<task>_gold.csv`, treat as a regression test. Don't edit casually.

## Scoring

`thyroid-mlx eval <task>` produces `results/<task>/eval/comparison.md` with:

- **Macro F1**: equal-weighted average of per-field F1. The headline number.
- **Micro F1**: aggregated TP/FP/FN across all fields. Sensitive to common fields.
- **Parse success rate**: fraction of cases where the model returned valid JSON matching the Pydantic schema.
- **Hallucination rate**: fraction of predicted values whose evidence substring is NOT present in the source text. CRITICAL signal — high rates mean the model is confabulating.
- **Average elapsed seconds**: throughput; informs cost-per-corpus.

## Decision rule

```
if best_model.macro_f1 >= 0.90 and best_model.hallucination_rate < 0.05:
    deploy best_model over the corpus
elif best_model.macro_f1 >= 0.80:
    try prompt tightening; re-eval
elif gold_set_size >= 1000:
    LoRA-fine-tune Llama-3-8B with the gold set
else:
    expand the gold set; revisit
```

## Regression cadence

- Every prompt edit: re-run eval, verify Macro F1 hasn't dropped >1 point.
- Every model release: re-run eval on current incumbent + new candidate, update recommendation only if F1 gain is statistically meaningful (paired t-test on per-case scores, p<0.05).
- Every quarter: re-eval all tasks against current model registry. Model leaders shift fast (DeepSeek V4 → Llama 5 → MedGemma 2 cycles).

## Anti-patterns to avoid

- **"Vibes-based" model selection.** Quoting benchmarks from a blog without running on YOUR gold set. The 11–32 point reality gap is real.
- **Editing gold to make a model look better.** Defeats the purpose.
- **Skipping the workspace step.** Direct writes to `pub_canonical` from a new run break the signoff pattern.
- **Single-model adjudication for high-stakes fields.** Cause-of-death, ETE grade, molecular risk call need two-model agreement.
