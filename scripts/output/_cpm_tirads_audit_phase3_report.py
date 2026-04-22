#!/usr/bin/env python3
"""Phase 3 + 4: pull the classification table from manuscript_workspace and emit
the final markdown report (CSV + ranked hitlists)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

OUT = Path(__file__).resolve().parent
con = MotherDuckClient(
    MotherDuckConfig(database="thyroid_canonical_publication_v1_0")
).connect_rw()

df = con.execute("""
    SELECT *
    FROM manuscript_workspace.cpm_tirads_audit_classification_v1
    ORDER BY recommendation, column_name
""").df()

# Save full CSV
df.to_csv(OUT / "_cpm_tirads_audit_classification_v1.csv", index=False)

# Markdown
buckets = ["DROP", "RENAME_TO_V2", "PRESERVE_DIFFERENT_SEMANTIC", "INVESTIGATE"]
counts = {b: int((df["recommendation"] == b).sum()) for b in buckets}

lines: list[str] = []
lines.append("# CPM TIRADS audit — Part A classification report")
lines.append("")
lines.append("**Database:** `thyroid_canonical_publication_v1_0`")
lines.append("**Table:** `main.canonical_patient_master` (10,871 rows)")
lines.append("**Run mode:** read-only audit (no schema mutations)")
lines.append("")
lines.append("## Headline findings")
lines.append("")
lines.append(
    "1. **No paired (legacy, v2) column on CPM is more than 84% in agreement** under "
    "type-coerced equality. The closest match is `worst_tirads_category` ↔ "
    "`tirads_v2_worst_category` at 83.7% (2,358/2,817 rows agree)."
)
lines.append(
    "2. **Several \"obvious pairs\" disagree on >70% of overlapping rows**. The legacy "
    "and v2 derivations are not interchangeable; they encode different aggregation "
    "logic, different upstream sources, or different vocabularies."
)
lines.append(
    "3. **`_v12` columns are still actively read by 8 cohort views in `manuscript_workspace`** "
    "(see `cohort_descriptive_full_cohort_v1`, `cohort_m011_*`, `cohort_m025_*`, "
    "`cohort_m045_*`, `cohort_m075_*`, `cohort_m076_*`). No `_v12` column can be dropped "
    "before those views are migrated."
)
lines.append(
    "4. **Genuine semantic mismatches** confirmed by column comments and value samples: "
    "`max_tirads_ever` (BIGINT category) vs `max_tirads_ever_v2` (DOUBLE points); "
    "`pathology_vs_imaging_laterality_concordant` (BOOLEAN) vs `_v271b` (5-valued VARCHAR); "
    "`tirads_source_v12` (`excel_complete_structured`) vs `tirads_source_system_v271` "
    "(`cunc_v1_points_acr2017`) — these label different pipelines, not the same thing."
)
lines.append(
    "5. **No clean auto-DROPs**. The audit emits 0 confident DROPs. Every legacy column "
    "needs Logan's sign-off before Part B; the heuristic surfaces them as "
    "PRESERVE_DIFFERENT_SEMANTIC or INVESTIGATE."
)
lines.append("")
lines.append("## Summary counts")
lines.append("")
lines.append("| recommendation | n |")
lines.append("|---|---:|")
for b in buckets:
    lines.append(f"| {b} | {counts[b]} |")
lines.append(f"| **TOTAL** | **{len(df)}** |")
lines.append("")

lines.append("## Full classification table")
lines.append("")
lines.append(
    "| col | type | v2 counterpart | v2 type | n_leg | n_v2 | n_both | n_agree | n_disagree | n_leg_only | n_v2_only | reco | rationale |"
)
lines.append("|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|")
for _, r in df.iterrows():
    lines.append(
        f"| `{r['column_name']}` | {r['data_type']} | "
        f"{('`' + r['proposed_v2_counterpart'] + '`') if r['proposed_v2_counterpart'] else '—'} | "
        f"{r['v2_data_type'] or '—'} | "
        f"{r['n_populated_legacy'] or 0} | {r['n_populated_v2'] if r['n_populated_v2'] is not None else '—'} | "
        f"{r['n_both_populated'] if r['n_both_populated'] is not None else '—'} | "
        f"{r['n_both_agree'] if r['n_both_agree'] is not None else '—'} | "
        f"{r['n_both_disagree'] if r['n_both_disagree'] is not None else '—'} | "
        f"{r['n_legacy_only'] if r['n_legacy_only'] is not None else '—'} | "
        f"{r['n_v2_only'] if r['n_v2_only'] is not None else '—'} | "
        f"**{r['recommendation']}** | {r['rationale']} |"
    )
lines.append("")

lines.append("## Hitlists by bucket")
lines.append("")
for b in buckets:
    sub = df[df["recommendation"] == b].copy()
    lines.append(f"### {b} ({len(sub)} cols)")
    lines.append("")
    if len(sub) == 0:
        lines.append("_(none)_")
        lines.append("")
        continue
    for i, (_, r) in enumerate(sub.iterrows(), start=1):
        v2 = (
            f" → `{r['proposed_v2_counterpart']}`"
            if r["proposed_v2_counterpart"]
            else " (no v2 counterpart)"
        )
        lines.append(f"{i}. `{r['column_name']}`{v2} — {r['rationale']}")
        lines.append(f"    - writers: {r['writer_scripts']}")
        lines.append(f"    - readers: {r['reader_scripts']}")
    lines.append("")

# v2 orphans appendix
extra = json.loads((OUT / "_cpm_tirads_audit_classification.json").read_text())
lines.append("## v2 columns NOT paired with any legacy (informational)")
lines.append("")
for v in extra["v2_orphans"]:
    lines.append(f"- `{v}`")
lines.append("")

lines.append("## Sample-disagreement tables in `manuscript_workspace`")
lines.append("")
for s in extra["samples"]:
    lines.append(f"- `{s}` (10 rows)")
lines.append("")

(OUT / "_cpm_tirads_audit_FINAL_REPORT.md").write_text("\n".join(lines) + "\n")
print(f"Wrote: scripts/output/_cpm_tirads_audit_FINAL_REPORT.md")
print(f"Bucket counts: {counts}")
