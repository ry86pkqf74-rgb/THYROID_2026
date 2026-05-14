// build_manuscript_docx.js — produce the v2 manuscript .docx for EXT2-4 Elicit expansion (2026-05-09).
// All numbers must reconcile to:
//   tables/table1_cohort_overall_and_2to4cm.csv
//   tables/table2_malignancy_by_bethesda_size_era.csv
//   tables/table2b_surgical_extent_by_bethesda_size_era.csv
//   tables/table3_v2_diagnostic_performance_actual_reported_call.csv
//   tables/table3_v2_rom_pct_descriptive_stats.csv
//   tables/table4_recurrence_by_molecular_status.csv
// and Figures 1-4 in figures/.

const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, HeadingLevel, BorderStyle, WidthType,
  ShadingType, LevelFormat, PageNumber, PageBreak, ImageRun, PageOrientation,
} = require("docx");

const OUT_PATH = process.argv[2];
const FIG_DIR = process.argv[3];
if (!OUT_PATH || !FIG_DIR) {
  console.error("Usage: node build_manuscript_docx.js <out.docx> <figures/dir>");
  process.exit(1);
}

const border = { style: BorderStyle.SINGLE, size: 4, color: "999999" };
const borders = { top: border, bottom: border, left: border, right: border };
const cellMargins = { top: 80, bottom: 80, left: 120, right: 120 };

function p(text, opts = {}) {
  return new Paragraph({
    children: [new TextRun({ text, ...opts })],
    spacing: { after: 120 },
  });
}

function bold(text) {
  return new TextRun({ text, bold: true });
}

function pmix(runs, opts = {}) {
  return new Paragraph({ children: runs, spacing: { after: 120 }, ...opts });
}

function h(level, text) {
  return new Paragraph({
    heading: level,
    children: [new TextRun({ text, bold: true })],
    spacing: { before: 240, after: 120 },
  });
}

function bullet(text) {
  return new Paragraph({
    numbering: { reference: "bullets", level: 0 },
    children: [new TextRun(text)],
    spacing: { after: 60 },
  });
}

// Build a simple table from a 2D array of strings; first row is header.
function table(rows, columnWidths) {
  return new Table({
    width: { size: 9360, type: WidthType.DXA },
    columnWidths,
    rows: rows.map((r, i) =>
      new TableRow({
        children: r.map((cell, j) =>
          new TableCell({
            borders,
            width: { size: columnWidths[j], type: WidthType.DXA },
            shading: i === 0
              ? { fill: "D5E8F0", type: ShadingType.CLEAR }
              : undefined,
            margins: cellMargins,
            children: [
              new Paragraph({
                children: [new TextRun({ text: String(cell), bold: i === 0, size: 18 })],
              }),
            ],
          })
        ),
      })
    ),
  });
}

function imageFig(filename, captionText, width = 600) {
  const data = fs.readFileSync(`${FIG_DIR}/${filename}`);
  return [
    new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [
        new ImageRun({
          type: "png",
          data,
          transformation: { width, height: width * 0.6 },
          altText: { title: filename, description: captionText, name: filename },
        }),
      ],
      spacing: { before: 120, after: 60 },
    }),
    new Paragraph({
      alignment: AlignmentType.LEFT,
      children: [new TextRun({ text: captionText, italics: true, size: 18 })],
      spacing: { after: 240 },
    }),
  ];
}

const children = [];

// ---------------------- TITLE ----------------------
children.push(new Paragraph({
  alignment: AlignmentType.CENTER,
  spacing: { after: 240 },
  children: [
    new TextRun({
      text: "Diagnostic performance of platform-reported Afirma and ThyroSeq calls in surgical Bethesda III/IV thyroid nodules: a 1999–2025 retrospective cohort, with focus on the 2–4 cm size band",
      bold: true,
      size: 28,
    }),
  ],
}));

children.push(new Paragraph({
  alignment: AlignmentType.CENTER,
  spacing: { after: 120 },
  children: [
    new TextRun({ text: "Working draft v3 — EXT2-4 Elicit expansion, post-mig_323 platform reclassification (2026-05-09)", italics: true }),
  ],
}));

children.push(new Paragraph({
  alignment: AlignmentType.CENTER,
  spacing: { after: 240 },
  children: [
    new TextRun({
      text: "Logan Glosser, MD; co-authors TBD per AUTHOR_INPUTS_REQUIRED_20260326.md. IRB/funding/COI to be inserted by senior author.",
      size: 18,
    }),
  ],
}));

// ---------------------- ABSTRACT ----------------------
children.push(h(HeadingLevel.HEADING_1, "Abstract"));

children.push(pmix([
  bold("Background. "),
  new TextRun(
    "For 2–4 cm thyroid nodules, the choice between thyroid lobectomy and total thyroidectomy depends on cytology, ultrasound features, and—when available—molecular testing. Published systematic reviews of Afirma and ThyroSeq performance in indeterminate cytology highlight (i) sparse 2–4 cm size-specific data, (ii) limited head-to-head comparisons using the actual platform-reported call (rather than derived risk tiers), and (iii) under-reported verification bias driven by molecular-negative patients who avoid surgery."
  ),
]));

children.push(pmix([
  bold("Methods. "),
  new TextRun(
    "Retrospective single-institution cohort drawn from a BigQuery canonical layer (project thyroid-canonical-pub-2026, table pub_canonical.manuscript_cohort_v1). Inclusion: first qualifying lobectomy or total thyroidectomy, surgery date 1999–2025. Diagnostic performance restricted to surgical patients with Bethesda III/IV cytology and a named platform (Afirma or ThyroSeq) in pub_canonical.canonical_molecular_genetics_v2. Reported test call: Afirma overall_result_class (suspicious or positive vs negative); ThyroSeq rom_descriptor (HIGH or INTERMEDIATE-HIGH vs LOW or INTERMEDIATE-LOW), with INTERMEDIATE-only treated as a third category and not pooled into the binary 2×2. Final histology: malignant by case-insensitive keyword classifier; NIFTP coded as benign in the strict rule and as malignant in an inclusive sensitivity analysis. Sensitivity, specificity, PPV, and NPV reported with Wilson 95% confidence intervals. Recurrence reported only when biopsy- or operative-pathology-documented (canonical_recurrence_resolved_v1.recurrence_path_proven). No PHI; research_id grain only."
  ),
]));

children.push(pmix([
  bold("Results. "),
  new TextRun(
    "8,368 surgical patients met inclusion (78.5% female; median age 52, IQR 40–63). Preoperative imaging-defined 2.0–4.0 cm subgroup: n=400 (392 in 2015+ era; 222/400 (55.5%) initial total thyroidectomy). Bethesda III/IV with named platform and final histology after platform reclassification (mig_323, 2026-05-09): n=317 evaluable in 2×2 (Afirma 91; ThyroSeq 226, plus 26 ThyroSeq INTERMEDIATE-band as a separate category; 17 ThyroSeq not-classifiable, reduced from 165 in v2 after removing mislabeled Afirma rows). Overall Bethesda III/IV diagnostic performance (strict, NIFTP=benign): Afirma sensitivity 90.4% (95% CI 79.4–95.8), specificity 20.5% (10.8–35.5), PPV 60.3% (49.2–70.4), NPV 61.5% (35.5–82.3); ThyroSeq sensitivity 69.7% (60.5–77.6), specificity 63.2% (54.2–71.4), PPV 63.9% (54.9–71.9), NPV 69.2% (59.9–77.1). In the Bethesda III/IV 2–4 cm subgroup (n=31 evaluable for ThyroSeq; n=5 for Afirma), ThyroSeq performed 86.7%/75.0%/76.5%/85.7% across sensitivity/specificity/PPV/NPV; the Afirma 2–4 cm cell remains small (n=5). ThyroSeq numeric ROM% tracked the descriptive bands (median ROM% 3% for negative call, 50% for INTERMEDIATE, 70% for positive call). Path-proven recurrence among malignant cases was 0/137 in Afirma (follow-up artifact, all post-2015), 4/161 (2.5%) in ThyroSeq, and 68/2,538 (2.7%) in patients with non-named-platform molecular evidence."
  ),
]));

children.push(pmix([
  bold("Conclusions. "),
  new TextRun(
    "After platform reclassification (mig_323: 191 ThyroSeq-mislabeled rows corrected to Afirma or Other), Afirma's GSC behaves like a high-sensitivity rule-out test (90% sensitivity, 21% specificity) and ThyroSeq's binary band call shows balanced performance (70%/63%) with a separately reported INTERMEDIATE category. In the 2–4 cm Bethesda III/IV subgroup specifically, ThyroSeq performs at 87%/75%/77%/86% across sensitivity/specificity/PPV/NPV (n=31); the Afirma 2–4 cm cell remains small (n=5). All conditional-on-surgery operating characteristics are subject to verification bias because molecular-negative patients largely avoid surgery and are unobservable; specificity and NPV are most affected. Reclassification resolved the majority of previously non-classifiable ThyroSeq calls (reduced from 165 to 17); 17 genuinely unresolvable ThyroSeq records remain (band text incomplete). Sixteen additional mislabeled records (reported_text guard) were flagged for manual review."
  ),
]));

// ---------------------- INTRODUCTION ----------------------
children.push(h(HeadingLevel.HEADING_1, "Introduction"));

children.push(p(
  "For thyroid nodules with indeterminate cytology (Bethesda categories III and IV), the choice between thyroid lobectomy and total thyroidectomy depends on cytologic risk category, ultrasound risk score, nodule size, comorbidities, and—when available—the result of a commercial molecular test. Two platforms dominate the U.S. market: Afirma (Veracyte; Genomic Sequencing Classifier with optional Xpression Atlas) and ThyroSeq (Sonic Healthcare USA; v3 multi-mutation panel with risk-of-malignancy band assignment). Both are used to refine the post-test probability of malignancy in Bethesda III/IV cytology and inform the decision to operate or to monitor."
));

children.push(p(
  "Recent systematic reviews of these platforms in indeterminate-cytology populations consistently identify three evidence gaps. First, size-specific subgroup analyses are sparse, particularly in the 2–4 cm band where the surgical decision (lobectomy vs total thyroidectomy) is most often debated and the 2015 American Thyroid Association guidelines explicitly endorse lobectomy as a reasonable choice. Second, head-to-head comparisons using the actual platform-reported call—Afirma's GSC binary Suspicious/Benign call, or ThyroSeq's reported ROM band—are scarce; most retrospective series collapse the platform call into a derived risk tier or count any positive mutation as a positive test, which conflates the binary classifier with the downstream mutation panel. Third, verification bias is rarely quantified, even though molecular-negative patients in routine practice typically avoid surgery and therefore do not contribute to the surgical reference standard."
));

children.push(p(
  "We re-analyzed our institutional thyroid surgery cohort (1999–2025; n=8,368 surgical patients on the BigQuery canonical layer) to (i) provide updated cohort-level statistics including a preoperative 2–4 cm subgroup, (ii) report Afirma vs ThyroSeq diagnostic performance using the actual platform-reported test call captured in canonical_molecular_genetics_v2 (with Wilson 95% confidence intervals), (iii) handle the ThyroSeq INTERMEDIATE band as a third category rather than pooling it with the binary call, and (iv) quantify the verification-bias problem in the surgical denominator."
));

// ---------------------- METHODS ----------------------
children.push(h(HeadingLevel.HEADING_1, "Methods"));

children.push(h(HeadingLevel.HEADING_2, "Data source and cohort"));
children.push(p(
  "Patient-level data were extracted from the institutional BigQuery canonical layer (project thyroid-canonical-pub-2026, datasets pub_canonical, pub_workspace, pub_signoff). The primary analytic table was pub_canonical.manuscript_cohort_v1 (n=10,871 patients), an integrated patient master that resolves demographics, FNA cytology (Bethesda derivation per pub_canonical.canonical_fna_events_v1), molecular testing (linked to pub_canonical.canonical_molecular_genetics_v2 and pub_canonical.specimen_genomic_assay_v1), preoperative imaging (linked to pub_canonical.canonical_us_nodule_v2 and pub_canonical.canonical_us_nodule_tirads_multisystem_v1), surgical extent, final histology, complications, and recurrence."
));
children.push(p(
  "Inclusion criteria for the analytic cohort: (i) resolved first surgery date in 1999–2025; (ii) surg_procedure_type ∈ {hemithyroidectomy, total_thyroidectomy}. After these filters, n=8,368 patients remained. Preoperative imaging-defined 2.0–4.0 cm subgroup: n=400 patients with imaging_nodule_size_cm in [2.0, 4.0]; <2 cm subgroup: n=1,636; >4 cm subgroup: n=2 (limited by the cohort's preop-size availability rather than a true absence of large nodules)."
));

children.push(h(HeadingLevel.HEADING_2, "Reported molecular call (correction from a prior derivation)"));
children.push(p(
  "An earlier version of this analysis (now superseded; see tables/superseded/) derived the molecular call from manuscript_cohort_v1.molecular_risk_tier and BRAF/RAS/TERT positivity flags. After review, that derivation was found to conflate Afirma's GSC binary call with downstream Xpression Atlas mutation findings, and to pool ThyroSeq's INTERMEDIATE band with positive. The corrected analysis presented here uses the actual platform-reported call from pub_canonical.canonical_molecular_genetics_v2:"
));
children.push(bullet("Afirma test-positive: overall_result_class IN ('suspicious','positive')."));
children.push(bullet("Afirma test-negative: overall_result_class = 'negative'."));
children.push(bullet("Afirma not classifiable: overall_result_class IS NULL or 'other' or 'non_diagnostic'."));
children.push(bullet("ThyroSeq test-positive: rom_descriptor IN ('HIGH','INTERMEDIATE-HIGH','INTERMEDIATEHIGH') OR overall_result_class = 'positive'."));
children.push(bullet("ThyroSeq test-negative: rom_descriptor IN ('LOW','INTERMEDIATE-LOW') OR overall_result_class = 'negative'."));
children.push(bullet("ThyroSeq INTERMEDIATE: rom_descriptor = 'INTERMEDIATE'. Reported as a third category and not pooled into the binary 2×2."));
children.push(bullet("ThyroSeq not classifiable: otherwise."));
children.push(p(
  "When a patient had multiple molecular tests, the latest preoperative test (test_date ≤ surgery date) was used; the most recent test was used as a fallback if no preoperative test existed. The numeric ROM% (rom_percent_point) is reported descriptively for ThyroSeq; Afirma's GSC does not emit a numeric ROM% on commercial reports in this dataset."
));

children.push(h(HeadingLevel.HEADING_2, "Histology and outcome definitions"));
children.push(p(
  "Final histology was extracted from manuscript_cohort_v1.histology_final, which is populated only for malignant categories (PTC and variants, MTC, follicular carcinoma, anaplastic, poorly differentiated, NUT carcinoma, etc.). Among surgical patients, histology_final IS NULL was treated as benign. NIFTP, NIFCP, NIFP, and NIFPT were grouped as NIFTP and treated as benign in the strict rule and as malignant in an inclusive sensitivity analysis. FTUMP and hyalinizing trabecular tumor were treated as borderline; benign adenoma cases were treated as benign. Era was binarized as pre-2015 vs 2015+, aligning with the 2015 ATA guideline release and the broader U.S. adoption of the Afirma GSC and ThyroSeq v3."
));
children.push(p(
  "Recurrence was restricted to biopsy- or operative-pathology-documented events (canonical_recurrence_resolved_v1.recurrence_path_proven = TRUE), per the principal investigator's specification. Imaging-suspicious-only and biochemical-only events were excluded from the headline recurrence metric. Survival and long-term clinical outcomes were excluded from this analysis a priori because of insufficient follow-up density in the 2015+ molecular-tested subset."
));

children.push(h(HeadingLevel.HEADING_2, "Statistical analysis"));
children.push(p(
  "Patient-level summaries are reported as count (%) for categorical variables and median [interquartile range] for continuous variables. Diagnostic performance metrics (sensitivity, specificity, positive predictive value, negative predictive value) are reported with Wilson score 95% confidence intervals. The 2×2 cells were tabulated separately for each combination of {Bethesda III, Bethesda IV, Bethesda III+IV} × {<2 cm, 2–4 cm, unknown size, all sizes} × {Strict (NIFTP=benign), Inclusive (NIFTP=malignant)}. ThyroSeq INTERMEDIATE-band patients and not-classifiable patients were tabulated as separate descriptive rows and excluded from the binary 2×2 calculation. Recurrence proportions are reported with Wilson 95% CIs. No p-values or formal hypothesis tests are reported here because all comparisons are observational and selection-biased; effect sizes and confidence intervals are reported instead. Reproducible BigQuery SQL, Python builders, and a complete data dictionary are provided in the accompanying study folder."
));

// ---------------------- RESULTS ----------------------
children.push(h(HeadingLevel.HEADING_1, "Results"));

children.push(h(HeadingLevel.HEADING_2, "Cohort characteristics (Table 1, Figure 1)"));
children.push(p(
  "The 8,368-patient surgical cohort included 6,572 (78.5%) females and 1,796 (21.5%) males, median age 52 [IQR 40–63]. Initial surgery was lobectomy in 3,809 (45.5%) and total thyroidectomy in 4,559 (54.5%). Bethesda categorization was resolved in 3,921 patients (Bethesda VI 964, Bethesda II 1,644, Bethesda IV 489, Bethesda III 444, Bethesda V 216, Bethesda I 164). Named molecular platform usage was 224 Afirma and 273 ThyroSeq; the pre-2015 era contained 9 named-platform tests vs 488 in 2015+. The preoperative imaging 2.0–4.0 cm subgroup contained 400 patients (median age 55 [42–65]; 222/400 (55.5%) initial total thyroidectomy; 232/400 (58.0%) malignant on final pathology among those with histology resolved). Cohort flow is shown in Figure 1. Full per-stratum cell counts are tabulated in Table 1."
));
children.push(...imageFig("fig_cohort_flow_bq_20260509.png",
  "Figure 1. Cohort flow on the BigQuery canonical layer (manuscript_cohort_v1; surgery 1999–2025, lobectomy or total thyroidectomy)."));

children.push(h(HeadingLevel.HEADING_2, "Malignancy and surgical extent stratified by Bethesda × size × era (Table 2, Table 2b)"));
children.push(p(
  "Malignancy rates rose monotonically with Bethesda category as expected: among surgical patients with histology resolved, Bethesda III malignancy was 224/258 cells (87% of cells in the 2015+ era when 2–4 cm B3 was 25/26 = 96% malignant, n=26; B4 was 10/10 = 100%, n=10). Bethesda VI in 2–4 cm 2015+ was 99/100 (99%) malignant. The full Bethesda × size × era × strict/inclusive contingency is provided in Table 2 with Wilson 95% CIs. Surgical extent showed the expected gradient: total-thyroidectomy rate in Bethesda VI 2–4 cm 2015+ was 89/107 (83.2%) vs Bethesda III 2–4 cm 2015+ 13/43 (30.2%) and Bethesda II 2–4 cm 2015+ 38/82 (46.3%) — consistent with the principle that cytologic risk category dominates the extent decision (Table 2b)."
));

children.push(h(HeadingLevel.HEADING_2, "Diagnostic performance — actual platform-reported call (Table 3 v2, Figure 2)"));
children.push(p(
  "Among 8,368 surgical patients, 933 had Bethesda III/IV cytology and 497 had a named molecular platform; after platform reclassification (mig_323, 2026-05-09), 317 patients had a classifiable platform-reported call AND a final histology. After patient-level deduplication (latest preoperative test) the 2×2-evaluable subset comprised 91 Afirma and 226 ThyroSeq patients, plus 26 ThyroSeq INTERMEDIATE-band patients reported separately, and 17 ThyroSeq not-classifiable (down from 165 in the pre-reclassification v2 analysis after removing mislabeled Afirma rows)."
));
children.push(p(
  "In Bethesda III/IV all sizes (Strict, NIFTP=benign): Afirma sensitivity was 90.4% (95% CI 79.4–95.8), specificity 20.5% (10.8–35.5), PPV 60.3% (49.2–70.4), NPV 61.5% (35.5–82.3); ThyroSeq sensitivity was 69.7% (60.5–77.6), specificity 63.2% (54.2–71.4), PPV 63.9% (54.9–71.9), NPV 69.2% (59.9–77.1). In the Bethesda III/IV 2–4 cm subgroup ThyroSeq sensitivity 86.7% (62.1–96.3), specificity 75.0% (50.5–89.8), PPV 76.5% (52.7–90.4), and NPV 85.7% (60.1–96.0) on n=31 evaluable patients; the corresponding Afirma cell remained small (n=5, sensitivity 75.0% [30.1–95.4]) and is reported descriptively rather than as a stable estimate. The ThyroSeq INTERMEDIATE band (n=26 across B3+B4) sat between the negative and positive bands on final-histology proportions as expected. Full Strict and Inclusive (NIFTP=malignant) tables, including B3-only and B4-only subgroup rows, appear in Table 3 v3 and Figure 2."
));
children.push(...imageFig("fig2_forest_diagnostic_performance.png",
  "Figure 2. Forest plot of sensitivity, specificity, PPV, and NPV (Wilson 95% CIs) for Afirma vs ThyroSeq in Bethesda III/IV; Strict NIFTP=benign rule. v3 numbers post-mig_323 platform reclassification. The 2–4 cm Afirma cell (n=5) is shown for completeness but is too small for stable interpretation."));

children.push(h(HeadingLevel.HEADING_2, "ThyroSeq numeric ROM% by reported call and histology (Figure 3)"));
children.push(p(
  "ThyroSeq numeric ROM% (rom_percent_point) tracked the descriptive bands without ambiguity. Negative-call median ROM was 3% [IQR 3–3] for benign histology (n with numeric ROM=58) and 3% [3–3] for malignant histology (n=21). INTERMEDIATE-call median ROM was 50% [40–50] for benign (n=11) and 50% [50–50] for malignant (n=13). Positive-call median ROM was 70% [70–70] for benign (n=35) and 70% [70–70] for malignant (n=72). The numeric ROM% in this cohort therefore validates the band assignments without contradiction (Figure 3)."
));
children.push(...imageFig("fig3_rom_pct_distribution.png",
  "Figure 3. ThyroSeq numeric ROM% distribution by reported call × histology. Box: IQR; bar: median; n shown above each box. Afirma omitted because the GSC reports a binary call only, with no numeric ROM% in this dataset."));

children.push(h(HeadingLevel.HEADING_2, "Era trends (Figure 4)"));
children.push(p(
  "Annual surgical volume rose from ~100 cases in 1999 to ~700 cases in 2019 with a partial-year drop in 2020–2022. The named-platform utilization rate rose from <1% pre-2015 to a peak of ~16% (2021), with Afirma plateauing at 6–8% and ThyroSeq at 8–10% in 2019–2022. Total-thyroidectomy rate fluctuated between 45% and 60% across the cohort with a modest mid-2010s peak at 65% (2013) and a decline back to ~50% by 2022 (Figure 4). The Bethesda III/IV rate rose from 2–8% pre-2015 to 11–17% in 2015–2022, consistent with broader cytopathology adoption of the Bethesda system over time."
));
children.push(...imageFig("fig4_era_trends.png",
  "Figure 4. Temporal trends in surgical extent and molecular platform utilization, 1999–2022. Top: total-thyroidectomy rate and Bethesda III/IV rate among surgical patients. Bottom: Afirma, ThyroSeq, and any named-platform utilization. Vertical dashed line: 2015 (ATA guideline release and broader Afirma GSC + ThyroSeq v3 adoption)."));

children.push(h(HeadingLevel.HEADING_2, "Recurrence (Table 4)"));
children.push(p(
  "Among patients with malignant histology (n=3,093), biopsy- or operative-pathology-documented recurrence was rare: 0/137 in Afirma-tested (Wilson 95% CI 0.0–2.7), 4/161 (2.5%, 1.0–6.2) in ThyroSeq-tested, 68/2,538 (2.7%, 2.1–3.4) in Other / historical / in-house molecular evidence, and 4/257 (1.6%, 0.6–4.0) in untested. The 0% Afirma path-proven recurrence reflects a follow-up artifact: Afirma testing in this cohort concentrated in 2015–2022 with median follow-up <5 years post-operation, and most early recurrence is detected on imaging and biochemical surveillance rather than confirmed by biopsy. This number should not be interpreted as a clinical recurrence comparison; long-term outcomes were excluded from this analysis a priori."
));

// ---------------------- DISCUSSION ----------------------
children.push(h(HeadingLevel.HEADING_1, "Discussion"));

children.push(p(
  "This BigQuery-canonical re-analysis of an 8,368-patient institutional thyroid surgery cohort produces three observations relevant to the systematic-review evidence gaps that motivated it. First, when diagnostic performance is computed using the actual platform-reported call rather than a derived risk tier, Afirma's GSC behaves like a high-sensitivity rule-out test (sensitivity 90.4%, specificity 20.5% in surgical Bethesda III/IV) and ThyroSeq's binary band call shows balanced performance (70%/63%) with INTERMEDIATE handled as a separate category. The earlier versions of Table 3 in this expansion (now superseded; v1 used a derived call, v2 was applied before the mig_323 platform reclassification) are preserved at tables/superseded/ and superseded_v2/ for audit. Second, the Bethesda III/IV 2–4 cm subgroup—the size band most relevant to the lobectomy-vs-total-thyroidectomy decision—has n=5 for Afirma and n=31 for ThyroSeq after platform reclassification. ThyroSeq metrics in 2–4 cm cluster at 87% sensitivity, 75% specificity, 77% PPV, and 86% NPV; the Afirma cell is too small for stable interpretation. Third, verification bias remains the dominant methodological limitation in any single-institution surgical retrospective of these platforms: a relatively small fraction of the surgical Bethesda III/IV cohort had a benign molecular call (patients with a benign call typically do not progress to surgery), which depletes the true-negative cell. Specificity and NPV are therefore conditional-on-surgery operating characteristics, not population-level test performance."
));

children.push(p(
  "Three methodological observations are worth flagging for follow-up. First, after the mig_321 parser fallback work and the mig_323 platform reclassification + Afirma rescue, ThyroSeq band coverage on the canonical layer rose from approximately 36% (pre-fix baseline) to 90.4% globally and to ~94% in the manuscript-relevant Bethesda III/IV surgical subset (VC-MOL-PARSE-001, VC-MOL-PARSE-002). Seventeen ThyroSeq records remain truly unresolvable from source signal alone (no parseable band text and no numeric ROM%), bounding the ThyroSeq precision of the v3 estimates. Second, sixteen mislabeled records with pre-existing mig_321 parser-assigned bands were flagged by the reported_text guard and not auto-reclassified; they need manual platform confirmation before they can shift arms (VC-MOL-PLATFORM-001). Third, the 2–4 cm Afirma cell remains underpowered (n=5) in this institutional cohort; multi-center pooling with reported-call—rather than risk-tier—granularity would be valuable, and is the highest-yield next external step."
));

children.push(p(
  "This expansion does not modify the previously frozen EXT2-4 v1 manuscript (studies/proposal_2to4cm_extent_molecular_20260326/manuscript_submission_v1.md) which reported a primary cohort of N=558 from a legacy DuckDB pipeline with strict nodal exclusion and a different size-resolution rule. The two cohorts are reconcilable but not identical; readers should treat the present numbers as a BigQuery-canonical-layer re-analysis with broader inclusion (lobectomy or total thyroidectomy with resolved date, no strict nodal exclusion, BQ-canonical preop imaging size resolution). On rerunning the strict EXT2-4 cohort definition against the BigQuery canonical layer the path-defined 2–4 cm sensitivity arm—which was N=0 in the legacy pipeline—now contains approximately 1,183 patients, opening a sensitivity analysis that the v1 manuscript could not perform. That analysis is deferred."
));

children.push(h(HeadingLevel.HEADING_2, "Limitations"));
children.push(p(
  "This is a single-institution retrospective cohort with the usual selection biases inherent to that design. The dominant methodological caveat is verification bias in the diagnostic-performance estimates, as discussed. A secondary caveat is that the long pre-2015 era (n=3,756 surgeries) contributes to surgical-extent and Bethesda-rate descriptions but does not contribute usefully to the diagnostic-performance estimates because named-platform utilization was <0.3% in that era. NIFTP coding is a known confound for any series spanning the 2016 reclassification; we report both the strict (NIFTP=benign) and inclusive (NIFTP=malignant) sensitivity analyses, and the headline numbers above use the strict rule consistent with current practice. We did not perform multivariable logistic regression on this BigQuery cohort in this session; the EXT2-4 v1 manuscript's regression on the legacy DuckDB N=558 cohort remains the institutional reference for adjusted associations between cytology, age, sex, molecular testing, bilateral nodule indicator, TIRADS, and initial total thyroidectomy. Long-term outcomes including survival and structural-recurrence-free survival are deferred."
));

children.push(h(HeadingLevel.HEADING_2, "Conclusions"));
children.push(p(
  "Using the actual platform-reported call rather than a derived risk tier, and after platform reclassification at the canonical layer, Afirma's GSC and ThyroSeq's binary band call show distinct operating characteristics in surgical Bethesda III/IV cytology that align with each platform's design (Afirma: rule-out, sensitivity 90.4%, specificity 20.5%; ThyroSeq: balanced, sensitivity 69.7%, specificity 63.2%, with INTERMEDIATE as a clinically distinct third bucket). In the Bethesda III/IV 2–4 cm subgroup specifically, ThyroSeq metrics cluster at 87% sensitivity, 75% specificity, 77% PPV, and 86% NPV (n=31 evaluable). All conditional-on-surgery estimates in this dataset are subject to verification bias and a residual parser-completeness gap on 17 ThyroSeq records with no parseable band text. Multi-center pooling using the actual platform-reported call—rather than derived risk tiers—remains the highest-yield next step for size-stratified diagnostic-performance estimation."
));

// ---------------------- TABLES (compact data tables in body) ----------------------
children.push(h(HeadingLevel.HEADING_1, "Tables"));

// Table 1
children.push(h(HeadingLevel.HEADING_2, "Table 1. Cohort characteristics (selected rows; full version in CSV)."));
children.push(table([
  ["Stratum", "N", "Female n (%)", "Age median [IQR]", "Total thyroid n (%)", "Lobectomy n (%)", "Bethesda III–IV", "Named platform", "Malig path n (%)"],
  ["Overall", "8,368", "6,572 (78.5)", "52 [40–63]", "4,559 (54.5)", "3,809 (45.5)", "933", "497", "3,093 (37.0)"],
  ["Initial lobectomy", "3,809", "3,022 (79.3)", "53 [41–64]", "0 (0.0)", "3,809 (100.0)", "610", "261", "996 (26.1)"],
  ["Initial total thyroidectomy", "4,559", "3,550 (77.9)", "51 [39–62]", "4,559 (100.0)", "0 (0.0)", "323", "236", "2,097 (46.0)"],
  ["Preop nodule <2 cm", "1,636", "1,324 (80.9)", "54 [42–65]", "821 (50.2)", "815 (49.8)", "326", "208", "687 (42.0)"],
  ["Preop nodule 2–4 cm", "400", "313 (78.3)", "55 [42–65]", "222 (55.5)", "178 (44.5)", "70", "57", "232 (58.0)"],
  ["Era pre-2015", "3,756", "2,956 (78.7)", "51 [39–61]", "1,919 (51.1)", "1,837 (48.9)", "304", "9", "1,215 (32.4)"],
  ["Era 2015+", "4,612", "3,616 (78.4)", "53 [41–64]", "2,640 (57.2)", "1,972 (42.8)", "629", "488", "1,878 (40.7)"],
], [1500, 700, 900, 1000, 1100, 1100, 800, 700, 1560]));

children.push(p(""));

// Table 3 v2 — headline
children.push(h(HeadingLevel.HEADING_2, "Table 3 v2. Diagnostic performance, actual reported call, Strict (NIFTP=benign). Wilson 95% CIs."));
children.push(table([
  ["Platform / stratum", "TP", "FP", "FN", "TN", "n_2×2", "Sensitivity", "Specificity", "PPV", "NPV"],
  ["Afirma B3+B4 all sizes", "47", "31", "5", "8", "91", "90.4 [79.4–95.8]", "20.5 [10.8–35.5]", "60.3 [49.2–70.4]", "61.5 [35.5–82.3]"],
  ["Afirma B3+B4 2–4 cm", "3", "1", "1", "0", "5", "75.0 [30.1–95.4]", "0.0 [0.0–79.3]", "75.0 [30.1–95.4]", "0.0 [0.0–79.3]"],
  ["ThyroSeq B3+B4 all sizes", "85", "48", "37", "82", "226", "69.7 [60.5–77.6]", "63.2 [54.2–71.4]", "63.9 [54.9–71.9]", "69.2 [59.9–77.1]"],
  ["ThyroSeq B3+B4 2–4 cm", "13", "4", "2", "12", "31", "86.7 [62.1–96.3]", "75.0 [50.5–89.8]", "76.5 [52.7–90.4]", "85.7 [60.1–96.0]"],
  ["ThyroSeq B3+B4 <2 cm", "18", "16", "14", "21", "69", "56.2 [39.3–71.8]", "56.8 [40.9–71.3]", "52.9 [36.7–68.5]", "60.0 [43.6–74.4]"],
], [2000, 500, 500, 500, 500, 600, 1240, 1240, 1140, 1140]));

children.push(p(
  "Plus 15 ThyroSeq INTERMEDIATE-band patients (47% malignant on path) reported separately, and 165 ThyroSeq tests with non-classifiable reported call (49% malignant on path) flagged for parser-completeness verification check."
));

children.push(p(""));

// Table 4 — recurrence
children.push(h(HeadingLevel.HEADING_2, "Table 4. Path-proven recurrence by molecular group (malignant histology only)."));
children.push(table([
  ["Molecular group", "n_malignant", "Path-proven recurrence n (%) [Wilson 95% CI]"],
  ["Afirma", "137", "0/137 (0.0% [0.0–2.7])"],
  ["ThyroSeq", "161", "4/161 (2.5% [1.0–6.2])"],
  ["Other / historical / in-house", "2,538", "68/2,538 (2.7% [2.1–3.4])"],
  ["Untested", "257", "4/257 (1.6% [0.6–4.0])"],
], [2400, 2000, 4960]));

children.push(p(
  "Afirma 0% reflects a short follow-up artifact (testing concentrated 2015–2022); not a recurrence comparison."
));

// Acknowledgments / IRB / COI placeholders
children.push(h(HeadingLevel.HEADING_1, "Acknowledgments, IRB, funding, COI"));
children.push(p(
  "Per the institutional submission checklist (AUTHOR_INPUTS_REQUIRED_20260326.md) the following must be inserted by the senior author before submission: institution and department affiliations; IRB number and approval date; funding sources; conflict-of-interest disclosures; data-availability statement (the canonical BigQuery layer is not externally accessible; aggregate counts and reproducible SQL are provided in the supplementary package); and authorship list per the journal's contributorship taxonomy."
));

// References
children.push(h(HeadingLevel.HEADING_1, "References"));
children.push(p(
  "References for this v2 expansion are inherited from the EXT2-4 v1 working bibliography (studies/proposal_2to4cm_extent_molecular_20260326/references_working_20260326.md, items 1–10 reconciled 2026-03-27 via Crossref/PMC). At least the following platform-foundational citations should be added prior to submission: the Afirma GSC validation paper (Patel et al., JAMA Surg 2018), the ThyroSeq v3 multi-institutional validation (Steward et al., JAMA Oncol 2019), the 2015 ATA guidelines (Haugen et al., Thyroid 2016), the NIFTP reclassification (Nikiforov et al., JAMA Oncol 2016), and the most recent Bethesda system update (Cibas & Ali, Thyroid 2023). The verification-bias methodological context derives from Begg & Greenes (Biometrics 1983) and is the canonical reference for conditional-on-surgery diagnostic-performance interpretation."
));
children.push(p(
  "Citation reconciliation, BibTeX export, and final journal-style numbered list are deferred to the manuscript-submission step; the bibliography is canonical only when the senior-author author-fill-ins (institution, IRB, COI) are inserted."
));

// ---------------------- DOC ASSEMBLY ----------------------
const doc = new Document({
  styles: {
    default: { document: { run: { font: "Calibri", size: 22 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 30, bold: true, font: "Calibri" },
        paragraph: { spacing: { before: 280, after: 140 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 26, bold: true, font: "Calibri" },
        paragraph: { spacing: { before: 200, after: 120 }, outlineLevel: 1 } },
    ],
  },
  numbering: {
    config: [
      { reference: "bullets",
        levels: [{ level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
    ],
  },
  sections: [{
    properties: {
      page: {
        size: { width: 12240, height: 15840 },
        margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 },
      },
    },
    headers: {
      default: new Header({
        children: [new Paragraph({
          alignment: AlignmentType.RIGHT,
          children: [new TextRun({
            text: "EXT2-4 Elicit-expansion v2 — working draft 2026-05-09",
            italics: true, size: 16,
          })],
        })],
      }),
    },
    footers: {
      default: new Footer({
        children: [new Paragraph({
          alignment: AlignmentType.CENTER,
          children: [
            new TextRun({ text: "Page ", size: 16 }),
            new TextRun({ children: [PageNumber.CURRENT], size: 16 }),
          ],
        })],
      }),
    },
    children,
  }],
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync(OUT_PATH, buf);
  console.log(`Wrote ${OUT_PATH}`);
});
