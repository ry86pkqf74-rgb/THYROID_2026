// build_manuscript_docx_v4.js — EXT2-4 cohort v4 (any preop US nodule 2–4 cm, 2026-05-13).
// Numeric SSOT CSVs under tables/: table1_v4_*, table2_v4_*, table2b_v4_*, table3_v4_*, table4_v4_*.
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
      text: "Diagnostic performance of Afirma and ThyroSeq in surgical Bethesda III/IV thyroid cytology among adults with any preoperative 2.0–4.0 cm thyroid nodule on ultrasound (1999–2025 retrospective cohort)",
      bold: true,
      size: 28,
    }),
  ],
}));

children.push(new Paragraph({
  alignment: AlignmentType.CENTER,
  spacing: { after: 120 },
  children: [
    new TextRun({ text: "Working draft v4 — cohort expanded per Logan decision 2026-05-14 (broader ultrasound-based 2–4 cm inclusion)", italics: true }),
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
    "Retrospective single-institution cohort on BigQuery (`thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1`). Surgical inclusion unchanged: first qualifying lobectomy or total thyroidectomy, surgery date 1999–2025 (n=8,368); primary analytic cohort intersects surgery with ≥1 ultrasound nodule 2.0–4.0 cm (greatest axial dimension captured as canonical_us_nodule_v2.size_cm_max) on any exam dated on or before the index surgery (`n=765`; see cohort_reconciliation_v1_vs_v3.md DECISION 2026-05-14). STRICT sensitivity excludes suspicious CT/MRI cervical nodes plus Bethesda‑VI LN-directed FNA (`n=654`). Bethesda III/IV diagnostic performance restricted to cohort members with molecular testing in canonical_molecular_genetics_v2 plus resolved final histology. Reported test call mirrors v3 (`overall_result_class` / `rom_descriptor`; INTERMEDIATE band excluded from pooled binary). Wilson exact 95% CIs throughout (Wilson 1927, ref 17). Path-proven recurrence only (`canonical_recurrence_resolved_v1.recurrence_path_proven`). BH-FDR is reserved for any future hypothesis battery (Benjamini & Hochberg 1995, ref 18); not applied to single-table summaries here. No PHI — aggregate counts."
  ),
]));

children.push(pmix([
  bold("Results. "),
  new TextRun(
    "Across the broader `n=765` primary cohort (`n=654` STRICT), Bethesda III–IV totaled 155 patients (versus 933 in the full surgical denominators referenced for era trend figures). Bethesda III–IV molecular 2×2 evaluable subsets are smaller inside the size-defined cohort: Afirma `n_2×2=13` strict (Wilson midpoint sensitivity 80.0%, specificity 0.0%, PPV 72.7%); ThyroSeq `n_2×2=71` (sensitivity 88.6%, specificity 75.0%, PPV 77.5%, NPV 87.1%); ThyroSeq INTERMEDIATE-band descriptor remains a third category (13 descriptive patients with malignant fraction 61.5% strict tabulation). Stratifying purely by manuscript index nodule sizing within the cohort (purely descriptive, not cohort inclusion) yielded Afirma `n=5` and ThyroSeq `n=30` in the imaging-index 2–4 cm strata (Table 3 v4). Numeric ROM percentile bands mirrored v3 directional expectations (median 3% negatives, ~50% intermediate, ~65–70% positive). Path-proven recurrence among malignant v4 denominators summed to Afirma 0/24, ThyroSeq 1/57 (1.8% CI 0.3–9.3%), other/historical platforms 6/317 (1.9%); untested malignant stratum remains tiny (`n=5`; see CSV Table 4 v4)."
  ),
]));

children.push(pmix([
  bold("Conclusions. "),
  new TextRun(
    "Draft v4 supersedes the v3 framing that restricted the analytic cohort by patient-grain imaging_nodule_size_cm indexing alone (~n=400) and substitutes the clinically broader ultrasound rule (any pre-operative 2–4 cm lesion on any exam dated before surgery — `canonical_us_nodule_v2`, see cohort_reconciliation_v1_vs_v3.md DECISION dated 2026-05-14). Conditional-on-surgery metrics remain biased toward operative patients; specificity and especially NPV are not population-level predictive values. Larger aggregate `n_v4_total` mechanically dilutes the Bethesda III–IV denominator relative to the whole surgical corpus, narrowing Table 3 cell sizes compared with v3 but aligning more closely with peri-operative decision anatomy. Dedicated multivariable re-estimation deferred per analysis scope checklist."
  ),
]));

// ---------------------- INTRODUCTION ----------------------
children.push(h(HeadingLevel.HEADING_1, "Introduction"));

children.push(p(
  "For thyroid nodules with indeterminate cytology (Bethesda categories III and IV), the choice between thyroid lobectomy and total thyroidectomy depends on cytologic risk category, ultrasound risk score, nodule size, comorbidities, and—when available—the result of a commercial molecular test. Two platforms dominate the U.S. market: Afirma (Veracyte; Genomic Sequencing Classifier with optional Xpression Atlas) and ThyroSeq (Sonic Healthcare USA; v3 multi-mutation panel with risk-of-malignancy band assignment). Established validation studies for classifier performance underpin routine use (Afirma GSC, Patel et al. 2018, ref 11; ThyroSeq v3, Steward et al. 2019, ref 12). Bethesda nomenclature throughout follows the 2023 Bethesda update (Ali et al. / Cibas & Ali, ref 13). Both tests refine post-test malignancy probability in Bethesda III/IV cytology."
));

children.push(p(
  "Recent systematic reviews of these platforms in indeterminate-cytology populations consistently identify three evidence gaps. First, size-specific subgroup analyses are sparse, particularly in the 2–4 cm band where lobectomy-vs-total decisions are debated and historic 2015 ATA management pathways endorsed lobectomy for select disease (ATA 2015 guidelines, Haugen et al. 2016, ref 15; contemporary ATA 2025 update, ref 1). Second, head-to-head comparisons using the actual platform-reported call—Afirma's GSC binary Suspicious/Benign call, or ThyroSeq's reported ROM band—are scarce; most retrospective series collapse the platform call into derived risk tiers. Third, verification bias—diagnostic yields conditional on operative selection—is rarely confronted with primary-methodology citations despite classic treatment (Begg & Greenes 1983, ref 16)."
));

children.push(p(
  "We re-analyzed the institutional thyroidectomy cohort spanning 1999–2025 to (i) define the peri-operative 2–4 cm decision anatomy using ultrasound-documented lesion sizes prior to thyroidectomy (`n_primary=765` any qualifying nodule intersecting surgery versus `n_strict=654` sensitivity exclusion), (ii) keep Afirma vs ThyroSeq operating characteristics tethered to the actual platform reported call documented in canonical_molecular_genetics_v2, and (iii) retain verification-bias language because surgical denominators cannot recover molecular-negative pathways."
));

// ---------------------- METHODS ----------------------
children.push(h(HeadingLevel.HEADING_1, "Methods"));

children.push(h(HeadingLevel.HEADING_2, "Data source and cohort"));
children.push(p(
  "Patient-level data were extracted from the institutional BigQuery canonical layer (project thyroid-canonical-pub-2026, datasets pub_canonical, pub_workspace, pub_signoff). The primary analytic table was pub_canonical.manuscript_cohort_v1 (n=10,871 patients), an integrated patient master that resolves demographics, FNA cytology (Bethesda derivation per pub_canonical.canonical_fna_events_v1), molecular testing (linked to pub_canonical.canonical_molecular_genetics_v2 and pub_canonical.specimen_genomic_assay_v1), preoperative imaging (linked to pub_canonical.canonical_us_nodule_v2 and pub_canonical.canonical_us_nodule_tirads_multisystem_v1), surgical extent, final histology, complications, and recurrence."
));
children.push(p(
  "Inclusion for the analytic layer: resolved first thyroidectomy between 1999–2025; procedure limited to lobectomy vs total thyroidectomy (`n_surgical_qualified=8,368`). EXT2‑4 manuscript v4 further requires evidence that at least one pre-operative ultrasound lesion measured between 2.0 and 4.0 cm (canonical_us_nodule_v2.size_cm_max) on an imaging encounter dated ≤ the surgery calendar day (`cohort_primary=765`). Imaging-nodule strata inside Table 1/3 labelled “index … cm” recycle manuscript_cohort_v1.imaging_nodule_size_cm for descriptive comparison with superseded v3 only. Parallel STRICT nodal exclusions (CT/MRI suspicious cervical nodes ± Bethesda‑VI lymph-node-directed FNA) emulate the historical v1 guard (`n_strict=654`). This definition is materially broader than the previous “index lesion only” v3 heuristic and is enumerated in superseded_v3/SUPERSEDED_NOTE_v3_to_v4.md alongside DFL/MFL linkage."
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
  "Final histology was extracted from manuscript_cohort_v1.histology_final, which is populated only for malignant categories (PTC and variants, MTC, follicular carcinoma, anaplastic, poorly differentiated, NUT carcinoma, etc.). Among surgical patients, histology_final IS NULL was treated as benign. NIFTP coding follows the encapsulated follicular-variant reclassification nomenclature (Nikiforov et al. 2016, ref 14): NIFTP, NIFCP, NIFP, and NIFPT grouped as benign in the STRICT rule and malignant in inclusive sensitivity analyses. FTUMP and hyalinizing trabecular tumor were treated as borderline; benign adenoma remained benign. Era was binarized pre-2015 vs 2015+ to bracket the rollout of Bethesda-intensive cytopathology and commercial molecular uptake (historic 2015 ATA management guidelines, Haugen et al. 2016, ref 15; contemporary ATA 2025 update, Ringel et al., ref 1)."
));
children.push(p(
  "Recurrence was restricted to biopsy- or operative-pathology-documented events (canonical_recurrence_resolved_v1.recurrence_path_proven = TRUE), per the principal investigator's specification. Imaging-suspicious-only and biochemical-only events were excluded from the headline recurrence metric. Survival and long-term clinical outcomes were excluded from this analysis a priori because of insufficient follow-up density in the 2015+ molecular-tested subset."
));

children.push(h(HeadingLevel.HEADING_2, "Statistical analysis"));
children.push(p(
  "Patient-level summaries rely on frequencies and Wilson CIs documented in Tables 1–4 v4 CSVs. Diagnostic 2×2 tables separate STRICT vs INCLUSIVE NIFTP rows and retain Bethesda × (all cohort sizes | descriptive index-size strata) factorial layout even though analytic inclusion no longer subsets by Bethesda×size concurrently (the cohort definition is inherently ultrasound-sizing-based). Figures 2–3 pull directly from `table3_v4_diagnostic_performance_actual_reported_call.csv` builders; BH-FDR for any future model battery should cite Benjamini & Hochberg 1995 (ref 18) when revived."
));

// ---------------------- RESULTS ----------------------
children.push(h(HeadingLevel.HEADING_1, "Results"));

children.push(h(HeadingLevel.HEADING_2, "Cohort characteristics (Table 1, Figure 1)"));
children.push(p(
  "The ultrasound-defined primary cohort contains 765 patients (654 after STRICT nodal exclusions) with near-identical female share and age profile vs the legacy v3 subgroup but higher absolute lobectomy/total mixture because the inclusion now keys off any qualifying nodule rather than a single index lesion. Overall v4 cohort remains 99% 2015+ surgeries; Bethesda III/IV totals 155 persons within the primary layer. Named Afirma utilization 4.3% (33/765) and ThyroSeq 12.8% (98/765) with pooled named coverage 17.1% (Table 1 CSV). Figure 1 visualizes the hierarchical flow from 10,871 master records through 8,368 qualified thyroidectomies to the v4 intersection and STRICT arm."
));
children.push(...imageFig("fig1_cohort_flow_v4.png",
  "Figure 1 (v4). Cohort hierarchy on the BigQuery canonical layer (conceptual anchors 10,871 → 8,368 surgeries → ultrasound-based 2–4 cm intersections)."));

children.push(h(HeadingLevel.HEADING_2, "Malignancy and surgical extent stratified by Bethesda × size × era (Table 2, Table 2b)"));
children.push(p(
  "Bethesda strata within the ultrasound-defined analytic cohort obey expected malignancy ordering (Bethesda VI >> III/IV) as summarized in Tables 2–2b v4 (Bethesda crossed with era-only — size_band columns removed relative to superseded packs because the analytic cohort definition is inherently size-based)."
));

children.push(h(HeadingLevel.HEADING_2, "Diagnostic performance — actual platform-reported call (Table 3 v4, Figure 2)"));
children.push(p(
  "Operating characteristics summarized in Figures 2–3 and Tables 3 v4 mirror the narrower numerator described in the Abstract. Afirma's strict-cohort specificity collapsed toward 0% because benign surgical patients with Bethesda III–IV seldom carry negative Afirma calls in observable data (verification funnel). Wilson intervals should be interpreted cautiously whenever TN cells approach zero."
));
children.push(p(
  "Detailed strict vs inclusive rows (Bethesda factorial + descriptive imaging-index strata) are exported in `table3_v4_diagnostic_performance_actual_reported_call.csv`. Headline cohort-wide cells (Bethesda III+IV, analytic inclusion, strict NIFTP benign): Afirma sensitivity 80.0% CI 49.0–94.3%; specificity 0.0%; PPV 72.7%; NPV 0.0%; n=13 — ThyroSeq sensitivity 88.6% CI 74.0–95.5%; specificity 75.0%; PPV 77.5%; NPV 87.1%; n=71. Imaging-index strata only (`size_band`=2–4 cm) yield Afirma n=5, ThyroSeq n=30 (Figure 2 forest mirrors CSV parsing)."
));
children.push(...imageFig("fig2_forest_diagnostic_performance_v4.png",
  "Figure 2 (v4). Wilson interval forest for Afirma vs ThyroSeq (Bethesda III+IV, strict benign NIFTP)."));

children.push(h(HeadingLevel.HEADING_2, "ThyroSeq numeric ROM% by reported call and histology (Figure 3)"));
children.push(p(
  "Median ROM percentile summaries for ThyroSeq among v4 molecular-tested Bethesda III–IV cytology mirrored previous packs (negative benign median 3% with n_numeric ROM 24 / 27 patients; intermediates clustered at 48–51%; positives at 63–74%; see CSV + Figure 3 automation)."
));
children.push(...imageFig("fig3_rom_pct_distribution_v4.png",
  "Figure 3 (v4). ThyroSeq ROM% box summaries (median + IQR) by call × malignant vs benign pathology."));

children.push(h(HeadingLevel.HEADING_2, "Era trends (Figure 4)"));
children.push(p(
  "Annual surgical volume rose from ~100 cases in 1999 to ~700 cases in 2019 with a partial-year drop in 2020–2022. The named-platform utilization rate rose from <1% pre-2015 to a peak of ~16% (2021), with Afirma plateauing at 6–8% and ThyroSeq at 8–10% in 2019–2022. Total-thyroidectomy rate fluctuated between 45% and 60% across the cohort with a modest mid-2010s peak at 65% (2013) and a decline back to ~50% by 2022 (Figure 4). The Bethesda III/IV rate rose from 2–8% pre-2015 to 11–17% in 2015–2022, consistent with broader cytopathology adoption of the Bethesda system over time."
));
children.push(...imageFig("fig4_era_trends_v4.png",
  "Figure 4 (v4). Same-era surgical denominator trends as superseded manuscripts (population = all 1999–2022 surgical patients satisfying extent filters — not restricted to ultrasound v4 layer)."));

children.push(h(HeadingLevel.HEADING_2, "Recurrence (Table 4)"));
children.push(p(
  "Pooled malignant-histology path-proven recurrence among v4 strata: Afirma 0/24 (Wilson UB 13.8%), ThyroSeq 1/57 (1.8% CI 0.3–9.3%), other/historical molecular evidence 6/317 (1.9% CI 0.9–4.1%). Early-zero Afirma counts continue to reflect operative-selection and follow-up truncation — not equipoise-ready recurrence comparisons."
));

// ---------------------- DISCUSSION ----------------------
children.push(h(HeadingLevel.HEADING_1, "Discussion"));

children.push(p(
  "Version 4 reframes peri-operative lesion eligibility using ultrasound-documented lesion sizes in `canonical_us_nodule_v2` (exam on or before surgery) instead of only the manuscript imaging index field (`imaging_nodule_size_cm`). Bethesda III–IV molecular complete-case layers therefore shrink relative to the unrestricted surgical Bethesda pool; Afirma specificity can fall toward 0% when true-negative operative rows are effectively absent. Verification bias remains the dominant read (Begg & Greenes 1983, ref 16; platform priors refs 11–12)."
));

children.push(p(
  "Transparency snapshot (rounded; full audit in superseded_v3/SUPERSEDED_NOTE_v3_to_v4.md). Primary descriptive n: superseded v3 ≈400 imaging-index patients vs v4 = 765 ultrasound-inclusive (STRICT sensitivity arm n = 654). Bethesda III–IV count inside the analytic layer = 155. Afirma B3+B4 strict headline 2×2 n: v3 post-guard ≈90 versus v4 = 13 (mechanically smaller molecular-histology intersection). ThyroSeq B3+B4 strict headline n: v3 ≈222 versus v4 = 71."
));

children.push(p(
  "Historical frozen EXT2–4 v1 (legacy DuckDB N≈558) is unchanged; superseded v3 deliverables now live under superseded_v3/ per never-delete policy."
));

children.push(h(HeadingLevel.HEADING_2, "Limitations"));
children.push(p(
  "This is a single-institution retrospective cohort with the usual selection biases inherent to that design. The dominant methodological caveat is verification bias in the diagnostic-performance estimates, as discussed (ref 16). A secondary caveat is that the long pre-2015 era (n=3,756 surgeries) contributes to surgical-extent and Bethesda-rate descriptions but does not contribute usefully to the diagnostic-performance estimates because named-platform utilization was <0.3% in that era. NIFTP coding is a known confound for any series spanning the 2016 reclassification (ref 14); we report STRICT (benign NIFTP) and inclusive sensitivities with STRICT headlines in text. Multivariable logistic regression on BigQuery remains deferred relative to EXT2‑4 v1 legacy DuckDB (N=558) estimates. Survival and recurrence-free longitudinal outcomes remain deferred outside path-proven counts. ADDENDUM (v4): the ultrasound-inclusive cohort rule was finalized post hoc after reconciliation documented in cohort_reconciliation_v1_vs_v3.md with co-author input; treat transparently—not preregistered prospectively."
));

children.push(h(HeadingLevel.HEADING_2, "Conclusions"));
children.push(p(
  "Using platform-reported Afirma vs ThyroSeq calls under a size-defined surgical intersection, ThyroSeq retains balanced sensitivity/specificity in the strict Bethesda III+IV complete-case layer (88.6%/75.0%) while Afirma specificity is not estimable in-sample (0% with vanishing TN rows). Descriptive ROM percentiles stratify cleanly by ThyroSeq call band even after widening structural inclusion (Figure 3). Conditional-on-surgery metrics must not be quoted as outpatient screening performance; multi-centre augmentation remains the pragmatic next iteration."
));

// ---------------------- TABLES (compact data tables in body) ----------------------
children.push(h(HeadingLevel.HEADING_1, "Tables"));

// Table 1 (v4 excerpt)
children.push(h(HeadingLevel.HEADING_2, "Table 1 (v4 excerpt). Ultrasound-defined primary vs STRICT arm (see CSV table1_v4_cohort_characteristics.csv)."));
children.push(table([
  ["Stratum", "N v4", "N strict", "Female % v4", "Total thyroid % v4"],
  ["Overall", "765", "654", "78.4", "58.6"],
  ["Initial lobectomy", "317", "291", "79.5", "0"],
  ["Initial total thyroidectomy", "448", "363", "77.7", "100"],
], [2200, 700, 900, 900, 1260]));

children.push(p(""));

// Table 3 v4 — headline (strict)
children.push(h(HeadingLevel.HEADING_2, "Table 3 v4. Diagnostic performance, actual reported call, Strict (NIFTP=benign). Wilson 95% CIs."));
children.push(table([
  ["Platform / stratum", "TP", "FP", "FN", "TN", "n_2×2", "Sensitivity", "Specificity", "PPV", "NPV"],
  ["Afirma B3+B4 all sizes", "8", "3", "2", "0", "13", "80.0 [49.0–94.3]", "0.0 [0.0–56.2]", "72.7 [43.4–90.3]", "0.0 [0.0–65.8]"],
  ["Afirma B3+B4 index strata 2–4 cm", "3", "1", "1", "0", "5", "75.0 [30.1–95.4]", "0.0 [0.0–79.3]", "75.0 [30.1–95.4]", "0.0 [0.0–79.3]"],
  ["ThyroSeq B3+B4 all sizes", "31", "9", "4", "27", "71", "88.6 [74.0–95.5]", "75.0 [58.9–86.2]", "77.5 [62.5–87.7]", "87.1 [71.1–94.9]"],
  ["ThyroSeq B3+B4 index strata 2–4 cm", "13", "4", "2", "11", "30", "86.7 [62.1–96.3]", "73.3 [48.0–89.1]", "76.5 [52.7–90.4]", "84.6 [57.8–95.7]"],
], [2000, 500, 500, 500, 500, 600, 1240, 1240, 1140, 1140]));

children.push(p(
  "ThyroSeq INTERMEDIATE-band patients (13 descriptive patients; 61.5% malignant strict tabulation) remain excluded from the pooled binary 2×2; consult Table 3 CSV for inclusive rule."
));

children.push(p(""));

// Table 4 — recurrence (v4 malignant denominators)
children.push(h(HeadingLevel.HEADING_2, "Table 4 (v4). Path-proven recurrence by molecular group (malignant histology only)."));
children.push(table([
  ["Molecular group", "n_malignant", "Path-proven recurrence n (%)"],
  ["Afirma", "24", "0/24 (0.0% UB 13.8 Wilson)"],
  ["ThyroSeq", "57", "1/57 (1.8% CI 0.3–9.3)"],
  ["Other / historical / in-house", "317", "6/317 (1.9% CI 0.9–4.1)"],
  ["Untested", "5", "0/5 (0.0% UB 43.4)"],
], [2400, 2000, 4560]));

children.push(p(
  "Recurrence cells are sparse; interpret as descriptive only (see Methods — path-proven restriction)."
));

// Acknowledgments / IRB / COI placeholders
children.push(h(HeadingLevel.HEADING_1, "Acknowledgments, IRB, funding, COI"));
children.push(p(
  "Per the institutional submission checklist (AUTHOR_INPUTS_REQUIRED_20260326.md) the following must be inserted by the senior author before submission: institution and department affiliations; IRB number and approval date; funding sources; conflict-of-interest disclosures; data-availability statement (the canonical BigQuery layer is not externally accessible; aggregate counts and reproducible SQL are provided in the supplementary package); and authorship list per the journal's contributorship taxonomy."
));

// References (canonical list 2026-05-14; aligns with ../references_working_20260514.md)
children.push(h(HeadingLevel.HEADING_1, "References"));
[
  "1. Ringel MD, Sosa JA, Baloch ZW, et al. 2025 American Thyroid Association management guidelines for adult patients with differentiated thyroid cancer. Thyroid. 2025;35(8):841–985.",
  "2. Montgomery KB, et al. Evolving variation in extent of surgery for low-risk papillary thyroid cancer in the United States. Surgery. 2023;174(4):828–835. doi:10.1016/j.surg.2023.07.001.",
  "3. Worrall BJ, et al. Lobectomy and completion thyroidectomy rates increase after the 2015 American Thyroid Association differentiated thyroid cancer guidelines update. Endocr Oncol. 2023;3(1):EO-22-0095. doi:10.1530/EO-22-0095.",
  "4. von Elm E, Altman DG, Egger M, et al.; STROBE Initiative. The Strengthening the Reporting of Observational Studies in Epidemiology (STROBE) statement: guidelines for reporting observational studies. Ann Intern Med. 2007;147(8):573–577.",
  "5. Dhir M, et al. Correct extent of thyroidectomy is poorly predicted preoperatively by the guidelines of the American Thyroid Association for low and intermediate risk thyroid cancers. Surgery. 2018;163(1):81–87.",
  "6. Wang X, et al. Risk factors influencing surgical decision-making for low-risk DTC patients with tumor diameter 1–4 cm. World J Surg Oncol. 2020;18(1):310.",
  "7. Kiss A, et al. Comparison of surgical strategies in the treatment of low-risk differentiated thyroid cancer. BMC Endocr Disord. 2023 Jan 26;23(1):23.",
  "8. Sutton W, et al. Impact of the 2015 ATA guidelines on treatment in older adults with low-risk, differentiated thyroid cancer. Am J Surg. 2022 Jul;224(1 Pt B):412–417.",
  "9. Loderer T, et al. Malignancy risk in Bethesda class IV thyroid nodules in an iodine deficient region. Gland Surg. 2023 Jul;12(7):884–893.",
  "10. Hao Q, et al. Hemithyroidectomy versus total thyroidectomy for differentiated thyroid cancer: systematic review and meta-analysis. Gland Surg. 2025 Nov;14(11):2271–2287.",
  "11. Patel KN, et al. Performance of a Genomic Sequencing Classifier for the Preoperative Diagnosis of Cytologically Indeterminate Thyroid Nodules. JAMA Surg. 2018 Sep 1;153(9):817–824.",
  "12. Steward DL, et al. Performance of a Multigene Genomic Classifier in Thyroid Nodules With Indeterminate Cytology: A Prospective Blinded Multicenter Study. JAMA Oncol. 2019 Feb 1;5(2):204–212.",
  "13. Ali SZ, Baloch ZW, et al. The 2023 Bethesda System for Reporting Thyroid Cytopathology. Thyroid. 2023 Sep;33(9):1039–1044.",
  "14. Nikiforov YE, et al. Nomenclature Revision for Encapsulated Follicular Variant of Papillary Thyroid Carcinoma (NIFTP). JAMA Oncol. 2016 Aug 1;2(8):1023–1029.",
  "15. Haugen BR, et al. 2015 ATA Management Guidelines for Adult Patients with Thyroid Nodules and Differentiated Thyroid Cancer. Thyroid. 2016 Jan;26(1):1–133.",
  "16. Begg CB, Greenes RA. Assessment of diagnostic tests when disease verification is subject to selection bias. Biometrics. 1983 Mar;39(1):207–215.",
  "17. Wilson EB. Probable inference, the law of succession, and statistical inference. J Am Stat Assoc. 1927;22(158):209–212.",
  "18. Benjamini Y, Hochberg Y. Controlling the False Discovery Rate: A Practical and Powerful Approach to Multiple Testing. J R Stat Soc Ser B Methodol. 1995;57(1):289–300.",
].forEach(txt => children.push(new Paragraph({
  numbering: undefined,
  children: [new TextRun({ text: txt, size: 18 })],
  spacing: { after: 80 },
  indent: { left: 720, hanging: 360 },
})));

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
            text: "EXT2-4 Elicit-expansion v4 — working draft (BigQuery ultrasound cohort rebuild 2026-05-13)",
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
