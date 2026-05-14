const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, LevelFormat, TableOfContents, HeadingLevel,
  BorderStyle, WidthType, ShadingType, PageNumber, PageBreak
} = require("docx");

// ---- helpers ---------------------------------------------------------------
const CONTENT_W = 9360;
const border = { style: BorderStyle.SINGLE, size: 1, color: "BFBFBF" };
const borders = { top: border, bottom: border, left: border, right: border };
const cellMargins = { top: 80, bottom: 80, left: 120, right: 120 };
const HEADER_FILL = "1F3864";
const HEADER_TEXT = "FFFFFF";
const ALT_FILL = "EEF2F8";
const CALLOUT_FILL = "E2EFDA";

function P(text, opts = {}) {
  return new Paragraph({
    spacing: { after: opts.after == null ? 120 : opts.after, before: opts.before || 0 },
    alignment: opts.align,
    children: [new TextRun({ text, bold: opts.bold, italics: opts.italics, size: opts.size, color: opts.color })],
  });
}
function PR(runs, opts = {}) {
  return new Paragraph({
    spacing: { after: opts.after == null ? 120 : opts.after, before: opts.before || 0 },
    children: runs,
  });
}
function R(text, opts = {}) {
  return new TextRun({ text, bold: opts.bold, italics: opts.italics, color: opts.color });
}
function H1(text) { return new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun(text)] }); }
function H2(text) { return new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(text)] }); }
function bullet(text, level = 0) {
  return new Paragraph({
    numbering: { reference: "bullets", level },
    spacing: { after: 60 },
    children: Array.isArray(text) ? text : [new TextRun(text)],
  });
}
function num(text) {
  return new Paragraph({
    numbering: { reference: "numbers", level: 0 },
    spacing: { after: 60 },
    children: Array.isArray(text) ? text : [new TextRun(text)],
  });
}
function cell(content, { width, fill, headerCell } = {}) {
  const paras = (Array.isArray(content) ? content : [content]).map((c) =>
    typeof c === "string"
      ? new Paragraph({ spacing: { after: 0 }, children: [new TextRun({ text: c, bold: !!headerCell, color: headerCell ? HEADER_TEXT : undefined })] })
      : c
  );
  return new TableCell({
    borders,
    width: { size: width, type: WidthType.DXA },
    shading: fill ? { fill, type: ShadingType.CLEAR } : undefined,
    margins: cellMargins,
    children: paras,
  });
}
function table(colWidths, headerRow, dataRows) {
  const rows = [];
  rows.push(new TableRow({
    tableHeader: true,
    children: headerRow.map((t, i) => cell(t, { width: colWidths[i], fill: HEADER_FILL, headerCell: true })),
  }));
  dataRows.forEach((r, ri) => {
    rows.push(new TableRow({
      children: r.map((c, i) => cell(c, { width: colWidths[i], fill: ri % 2 ? ALT_FILL : undefined })),
    }));
  });
  return new Table({ width: { size: CONTENT_W, type: WidthType.DXA }, columnWidths: colWidths, rows });
}
function calloutBox(title, lines) {
  const kids = [new Paragraph({ spacing: { after: 60 }, children: [new TextRun({ text: title, bold: true, color: "375623" })] })];
  lines.forEach((l) => kids.push(new Paragraph({
    spacing: { after: 40 }, children: Array.isArray(l) ? l : [new TextRun({ text: l, size: 21 })],
  })));
  return new Table({
    width: { size: CONTENT_W, type: WidthType.DXA },
    columnWidths: [CONTENT_W],
    rows: [new TableRow({ children: [new TableCell({
      borders: { top: { style: BorderStyle.SINGLE, size: 1, color: "A9D08E" }, bottom: { style: BorderStyle.SINGLE, size: 1, color: "A9D08E" }, left: { style: BorderStyle.SINGLE, size: 12, color: "70AD47" }, right: { style: BorderStyle.SINGLE, size: 1, color: "A9D08E" } },
      width: { size: CONTENT_W, type: WidthType.DXA },
      shading: { fill: CALLOUT_FILL, type: ShadingType.CLEAR },
      margins: { top: 120, bottom: 120, left: 180, right: 180 },
      children: kids,
    })] })],
  });
}
function spacer() { return new Paragraph({ spacing: { after: 120 }, children: [] }); }

// ---- document --------------------------------------------------------------
const children = [];

// Title block
children.push(new Paragraph({
  spacing: { after: 60 },
  children: [new TextRun({ text: "BigQuery Studio Feature Integration Plan", bold: true, size: 44, color: HEADER_FILL })],
}));
children.push(new Paragraph({
  spacing: { after: 240 },
  children: [new TextRun({ text: "Reducing data-quality, lineage, and legacy-migration friction in the Thyroid Canonical Publication database", size: 26, color: "555555" })],
}));
children.push(new Paragraph({
  border: { bottom: { style: BorderStyle.SINGLE, size: 8, color: HEADER_FILL, space: 4 } },
  spacing: { after: 200 },
  children: [],
}));
children.push(PR([R("Project: ", { bold: true }), R("thyroid-canonical-pub-2026   "), R("Prepared for: ", { bold: true }), R("the research data team   "), R("Date: ", { bold: true }), R("May 14, 2026")], { after: 60 }));
children.push(PR([R("Status: ", { bold: true }), R("Draft for team review — first deliverables already executed (see Section 2)")], { after: 60 }));
children.push(PR([R("Scope: ", { bold: true }), R("BigQuery Studio (Data Preparation, Data Canvas, Notebooks, Visualizations, Apache Spark, Pipelines, custom Agents, Gemini) plus the broader Google Cloud analytics ecosystem")], { after: 240 }));

children.push(new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun("Contents")] }));
children.push(new TableOfContents("Table of Contents", { hyperlink: true, headingStyleRange: "1-2" }));
children.push(new Paragraph({ children: [new PageBreak()] }));

// 1. Executive summary
children.push(H1("1. Executive summary"));
children.push(P("Our BigQuery project is large, mature, and carefully governed — roughly 10,871 patients spread across nine datasets and several hundred tables, with a real sign-off registry, a query-cost log, and a snapshot-everything discipline. The problem is not that the data is bad. The problem is that almost all of our quality control happens after the fact: we discover a broken link, a disagreeing column, or a builder that did not re-run only once it has already propagated into a cohort or a manuscript number, and then we spend days backtracking."));
children.push(P("BigQuery Studio — together with the wider Google Cloud analytics stack — now ships a set of features that let us move that quality control to the front of the process and make the whole dataset legible to the team. This plan maps each feature to a specific pain point we are actually hitting, records the first deliverables that have already been built, proposes a new “global evaluation layer” of patient-level census views, evaluates the broader GCP toolset, and lays out a phased rollout."));
children.push(P("The single highest-leverage change is to stop treating our QC rules as a script we run when we remember to, and instead make them a Pipeline gate that canonical tables must pass before they are published. Everything else in this plan supports or extends that idea.", { bold: true }));

// 2. What has already been executed
children.push(H1("2. What has already been executed"));
children.push(P("This is a working plan, not just a proposal. The directly-executable piece — the global evaluation layer — has already been built in the live project, and the data issues it surfaced have been logged. The rest of this section records what is in place as of May 14, 2026."));

children.push(calloutBox("Live in the project today", [
  [R("New dataset ", {}), R("pub_eval", { bold: true }), R(" — isolated, views only, no underlying data mutated.")],
  [R("vw_patient_workup_census_v1", { bold: true }), R(" — one row per patient (10,871). Preop and postop performed-flags and intervals for ultrasound, CT, MRI, FNA, and nuclear medicine; reoperation signals; and a prior-thyroid-procedure pathology-gap review flag.")],
  [R("vw_workup_census_summary_v1", { bold: true }), R(" — long-format aggregate roll-up, built to drive a dashboard.")],
  [R("vw_nuclear_med_dated_v1", { bold: true }), R(" — nuclear-medicine scans with scan dates recovered (see below).")],
]));
children.push(spacer());

children.push(H2("2.1 Data issue found and fixed: nuclear medicine had no usable dates"));
children.push(P("The first build of the census view showed nuclear medicine as entirely absent — zero patients, preop or postop. The cause was not missing data: pub_workspace.nuclear_med_clean.scandate_parsed was NULL for all 2,220 rows because the parser step never ran, even though the raw scandate column is a clean MM/DD/YYYY string that parses for 2,218 of the 2,220 rows. A new view, vw_nuclear_med_dated_v1, parses those dates, and the census now correctly resolves nuclear medicine to 1,148 patients with scans on file — 511 with a preoperative scan (median 174 days before surgery) and 686 with a postoperative scan (median 99 days after). Two rows (a typo'd year and one blank) were routed to chart review."));

children.push(H2("2.2 Issues logged to Linear"));
children.push(P("Every issue that surfaced during this work was filed in Linear under the Thyroid Database team so nothing is lost:"));
children.push(table(
  [1300, 4400, 3660],
  ["Issue", "Title", "Disposition"],
  [
    ["THY-86", "Nuclear medicine scan dates unparsed — scandate_parsed NULL for all 2,220 rows", "High priority. Interim fix shipped in pub_eval; recommends an upstream backfill plus a QC assertion."],
    ["THY-87", "Three competing surgery-date columns on canonical_patient_master (SURG01)", "Medium. The census view had to pick first_surgery_date as its anchor; one canonical column should be chosen."],
    ["THY-88", "pub_eval global evaluation layer created — wire into a QC gate / Pipeline refresh", "Medium. Tracks promoting the views, adding QC assertions, and resolving the US/CT/MRI lineage caveats."],
  ]
));
children.push(spacer());

children.push(H2("2.3 What the census shows"));
children.push(P("Early numbers from the census, useful as a baseline for the dataset’s completeness:"));
children.push(table(
  [2400, 2200, 2380, 2380],
  ["Modality", "Preop coverage", "Median days pre-op", "Postop coverage"],
  [
    ["FNA", "46.6% (5,063)", "91", "3.1% (336)"],
    ["Ultrasound", "32.6% (3,539)", "212", "15.6% (1,699)"],
    ["CT", "23.8% (2,592)", "181", "8.6% (930)"],
    ["Nuclear medicine", "4.7% (511)", "174", "6.3% (686)"],
    ["MRI", "2.9% (319)", "339", "1.0% (113)"],
  ]
));
children.push(spacer());
children.push(P("Patient-level flags: reoperation 10.1%, completion thyroidectomy 6.1%, prior thyroid procedure documented 8.7%, and 42.8% of patients in the “sparse” preop-workup tier (no preoperative ultrasound and no preoperative FNA on record) — itself a finding worth a closer look."));

children.push(new Paragraph({ children: [new PageBreak()] }));

// 3. Where the project stands today
children.push(H1("3. Where the project stands today"));
children.push(P("A walk through the live project shows a great deal of infrastructure already in place — and a clear set of recurring failure modes that the new Studio features are well suited to close."));

children.push(H2("3.1 What is already working well"));
children.push(bullet("A layered dataset structure exists: pub_raw, pub_staging, pub_legacy_source_20260416, pub_canonical, pub_semantic, pub_views_readable, pub_workspace, pub_signoff, and pub_archive."));
children.push(bullet("A genuine governance layer exists in pub_signoff: a table sign-off registry (239 tables), a column verification registry (6,819 columns), a deprecation registry, a 111-row migration log, a master drift gate, and daily spend / expensive-query logs."));
children.push(bullet("A QC rule framework exists: 20 documented rules in qc_rules_v1, 368 logged violations, 18 run-based assertions, and daily/weekly QC summaries."));
children.push(bullet("Discipline is strong: every mutation is snapshotted into pub_archive, conventions are written down in a __conventions table, and a __readme change-log records what each build script did."));

children.push(H2("3.2 The recurring pain points"));
children.push(P("These are not hypothetical — each one is visible in the project today:"));
children.push(table(
  [2200, 4400, 2760],
  ["Pattern", "What it looks like in the project", "Cost to us"],
  [
    ["Validation is reactive", "QC rules live in pub_workspace and are run as scripts; violations are logged but do not block a table from being published or read.", "Bad values reach cohorts and manuscript numbers before anyone notices."],
    ["Competing sources of truth", "Rules flag three disagreeing surgery-date columns, two LN-positive columns that disagree, and a cross-source T/N/M disagreement table with 4,256 rows.", "Analysts must know which column to trust; different analyses silently pick differently."],
    ["Legacy-migration scarring", "~135 frozen tables in pub_legacy_source, ~175 pre-migration snapshots in pub_archive, dozens of MIG_* tables, and a __readme full of post-migration patches.", "Work done in the legacy system breaks when carried over; fixes are one-off."],
    ["Broken / opaque lineage", "Orphan-reference and unresolved-pointer audit tables exist; research_id is VARCHAR in some tables and INTEGER in others, which silently produces 0% join overlap.", "Backtracking to find why something is not linked is a frequent, manual task."],
    ["Builder gaps found late", "The __readme repeatedly notes “builder did not retrigger” and “pipeline gap — should be added to the next builder run.” The nuclear-medicine date gap in Section 2 is exactly this pattern.", "Derived tables go stale silently; nobody is told a dependency moved."],
    ["Workspace sprawl", "pub_workspace holds ~330 tables: many near-duplicate cohort versions, dry-run tables, and snapshots with no lifecycle policy.", "Hard to tell live from dead; pub_staging sits empty while pub_raw holds only two tables."],
  ]
));
children.push(spacer());
children.push(PR([
  R("Read together, the theme is clear: ", {}),
  R("we have the discipline and the audit trail, but not the automation and the visibility.", { bold: true }),
  R(" The features below are aimed squarely at that gap."),
]));

children.push(new Paragraph({ children: [new PageBreak()] }));

// 4. Feature map
children.push(H1("4. BigQuery Studio feature map"));
children.push(P("Each feature below is matched to the pain point it most directly addresses, with a concrete first use in our project."));

children.push(H2("4.1 Data Preparation"));
children.push(PR([R("Addresses: ", { bold: true }), R("data validation & quality; legacy migration.")], { after: 60 }));
children.push(P("Data Preparation profiles a table, surfaces anomalies and schema drift, and lets you build a Gemini-assisted, reusable cleaning recipe instead of a one-off SQL script."));
children.push(bullet("Build prep recipes for the known normalization problems: histology vocabulary, free-text extrathyroidal-extension strings, the VARCHAR-vs-INTEGER research_id mismatch, and exactly the kind of unparsed date column that the nuclear-medicine fix in Section 2 had to handle by hand."));
children.push(bullet("Use it as the standard intake path for pub_legacy_source_20260416 — profile each legacy table, capture the transform as a recipe, and land the result in pub_staging (currently empty, and meant to be the staging layer it was named for)."));
children.push(bullet("Recipes are versioned and re-runnable, so a legacy fix is documented once and applied consistently rather than re-discovered."));

children.push(H2("4.2 Data Canvas"));
children.push(PR([R("Addresses: ", { bold: true }), R("broken / opaque lineage; visibility & collaboration.")], { after: 60 }));
children.push(P("Data Canvas is a visual, node-based workspace for joining, exploring, and tracing data with natural-language assistance."));
children.push(bullet("Prototype new evaluation views on a canvas before committing SQL — the joins across imaging, FNA, operative, and nuclear-medicine tables behind the census view are exactly what a canvas is for."));
children.push(bullet("When two columns disagree (for example the LN-positive columns), trace each one back to its feeder tables visually, so the “which source do we trust” conversation happens over a picture."));
children.push(bullet("Use saved canvases as onboarding artifacts: a canvas of the canonical patient spine and its main feeders is worth more than a paragraph of documentation."));

children.push(H2("4.3 Notebooks (and the Notebook gallery)"));
children.push(PR([R("Addresses: ", { bold: true }), R("validation & quality; visibility & collaboration.")], { after: 60 }));
children.push(P("Notebooks combine SQL, Python, BigQuery ML, and inline charts in one shareable, re-runnable document. The Notebook gallery lets us start from standardized templates."));
children.push(bullet("Convert the ad hoc decision-log JSON files into notebooks: a data-quality report notebook that runs the QC rules, charts the violation trend, and is readable by a non-engineer."));
children.push(bullet("Create a gallery template for “new manuscript cohort” so every cohort starts with the same cohort-scoping checks and RID-cast conventions — this directly attacks the cohort_m* sprawl."));
children.push(bullet("Make notebooks the home of cohort feasibility analysis, so the reasoning behind an N is attached to the cohort, not buried in commit history."));

children.push(H2("4.4 Visualizations"));
children.push(PR([R("Addresses: ", { bold: true }), R("visibility & collaboration.")], { after: 60 }));
children.push(bullet("Stand up a QC dashboard: violation counts by rule and severity over time, drawing on qc_violations_v1 and the daily/weekly summaries."));
children.push(bullet("Publish vw_workup_census_summary_v1 as a standing completeness dashboard — it was built in long format specifically to feed this."));
children.push(bullet("Build a cohort-attrition waterfall so the team can see where the dataset is thin before designing an analysis."));

children.push(H2("4.5 Apache Spark (serverless)"));
children.push(PR([R("Addresses: ", { bold: true }), R("broken lineage; legacy migration at scale.")], { after: 60 }));
children.push(bullet("Run cross-source disagreement detection at scale — the kind of work behind the 4,256-row T/N/M disagreement table — as a repeatable job rather than a one-off script."));
children.push(bullet("Use it for fuzzy matching when repairing broken linkages (orphaned pointers, unresolved references) where exact joins fail."));
children.push(bullet("Use it for batch reconciliation of LLM-extracted entity tables against the structured canonical tables."));

children.push(H2("4.6 Pipelines"));
children.push(PR([R("Addresses: ", { bold: true }), R("all four priorities — especially “builder did not retrigger.”")], { after: 60 }));
children.push(bullet("Model the canonical build as a DAG: ingest → stage → validate (gate) → canonical → rollup → QC → publish. When an upstream table changes, every dependent rebuild is triggered automatically — the nuclear-medicine date gap is precisely the failure this prevents."));
children.push(bullet("Insert the QC rules as a blocking gate: a canonical table is not promoted until critical-severity rules pass."));
children.push(bullet("Schedule the QC dashboard refresh and the pub_eval evaluation-layer refresh so they are never stale (tracked in THY-88)."));

children.push(H2("4.7 Custom Agents and Gemini in queries"));
children.push(PR([R("Addresses: ", { bold: true }), R("visibility & collaboration; validation.")], { after: 60 }));
children.push(bullet("Build a project-aware agent seeded with the rules already written in __conventions — always CAST research_id, use canonical_patient_master for cohort-wide questions, never read pub_workspace as production."));
children.push(bullet("Let clinicians and analysts ask questions like “how many patients had a preoperative FNA but no pathology on file?” without hand-writing joins — the pub_eval census view answers exactly this class of question."));
children.push(bullet("Use Gemini in queries for day-to-day SQL assistance, with the agent as the guardrail that keeps answers consistent with our conventions."));

children.push(H2("4.8 Connections and Files explorer"));
children.push(PR([R("Addresses: ", { bold: true }), R("legacy migration; project hygiene.")], { after: 60 }));
children.push(bullet("Use Connections to bring legacy Excel and Parquet sources in through managed external tables rather than one-off load scripts, so re-ingestion is reproducible."));
children.push(bullet("Use the Files explorer to keep notebook and pipeline assets organized as the team grows, instead of tracking them by script number."));

children.push(new Paragraph({ children: [new PageBreak()] }));

// 5. Global evaluation layer
children.push(H1("5. The global evaluation layer (built)"));
children.push(P("A recurring request is a way to evaluate the dataset globally — to ask, for any slice of patients, what work-up actually happened and where the record is thin. That answer used to be scattered across imaging, FNA, operative, nuclear-medicine, and past-surgical-history tables, each with its own grain. It is now consolidated in the pub_eval dataset."));

children.push(H2("5.1 vw_patient_workup_census_v1"));
children.push(P("One row per patient (10,871, validated: no null IDs, no null anchors, no negative intervals). For each imaging / tissue modality, the same pattern: was it done before surgery, how long before, was it done after surgery, how long after. The surgery anchor is first_surgery_date, exposed as surgery_anchor_date for transparency (see THY-87)."));
children.push(table(
  [2700, 2400, 4260],
  ["Field group", "Type", "Definition / source"],
  [
    ["us_preop_performed / _first_date / _interval_days; us_postop_performed / _last_date / _interval_days", "BOOL / DATE / INT", "Ultrasound, derived from canonical_patient_master us_first_exam_date / us_last_exam_date."],
    ["ct_* and mri_* (same pattern) plus ct_n_exams / mri_n_exams", "BOOL / DATE / INT", "CT and MRI, derived from the canonical *_first_date / *_last_date columns."],
    ["fna_preop_performed / _first_date / _interval_days / _n; fna_postop_*", "BOOL / DATE / INT", "FNA, event-level from canonical_fna_events_v1 — exact preop/postop counts and first dates."],
    ["nucmed_any_on_file / nucmed_n_total / nucmed_date_resolved; nucmed_preop_* / nucmed_postop_*", "BOOL / INT / DATE", "Nuclear medicine, event-level from pub_eval.vw_nuclear_med_dated_v1 (dates recovered — see Section 2.1)."],
    ["n_preop_modalities / preop_core_workup_score / preop_workup_tier", "INT / FLOAT / STRING", "Derived completeness summary over the preoperative modalities (tier: core_complete / partial / sparse)."],
    ["n_surgeries / n_completion_thyroidectomies / any_reoperative_field / reoperation_flag", "INT / FLOAT / BOOL", "From canonical_operative_patient_rollup_v1_1."],
    ["prior_thyroidectomy_documented / prior_neck_surgery_documented / prior_thyroid_procedure_documented", "BOOL", "From canonical_psh_patient_rollup_v1 (past-surgical-history evidence flags)."],
    ["has_surgical_pathology_on_file / prior_procedure_path_gap_flag", "BOOL", "prior_procedure_path_gap_flag = prior procedure documented AND no surgical pathology on file — a chart-review trigger, not a definitive determination."],
    ["lobectomy_first_flag / completion_path_definite_flag / lobectomy_first_no_completion_path_flag", "BOOL", "From patient_completion_oed_path_linkage_v1 — completion-thyroidectomy pathology documentation gaps."],
  ]
));
children.push(spacer());

children.push(H2("5.2 vw_workup_census_summary_v1"));
children.push(P("A long-format aggregate roll-up — one row per metric — built to drive the Visualization dashboard. imaging_modality rows give preop/postop coverage and interval medians per modality; cohort_flag rows give patient-level prevalences (reoperation, prior-procedure path gap, workup tiers, and the nuclear-medicine coverage flags)."));

children.push(H2("5.3 Known caveats (tracked in THY-88)"));
children.push(bullet("US, CT, and MRI pre/post are derived from patient-level first/last date columns, so n_exams is a patient total and the postop reference is the last exam, not the first postop exam. FNA and nuclear medicine are event-level and exact. Building event-level US/CT/MRI feeds would make all five modalities symmetric."));
children.push(bullet("These are views, not materialized tables. Promotion plus a scheduled Pipeline refresh and census-specific QC assertions are the next step."));
children.push(bullet("prior_procedure_path_gap_flag should be validated against a chart-reviewed sample before it is used in any manuscript."));

children.push(new Paragraph({ children: [new PageBreak()] }));

// 6. Broader GCP analytics ecosystem
children.push(H1("6. The broader Google Cloud analytics ecosystem"));
children.push(P("BigQuery Studio is the core, but several adjacent Google Cloud services map cleanly onto our pain points. The table below rates each on fit — Recommended, Situational, or Not applicable — for this project specifically. The guiding principle is to keep the stack consolidated around BigQuery and only add a service where it closes a real gap."));

children.push(H2("6.1 Recommended"));
children.push(table(
  [2200, 7160],
  ["Service", "How we would use it"],
  [
    ["Looker / Looker Studio", "The natural front end for the work already built. Point Looker at vw_workup_census_summary_v1 and the pub_signoff QC tables to give the team a standing completeness dashboard and a QC-trend dashboard without anyone re-running a query. Looker Studio is the lightweight, no-cost starting point; Looker proper if we need governed, shared models."],
    ["Data Catalog / Dataplex (Knowledge Catalog)", "Directly targets the broken-lineage pain. A searchable catalog with table- and column-level tags and lineage means people stop guessing which table is canonical, and the orphan-reference / unresolved-pointer problem becomes visible instead of discovered by accident. This is the metadata backbone the project is missing."],
    ["Managed Airflow (Cloud Composer)", "Workflow orchestration for the ingest → stage → validate → canonical → rollup → QC → publish DAG. BigQuery Pipelines may be enough on its own; Composer is the answer if we outgrow it or need cross-service orchestration. Either way, the numbered-script chain becomes a real dependency graph."],
    ["Managed Apache Spark / Dataproc", "Serverless PySpark for the heavy reconciliation work — cross-source disagreement detection, fuzzy linkage repair, LLM-output reconciliation — that is awkward in pure SQL. Same recommendation as Section 4.5, delivered as a managed service."],
    ["Document AI", "High value for a chart-heavy dataset. Document AI can parse scanned or free-text pathology reports, operative notes, and nuclear-medicine narratives into structured fields — directly relevant to recovering the two undateable nuclear-medicine rows in THY-86 and to the broader LLM-extraction effort."],
    ["Agent Platform / Gemini Enterprise", "Where the project-aware custom agent from Section 4.7 would actually be built and deployed — an agent that knows our conventions and can answer work-up and cohort questions in plain language against the pub_eval views."],
  ]
));
children.push(spacer());

children.push(H2("6.2 Situational — adopt if a specific need appears"));
children.push(table(
  [2200, 7160],
  ["Service", "When it would make sense"],
  [
    ["Healthcare API / Healthcare data storage", "If we ever ingest EHR data directly or need a FHIR/HL7 representation of the cohort. For a research database built from periodic Excel/Parquet extracts, this is not needed today."],
    ["NotebookLM for Enterprise", "A useful adjunct for the team to query and understand its own protocols, manuscript drafts, and documentation — not part of the data pipeline, but a low-cost productivity aid."],
    ["Data Fusion / Alteryx Designer Cloud", "Visual ETL / data wrangling. These overlap with BigQuery Data Preparation, which is the lighter-weight native option we should try first; reach for these only if Data Preparation proves too limited."],
    ["Lakehouse (Iceberg on Cloud Storage)", "Could serve as the landing format for the legacy Parquet sources feeding pub_raw / pub_staging. A nice-to-have for storage hygiene, not a priority."],
    ["Datastream", "Only relevant if live EHR replication ever becomes a requirement; not applicable to the current batch-extract model."],
  ]
));
children.push(spacer());

children.push(H2("6.3 Not applicable to this project"));
children.push(P("Pub/Sub, Dataflow, Managed Service for Apache Kafka, and Confluent Cloud are real-time streaming and messaging tools. This is a static, batch research dataset assembled from periodic data drops — there is no streaming workload to justify them. Databricks and Elastic Cloud are capable platforms but would fragment a stack that BigQuery plus serverless Spark already covers. Earth Engine, Search for commerce, Talent Solution, AI Edge Portal, and the CCAI Platform are aimed at domains (geospatial, retail, recruiting, on-device ML, contact centers) unrelated to clinical thyroid research."));

children.push(new Paragraph({ children: [new PageBreak()] }));

// 7. Phased rollout
children.push(H1("7. Phased rollout"));
children.push(P("Four phases over roughly ten weeks. Phase 0 is partly done; each remaining phase produces something the team can use immediately."));

children.push(H2("Phase 0 — Foundations and quick wins (Weeks 1–2)"));
children.push(num("DONE — pub_eval global evaluation layer built; nuclear-medicine dates recovered; issues filed as THY-86, THY-87, THY-88."));
children.push(num("Build the first data-quality notebook from a Notebook gallery template: run the 20 QC rules, chart the violation trend, share it."));
children.push(num("Recreate the canonical patient spine and its main feeders as a saved Data Canvas — the first shared lineage artifact."));
children.push(num("Agree a naming and lifecycle convention for pub_workspace (what gets a TTL, what gets archived)."));

children.push(H2("Phase 1 — Validation as a gate (Weeks 3–6)"));
children.push(num("Consolidate the QC rules into one location and wrap them in a Pipeline step; add the census-specific assertions from THY-88."));
children.push(num("Make that step a blocking gate: critical-severity rules must pass before a canonical or safe-view table is published."));
children.push(num("Build Data Preparation recipes for histology vocabulary, ETE strings, the research_id type mismatch, and unparsed date columns."));
children.push(num("Publish the QC dashboard and the workup-census dashboard in Looker Studio."));

children.push(H2("Phase 2 — Lineage and metadata (Weeks 6–10)"));
children.push(num("Stand up Data Catalog / Dataplex: tag canonical vs workspace vs archive, and turn on lineage tracking."));
children.push(num("Promote the pub_eval views to materialized tables with a scheduled Pipeline refresh; resolve the US/CT/MRI event-level caveat."));
children.push(num("Adopt Data Canvas lineage tracing as the standard first step whenever two columns disagree."));
children.push(num("Stand up the first serverless Spark job for cross-source disagreement detection; route legacy intake through Connections + Data Preparation into pub_staging."));

children.push(H2("Phase 3 — Agents and self-serve (Weeks 10+)"));
children.push(num("Build the project-aware custom Agent on Agent Platform, seeded with the __conventions rules, and give the team access."));
children.push(num("Pilot Document AI on a batch of pathology / nuclear-medicine narratives to recover structured fields."));
children.push(num("Publish the “new manuscript cohort” Notebook gallery template; automate the pub_workspace lifecycle policy."));
children.push(num("Review: re-run the Section 3.2 pain-point table and confirm each pattern has a gate, a view, or a catalog entry behind it."));

// 8. Governance
children.push(H1("8. Governance and ownership"));
children.push(P("The features only help if a few habits change with them:"));
children.push(bullet("One QC home. The rules, the violations, and the gate live together and are versioned; new rules are added there, not in ad hoc tables."));
children.push(bullet("The gate is real. A canonical or safe-view table that fails a critical rule is not published — the Pipeline enforces it, not a person remembering to check."));
children.push(bullet("pub_staging is used. Legacy and raw intake lands there first and is promoted only after a prep recipe and the QC gate."));
children.push(bullet("Workspace has a lifecycle. Every pub_workspace table has an owner and a sunset trigger, or it gets a TTL; dry-run and snapshot tables are swept on a schedule."));
children.push(bullet("Lineage is a picture and a catalog. Disagreements are investigated on a Data Canvas and recorded in Data Catalog, so the next person does not start from zero."));
children.push(bullet("Findings are tracked. Issues surfaced by the QC gate or by analysis are filed in Linear — as THY-86/87/88 already are — not left in comments."));

children.push(new Paragraph({ children: [new PageBreak()] }));

// Appendix
children.push(H1("Appendix A — Feature-to-pain matrix"));
children.push(table(
  [2200, 1780, 1780, 1780, 1820],
  ["Feature", "Validation & quality", "Legacy migration", "Lineage / linking", "Visibility"],
  [
    ["Data Preparation", "Primary", "Primary", "Supporting", "—"],
    ["Data Canvas", "—", "Supporting", "Primary", "Primary"],
    ["Notebooks", "Primary", "Supporting", "—", "Primary"],
    ["Visualizations / Looker", "Supporting", "—", "—", "Primary"],
    ["Apache Spark / Dataproc", "Supporting", "Primary", "Primary", "—"],
    ["Pipelines / Composer", "Primary", "Primary", "Primary", "Supporting"],
    ["Custom Agents / Gemini", "Supporting", "—", "Supporting", "Primary"],
    ["Data Catalog / Dataplex", "Supporting", "Supporting", "Primary", "Primary"],
    ["Document AI", "Supporting", "Primary", "—", "—"],
  ]
));
children.push(spacer());

children.push(H1("Appendix B — pub_eval objects created"));
children.push(table(
  [2900, 6460],
  ["Object", "Description"],
  [
    ["pub_eval", "New dataset (us-central1). Views only — non-destructive."],
    ["vw_patient_workup_census_v1", "One row per patient (10,871). Preop/postop modality flags + intervals, reoperation signals, prior-procedure pathology-gap flags."],
    ["vw_workup_census_summary_v1", "Long-format aggregate roll-up of the census, for dashboards."],
    ["vw_nuclear_med_dated_v1", "Nuclear-medicine scans with scan dates parsed from the raw scandate string; carries a scandate_quality flag."],
  ]
));
children.push(spacer());

children.push(H1("Appendix C — First-two-weeks checklist"));
children.push(bullet("DONE — pub_eval evaluation layer built and validated."));
children.push(bullet("DONE — nuclear-medicine dates recovered; THY-86/87/88 filed."));
children.push(bullet("Data-quality notebook live and shared."));
children.push(bullet("Canonical-spine Data Canvas saved."));
children.push(bullet("pub_workspace naming + lifecycle convention agreed in writing."));
children.push(bullet("Owners named for Phase 1 (QC gate) and Phase 2 (metadata & lineage)."));
children.push(spacer());
children.push(P("Prepared as a draft for team review. Figures (dataset, table, rule, and census counts) reflect a live inspection of thyroid-canonical-pub-2026 on May 14, 2026 and should be re-checked before this plan is circulated widely.", { italics: true, size: 20, color: "777777" }));

// ---- assemble --------------------------------------------------------------
const doc = new Document({
  styles: {
    default: { document: { run: { font: "Arial", size: 22 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 30, bold: true, font: "Arial", color: "1F3864" },
        paragraph: { spacing: { before: 300, after: 160 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 25, bold: true, font: "Arial", color: "2E5395" },
        paragraph: { spacing: { before: 220, after: 120 }, outlineLevel: 1 } },
    ],
  },
  numbering: {
    config: [
      { reference: "bullets", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 460, hanging: 280 } } } }] },
      { reference: "numbers", levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 460, hanging: 280 } } } }] },
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
      default: new Header({ children: [new Paragraph({
        spacing: { after: 0 },
        border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: "BFBFBF", space: 4 } },
        children: [new TextRun({ text: "BigQuery Studio Feature Integration Plan  —  thyroid-canonical-pub-2026", size: 16, color: "888888" })],
      })] }),
    },
    footers: {
      default: new Footer({ children: [new Paragraph({
        spacing: { before: 0 },
        children: [
          new TextRun({ text: "Draft for team review  ·  May 14, 2026  ·  Page ", size: 16, color: "888888" }),
          new TextRun({ children: [PageNumber.CURRENT], size: 16, color: "888888" }),
        ],
      })] }),
    },
    children,
  }],
});

Packer.toBuffer(doc).then((buf) => {
  const out = process.argv[2] || "BigQuery_Studio_Integration_Plan.docx";
  fs.writeFileSync(out, buf);
  console.log("written " + out + " (" + buf.length + " bytes)");
});
