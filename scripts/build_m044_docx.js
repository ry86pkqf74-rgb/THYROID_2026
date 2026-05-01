// Build M044_ETE_manuscript.docx from M044_ETE_manuscript_draft.md
const fs = require('fs');
const path = require('path');
const {
  Document, Packer, Paragraph, TextRun, ExternalHyperlink, Footer, Header,
  AlignmentType, BorderStyle, HeadingLevel, LevelFormat, PageBreak, PageNumber,
  PageOrientation, Table, TableCell, TableRow, ShadingType, TabStopType,
  TabStopPosition, WidthType, ImageRun
} = require('/tmp/node_modules/docx');

const REPO = '/sessions/wonderful-trusting-babbage/mnt/THyroid 2026';
const MD_PATH = path.join(REPO, 'M044_ETE_manuscript_draft.md');
const FIG_DIR = path.join(REPO, 'figures');
const OUT_DOCX = path.join(REPO, 'M044_ETE_manuscript_v1_0.docx');

const md = fs.readFileSync(MD_PATH, 'utf8');

function applyEntities(s) { return s.replace(/'/g,'’').replace(/--/g,'—').replace(/\.\.\./g,'…'); }

function parseInline(text) {
  const out = [];
  let buf = ''; let i = 0;
  function flush(opts={}) { if (buf.length) { out.push(new TextRun({text:applyEntities(buf), ...opts})); buf=''; } }
  while (i < text.length) {
    if (text.startsWith('**', i)) {
      flush();
      const end = text.indexOf('**', i + 2);
      if (end === -1) { buf += text.slice(i); break; }
      out.push(new TextRun({ text: applyEntities(text.slice(i+2, end)), bold: true }));
      i = end + 2;
    } else if (text[i]==='*' && text[i+1]!==' ') {
      flush();
      const end = text.indexOf('*', i + 1);
      if (end === -1) { buf += text.slice(i); break; }
      out.push(new TextRun({ text: applyEntities(text.slice(i+1, end)), italics: true }));
      i = end + 1;
    } else if (text[i]==='`') {
      flush();
      const end = text.indexOf('`', i + 1);
      if (end === -1) { buf += text.slice(i); break; }
      out.push(new TextRun({ text: text.slice(i+1, end), font: 'Courier New', size: 20 }));
      i = end + 1;
    } else if (text[i]==='[') {
      const close = text.indexOf(']', i + 1);
      if (close === -1 || text[close+1] !== '(') { buf += text[i]; i++; continue; }
      const urlEnd = text.indexOf(')', close + 1);
      if (urlEnd === -1) { buf += text[i]; i++; continue; }
      flush();
      const linkText = text.slice(i+1, close);
      const url = text.slice(close+2, urlEnd);
      out.push(new ExternalHyperlink({
        children: [new TextRun({ text: applyEntities(linkText), color: '0563C1', underline: {} })],
        link: url,
      }));
      i = urlEnd + 1;
    } else { buf += text[i]; i++; }
  }
  flush();
  return out.length ? out : [new TextRun({ text: applyEntities(text) })];
}

function mdToParagraphs(md) {
  const lines = md.split('\n');
  const out = [];
  let i = 0;
  while (i < lines.length) {
    let line = lines[i];
    if (line.match(/^---$/)) { i++; continue; }
    if (line.startsWith('# ')) {
      out.push(new Paragraph({
        heading: HeadingLevel.TITLE,
        spacing: { before: 0, after: 240 },
        children: [new TextRun({ text: line.replace(/^# /, '').trim(), bold: true, size: 32 })]
      })); i++; continue;
    }
    if (line.startsWith('## ')) {
      out.push(new Paragraph({
        heading: HeadingLevel.HEADING_1,
        spacing: { before: 360, after: 180 },
        children: [new TextRun({ text: line.replace(/^## /, '').trim(), bold: true, size: 28 })]
      })); i++; continue;
    }
    if (line.startsWith('### ')) {
      out.push(new Paragraph({
        heading: HeadingLevel.HEADING_2,
        spacing: { before: 300, after: 120 },
        children: [new TextRun({ text: line.replace(/^### /, '').trim(), bold: true, size: 24 })]
      })); i++; continue;
    }
    if (line.startsWith('#### ')) {
      out.push(new Paragraph({
        heading: HeadingLevel.HEADING_3,
        spacing: { before: 240, after: 120 },
        children: [new TextRun({ text: line.replace(/^#### /, '').trim(), bold: true, italics: true, size: 22 })]
      })); i++; continue;
    }
    if (line.match(/^[-*]\s+/)) {
      out.push(new Paragraph({
        numbering: { reference: 'bullets', level: 0 },
        children: parseInline(line.replace(/^[-*]\s+/, ''))
      })); i++; continue;
    }
    if (line.match(/^\d+\.\s+/)) {
      out.push(new Paragraph({
        numbering: { reference: 'numbers', level: 0 },
        children: parseInline(line.replace(/^\d+\.\s+/, ''))
      })); i++; continue;
    }
    if (line.match(/^\|/)) {
      while (i < lines.length && lines[i].match(/^\|/)) i++;
      out.push(new Paragraph({
        spacing: { before: 120, after: 120 },
        children: [new TextRun({
          text: '[Table from M044_ETE_tables.xlsx — see separate file]',
          italics: true, color: '777777'
        })]
      })); continue;
    }
    if (line.trim() === '') { i++; continue; }
    out.push(new Paragraph({
      spacing: { before: 0, after: 120, line: 360 },
      children: parseInline(line)
    }));
    i++;
  }
  return out;
}

function figurePage(num, name, file, caption) {
  const data = fs.readFileSync(file);
  const ext = path.extname(file).slice(1).toLowerCase();
  return [
    new Paragraph({ pageBreakBefore: true,
      heading: HeadingLevel.HEADING_2,
      children: [new TextRun({ text: `Figure ${num}.`, bold: true, size: 28 })]
    }),
    new Paragraph({
      spacing: { after: 240 },
      children: [new ImageRun({
        type: ext, data,
        transformation: { width: 540, height: 360 },
        altText: { title: name, description: caption, name }
      })]
    }),
    new Paragraph({
      alignment: AlignmentType.LEFT,
      children: [new TextRun({ text: caption, italics: true })]
    })
  ];
}

const bodyParagraphs = mdToParagraphs(md);

const figureCaptions = [
  ['Cohort flow diagram', 'Cohort flow from the THYROID_2026 publication v1.0 (n = 4,128) through strict-DTC inclusion (n = 3,789) and the primary 3-level analytic subset (n = 3,756); Cox subset n = 2,025.', 'm044_fig1_cohort_flow.png'],
  ['ETE group distribution', 'ETE group distribution: full cohort vs strict-DTC subset.', 'm044_fig2_ete_distribution.png'],
  ['Path-proven recurrence rate by ETE group with 95% Wilson CI (strict-DTC).', 'Path-proven recurrence rate by ETE group with 95% Wilson confidence intervals (strict-DTC).', 'm044_fig3_pp_rate.png'],
  ['Path-proven recurrence per 100 person-years by ETE group (strict-DTC; FU > 0).', 'Path-proven recurrence per 100 person-years by ETE group on positive-follow-up denominator with Poisson 95% CI (strict-DTC).', 'm044_fig4_pp_per_100py.png'],
  ['Forest plot, strict-DTC + no-RAI primary logistic regression.', 'Adjusted odds ratios from the strict-DTC + no-RAI primary logistic regression (n = 3,756; events = 139).', 'm044_fig5_forest_primary.png'],
  ['Kaplan-Meier path-proven recurrence-free survival by ETE group (strict-DTC; surgery-date known; FU > 0).', 'Kaplan-Meier curves of path-proven recurrence-free survival by ETE group, restricted to strict-DTC patients with documented surgery date and positive follow-up.', 'm044_fig6_km_pp.png'],
  ['No/negative ETE explanatory panel.', 'Explanatory panel for the no/negative ETE subgroup: tumor size, lateral-LN positivity, reoperative pathway, and days-to-second-surgery comparing recurred vs non-recurred patients.', 'm044_fig7_noneg_panel.png'],
];

const figurePages = figureCaptions.flatMap(([title, caption, file], idx) =>
  figurePage(idx + 1, title, path.join(FIG_DIR, file), caption)
);

const doc = new Document({
  creator: 'Logan Glosser',
  title: 'Microscopic Versus Gross Extrathyroidal Extension in Differentiated Thyroid Cancer',
  styles: {
    default: { document: { run: { font: 'Arial', size: 22 } } },
    paragraphStyles: [
      { id: 'Title', name: 'Title', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 36, bold: true, font: 'Arial' },
        paragraph: { spacing: { before: 0, after: 240 }, outlineLevel: 0 } },
      { id: 'Heading1', name: 'Heading 1', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 28, bold: true, font: 'Arial' },
        paragraph: { spacing: { before: 360, after: 180 }, outlineLevel: 0 } },
      { id: 'Heading2', name: 'Heading 2', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 24, bold: true, font: 'Arial' },
        paragraph: { spacing: { before: 300, after: 120 }, outlineLevel: 1 } },
      { id: 'Heading3', name: 'Heading 3', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 22, bold: true, italics: true, font: 'Arial' },
        paragraph: { spacing: { before: 240, after: 120 }, outlineLevel: 2 } },
    ]
  },
  numbering: {
    config: [
      { reference: 'bullets',
        levels: [{ level: 0, format: LevelFormat.BULLET, text: '•', alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
      { reference: 'numbers',
        levels: [{ level: 0, format: LevelFormat.DECIMAL, text: '%1.', alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
    ]
  },
  sections: [{
    properties: {
      page: {
        size: { width: 12240, height: 15840 },
        margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 }
      }
    },
    headers: { default: new Header({ children: [new Paragraph({
      alignment: AlignmentType.RIGHT,
      children: [new TextRun({ text: 'Microscopic vs Gross ETE in DTC', size: 18, color: '777777' })]
    })] }) },
    footers: { default: new Footer({ children: [new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [
        new TextRun({ text: 'Page ', size: 18 }),
        new TextRun({ children: [PageNumber.CURRENT], size: 18 }),
        new TextRun({ text: ' of ', size: 18 }),
        new TextRun({ children: [PageNumber.TOTAL_PAGES], size: 18 }),
      ]
    })] }) },
    children: [...bodyParagraphs, ...figurePages]
  }]
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync(OUT_DOCX, buf);
  console.log(`Wrote ${OUT_DOCX} (${buf.length.toLocaleString()} bytes)`);
}).catch(e => { console.error(e); process.exit(1); });
