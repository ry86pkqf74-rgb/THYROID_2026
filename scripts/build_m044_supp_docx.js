const fs = require('fs');
const path = require('path');
const {
  Document, Packer, Paragraph, TextRun, ExternalHyperlink, Footer, Header,
  AlignmentType, BorderStyle, HeadingLevel, LevelFormat, PageNumber,
  WidthType
} = require('/tmp/node_modules/docx');

const REPO = '/sessions/wonderful-trusting-babbage/mnt/THyroid 2026';
const MD = path.join(REPO, 'M044_ETE_supplement.md');
const OUT = path.join(REPO, 'M044_ETE_supplement_v1_0.docx');
const md = fs.readFileSync(MD, 'utf8');

function ents(s){return s.replace(/'/g,'’').replace(/--/g,'—').replace(/\.\.\./g,'…');}
function inline(t){
  const r=[]; let b=''; let i=0;
  const flush=(o={})=>{if(b){r.push(new TextRun({text:ents(b),...o}));b='';}};
  while(i<t.length){
    if(t.startsWith('**',i)){flush();const e=t.indexOf('**',i+2);if(e<0){b+=t.slice(i);break;}r.push(new TextRun({text:ents(t.slice(i+2,e)),bold:true}));i=e+2;}
    else if(t[i]==='*'&&t[i+1]!==' '){flush();const e=t.indexOf('*',i+1);if(e<0){b+=t.slice(i);break;}r.push(new TextRun({text:ents(t.slice(i+1,e)),italics:true}));i=e+1;}
    else if(t[i]==='`'){flush();const e=t.indexOf('`',i+1);if(e<0){b+=t.slice(i);break;}r.push(new TextRun({text:t.slice(i+1,e),font:'Courier New',size:20}));i=e+1;}
    else{b+=t[i];i++;}
  }
  flush(); return r.length?r:[new TextRun({text:ents(t)})];
}
function md2p(md){
  const ls=md.split('\n');const o=[];let i=0;
  while(i<ls.length){const L=ls[i];
    if(L.match(/^---$/)){i++;continue;}
    if(L.startsWith('# ')){o.push(new Paragraph({heading:HeadingLevel.TITLE,spacing:{before:0,after:240},children:[new TextRun({text:L.replace(/^# /,'').trim(),bold:true,size:32})]}));i++;continue;}
    if(L.startsWith('## ')){o.push(new Paragraph({heading:HeadingLevel.HEADING_1,spacing:{before:360,after:180},children:[new TextRun({text:L.replace(/^## /,'').trim(),bold:true,size:28})]}));i++;continue;}
    if(L.startsWith('### ')){o.push(new Paragraph({heading:HeadingLevel.HEADING_2,spacing:{before:300,after:120},children:[new TextRun({text:L.replace(/^### /,'').trim(),bold:true,size:24})]}));i++;continue;}
    if(L.startsWith('#### ')){o.push(new Paragraph({heading:HeadingLevel.HEADING_3,spacing:{before:240,after:120},children:[new TextRun({text:L.replace(/^#### /,'').trim(),bold:true,italics:true,size:22})]}));i++;continue;}
    if(L.match(/^[-*]\s+/)){o.push(new Paragraph({numbering:{reference:'b',level:0},children:inline(L.replace(/^[-*]\s+/,''))}));i++;continue;}
    if(L.match(/^\d+\.\s+/)){o.push(new Paragraph({numbering:{reference:'n',level:0},children:inline(L.replace(/^\d+\.\s+/,''))}));i++;continue;}
    if(L.match(/^\|/)){while(i<ls.length&&ls[i].match(/^\|/))i++;o.push(new Paragraph({spacing:{before:120,after:120},children:[new TextRun({text:'[Table — see M044_ETE_tables.xlsx]',italics:true,color:'777777'})]}));continue;}
    if(L.trim()===''){i++;continue;}
    o.push(new Paragraph({spacing:{before:0,after:120,line:360},children:inline(L)}));i++;
  }
  return o;
}
const doc=new Document({
  creator:'Logan Glosser',title:'M044 Supplement',
  styles:{default:{document:{run:{font:'Arial',size:22}}},
    paragraphStyles:[
      {id:'Title',name:'Title',basedOn:'Normal',next:'Normal',quickFormat:true,run:{size:36,bold:true,font:'Arial'},paragraph:{spacing:{before:0,after:240},outlineLevel:0}},
      {id:'Heading1',name:'Heading 1',basedOn:'Normal',next:'Normal',quickFormat:true,run:{size:28,bold:true,font:'Arial'},paragraph:{spacing:{before:360,after:180},outlineLevel:0}},
      {id:'Heading2',name:'Heading 2',basedOn:'Normal',next:'Normal',quickFormat:true,run:{size:24,bold:true,font:'Arial'},paragraph:{spacing:{before:300,after:120},outlineLevel:1}},
      {id:'Heading3',name:'Heading 3',basedOn:'Normal',next:'Normal',quickFormat:true,run:{size:22,bold:true,italics:true,font:'Arial'},paragraph:{spacing:{before:240,after:120},outlineLevel:2}},
    ]},
  numbering:{config:[
    {reference:'b',levels:[{level:0,format:LevelFormat.BULLET,text:'•',alignment:AlignmentType.LEFT,style:{paragraph:{indent:{left:720,hanging:360}}}}]},
    {reference:'n',levels:[{level:0,format:LevelFormat.DECIMAL,text:'%1.',alignment:AlignmentType.LEFT,style:{paragraph:{indent:{left:720,hanging:360}}}}]},
  ]},
  sections:[{properties:{page:{size:{width:12240,height:15840},margin:{top:1440,right:1440,bottom:1440,left:1440}}},
    headers:{default:new Header({children:[new Paragraph({alignment:AlignmentType.RIGHT,children:[new TextRun({text:'M044 Supplement',size:18,color:'777777'})]})]})},
    footers:{default:new Footer({children:[new Paragraph({alignment:AlignmentType.CENTER,children:[new TextRun({text:'Page ',size:18}),new TextRun({children:[PageNumber.CURRENT],size:18}),new TextRun({text:' of ',size:18}),new TextRun({children:[PageNumber.TOTAL_PAGES],size:18})]})]})},
    children:md2p(md)}]
});
Packer.toBuffer(doc).then(b=>{fs.writeFileSync(OUT,b);console.log(`Wrote ${OUT} (${b.length.toLocaleString()} bytes)`)}).catch(e=>{console.error(e);process.exit(1);});
