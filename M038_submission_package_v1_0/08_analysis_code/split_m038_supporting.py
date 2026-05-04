"""Split the m038_supporting_docs.md bundle into individual deliverables."""
from pathlib import Path
import re, subprocess

# mig_299: portable paths. SRC is a one-off Claude-session bundle from the
# original 2026-05-01 build run — kept here for provenance only; override via env
# var M038_SUPPORTING_DOCS_MD if the bundle was archived elsewhere.
import os
PKG = Path(__file__).resolve().parents[1]
SRC = Path(os.environ.get(
    "M038_SUPPORTING_DOCS_MD",
    str(PKG / "_archive" / "m038_supporting_docs.md"),
))

src = SRC.read_text()
sections = re.split(r"^# ", src, flags=re.MULTILINE)[1:]  # split on top-level # headings

mapping = {
    "M038 Title Page":                 ("01_title_page.md",                 "01_title_page.docx",                 "docx"),
    "M038 Supplement":                 ("03_supplement.md",                 "03_supplement.docx",                 "docx"),
    "M038 Response to Reviewers Template": ("07_response_to_reviewers_template.md", "07_response_to_reviewers_template.docx", "docx"),
    "M038 README":                     ("00_README.md",                     "00_README.md",                       "md"),
    "M038 Validation Report":          ("09_validation_report.md",          "09_validation_report.md",            "md"),
    "CLOSEOUT NOTES":                  ("CLOSEOUT_NOTES.md",                "CLOSEOUT_NOTES.md",                  "md"),
}

for sect in sections:
    title_line = sect.split("\n", 1)[0].strip()
    body = "# " + sect  # restore the leading hash
    matched = None
    for key, (md_name, out_name, kind) in mapping.items():
        if title_line.startswith(key):
            matched = (md_name, out_name, kind)
            break
    if not matched:
        print(f"  skip: {title_line!r}")
        continue
    md_name, out_name, kind = matched
    md_path = PKG / md_name
    md_path.write_text(body)
    if kind == "docx":
        out_path = PKG / out_name
        # Use pandoc
        cmd = ["pandoc", str(md_path),
               "--from=markdown+pipe_tables",
               "--to=docx",
               "--output=" + str(out_path)]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            print(f"  ERR {out_name}: {r.stderr[:200]}")
        else:
            print(f"  ✓ {out_name}")
        # remove temp md
        if md_name != out_name:
            md_path.unlink()
    else:
        print(f"  ✓ {out_name} (markdown)")

# Post-process the 3 docx files for Arial 11pt + US Letter
import importlib
docx = importlib.import_module("docx")
from docx.shared import Inches
from docx.oxml.ns import qn

def style_docx(path):
    if not path.exists():
        return
    d = docx.Document(str(path))
    for s in d.sections:
        s.page_width = Inches(8.5); s.page_height = Inches(11)
        s.left_margin = s.right_margin = s.top_margin = s.bottom_margin = Inches(1)
    # Set Arial on Normal style
    for sname in ["Normal", "Body Text", "Heading 1", "Heading 2", "Heading 3"]:
        if sname in [st.name for st in d.styles]:
            try:
                style = d.styles[sname]
                rpr = style.element.get_or_add_rPr()
                rfonts = rpr.find(qn("w:rFonts"))
                if rfonts is None:
                    rfonts = rpr.makeelement(qn("w:rFonts"), {})
                    rpr.insert(0, rfonts)
                for attr in ("w:ascii","w:hAnsi","w:cs","w:eastAsia"):
                    rfonts.set(qn(attr), "Arial")
            except Exception as e:
                print(f"  warn {path.name}/{sname}: {e}")
    d.save(str(path))

for n in ["01_title_page.docx", "03_supplement.docx", "07_response_to_reviewers_template.docx"]:
    style_docx(PKG / n)

print("Done")
print("Package contents:")
for p in sorted(PKG.glob("*")):
    if p.is_file():
        print(f"  {p.name} ({p.stat().st_size:,} bytes)")
    else:
        print(f"  {p.name}/ (dir)")
