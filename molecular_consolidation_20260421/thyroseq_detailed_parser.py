"""
ThyroSeq V3 "DETAILED RESULTS" parser - v3.

Handles both ThyroSeq V3 GC and Afirma GSC reports.
Extracts 7 DETAILED RESULTS sub-fields + TEST RESULT/ROM/TERT/GEP detail.
"""
from __future__ import annotations

import json
import re
import sys
from typing import Dict, List, Tuple


# ---------- OCR normalization ----------
def normalize(text: str) -> str:
    t = text
    t = t.replace("\x00", " ").replace("\u00a0", " ")
    repl = [
        (r"(?i)gene\s*mul[aeto][a-z]*",  "Gene mutations"),
        (r"(?i)gene\s*mur?[at]tions?",   "Gene mutations"),
        (r"(?i)gene\s*raut[aeiouy]tions?","Gene mutations"),
        (r"(?i)gene\s*muta[ts][a-z]*",   "Gene mutations"),
        (r"(?i)gene\s*mutatione?",       "Gene mutations"),
        (r"(?i)gere\s*mu[a-z]*",         "Gene mutations"),
        (r"(?i)\bgene\s+mulations?",     "Gene mutations"),
        (r"(?i)gene\s*[flt]us[il]on[s]?", "Gene fusions"),
        (r"(?i)gere\s*fus[il]on[s]?",    "Gene fusions"),
        (r"(?i)copy\s*num[bv]er\s*al[tfi]eratio[nr]s?",  "Copy number alterations"),
        (r"(?i)copy\s*num[bv]er\s*[afi]?[itl]era[ti][oi]rs?", "Copy number alterations"),
        (r"(?i)copy\s*num[bv]er\s*a[ift]?[itl][a-z]*ations?", "Copy number alterations"),
        (r"(?i)copy\s*numbera?[itlf]ter[a-z]*",          "Copy number alterations"),
        (r"(?i)copy\s*aunter\s*elter[a-z]*",             "Copy number alterations"),
        (r"(?i)copy\s*nurber\s*alter[a-z]*",             "Copy number alterations"),
        (r"(?i)copy\s*numbecalter[a-z]*",                "Copy number alterations"),
        (r"(?i)gene\s*expresi?[sc]on\s*pro[ft]?[il]+[le]*","Gene expression profile"),
        (r"(?i)expression\s*pro[ft]?[il]+[le]*",          "Gene expression profile"),
        (r"(?i)\bparat?h?y[troild]{3,6}[:.]?", "Parathyroid"),
        (r"(?i)\bparathy[a-z]{0,4}\b", "Parathyroid"),
        (r"(?i)\bparain?y[a-z]{3,6}\b", "Parathyroid"),
        (r"(?i)\bparathyro\s*[id][d:]?\b", "Parathyroid"),
        (r"(?i)medu[il]l?ar?y?\s*[/ ]?\s*c[-\s]?ce[li]+[sl]?", "Medullary/C-cells"),
        (r"(?i)medullar[a-z]?\s*c[-\s]?bells?",               "Medullary/C-cells"),
        (r"(?i)medu[il]l?ar?y?\s*c[-\s]?ce[li]+s?",           "Medullary/C-cells"),
        (r"(?i)spec[ij][rm]?en\s+cell[a-z]*\s*[/]?\s*ade?\s*quacy\s*for?\s*inter[a-z]*\s*[:;]?",
         "Specimen cellularity/adequacy for interpretation:"),
        (r"(?i)spec[ij][rm]?en\s+cell[a-z]*\s+ty\s*radequa\s*cy\s+for\s+interpretation\s*[:;]?",
         "Specimen cellularity/adequacy for interpretation:"),
        (r"(?i)spec[ij][rm]?en\s+cellularityiadequacy\s+for\s+interpretation\s*[:;]?",
         "Specimen cellularity/adequacy for interpretation:"),
        (r"(?i)spec[ij][rm]?en\s+cellulanty[/]?adequacy\s+for\s+interpretation\s*[:;]?",
         "Specimen cellularity/adequacy for interpretation:"),
        (r"(?i)spec[ij][rm]?en\s+cellularr?y[/]?adequacy\s+for\s+interpretation\s*[:;]?",
         "Specimen cellularity/adequacy for interpretation:"),
        (r"(?i)spec[ij][rm]?en\s+cellulari?ty\s+for\s+interpretation\s*[:;]?",
         "Specimen cellularity/adequacy for interpretation:"),
        (r"(?i)spec[ij][rm]?en\s+cell[a-z]*\s*[/]?\s*ade?\s*quacy\s+for\s+interpretation\s*[:;]?",
         "Specimen cellularity/adequacy for interpretation:"),
        (r"(?i)spec[ij][rm]?en\s+cellularity[/]a?\s*de\s*quacy\s+for\s+interpretation\s*[:;]?",
         "Specimen cellularity/adequacy for interpretation:"),
    ]
    for pat, rep in repl:
        t = re.sub(pat, rep, t)
    return t


# ---------- Status classification ----------
NEG_RX  = re.compile(r"(?i)\bnegat[il]v[es]?\b|\bnegauve\b|\bnenative\b|\bnegaive\b|\bnegetiv\b|\bnegat\b|\bnegate\b|\bnamative\b|\bnegete\b|\bnegative\b|\bnegatve\b|\bnegatie\b|\bnegavuesn\b|\bneg[äa]tive\b|\bnega[uv]ive\b|\bnegative[l]?\b|\bnegat[iu]v[es]?\b|\bnegat\w*\b")
POS_RX  = re.compile(r"(?i)\bposit[il]v[es]?\b|\bpositive\b|\bposvitive\b|\bpositve\b|\bpos\b")
FAIL_RX = re.compile(r"(?i)\bfail(?:ed)?\b|\bnon[- ]informa[ti][iv]ve\b")
LOW_RX  = re.compile(r"(?i)\blow\b")
HIGH_RX = re.compile(r"(?i)\bhigh\b")


def classify_status(val: str) -> str:
    if not val:
        return ""
    v = val.strip()
    has_pos = bool(POS_RX.search(v))
    has_neg = bool(NEG_RX.search(v))
    has_fail = bool(FAIL_RX.search(v))
    has_low = bool(LOW_RX.search(v))
    has_high = bool(HIGH_RX.search(v))
    if has_fail and not has_pos and not has_neg:
        return "Failed"
    if has_pos and not has_neg:
        if has_low:
            return "Positive_low"
        if has_high:
            return "Positive_high"
        return "Positive"
    if has_neg and not has_pos:
        return "Negative"
    if has_pos and has_neg:
        if re.search(r"p\.[A-Z]\d", v) or re.search(r"c\.\d", v) or re.search(r"\d+%", v):
            if has_low:
                return "Positive_low"
            if has_high:
                return "Positive_high"
            return "Positive"
        return "Negative"
    return ""


def classify_adequacy(val: str) -> str:
    v = (val or "").strip().upper()
    if not v:
        return ""
    if "DNA ADEQUATE" in v or "RNA FAILED" in v or "RNA INADEQUATE" in v:
        return "DNA_ADEQUATE_RNA_FAILED"
    if v.startswith("INADEQUATE") or "INADEQUATE" in v[:20]:
        return "INADEQUATE"
    if "INSUFFICIENT" in v:
        return "LOW_THYROID_CELL_CONTENT"
    if "LOW THYROID CELL" in v or "LOW THYROID" in v:
        return "LOW_THYROID_CELL_CONTENT"
    if v.startswith("LIMITED") or "LIMITED" in v[:20]:
        return "LIMITED"
    if v.startswith("FAILED") or "FAILED" in v[:20]:
        return "FAILED"
    if "ADEQUATE" in v:
        return "ADEQUATE"
    if "A DEQUATE" in v or "ADE QUATE" in v or "A DE QUATE" in v:
        return "ADEQUATE"
    return v[:50]


# ---------- Variant / fusion extraction ----------
FUSION_RX = re.compile(r"\b([A-Z][A-Z0-9]{1,8})\s*[\/\-]\s*([A-Z][A-Z0-9]{1,8})\b")
GENE_TOKEN_RX = re.compile(r"\b(BRAF|NRAS|KRAS|HRAS|TERT|EIF1AX|EIFIAX|EIFLAX|EIFTAX|TP53|PTEN|GNAS|DICER1|DICERI|PAX8|PPARG|RET|NTRK1|NTRK3|ETV6|ALK|TSHR|THADA|IGF2BP3|VHL|STRN|AKT1|CTNNB1|APC|MET|PIK3CA|SMAD4|EGFR|ERBB2|FGFR2|FBXW7|FOXL2|GNAQ|KIT|MAP2K1|MSH6|PDGFRA|SRC|STK11|CDH1|NCOA4|EML4)\b")
PROT_RX = re.compile(r"p\.?\s*[A-Z][a-zA-Z_]?\d{1,4}[A-Za-z_*]*")
CDNA_RX = re.compile(r"c\.[\d\-+_]+[A-Z]?[>»]?[A-Z]?")
AF_RX   = re.compile(r"(\d{1,3})\s*%")


# ---------- Header parsing ----------
ROM_PCT_RX = re.compile(r"""
    [\(\[]?\s*(?P<approx>[~\-])?\s*(?P<op>[<>])?\s*
    (?P<num1>\d{1,3})\s*(?:-\s*(?P<num2>\d{1,3}))?\s*%
    """, re.X)
DESCRIPTOR_RX = re.compile(r"(?i)\b(VERY\s*HIGH|INTERMEDIATE[-\s]*HIGH|INTERMEDIATE[-\s]*LOW|INTERMEDIATE|HIGH|LOW)\b")
TEST_RESULT_SUMMARY_RX = re.compile(r"(?i)\b(CURRENTLY\s+NEGATIVE|POSITIVE|NEGATIVE)\b")
TERT_PROMOTER_VARIANT_RX = re.compile(r"\b(C\s*228\s*T|C\s*250\s*T|228T|250T)\b", re.I)


def parse_header_block(full_text: str) -> Dict:
    out: Dict = {}
    if not full_text:
        return out
    upper = full_text.upper()
    dr_idx = upper.find("DETAILED RESULTS")
    header_text = full_text if dr_idx < 0 else full_text[:dr_idx]
    tr_match = re.search(r"(?i)TEST\s+RE[SZ]ULT[S]?", header_text)
    if tr_match:
        window = header_text[tr_match.start(): tr_match.start() + 600]
        sm = TEST_RESULT_SUMMARY_RX.search(window)
        if sm:
            out["test_result_summary"] = re.sub(r"\s+", "_", sm.group(1).upper())
        dm = DESCRIPTOR_RX.search(window)
        if dm:
            out["rom_descriptor"] = re.sub(r"[-\s]+", "-", dm.group(1).upper())
        rm = ROM_PCT_RX.search(window)
        if rm:
            out["rom_percent_raw"] = rm.group(0).strip()
            n1 = int(rm.group("num1"))
            n2 = int(rm.group("num2")) if rm.group("num2") else None
            op = rm.group("op")
            if n2 is not None:
                out["rom_percent_low"] = float(n1)
                out["rom_percent_high"] = float(n2)
                out["rom_percent_point"] = (n1 + n2) / 2.0
            elif op == ">":
                out["rom_percent_low"] = float(n1)
                out["rom_percent_high"] = None
                out["rom_percent_point"] = float(n1)
            elif op == "<":
                out["rom_percent_low"] = None
                out["rom_percent_high"] = float(n1)
                out["rom_percent_point"] = float(n1)
            else:
                out["rom_percent_low"] = float(n1)
                out["rom_percent_high"] = float(n1)
                out["rom_percent_point"] = float(n1)
    interp_match = re.search(r"(?i)INTERPRETATION\s*:?", header_text)
    if interp_match:
        interp_start = interp_match.end()
        interp_text = header_text[interp_start:]
        interp_text = re.split(r"(?i)\bDETAILED\s+RESULTS\b", interp_text)[0]
        interp_text = re.split(r"\n[A-Z]{4,}[A-Z\s]{2,}\n", interp_text)[0]
        interp_text = re.sub(r"\s+", " ", interp_text).strip()
        if interp_text:
            out["rom_description"] = interp_text[:1000]
        if "rom_percent_raw" not in out:
            for m in ROM_PCT_RX.finditer(interp_text):
                ctx = interp_text[max(0, m.start() - 80): m.end() + 80].lower()
                if "cancer" in ctx or "malignan" in ctx or "niftp" in ctx or "probab" in ctx:
                    out["rom_percent_raw"] = m.group(0).strip()
                    n1 = int(m.group("num1"))
                    n2 = int(m.group("num2")) if m.group("num2") else None
                    op = m.group("op")
                    if n2 is not None:
                        out["rom_percent_low"] = float(n1)
                        out["rom_percent_high"] = float(n2)
                        out["rom_percent_point"] = (n1 + n2) / 2.0
                    elif op == ">":
                        out["rom_percent_low"] = float(n1)
                        out["rom_percent_high"] = None
                        out["rom_percent_point"] = float(n1)
                    elif op == "<":
                        out["rom_percent_low"] = None
                        out["rom_percent_high"] = float(n1)
                        out["rom_percent_point"] = float(n1)
                    else:
                        out["rom_percent_low"] = float(n1)
                        out["rom_percent_high"] = float(n1)
                        out["rom_percent_point"] = float(n1)
                    break
    return out


# ---------- Afirma parser ----------
AFIRMA_BRAF_RX      = re.compile(r"(?i)(?:afirma\s+)?BRAF[^\n:]*:\s*([A-Za-z/][A-Za-z/ ]*?)(?=\s*(?:\n|,|\.|RET|MTC|Paratir?yroid|$))")
AFIRMA_MTC_RX       = re.compile(r"(?i)(?:afirma\s+)?MTC(?:\s+Result)?:\s*([A-Za-z/][A-Za-z/ ]*?)(?=\s*(?:\n|,|\.|BRAF|RET|Paratir?yroid|$))")
AFIRMA_PARA_RX      = re.compile(r"(?i)Parathy?r?oid:\s*([A-Za-z/][A-Za-z/ ]*?)(?=\s*(?:\n|,|\.|$))")
AFIRMA_RETPTC_RX    = re.compile(r"(?i)RET[/\s]PTC[\s\d,/]*:\s*([A-Za-z][A-Za-z ]*?)(?=\s*(?:\n|,|\.|$))")
AFIRMA_TERT228_RX   = re.compile(r"(?i)TERT\s*c\.\s*-?\s*124\s*C\s*[>»]\s*T\s*\(?\s*(?:C\s*22[8B]|C228)\s*T?\)?\s*:\s*([A-Za-z][A-Za-z ]*?)(?=\s*(?:\n|,|\.|TERT|$))")
AFIRMA_TERT250_RX   = re.compile(r"(?i)TERT\s*c\.\s*-?\s*146\s*C\s*[>»]\s*T\s*\(?\s*C\s*250\s*T?\)?\s*:\s*([A-Za-z][A-Za-z ]*?)(?=\s*(?:\n|,|\.|$))")
AFIRMA_NRAS_RX      = re.compile(r"(?i)NRAS\s*(?:p\.\s*[A-Z]\d+[A-Z])?\s*(positive|negative|not\s*detected)", re.I)


def _afirma_classify(val):
    if not val:
        return None
    v = val.strip().lower()
    if "not ordered" in v or "test not ordered" in v:
        return "Not_Ordered"
    if "not detected" in v or "undetected" in v:
        return "Not_Detected"
    if "positive" in v:
        return "Positive"
    if "negative" in v or "neg" == v[:3]:
        return "Negative"
    if "n/a" in v or "na" == v[:2]:
        return "N/A"
    return v[:30]


def parse_afirma(text: str) -> Dict:
    out: Dict = {"parser": "afirma"}
    if not text or not text.strip() or text.strip() in ("x", "-"):
        return {"parse_status": "empty_afirma_block", "parser": "afirma"}
    braf_match = AFIRMA_BRAF_RX.search(text)
    braf_val = _afirma_classify(braf_match.group(1) if braf_match else None)
    mtc_match = AFIRMA_MTC_RX.search(text)
    mtc_val = _afirma_classify(mtc_match.group(1) if mtc_match else None)
    para_match = AFIRMA_PARA_RX.search(text)
    para_val = _afirma_classify(para_match.group(1) if para_match else None)
    ret_match = AFIRMA_RETPTC_RX.search(text)
    ret_val = _afirma_classify(ret_match.group(1) if ret_match else None)
    tert228_match = AFIRMA_TERT228_RX.search(text)
    tert250_match = AFIRMA_TERT250_RX.search(text)
    tert228_val = _afirma_classify(tert228_match.group(1) if tert228_match else None)
    tert250_val = _afirma_classify(tert250_match.group(1) if tert250_match else None)
    nras_match = AFIRMA_NRAS_RX.search(text)
    low = text.lower()
    if not braf_val and "braf" in low and "negative" in low:
        braf_val = "Negative"
    if not mtc_val and "mtc" in low and "negative" in low:
        mtc_val = "Negative"
    if not ret_val and ("ret/ptc" in low or "ret ptc" in low) and "not detected" in low:
        ret_val = "Not_Detected"
    variants: List[Dict] = []
    if braf_val == "Positive":
        variants.append({"gene": "BRAF", "protein": "p.V600E", "cdna": "c.1799T>A", "af_pct": None, "source_call": "Afirma_BRAF"})
    if nras_match:
        prot = None
        prot_m = re.search(r"p\.[A-Z]\d+[A-Z]", nras_match.group(0))
        if prot_m:
            prot = prot_m.group(0)
        call = nras_match.group(1).lower()
        if "positive" in call:
            variants.append({"gene": "NRAS", "protein": prot, "cdna": None, "af_pct": None, "source_call": "Afirma_NRAS"})
    if tert228_val == "Positive":
        variants.append({"gene": "TERT", "protein": "C228T", "cdna": "c.-124C>T", "af_pct": None, "source_call": "Afirma_TERT_C228T"})
    if tert250_val == "Positive":
        variants.append({"gene": "TERT", "protein": "C250T", "cdna": "c.-146C>T", "af_pct": None, "source_call": "Afirma_TERT_C250T"})
    fusions: List[Dict] = []
    if ret_val == "Positive":
        fusions.append({"gene1": "RET", "gene2": "PTC?", "source_call": "Afirma_RET_PTC"})
    if variants:
        mut_status = "Positive"
    elif braf_val == "Negative" or mtc_val == "Negative" or para_val == "Negative":
        mut_status = "Negative"
    else:
        mut_status = None
    if fusions:
        fus_status = "Positive"
    elif ret_val in ("Not_Detected", "Negative"):
        fus_status = "Negative"
    else:
        fus_status = None
    med_status = mtc_val
    para_status = para_val
    tert_present = any(v.get("gene") == "TERT" for v in variants)
    tert_variant = "C228T" if tert228_val == "Positive" else ("C250T" if tert250_val == "Positive" else None)
    out.update({
        "specimen_adequacy_raw": None, "specimen_adequacy_norm": None,
        "gene_mutations_raw": None, "gene_mutations_status": mut_status,
        "gene_mutations_variants": variants,
        "gene_fusions_raw": None, "gene_fusions_status": fus_status,
        "gene_fusions_list": fusions,
        "cna_raw": None, "cna_status": None,
        "gep_raw": None, "gep_status": None, "gep_detail": None,
        "parathyroid_raw": None, "parathyroid_status": para_status,
        "medullary_raw": None, "medullary_status": med_status,
        "tert_present": tert_present, "tert_promoter_variant": tert_variant,
        "afirma_braf_result": braf_val, "afirma_mtc_result": mtc_val,
        "afirma_tert_c228t_result": tert228_val, "afirma_tert_c250t_result": tert250_val,
        "afirma_retptc_result": ret_val,
    })
    filled = sum(1 for v in (mut_status, med_status, para_status, fus_status) if v)
    out["n_fields_parsed"] = filled
    out["parse_status"] = "ok" if filled >= 3 else ("partial" if filled >= 1 else "minimal")
    return out


def parse(text, platform=None):
    plat = str(platform or "").strip().lower()
    if not text or not text.strip():
        if plat.startswith("thyroseq"):
            return {"parse_status": "empty_block", "parser": "thyroseq"}
        return {"parse_status": "empty_block", "parser": None}
    # mig_320: never route ThyroSeq-platform reports through the Afirma parser.
    # Afirma heuristics (e.g. "tert promoter region") appear inside ThyroSeq PDFs
    # and caused parser='afirma' + platform='ThyroSeq' mismatches (M083 blocker).
    if plat.startswith("thyroseq"):
        return parse_block(text)
    is_afirma = False
    if platform:
        is_afirma = str(platform).lower().startswith("afirma")
    tl = text.lower()
    if ("afirma mtc" in tl or "afirma braf" in tl or "tert promoter region" in tl
            or ("mtc result" in tl and "detailed results" not in tl)):
        is_afirma = True
    if "detailed results" in tl or "specimen cellularity" in tl:
        is_afirma = False
    return parse_afirma(text) if is_afirma else parse_block(text)


# ---------- ThyroSeq block parser ----------
LABELS = [
    ("adeq", "Specimen cellularity/adequacy for interpretation:"),
    ("mut",  "Gene mutations"),
    ("fus",  "Gene fusions"),
    ("cna",  "Copy number alterations"),
    ("gep",  "Gene expression profile"),
    ("para", "Parathyroid"),
    ("med",  "Medullary/C-cells"),
]


def find_label_positions(norm: str) -> List[Tuple[str, int, int]]:
    positions = []
    for key, lab in LABELS:
        idx = norm.find(lab)
        if idx >= 0:
            positions.append((key, idx, idx + len(lab)))
    positions.sort(key=lambda x: x[1])
    return positions


def _variants_from_freeform(mut_source: str) -> List[Dict]:
    """Scan arbitrary report text for gene tokens + HGVS / AF (mig_320 fallback)."""
    variants: List[Dict] = []
    seen_keys = set()
    if not mut_source:
        return variants
    for gene_m in GENE_TOKEN_RX.finditer(mut_source):
        g = gene_m.group(1).upper()
        g_norm = {"EIFIAX": "EIF1AX", "EIFLAX": "EIF1AX", "EIFTAX": "EIF1AX", "DICERI": "DICER1"}.get(g, g)
        window = mut_source[gene_m.start(): gene_m.start() + 220]
        prot = PROT_RX.search(window)
        cdna = CDNA_RX.search(window)
        af = AF_RX.search(window)
        key = (g_norm, prot.group(0) if prot else None)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        variants.append({
            "gene": g_norm,
            "protein": prot.group(0) if prot else None,
            "cdna": cdna.group(0) if cdna else None,
            "af_pct": int(af.group(1)) if af else None,
            "source_call": "ThyroSeq_FREEFORM_FALLBACK",
        })
    return variants


def parse_block(text: str) -> Dict:
    header_out = parse_header_block(text or "")
    if not text or "DETAILED RESULTS" not in text.upper():
        fb_variants = _variants_from_freeform(normalize(text or ""))
        if not fb_variants:
            fb_variants = _variants_from_freeform(text or "")
        if fb_variants:
            out: Dict = {"parse_status": "partial", "parser": "thyroseq", **header_out}
            out["specimen_adequacy_raw"] = None
            out["specimen_adequacy_norm"] = None
            out["gene_mutations_raw"] = (text or "")[:400]
            out["gene_mutations_status"] = "Positive"
            out["gene_mutations_variants"] = fb_variants
            out["gene_fusions_raw"] = None
            out["gene_fusions_status"] = None
            out["gene_fusions_list"] = []
            out["cna_raw"] = None
            out["cna_status"] = None
            out["gep_raw"] = None
            out["gep_status"] = None
            out["gep_detail"] = None
            out["parathyroid_raw"] = None
            out["parathyroid_status"] = None
            out["medullary_raw"] = None
            out["medullary_status"] = None
            out["labels_found"] = []
            tert_present = False
            tert_variant = None
            for v in fb_variants:
                if v.get("gene") == "TERT":
                    tert_present = True
                    prot = (v.get("protein") or "")
                    tm = TERT_PROMOTER_VARIANT_RX.search(prot)
                    tert_variant = re.sub(r"\s+", "", tm.group(1).upper()) if tm else (prot or "OTHER")
                    break
            out["tert_present"] = tert_present
            out["tert_promoter_variant"] = tert_variant
            filled = sum(1 for k in ("gene_mutations_status",) if out.get(k))
            out["n_fields_parsed"] = filled
            return out
        return {"parse_status": "no_detailed_block", "parser": "thyroseq", **header_out}
    start = text.upper().find("DETAILED RESULTS")
    block = text[start + len("DETAILED RESULTS"):]
    block = re.split(r";\s*INTERPRETATION", block, flags=re.I)[0]
    block = re.split(r"\n[A-Z]{3,},\s*[A-Z]", block)[0]
    norm = normalize(block)
    positions = find_label_positions(norm)
    sections: Dict[str, str] = {k: "" for k, _ in LABELS}
    for i, (key, s, e) in enumerate(positions):
        end = positions[i + 1][1] if i + 1 < len(positions) else len(norm)
        sections[key] = norm[e:end].strip(" :;,.\n\r\t-")
    out: Dict = {"parse_status": "ok", "parser": "thyroseq"}
    adeq_raw = sections["adeq"].strip()
    first_line = adeq_raw.split("\n")[0].strip() if adeq_raw else ""
    out["specimen_adequacy_raw"]  = first_line[:200]
    out["specimen_adequacy_norm"] = classify_adequacy(first_line)
    mut_raw = sections["mut"].strip()
    out["gene_mutations_raw"]    = mut_raw[:400]
    out["gene_mutations_status"] = classify_status(mut_raw)
    variants: List[Dict] = []
    seen_keys = set()
    for gene_m in GENE_TOKEN_RX.finditer(mut_raw):
        g = gene_m.group(1).upper()
        g_norm = {"EIFIAX": "EIF1AX", "EIFLAX": "EIF1AX", "EIFTAX": "EIF1AX", "DICERI": "DICER1"}.get(g, g)
        window = mut_raw[gene_m.start(): gene_m.start() + 200]
        prot = PROT_RX.search(window)
        cdna = CDNA_RX.search(window)
        af = AF_RX.search(window)
        key = (g_norm, prot.group(0) if prot else None)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        variants.append({
            "gene": g_norm,
            "protein": prot.group(0) if prot else None,
            "cdna":    cdna.group(0) if cdna else None,
            "af_pct":  int(af.group(1)) if af else None,
            "source_call": "ThyroSeq_DETAILED",
        })
    out["gene_mutations_variants"] = variants
    if variants and out["gene_mutations_status"] in ("", "Negative"):
        out["gene_mutations_status"] = "Positive"
    fus_raw = sections["fus"].strip()
    out["gene_fusions_raw"]    = fus_raw[:400]
    out["gene_fusions_status"] = classify_status(fus_raw)
    fusions: List[Dict] = []
    for a, b in FUSION_RX.findall(fus_raw):
        if a == b:
            continue
        if a in ("AF", "AA", "TG", "AT", "CT") or b in ("AF", "AA", "TG", "AT", "CT"):
            continue
        fusions.append({"gene1": a, "gene2": b, "source_call": "ThyroSeq_DETAILED"})
    out["gene_fusions_list"] = fusions
    if fusions and out["gene_fusions_status"] in ("", "Negative"):
        out["gene_fusions_status"] = "Positive"
    cna_raw = sections["cna"].strip()
    out["cna_raw"] = cna_raw[:200]
    out["cna_status"] = classify_status(cna_raw)
    gep_raw = sections["gep"].strip()
    out["gep_raw"] = gep_raw[:200]
    out["gep_status"] = classify_status(gep_raw)
    para_raw = sections["para"].strip()
    out["parathyroid_raw"] = para_raw[:200]
    out["parathyroid_status"] = classify_status(para_raw)
    med_raw = sections["med"].strip()
    out["medullary_raw"] = med_raw[:200]
    out["medullary_status"] = classify_status(med_raw)
    out["labels_found"] = [k for k, _, _ in positions]
    tert_present = False
    tert_variant = None
    for v in out.get("gene_mutations_variants", []) or []:
        if v.get("gene") == "TERT":
            tert_present = True
            prot = (v.get("protein") or "")
            tm = TERT_PROMOTER_VARIANT_RX.search(prot)
            tert_variant = re.sub(r"\s+", "", tm.group(1).upper()) if tm else (prot or "OTHER")
            break
    out["tert_present"] = tert_present
    out["tert_promoter_variant"] = tert_variant
    if out.get("gep_status") in ("Positive", "Positive_low", "Positive_high"):
        gep_raw_full = sections["gep"].strip()
        qual = re.sub(r"(?i)^\s*positive[,:;\s]*", "", gep_raw_full)
        qual = qual.split("\n")[0].strip(" :;,.-")
        out["gep_detail"] = qual[:300] if qual and qual.lower() != "positive" else None
    else:
        out["gep_detail"] = None
    out.update(header_out)
    filled = sum(1 for k in ("specimen_adequacy_norm", "gene_mutations_status", "gene_fusions_status",
                             "cna_status", "gep_status", "parathyroid_status", "medullary_status")
                 if out.get(k))
    out["n_fields_parsed"] = filled
    out["parse_status"] = "ok" if filled >= 6 else ("partial" if filled >= 1 else "minimal")
    return out


if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:
            continue
        text = rec.get("det_block") or rec.get("txt") or rec.get("report_text") or ""
        platform = rec.get("platform")
        result = parse(text, platform=platform)
        rec["parsed"] = result
        print(json.dumps(rec))
