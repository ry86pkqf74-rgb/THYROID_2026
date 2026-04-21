import json
from pathlib import Path
ckpt = Path("runs/tirads_granular/smoke3_v2/note_entities_llm_tirads_granular.ckpt.jsonl")
print(f"file: {ckpt.name}  size={ckpt.stat().st_size}B  lines={sum(1 for _ in ckpt.open())}")
print("=" * 100)
for i, line in enumerate(ckpt.read_text().splitlines()):
    rec = json.loads(line)
    rid = rec.get("note_row_id")
    raw = rec.get("result_json") or ""
    try:
        parsed = json.loads(raw)
    except Exception as e:
        print(f"\n*** REC {i} {rid}: JSON PARSE ERROR {e}")
        print(f"  raw[-300:]: {raw[-300:]}")
        continue
    nodules = parsed.get("nodules", []) or []
    rpt = parsed.get("report_level") or {}
    print(f"\n=== REC {i}: {rid}  research_id={rec.get('research_id')}  note_date={rec.get('note_date')}")
    print(f"    result_json_len={len(raw)}B  nodules={len(nodules)}  report_level_keys={list(rpt.keys())}")
    for j, n in enumerate(nodules):
        tr = n.get("tirads_category") or n.get("tirads_reported_in_text")
        loc_parts = []
        for k in ("laterality", "pole", "position"):
            if n.get(k): loc_parts.append(n[k])
        loc = "/".join(loc_parts) or n.get("location_raw", "?")[:30]
        dim = f"{n.get('size_mm_ap')},{n.get('size_mm_tr')},{n.get('size_mm_cc')}mm"
        comp = f"{n.get('composition') or '-'}/{n.get('echogenicity') or '-'}/{n.get('shape') or '-'}/{n.get('margin') or '-'}"
        foci = n.get('echogenic_foci')
        cmp_st = n.get("comparison_statement")
        prior = n.get("prior_size_mm_max")
        vasc = n.get("vascularity")
        print(f"  [{j}] {n.get('nodule_id','?'):3s} {loc:20s}  {dim:18s}  {comp:38s}  foci={foci}  cmp={cmp_st}  prior_mm={prior}  vasc={vasc}  TR={tr}  date={n.get('entity_date')}")
    print(f"  REPORT: rec={rpt.get('overall_recommendation')}  fu_mo={rpt.get('follow_up_interval_months')}  susp_ln={rpt.get('suspicious_ln_present')}  n_nod_reported={rpt.get('n_nodules_in_report')}  dom={rpt.get('dominant_nodule_id_by_radiologist')}")
