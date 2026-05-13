# mig_323 Platform Reclassification Diff Report — 20260513

**run_id:** `mig_323_20260513_bfa73503`  
**rows analyzed:** 967

## Platform changes

| Change | n |
|---|---|
| NGS_unspecified → ThyroSeq | 9 |
| NGS_unspecified → Afirma | 6 |
| ThyroSeq → Other | 18 |
| ThyroSeq → Afirma | 158 |

**Total allowed auto-changes:** 191  
**Flagged (reported_text guard — requires manual review):** 16  

### Platform change source tier breakdown

| Tier | n |
|---|---|
| gep_norm_thyroseq | 9 |
| gep_norm_afirma | 159 |
| gep_norm_quest | 18 |
| thyroseq_afirma_text_afirma | 5 |

## Afirma call updates

| Type | n |
|---|---|
| New call (current ORC was NULL) | 55 |
| Overwrite (ThyroSeq semantics → Afirma) | 93 |
| Pre-existing disagrees with proposed | 48 |

### Pre-existing call disagreements (INSPECT BEFORE APPLY)

- research_id=9131: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=7783: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=6778: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=8855: current=other → proposed=negative (bbs=None)
- research_id=8855: current=other → proposed=negative (bbs=None)
- research_id=9591: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=11148: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9957: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=6553: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=8871: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=8947: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9660: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9726: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=7166: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9407: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9868: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=10874: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9659: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=7748: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=11238: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=10818: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=7725: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=10161: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=10420: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=11069: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9897: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9804: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=8561: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=6904: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=7795: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9359: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=10491: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=10706: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9330: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=8592: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9366: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9772: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9761: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=10311: current=negative → proposed=positive (bbs=numeric_rom_inferred)
- research_id=7626: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9000: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=8854: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=10510: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9652: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=9998: current=negative → proposed=positive (bbs=numeric_rom_inferred)
- research_id=10463: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)
- research_id=11122: current=negative → proposed=positive (bbs=numeric_rom_inferred)
- research_id=10696: current=intermediate → proposed=positive (bbs=numeric_rom_inferred)

### Rows flagged (reported_text guard — NOT auto-applied)

- research_id=9154: ThyroSeq → Afirma, orc=positive, bbs=reported_text
- research_id=8218: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=5999: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=7012: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=5724: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=11039: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=9991: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=10699: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=10174: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=8233: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=11156: ThyroSeq → Other, orc=negative, bbs=reported_text
- research_id=10926: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=10939: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=10237: ThyroSeq → Afirma, orc=positive, bbs=reported_text
- research_id=11087: ThyroSeq → Afirma, orc=negative, bbs=reported_text
- research_id=8729: ThyroSeq → Afirma, orc=positive, bbs=reported_text