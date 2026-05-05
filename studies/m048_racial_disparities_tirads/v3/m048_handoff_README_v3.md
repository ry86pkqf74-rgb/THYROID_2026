# M048 v3.2 handoff (numbers + paths; no manuscript prose)

- v3.2 git_sha: 61624d9 (post Bug E commit; recompute sync at 379223d)
- mig_id: mig_317b (v3.2 sign-off committed to MotherDuck 2026-05-05 13:08 EDT)
- run_timestamp_utc: 2026-05-05T17:03:21.386671+00:00
- outputs: `/Users/loganglosser/THYROID_2026/studies/m048_racial_disparities_tirads/v3`
- figures: built; 8 figures in `M048_submission_package/figures/v3/` (300 dpi PNG + vector PDF)
- mediation_boot: 200 (matches stable bootstrap CI behaviour; raise to 1000 for final manuscript figures only if requested)
- independent_recompute_v3.py: 5/5 PASS at <=2 pct rel (Black + Asian full ORs both reproduce at 0.0% diff)

## v3.2 bug history (delta from v3.1 e7984c4)

- **Bug A** (commit 6e3a9d8) — `is_malignant` was bool, Patsy treated it as a 2-column categorical endog; cast to int(0/1).
- **Bug B** (commit ebb1954) — `had_any_fna` perfectly collinear with `bethesda_bucket=='missing'` (no FNA -> Bethesda='missing'); dropped `had_any_fna` from all formulas. `had_repeat_fna` and `n_fnas_total` retained for FNA-pattern signal.
- **Bug C** (commit e116c58) — `has_clt`, `has_mng`, `has_graves` are all-zero columns (extracted from `histology_final` which only carries malignant categorisations). `has_niftp` (n=56, all benign) and `has_ftump` (n=21, all malignant) are perfect-separation indicators because they're path-diagnostic categories that *derive* `is_malignant`. All five dropped from regression formulas. `m4_background` step removed from cascade because it was identical to `m3_genetics_nm` after the drop.
- **Bug D** (commit e116c58) — `days_us_to_surg_approx` had range -10582 to 8019 days (negatives are coding errors); huge magnitude caused lbfgs to overflow the linear predictor and fall back to all-zero coefficients. Now clipped to >=0 and converted to years (range 0 to ~22). `surg_year` (1994-2025) now centred to its median.
- **Bug E** (commit 61624d9) — sensitivity-arms block produced `surg_first_date_x`/`surg_first_date_y` suffix collision when merging `df_raw` onto `df_model` (which already carried the column through `prepare_v3_frame`); crashed the run after mediation finished. Fixed by using `df_model` directly with a `surg_year`-derived fallback for `surg_dt`.
- **Recompute sync** (commit 379223d) — `independent_recompute_v3.py` was using the pre-Bug-B/C formula and producing a different (still-degenerate) refit. Synced to match `m6_full` exactly; all assertions now PASS at 0.0% rel.

## Decision items for senior author (manuscript writing)

1. **Black M0 OR 0.317 only attenuates to M6 OR 0.432** — disparity narrows but remains highly significant (p<<0.001). Framing: Black operative-cohort patients are about 2x less likely than White to have malignant pathology at the same TI-RADS level. Possible interpretations include selection / pathway-routing bias (more Black patients reach surgery for benign indications), residual unmeasured confounding, and true performance differences in TI-RADS calibration. Senior author needs to weigh framing.
2. **Asian Bethesda II OR 2.32 (1.02-5.30, p=0.045)** — Asian patients with cytologically benign FNA had ~2.3x the malignancy rate of White Bethesda II patients. Could be true higher false-negative cytology rate, differential follow-up, or small-N artifact (Asian Bethesda II n=27 with 7 events expected vs more in larger groups).
3. **Black TR4 cell shows under_referral_signature** in the disparity-direction table: 71 malignant of 207 total (ROM 34.3%) vs ACR-mid 18%. Mean tumor size 2.81 cm (vs White 2.24, Asian 1.91). Combined with the M0 OR <1 finding, this suggests Black patients reaching surgery at TR4 have more advanced disease per imaging score.
4. **Mediation indirect effects for `had_any_genetics` and `had_any_nm` on Black-vs-White are very large in magnitude** (-8.37 and -4.62 on the log-OR product scale, with wide bootstrap CIs). These are likely numerical-stability artifacts of fitting the b-path through a small mediator-target subgroup; the *direction* (negative IE -> mediator partially explains the Black-low-OR pattern) is reliable, but the magnitudes should be treated as qualitative ordering rather than quantitative shares-explained.

## QA gate WARN flags (do not block handoff)

- `repeat_fna_pct_among_biopsied`: WARN (41.75 vs expected ~15-25). The cohort definition or repeat-FNA detection logic differs from the v2 reference. Investigate before quoting in manuscript.
- `multifocal_pct_malignant`: WARN (0.00 vs expected ~61). The multifocality flag did not propagate into the v3 patient master from the canonical cohort. Disparity-direction `pct_multifocal` is therefore 0 for every cell — do not surface multifocality numbers from this run; pull them from M025 directly.

## Framing guidance
- If race effect attenuates **>70%** M0→M6 and Bethesda-stratified analysis removes the TR gradient: apparent disparity explained by access/FNA/multinodular pathway.
- If **<30%** attenuation and Bethesda-stratified gradient persists: residual race × TI-RADS performance signal.
- **30–70%**: lead with attenuation cascade; add Bethesda-stratified + disparity-direction quadrant as clinical interpretation.
- Always report disparity-direction signatures (over- vs under-referral) for TR4/TR5 × race.

## Race OR vs White (patient grain)
- Black M0: OR 0.317 (0.272–0.369)
- Black M3 ( + genetics + NM, last pre-FNA step after Bug C drop): OR 0.359 (0.305–0.424)
- Black M6 (full v3): OR 0.432 (0.358–0.521)
- Asian M6: OR 1.203 (0.828–1.747)

## Disparity-direction (TR4/TR5 × race)
```
race_strat,tr_category,n_malignant_cell,rom_pct,mean_tumor_size_cm,pct_multifocal,pct_any_ete,pct_ln_positive,dominant_histology,acr_rom_mid_ref,acr_rom_high_ref,signature
Black,TR4,71,34.3,2.81,0.0,94.35483870967742,75.0,differentiated,18.0,28.0,under_referral_signature
White,TR4,116,53.95,2.24,0.0,91.13924050632912,70.88607594936708,differentiated,18.0,28.0,calibrated
Asian,TR4,26,78.79,1.91,0.0,90.2439024390244,60.97560975609756,differentiated,18.0,28.0,calibrated
Black,TR5,199,40.04,2.47,0.0,91.61490683229812,71.73913043478261,differentiated,42.0,55.0,calibrated
White,TR5,464,68.74,2.21,0.0,93.73626373626374,70.98901098901099,differentiated,42.0,55.0,calibrated
Asian,TR5,76,73.79,1.88,0.0,95.40229885057472,72.41379310344827,differentiated,42.0,55.0,calibrated
```

## Top mediators by |bootstrap IE| (Black vs White)
```
mediator,type,race_target,scope,indirect_mean,ci_lo,ci_hi,abs_ie
had_any_genetics,binary,Black,univariate_black_vs_white,-8.372569193004294,-24.949678273676668,-2.529579127058688,8.372569193004294
had_any_nm,binary,Black,univariate_black_vs_white,-4.616274553071602,-12.536742912292146,-1.1589520350298876,4.616274553071602
had_any_nm,binary,Asian,univariate_asian_vs_white,-1.3444285401601743,-3.118525222070729,-0.1244581868224709,1.3444285401601743
```

## Race × TR interaction
```
param,coef,p,p_bonf
"C(race_strat, Treatment('White'))[T.Asian]:max_tr_int",-0.0416707471420231,0.7640467197104146,1.0
"C(race_strat, Treatment('White'))[T.Black]:max_tr_int",-0.0737183452885925,0.2960943872862466,1.0
```

## Bethesda-stratified Model B (first 24 rows)
```
bethesda_bucket,race_level,or,ci_lo,ci_hi,p,n,n_events
III_AUS,Black,0.6483777668240114,0.4258304463849477,0.9872326699055688,0.0433973177907521,389,188
III_AUS,Asian,0.3810041700013583,0.1563142118758039,0.9286690942328456,0.03377387535859,389,188
II_benign,Black,0.4860957430410033,0.3297635080858853,0.7165409925862531,0.0002688677953043,845,139
II_benign,Asian,2.323144238988395,1.0191554686855893,5.295560217231143,0.0449524516586447,845,139
IV_FN,Black,1.1080336800043258,0.6679778748836628,1.83799296681459,0.6911486566303584,260,132
IV_FN,Asian,0.4099275713840824,0.0758884428065608,2.2143110013363474,0.3000891405345299,260,132
I_nondiagnostic,Black,0.6669906135674428,0.2304549760737505,1.9304268719487483,0.4551245274278129,84,23
I_nondiagnostic,Asian,7.025904919186902e-06,0.0,inf,0.9832559502911477,84,23
VI_malig,Black,0.5350299443503334,0.3200847035037617,0.8943165300248613,0.0170283628689846,522,432
VI_malig,Asian,1.0678929235688357,0.4536381754682246,2.5138874060396947,0.8804663607841312,522,432
V_susp_malig,Black,0.4749622466700577,0.1399732457867679,1.6116589602096254,0.2323428263805923,119,106
V_susp_malig,Asian,0.9249656909639274,0.1003667449570786,8.524352661095595,0.9451220012649192,119,106
missing,Black,0.2176847165623383,0.1583506297598862,0.2992513253447741,5.975788545905906e-21,902,323
missing,Asian,1.4279730105766315,0.8350387932895151,2.441930764560675,0.1931185551426899,902,323
```
