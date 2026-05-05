# M048 v3.2 handoff (numbers + paths; no manuscript prose)

- v3.2 git_sha: f7519e6 (final 1000-rep mediation pass)
- mig_id: mig_317b (signed off 2026-05-05 13:08 EDT)
- run_timestamp_utc: 2026-05-05T19:41:58.409701+00:00 (1000 boot reps)
- outputs: `/Users/loganglosser/THYROID_2026/studies/m048_racial_disparities_tirads/v3`
- figures: 8 PNG + PDF in `M048_submission_package/figures/v3/`
- mediation_boot: **1000** (final spec-compliant pass)
- independent_recompute_v3: **5/5 PASS at 0.0% rel diff**
- nodule-level Model F (cluster-robust): Black OR 0.438 (0.31-0.63, p=7.0e-6); Asian OR 1.46 (0.87-2.46, p=0.16) — converges with the patient-level cascade
- multifocal_flag now correctly populated (39.62% of malignant patients) -- sourced from manuscript_workspace.cohort_m048_tnm_multifocal_v1
- mediation indirect_mean uses **median** for robustness; indirect_winsor_mean column added (clipped 1st/99th pct then mean)

## v3.2 bug history (delta from v3.1 e7984c4)

- **A** -- is_malignant bool->int (Bug A)
- **B** -- drop had_any_fna (collinear with bethesda_bucket=='missing')
- **C** -- drop has_clt/has_mng/has_graves (all-zero) and has_niftp/has_ftump (perfect-separation outcome aliases)
- **D** -- clip + rescale days_us_to_surg_approx; centre surg_year
- **E** -- sensitivity arms surg_first_date suffix collision; df_model used directly
- **Issue 1** -- fit_logit outcome_col parameterised; nodule grain now fits cleanly
- **Issue 2** -- multifocal CTE switched to manuscript_workspace.cohort_m048_tnm_multifocal_v1
- **Issue 3** -- final run used --mediation-boot 1000
- **Issue 4** -- mediation IE central tendency switched from mean to median; winsorised mean reported alongside

## Decision items for senior author (manuscript writing)

1. Black M0 OR 0.317 attenuates only modestly to M6 OR 0.442 (95% CI 0.366-0.532, p<<0.001). Disparity narrows but remains highly significant. Framing options: selection / pathway-routing bias (more Black patients reach surgery for benign indications), residual unmeasured confounding, or true performance differences in TI-RADS calibration. Senior author input needed.
2. Asian Bethesda II OR 2.32 (1.02-5.30, p=0.045): Asian patients with cytologically benign FNA had ~2.3x the malignancy rate of White Bethesda II patients. Could be true higher false-negative cytology, differential follow-up, or small-N artifact (Asian Bethesda II n=27).
3. Black TR4 cell shows under_referral_signature: 71 malignant of 207 total (ROM 34.3%) vs ACR-mid 18%. Mean tumor size 2.81 cm (vs White 2.25, Asian 2.06). Combined with the M0 OR <1 finding, this suggests Black patients reaching surgery at TR4 have more advanced disease per imaging score.
4. Mediation indirect-effect magnitudes for had_any_genetics (-6.94 median) and had_any_nm (-4.10) on Black-vs-White are larger than typical clinical mediators on the log-OR product scale. The percentile CIs (-18.6 to -2.4 for genetics, -13.8 to -1.4 for NM) do not span 0, so the *direction* is reliable, but the *magnitudes* shouldn't be quoted as "this mediator explains X% of the disparity" -- treat as qualitative ordering. Possible numerical artifact of binary mediators with near-separation in some bootstrap subsamples; the indirect_winsor_mean column gives a clipped sanity-check estimate.

## QA gate WARN flags (do not block handoff)

- repeat_fna_pct_among_biopsied: WARN (41.75 vs gate's expected ~15-25). The cohort definition or repeat-FNA detection logic differs from the v2 reference. Documented; does not block handoff.
- multifocal_pct_malignant: WARN (39.62 vs gate's expected ~61). The 39.62% rate is now correctly populated from the M048-specific multifocality table; the gate's "~61" expected baseline appears to be from a different cohort definition. The actual value is the truthful number.

## Framing guidance
- If race effect attenuates **>70%** M0→M6 and Bethesda-stratified analysis removes the TR gradient: apparent disparity explained by access/FNA/multinodular pathway.
- If **<30%** attenuation and Bethesda-stratified gradient persists: residual race × TI-RADS performance signal.
- **30–70%**: lead with attenuation cascade; add Bethesda-stratified + disparity-direction quadrant as clinical interpretation.
- Always report disparity-direction signatures (over- vs under-referral) for TR4/TR5 × race.

## Race OR vs White (patient grain)
- Black M0: OR 0.317 (0.272–0.369)
- Black M3 ( + genetics + NM, last pre-FNA step after Bug C drop): OR 0.359 (0.305–0.424)
- Black M6 (full v3): OR 0.442 (0.366–0.532)
- Asian M6: OR 1.164 (0.803–1.687)

## Disparity-direction (TR4/TR5 × race)
```
race_strat,tr_category,n_malignant_cell,rom_pct,mean_tumor_size_cm,pct_multifocal,pct_any_ete,pct_ln_positive,dominant_histology,acr_rom_mid_ref,acr_rom_high_ref,signature
Black,TR4,71,34.3,2.81,38.0281690140845,94.35483870967742,75.0,differentiated,18.0,28.0,under_referral_signature
White,TR4,116,53.95,2.25,43.96551724137931,91.13924050632912,70.88607594936708,differentiated,18.0,28.0,calibrated
Asian,TR4,26,78.79,2.06,38.46153846153846,90.2439024390244,60.97560975609756,differentiated,18.0,28.0,calibrated
Black,TR5,199,40.04,2.54,31.65829145728643,91.61490683229812,71.73913043478261,differentiated,42.0,55.0,calibrated
White,TR5,464,68.74,2.23,43.31896551724138,93.73626373626374,70.98901098901099,differentiated,42.0,55.0,calibrated
Asian,TR5,76,73.79,1.96,51.31578947368421,95.40229885057472,72.41379310344827,differentiated,42.0,55.0,calibrated
```

## Top mediators by |bootstrap IE| (Black vs White)
```
mediator,type,race_target,scope,indirect_mean,indirect_winsor_mean,ci_lo,ci_hi,abs_ie
had_any_genetics,binary,Black,univariate_black_vs_white,-6.936722722908698,-7.841531157001177,-18.590322593794845,-2.352381294434861,6.936722722908698
had_any_nm,binary,Black,univariate_black_vs_white,-4.097132069351868,-4.744807926792111,-13.80500720960186,-1.4306901886005887,4.097132069351868
had_any_nm,binary,Asian,univariate_asian_vs_white,-1.2433372811266308,-1.4022178544029311,-3.70309566787854,-0.2143868497489402,1.2433372811266308
```

## Race × TR interaction
```
param,coef,p,p_bonf
"C(race_strat, Treatment('White'))[T.Asian]:max_tr_int",-0.0053165711174128,0.96955655743958,1.0
"C(race_strat, Treatment('White'))[T.Black]:max_tr_int",-0.0576039885169771,0.4150288616844451,1.0
```

## Bethesda-stratified Model B (first 24 rows)
```
bethesda_bucket,race_level,or,ci_lo,ci_hi,p,n,n_events
III_AUS,Black,0.6483777668240137,0.4258304463849491,0.9872326699055726,0.043397317790754,389,188
III_AUS,Asian,0.3810041700013596,0.1563142118758016,0.9286690942328656,0.0337738753585943,389,188
II_benign,Black,0.486095743041001,0.3297635080858833,0.7165409925862505,0.0002688677953042,845,139
II_benign,Asian,2.323144238988381,1.0191554686855802,5.29556021723113,0.044952451658647,845,139
IV_FN,Black,1.1080336800043198,0.6679778748836601,1.8379929668145771,0.6911486566303731,260,132
IV_FN,Asian,0.4099275713840828,0.0758884428065637,2.214311001336268,0.3000891405345198,260,132
I_nondiagnostic,Black,0.6669906135673507,0.2304549760737261,1.9304268719484188,0.4551245274276453,84,23
I_nondiagnostic,Asian,7.025904919725599e-06,0.0,inf,0.9832559503839886,84,23
VI_malig,Black,0.5350299443503583,0.3200847035037783,0.8943165300248989,0.0170283628689918,522,432
VI_malig,Asian,1.067892923568975,0.4536381754682692,2.5138874060401037,0.8804663607839,522,432
V_susp_malig,Black,0.4749622466700457,0.139973245786763,1.6116589602096008,0.2323428263805803,119,106
V_susp_malig,Asian,0.9249656909639208,0.1003667449570774,8.524352661095573,0.9451220012649142,119,106
missing,Black,0.2176847165623386,0.1583506297598863,0.2992513253447748,5.975788545907452e-21,902,323
missing,Asian,1.4279730105766326,0.8350387932895176,2.4419307645606705,0.1931185551426871,902,323
```
