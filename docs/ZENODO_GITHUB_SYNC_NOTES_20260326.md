# Zenodo ↔ GitHub consistency (2026-03-26)

## Current state

| Channel | Role | Identifier |
|---------|------|------------|
| **GitHub** `main` | Living code, study outputs, manuscript revision packets | https://github.com/ry86pkqf74-rgb/THYROID_2026 |
| **Zenodo** | Point-in-time archive of a selected tree (code + bundle) | DOI [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510) |

**Zenodo does not auto-update** when you push to GitHub unless the repository is connected to Zenodo’s GitHub integration *and* you publish a new **GitHub Release** (or you manually upload a new Zenodo version).

## After merging ETE manuscript-revision artifacts

1. **Pull / verify `main` on GitHub** — includes `manuscripts/ete_ajcc8_202603/MANUSCRIPT_REVISION_PACKET_20260326.md` and `manuscripts/ete_ajcc8_202603/revision_rerun_20260326/` (no `.venv`).

2. **Git tag (done for this drop)** — `v2026.03.26-ete-manuscript-revision` is on GitHub; use it as the **GitHub Release** title/tag if Zenodo is wired to releases.

3. **Rebuild Zenodo bundle (local)** — from repo root:
   ```bash
   .venv/bin/python scripts/32_zenodo_archive_prep.py
   ```
   Confirm `studies/proposal2_ete_staging` and `manuscripts/` are copied into the bundle (see `scripts/32_zenodo_archive_prep.py`). Output goes under `exports/zenodo_archive_*` (gitignored).

4. **Publish new Zenodo version** — Zenodo → upload new zip from the rebuilt `exports/zenodo_archive_...` folder **or** trigger via linked GitHub release. Update manuscript “Data availability” if the DOI version number changes.

5. **If DOI is unchanged** — Only metadata/description on Zenodo can be edited without a new version; the **files** behind `10.5281/zenodo.18945510` stay fixed until you mint a **new version** (Zenodo gives a new DOI for the version; concept DOI may stay the same depending on settings).

## CITATION.cff

`CITATION.cff` at repo root should **not** claim results that contradict frozen study outputs. It now describes ETE work generically and points readers to `manuscripts/ete_ajcc8_202603/MANUSCRIPT_REVISION_PACKET_20260326.md` for exact manuscript numbers. When you cut a **new** Zenodo release, update `date-released`, `version`, and `doi` in `CITATION.cff` to match that release.
