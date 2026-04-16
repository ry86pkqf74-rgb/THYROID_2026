# Canonical Version Registry

The source of truth for which canonical versions exist and what each contains.
Updated automatically by `scripts/225_promote_canonical_version.py`.

| Version | Date       | Type     | Summary                                         | Status |
|---------|------------|----------|-------------------------------------------------|--------|
| v1_0    | 2026-04-16 | baseline | Initial canonical publication (10,871 patients) | active |

## Naming Conventions

| Tier | Pattern |
|------|---------|
| MotherDuck database | `thyroid_canonical_publication_v{X}_{Y}` |
| MotherDuck RC | `thyroid_canonical_publication_v{X}_{Y}_rc` |
| Parquet folder | `data/v{X}_{Y}/` in analysis repo |
| Fabric schema | `thyroid_canonical_publication_v{X}_{Y}` |

Database names use underscores (`v1_0`), never dots (`v1.0`), because SQL
identifiers cannot contain dots.

## Version Semantics

| Bump | Rule | Effect on existing analyses |
|------|------|-----------------------------|
| **Patch** `v1_0` → `v1_0_1` | Additive rows only. Same schema, same shape. | None — analyses keep working unchanged. |
| **Minor** `v1_0` → `v1_1` | New columns or new tables added. Existing columns and tables are unchanged. | None — analyses keep working unchanged. |
| **Major** `v1_0` → `v2_0` | Any breaking change: column removed, renamed, type changed, or semantic change. | Manuscripts must be explicitly migrated before they run against v2_0. |

## Workflow for New Versions

```bash
# 1. Build a release candidate (safe: creates _rc DB, not a release)
python scripts/223_publish_canonical.py --version v1_1 --candidate --skip-ingest

# 2. Compare RC to current release baseline
python scripts/224_compare_canonical_versions.py --from v1_0 --to v1_1_rc

# 3. Review diff_report.md — confirm classification is correct

# 4. Promote RC to release (renames _rc, updates RELEASE.md + CHANGELOG.md, tags repo)
python scripts/225_promote_canonical_version.py --candidate v1_1_rc --release v1_1

# 5. Push tag
git push origin canonical-v1_1

# 6. Add v1_1 to SUPPORTED_VERSIONS in thyroid-2026-analysis/thyroid/connection.py
```

## Active Versions

- **v1_0** — initial analysis baseline; all current manuscripts pinned here

## Release Candidates

*(none currently)*

## Pruned Versions

*(none — pruning happens only after all manuscripts on a version are submitted
AND it has been superseded for at least 60 days)*
