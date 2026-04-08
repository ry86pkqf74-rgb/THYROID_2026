#!/usr/bin/env python3
"""READ-ONLY MotherDuck evidence export for live_state_refresh (no DDL/DML)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

STUDY = Path(__file__).resolve().parent
DB_FALLBACK = ROOT / "thyroid_master.duckdb"

import pandas as pd  # noqa: E402

import motherduck_client as mc  # noqa: E402
from motherduck_client import MotherDuckClient, resolve_database_for_env  # noqa: E402


def _safe_df(con, sql: str) -> pd.DataFrame:
    try:
        return con.execute(sql).df()
    except Exception as e:
        return pd.DataFrame([{"error": str(e)}])


def _connect_primary(*, prefer_sa: bool):
    from utils.md_connect import connect_md_fail_closed

    return connect_md_fail_closed(
        DB_FALLBACK,
        prefer_service_account=prefer_sa,
        custom_user_agent=os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT"),
        motherduck_session_hint=os.environ.get("MOTHERDUCK_SESSION_HINT"),
    )


def _connect_rs():
    cli = MotherDuckClient.for_env(
        "prod",
        use_service_account=False,
        custom_user_agent=os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT"),
        motherduck_session_hint=os.environ.get("MOTHERDUCK_SESSION_HINT"),
    )
    hint = (os.environ.get("MD_READ_SCALING_SESSION_HINT") or "").strip() or None
    return cli.connect_read_scaling(session_hint=hint)


def main() -> int:
    prefer_sa = mc.token_mode() in (
        "env:MD_SA_TOKEN",
        "motherduck.local.toml:MD_SA_TOKEN",
        "secrets.toml:MD_SA_TOKEN",
    )
    rw = mc.get_token() is not None
    rs = mc.get_read_scaling_token() is not None

    metrics_rows: list[dict] = []

    con = None
    mode = ""
    rs_err = ""
    # Prefer read-scaling for SELECT-only exports when available; fall back to RW on attach conflict.
    if rs:
        try:
            con = _connect_rs()
            mode = "read_scaling"
        except Exception as exc:
            rs_err = str(exc)
            print(f"read_scaling connect failed; falling back to RW. ({rs_err})", file=sys.stderr)
            con = None
    if con is None and rw:
        con = _connect_primary(prefer_sa=prefer_sa)
        mode = "rw_fail_closed" if not rs_err else "rw_fail_closed_after_rs_error"
    if con is None:
        print("No MotherDuck token resolved (neither RW nor read_scaling).", file=sys.stderr)
        return 1

    try:
        db_row = con.execute(
            "SELECT current_database() AS current_database, current_catalog() AS current_catalog"
        ).fetchdf()
        metrics_rows.append({"metric": "connection_mode", "value": mode})
        metrics_rows.append(
            {
                "metric": "current_database",
                "value": str(db_row.iloc[0]["current_database"]),
            }
        )
        metrics_rows.append(
            {
                "metric": "current_catalog",
                "value": str(db_row.iloc[0]["current_catalog"]),
            }
        )

        pragma = _safe_df(con, "PRAGMA database_list")
        pragma.to_csv(STUDY / "pragma_database_list.csv", index=False)

        # Release manifest
        rm = _safe_df(
            con,
            """
            SELECT * FROM qa.release_manifest
            ORDER BY created_at DESC NULLS LAST
            LIMIT 50
            """,
        )
        rm.to_csv(STUDY / "release_manifest_latest.csv", index=False)

        mrq = _safe_df(
            con,
            """
            SELECT verification_status, COUNT(*)::BIGINT AS n
            FROM qa.manual_review_queue
            GROUP BY 1
            ORDER BY n DESC
            """,
        )
        mrq.to_csv(STUDY / "mrq_status_distribution.csv", index=False)

        null_ct = _safe_df(
            con,
            """
            SELECT COUNT(*)::BIGINT AS n_null_verification_status
            FROM qa.manual_review_queue
            WHERE verification_status IS NULL
            """,
        )
        null_ct.to_csv(STUDY / "mrq_null_verification_count.csv", index=False)

        promo = _safe_df(
            con, "SELECT COUNT(*)::BIGINT AS n FROM qa.promotion_review_decisions"
        )
        promo.to_csv(STUDY / "promotion_review_decisions_count.csv", index=False)

        waves = _safe_df(
            con,
            """
            SELECT COALESCE(ingestion_wave::VARCHAR, '(null)') AS ingestion_wave,
                   COUNT(*)::BIGINT AS n
            FROM main.longitudinal_lab_canonical_v1
            GROUP BY 1
            ORDER BY n DESC
            """,
        )
        waves.to_csv(STUDY / "lab_wave_distribution.csv", index=False)

        analyte = _safe_df(
            con,
            """
            SELECT COALESCE(analyte_group::VARCHAR, '(null)') AS analyte_group,
                   COUNT(*)::BIGINT AS n
            FROM main.longitudinal_lab_canonical_v1
            GROUP BY 1
            ORDER BY n DESC
            """,
        )
        analyte.to_csv(STUDY / "lab_analyte_distribution.csv", index=False)

        # Specimen / FHIR diagnostics
        diag_sql = """
        SELECT metric, n_value FROM (
          SELECT 'broken_fhir_refs' AS metric,
                 COUNT(*)::BIGINT AS n_value
            FROM qa.v_diag_specimen_fhir_broken_refs_v1
          UNION ALL
          SELECT 'high_tier_null_spec',
                 COALESCE(MAX(n_high_tier_null_specimen), 0)::BIGINT
            FROM qa.v_diag_specimen_provenance_genomic_v1
          UNION ALL
          SELECT 'dup_master_fp',
                 COUNT(*)::BIGINT FROM qa.v_diag_specimen_duplicate_master_fp_v1
          UNION ALL
          SELECT 'dup_focus_fp_groups',
                 COUNT(*)::BIGINT FROM qa.v_diag_specimen_duplicate_focus_fp_v1
          UNION ALL
          SELECT 'orphan_focus_master',
                 COUNT(*)::BIGINT FROM qa.v_diag_specimen_orphan_focus_master_v1
          UNION ALL
          SELECT 'orphan_genomic_master',
                 COUNT(*)::BIGINT FROM qa.v_diag_specimen_orphan_genomic_master_v1
          UNION ALL
          SELECT 'orphan_genomic_focus',
                 COUNT(*)::BIGINT FROM qa.v_diag_specimen_orphan_genomic_focus_v1
        ) q
        """
        diag = _safe_df(con, diag_sql)
        diag.to_csv(STUDY / "specimen_fhir_diag_counts.csv", index=False)

        md_dbs = _safe_df(con, "SELECT * FROM MD_INFORMATION_SCHEMA.DATABASES ORDER BY name")
        md_dbs.to_csv(STUDY / "md_information_schema_databases.csv", index=False)

        md_snaps = _safe_df(
            con, "SELECT * FROM MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS ORDER BY created_ts DESC LIMIT 200"
        )
        md_snaps.to_csv(STUDY / "md_information_schema_database_snapshots.csv", index=False)

        schemas = _safe_df(
            con,
            """
            SELECT catalog_name, schema_name
            FROM information_schema.schemata
            WHERE schema_name LIKE 'release_%' OR schema_name IN ('main','qa','v2_stage')
            ORDER BY catalog_name, schema_name
            """,
        )
        schemas.to_csv(STUDY / "schemas_release_subset.csv", index=False)

        qh_sql = """
        SELECT query_id, start_time, end_time, total_elapsed_time, error_type,
               user_agent, session_name, query_type,
               substring(query_text, 1, 320) AS query_text_preview
        FROM MD_INFORMATION_SCHEMA.QUERY_HISTORY
        WHERE user_agent ILIKE '%thyroid_live_state_refresh%'
        ORDER BY start_time DESC
        LIMIT 500
        """
        try:
            qh = con.execute(qh_sql).df()
        except Exception as e:
            qh = pd.DataFrame(
                [
                    {
                        "query_id": "",
                        "start_time": "",
                        "end_time": "",
                        "total_elapsed_time": "",
                        "error_type": "permission_or_tier",
                        "user_agent": "",
                        "session_name": "",
                        "query_type": "",
                        "query_text_preview": str(e),
                    }
                ]
            )
        qh.to_csv(STUDY / "query_history_filtered.csv", index=False)

        # Cross-env row counts (best-effort; separate connections)
        env_lines = []
        if rw:
            for env in ("dev", "qa", "prod"):
                c2 = None
                dbn = resolve_database_for_env(env)
                try:
                    c2 = MotherDuckClient.for_env(
                        env,
                        use_service_account=prefer_sa,
                        custom_user_agent=os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT"),
                        motherduck_session_hint=os.environ.get("MOTHERDUCK_SESSION_HINT"),
                    ).connect_rw()
                    for label, sql in (
                        ("main.longitudinal_lab_canonical_v1", "SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1"),
                        ("qa.manual_review_queue", "SELECT COUNT(*) FROM qa.manual_review_queue"),
                    ):
                        try:
                            n = int(c2.execute(sql).fetchone()[0])
                            env_lines.append(
                                {
                                    "env": env,
                                    "database": dbn,
                                    "object": label,
                                    "row_count": n,
                                    "error": "",
                                }
                            )
                        except Exception as e:
                            env_lines.append(
                                {
                                    "env": env,
                                    "database": dbn,
                                    "object": label,
                                    "row_count": "",
                                    "error": str(e),
                                }
                            )
                except Exception as e:
                    env_lines.append(
                        {
                            "env": env,
                            "database": dbn,
                            "object": "_connect",
                            "row_count": "",
                            "error": str(e),
                        }
                    )
                finally:
                    if c2 is not None:
                        c2.close()
        pd.DataFrame(env_lines).to_csv(STUDY / "env_row_counts_probe.csv", index=False)

        if "n" in mrq.columns and len(mrq) and mrq["n"].dtype != object:
            metrics_rows.append(
                {"metric": "mrq_total_rows", "value": str(int(mrq["n"].sum()))}
            )
        for _, row in promo.iterrows():
            metrics_rows.append(
                {"metric": "promotion_review_decisions_count", "value": str(row.iloc[0])}
            )
        for _, row in null_ct.iterrows():
            metrics_rows.append(
                {
                    "metric": "mrq_null_verification_status",
                    "value": str(row.iloc[0]),
                }
            )
        if len(diag) and "error" not in diag.columns:
            for _, r in diag.iterrows():
                metrics_rows.append(
                    {"metric": f"specimen_diag_{r.iloc[0]}", "value": str(r.iloc[1])}
                )
        pd.DataFrame(metrics_rows).to_csv(STUDY / "motherduck_metrics.csv", index=False)

    finally:
        con.close()

    print(f"OK collect_evidence mode={mode} study={STUDY.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
