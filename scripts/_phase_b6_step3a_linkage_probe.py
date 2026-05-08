"""Probe candidate linkage chains: direct linked_pathology_tumor_id vs FNA->specimen chain."""
from __future__ import annotations
from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
client = bigquery.Client(project=PROJECT)


def q(sql: str, label: str) -> None:
    print(f"\n=== {label} ===")
    print(sql.strip())
    rows = list(client.query(sql).result())
    for r in rows:
        print("  ", dict(r))


# --- A: direct nodule_id -> linked_pathology_tumor_id coverage ---
q(
    """
    SELECT
      COUNT(*) AS n_nodules_v2,
      COUNTIF(linked_pathology_tumor_id IS NOT NULL) AS n_with_path_link,
      COUNTIF(linked_fna_episode_id IS NOT NULL) AS n_with_fna_link,
      COUNT(DISTINCT linked_pathology_tumor_id) AS n_distinct_path_links
    FROM `pub_canonical.imaging_nodule_long_v2`
    """,
    "A1: imaging_nodule_long_v2 link coverage",
)

q(
    """
    SELECT
      COUNT(*) AS n_us_v2_nodules,
      COUNT(DISTINCT n.nodule_id) AS n_distinct_nodule_ids,
      COUNTIF(il.nodule_id IS NOT NULL) AS n_join_to_inl_v2,
      COUNTIF(il.linked_pathology_tumor_id IS NOT NULL) AS n_with_path_link
    FROM `pub_canonical.canonical_us_nodule_v2` n
    LEFT JOIN `pub_canonical.imaging_nodule_long_v2` il USING (nodule_id)
    """,
    "A2: canonical_us_nodule_v2 -> imaging_nodule_long_v2 join coverage",
)

# --- A3: what does linked_pathology_tumor_id actually look like? ---
q(
    """
    SELECT linked_pathology_tumor_id, COUNT(*) n
    FROM `pub_canonical.imaging_nodule_long_v2`
    WHERE linked_pathology_tumor_id IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 5
    """,
    "A3: sample linked_pathology_tumor_id values",
)

# --- B1: FNA linkage chain ---
q(
    """
    SELECT
      COUNT(*) AS n_links,
      COUNTIF(score_rank = 1) AS n_rank1,
      COUNTIF(analysis_eligible_link_flag) AS n_eligible,
      COUNT(DISTINCT nodule_id) AS n_distinct_nodules,
      COUNT(DISTINCT fna_episode_id) AS n_distinct_fnas
    FROM `pub_canonical.imaging_fna_linkage_v3`
    """,
    "B1: imaging_fna_linkage_v3 stats",
)

# --- B2: how does fna_episode_id (INT64) relate to fna_event_id (STRING)? ---
q(
    """
    SELECT
      'imaging_fna_linkage_v3' AS src, fna_episode_id AS id_value, NULL AS event_id, COUNT(*) n
    FROM `pub_canonical.imaging_fna_linkage_v3` GROUP BY 1,2,3 ORDER BY n DESC LIMIT 3
    """,
    "B2a: imaging_fna_linkage_v3.fna_episode_id sample",
)
q(
    """
    SELECT fna_event_id, fna_index, fna_seq_n, COUNT(*) n
    FROM `pub_canonical.canonical_fna_events_v1`
    GROUP BY 1,2,3 ORDER BY n DESC LIMIT 3
    """,
    "B2b: canonical_fna_events_v1.fna_event_id sample",
)

# --- C: specimen_source_xref_v1 — what domains exist, and does fna show up? ---
q(
    """
    SELECT domain, source_table, COUNT(*) n,
      COUNT(DISTINCT specimen_id) n_distinct_specimens,
      COUNT(DISTINCT source_row_key) n_distinct_source_keys
    FROM `pub_canonical.specimen_source_xref_v1`
    GROUP BY 1,2 ORDER BY n DESC
    """,
    "C1: specimen_source_xref_v1 domains",
)

# --- C2: sample source_row_key for fna domain (if exists) ---
q(
    """
    SELECT domain, source_table, source_row_key, specimen_id, specimen_focus_id
    FROM `pub_canonical.specimen_source_xref_v1`
    WHERE LOWER(domain) LIKE '%fna%' OR LOWER(source_table) LIKE '%fna%'
    LIMIT 5
    """,
    "C2: FNA xref samples",
)

# --- D: nodule_master_id on canonical_us_nodule_v2 vs imaging_exam_id elsewhere? ---
q(
    """
    SELECT
      COUNT(*) AS n,
      COUNTIF(nodule_master_id IS NOT NULL) AS n_with_master_id
    FROM `pub_canonical.canonical_us_nodule_v2`
    """,
    "D1: canonical_us_nodule_v2.nodule_master_id coverage",
)
