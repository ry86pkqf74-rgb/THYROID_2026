#!/usr/bin/env python3
"""Create MotherDuck Dive via MD_CREATE_DIVE (dev catalog)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import duckdb
from motherduck_client import get_token
from urllib.parse import quote_plus
import os

DBX = "Thyroid 2026 Molecular Dev 20260407"

JSX = r"""
import { useSQLQuery } from "@motherduck/react-sql-query";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";

export default function Dive() {
  const q1 = useSQLQuery(
    `SELECT COALESCE(platform, '(null)') AS platform, COUNT(*)::BIGINT AS n FROM "Thyroid 2026 Molecular Dev 20260407".main.molecular_assay_dictionary GROUP BY 1 ORDER BY n DESC`
  );
  const q2 = useSQLQuery(
    `SELECT entity_value_norm AS label, COUNT(*)::BIGINT AS n FROM "Thyroid 2026 Molecular Dev 20260407".main.note_entities_genetics WHERE entity_type = 'gene' GROUP BY 1 ORDER BY n DESC LIMIT 12`
  );
  const q3 = useSQLQuery(
    `SELECT COALESCE(verification_status, '(null)') AS status, COUNT(*)::BIGINT AS n FROM "Thyroid 2026 Molecular Dev 20260407".main.note_entities_genetics GROUP BY 1 ORDER BY n DESC`
  );

  const err = q1.isError ? q1.error : q2.isError ? q2.error : q3.isError ? q3.error : null;
  if (err) {
    return <div style={{ padding: 16, color: "crimson" }}>Query error: {String(err)}</div>;
  }
  if (q1.isLoading || q2.isLoading || q3.isLoading) {
    return <div style={{ padding: 16 }}>Loading…</div>;
  }

  return (
    <div style={{ padding: 16, display: "grid", gap: 24 }}>
      <h2>THYROID 2026 molecular dev — assays and genetics</h2>
      <section>
        <h3>Assay dictionary — counts by platform</h3>
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={q1.data || []}>
            <XAxis dataKey="platform" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="n" name="assays" fill="#4c6ef5" />
          </BarChart>
        </ResponsiveContainer>
      </section>
      <section>
        <h3>Top gene mentions (note_entities_genetics)</h3>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={q2.data || []} layout="vertical" margin={{ left: 80 }}>
            <XAxis type="number" />
            <YAxis type="category" dataKey="label" width={72} />
            <Tooltip />
            <Bar dataKey="n" name="notes" fill="#2fb344" />
          </BarChart>
        </ResponsiveContainer>
      </section>
      <section>
        <h3>Genetics rows — verification_status (QC proxy)</h3>
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={q3.data || []}>
            <XAxis dataKey="status" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="n" name="rows" fill="#f06595" />
          </BarChart>
        </ResponsiveContainer>
      </section>
    </div>
  );
}
""".strip()


def qi(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def main() -> None:
    token = get_token(prefer_service_account=False)
    qs = f"motherduck_token={quote_plus(token)}"
    ua = os.getenv("MOTHERDUCK_CUSTOM_USER_AGENT", "THYROID_2026_create_dive/1")
    con = duckdb.connect(f"md:?{qs}&custom_user_agent={quote_plus(ua)}")
    try:
        con.execute(f"USE {qi(DBX)}")
        con.execute(
            'SELECT COUNT(*) FROM "Thyroid 2026 Molecular Dev 20260407".main.molecular_assay_dictionary'
        ).fetchone()

        sql = f"""
SELECT id, title, current_version, created_at
FROM MD_CREATE_DIVE(
  title = 'THYROID 2026 Molecular Dev — molecular QC (2026-04-07)',
  description = 'Assay counts by platform, top gene mentions from note_entities_genetics, verification_status distribution.',
  content = $jsx${JSX}$jsx$,
  api_version = 1
)
"""
        df = con.execute(sql).fetchdf()
        print(df.to_string(index=False))
        rid = str(df.iloc[0]["id"])
        print(f"\nhttps://app.motherduck.com/dives/{rid}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
