"""Force-drop and recreate VALIDATION_RUN_LOG_v1 with VARCHAR cols."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")

cur.execute("DROP TABLE IF EXISTS VALIDATION_RUN_LOG_V1")
cur.execute("DROP TABLE IF EXISTS VALIDATION_RUN_LOG_v1")
cur.execute("""
CREATE TABLE VALIDATION_RUN_LOG_V1 (
  RUN_ID NUMBER AUTOINCREMENT,
  RUN_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
  CHECK_NAME VARCHAR,
  EXPECTED VARCHAR,
  OBSERVED VARCHAR,
  STATUS VARCHAR,
  NOTES VARCHAR,
  PRIMARY KEY (RUN_ID)
)
""")
cur.execute("DESC TABLE VALIDATION_RUN_LOG_V1")
print("Recreated. Schema:")
for r in cur.fetchall():
    print(f"  {r[0]:15s} {r[1]}")
ctx.close()
