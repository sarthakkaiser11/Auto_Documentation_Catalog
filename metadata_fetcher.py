import re
from databricks import sql
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# ── Regex guard: only allow letters, digits, underscores ──
SAFE_ID = re.compile(r"^[a-zA-Z0-9_]+$")

def _validate(value, label):
    """Reject anything that isn't a safe SQL identifier."""
    if not SAFE_ID.match(value):
        raise ValueError(f"Invalid {label}: '{value}'. Only letters, digits, and underscores allowed.")
    return value

def _connect():
    """Create and return a Databricks SQL connection."""
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )


# ── Mode 1: Single table ──────────────────────────────────
def fetch_metadata(catalog, schema, table):
    """Fetch column metadata for one specific table."""

    _validate(catalog, "catalog")
    _validate(schema, "schema")
    _validate(table, "table")

    print(f"\n🔌 Connecting to Databricks...\n")

    conn = _connect()

    query = f"""
    SELECT
        table_catalog,
        table_schema,
        table_name,
        column_name,
        data_type,
        ordinal_position
    FROM {catalog}.information_schema.columns
    WHERE table_schema = '{schema}'
      AND table_name   = '{table}'
    ORDER BY ordinal_position
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
    finally:
        conn.close()

    print("✅ Metadata query executed")
    print("Rows fetched:", len(rows))

    return pd.DataFrame(rows, columns=cols)


# ── Mode 2: All catalogs ──────────────────────────────────
def fetch_all_metadata():
    """Iterate over every catalog and return metadata for all tables."""

    print("🔌 Connecting to Databricks...")

    conn = _connect()
    all_rows = []

    try:
        with conn.cursor() as cursor:

            print("📚 Fetching catalogs...")
            cursor.execute("SHOW CATALOGS")
            catalogs = [row[0] for row in cursor.fetchall()]
            print("Found catalogs:", catalogs)

            for catalog in catalogs:
                print(f"\n📂 Processing catalog: {catalog}")

                try:
                    query = f"""
                    SELECT
                        table_catalog,
                        table_schema,
                        table_name,
                        column_name,
                        data_type,
                        ordinal_position
                    FROM {catalog}.information_schema.columns
                    ORDER BY table_schema, table_name, ordinal_position
                    """

                    cursor.execute(query)
                    rows = cursor.fetchall()
                    print(f"   ✅ Rows fetched: {len(rows)}")
                    all_rows.extend(rows)

                except Exception as e:
                    print(f"   ⚠ Skipping catalog {catalog} → {e}")
    finally:
        conn.close()

    if not all_rows:
        print("\n⚠ No metadata found across catalogs")
        return pd.DataFrame()

    cols = [
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "data_type",
        "ordinal_position"
    ]

    df = pd.DataFrame(all_rows, columns=cols)

    print("\n✅ Metadata collection complete")
    print("Total rows:", len(df))

    return df
