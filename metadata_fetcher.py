from databricks import sql
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def fetch_metadata():

    print("🔌 Connecting to Databricks...")

    conn=sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )

    query="""
    SELECT
        table_catalog,
        table_schema,
        table_name,
        column_name,
        data_type,
        ordinal_position
    FROM intelliegencedatacatalog.information_schema.columns
    where table_schema = "claims_info" and table_name IN ('gold_patient_provider', 'silver_provider', 'silver_patient_provider')
    ORDER BY table_catalog,table_schema,table_name,ordinal_position
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        rows=cursor.fetchall()
        cols=[d[0] for d in cursor.description]

    print("✅ Metadata query executed")
    print("Rows fetched:",len(rows))

    return pd.DataFrame(rows,columns=cols)






#General Case
# from databricks import sql
# import pandas as pd
# import os
# from dotenv import load_dotenv

# load_dotenv()

# def fetch_metadata():

#     print("🔌 Connecting to Databricks...")

#     conn=sql.connect(
#         server_hostname=os.getenv("DATABRICKS_HOST"),
#         http_path=os.getenv("DATABRICKS_HTTP_PATH"),
#         access_token=os.getenv("DATABRICKS_TOKEN")
#     )

#     all_rows=[]

#     with conn.cursor() as cursor:

#         print("📚 Fetching catalogs...")

#         cursor.execute("SHOW CATALOGS")
#         catalogs=[row[0] for row in cursor.fetchall()]

#         print("Found catalogs:",catalogs)

#         for catalog in catalogs:

#             print(f"\n📂 Processing catalog: {catalog}")

#             try:
#                 query=f"""
#                 SELECT
#                     table_catalog,
#                     table_schema,
#                     table_name,
#                     column_name,
#                     data_type,
#                     ordinal_position
#                 FROM {catalog}.information_schema.columns
#                 ORDER BY table_schema,table_name,ordinal_position
#                 """

#                 cursor.execute(query)
#                 rows=cursor.fetchall()

#                 print(f"   ✅ Rows fetched: {len(rows)}")

#                 all_rows.extend(rows)

#             except Exception as e:
#                 print(f"   ⚠ Skipping catalog {catalog} → {str(e)}")

#     if not all_rows:
#         print("\n⚠ No metadata found across catalogs")
#         return pd.DataFrame()

#     cols=[
#         "table_catalog",
#         "table_schema",
#         "table_name",
#         "column_name",
#         "data_type",
#         "ordinal_position"
#     ]

#     df=pd.DataFrame(all_rows,columns=cols)

#     print("\n✅ Metadata collection complete")
#     print("Total rows:",len(df))

#     return df
