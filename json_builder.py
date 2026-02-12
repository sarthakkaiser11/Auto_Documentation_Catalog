# import json
# import time
# from metadata_fetcher import fetch_metadata
# from doc_generator import generate_table_and_columns

# def build_json(catalog_filter=None,schema_filter=None):

#     print("\n🚀 Starting documentation generation...\n")

#     try:
#         df=fetch_metadata()
#     except Exception as e:
#         print("❌ Failed to fetch metadata:",str(e))
#         return

#     print("✅ Metadata fetched successfully")
#     print("Metadata rows:",len(df))

#     if df.empty:
#         print("⚠ No metadata returned from Databricks.")
#         return

#     if catalog_filter:
#         df=df[df["table_catalog"]==catalog_filter]

#     if schema_filter:
#         df=df[df["table_schema"]==schema_filter]

#     if df.empty:
#         print("⚠ No tables left after applying filters.")
#         return

#     final_output=[]

#     grouped=df.groupby(["table_catalog","table_schema","table_name"])

#     for (catalog,schema,table),table_df in grouped:

#         full_table=f"{catalog}.{schema}.{table}"
#         print(f"\n📘 Processing table: {full_table}")

#         columns_with_types=[
#             {
#                 "column_name":row["column_name"],
#                 "data_type":row["data_type"]
#             }
#             for _,row in table_df.iterrows()
#         ]

#         try:
#             doc=generate_table_and_columns(full_table,columns_with_types)

#             table_desc=doc.get("table_description","Generation failed")

#             col_entries=[]
#             for col in doc.get("columns",[]):

#                 col_entries.append({
#                     "column_name":col.get("column_name",""),
#                     "data_type":"",
#                     "description":col.get("description","")
#                 })

#         except Exception as e:
#             print("❌ LLM generation failed:",str(e))
#             table_desc="Generation failed"
#             col_entries=[]

#         table_entry={
#             "table_name":table,
#             "table_description":table_desc,
#             "columns":col_entries
#         }

#         final_output.append({
#             "catalog":catalog,
#             "schema":schema,
#             "table":table_entry
#         })

#         # Prevent rate limits in larger runs
#         time.sleep(0.5)

#     with open("auto_documentation.json","w",encoding="utf-8") as f:
#         json.dump(final_output,f,indent=2,ensure_ascii=False)

#     print("\n✅ Documentation saved to auto_documentation.json\n")


# if __name__=="__main__":
#     build_json()




#Gemini Case
import json
from metadata_fetcher import fetch_metadata
from doc_generator import generate_column_desc,generate_table_desc

def build_json(catalog_filter=None,schema_filter=None):

    print("\n🚀 Starting documentation generation...\n")

    try:
        df=fetch_metadata()
    except Exception as e:
        print("❌ Failed to fetch metadata:",str(e))
        return

    print("✅ Metadata fetched successfully")
    print("Metadata rows:",len(df))

    if df.empty:
        print("⚠ No metadata returned from Databricks.")
        print("Check catalog permissions or query scope.\n")
        return

    if catalog_filter:
        df=df[df["table_catalog"]==catalog_filter]

    if schema_filter:
        df=df[df["table_schema"]==schema_filter]

    if df.empty:
        print("⚠ No tables left after applying filters.")
        return

    final_output=[]

    grouped=df.groupby(["table_catalog","table_schema","table_name"])

    for (catalog,schema,table),table_df in grouped:

        full_table=f"{catalog}.{schema}.{table}"
        print(f"\n📘 Processing table: {full_table}")

        columns=table_df["column_name"].tolist()

        try:
            table_desc=generate_table_desc(full_table,columns)
        except Exception as e:
            print("❌ Table description failed:",str(e))
            table_desc="Description generation failed"

        col_entries=[]

        for _,row in table_df.iterrows():

            col_name=row["column_name"]
            data_type=row["data_type"]

            print(f"   └─ Generating column: {col_name}")

            try:
                col_desc=generate_column_desc(full_table,col_name,data_type)
            except Exception as e:
                print(f"❌ Column {col_name} failed:",str(e))
                col_desc="Description generation failed"

            col_entries.append({
                "column_name":col_name,
                "data_type":data_type,
                "description":col_desc
            })

        table_entry={
            "table_name":table,
            "table_description":table_desc,
            "columns":col_entries
        }

        final_output.append({
            "catalog":catalog,
            "schema":schema,
            "table":table_entry
        })

    with open("auto_documentation.json","w",encoding="utf-8") as f:
        json.dump(final_output,f,indent=2,ensure_ascii=False)

    print("\n✅ Documentation saved to auto_documentation.json\n")


if __name__=="__main__":
    build_json(
        catalog_filter=None,   # Optional: "your_catalog"
        schema_filter=None     # Optional: "your_schema"
    )
