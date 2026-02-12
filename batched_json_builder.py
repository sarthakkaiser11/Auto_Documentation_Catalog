# import json
# from metadata_fetcher import fetch_metadata
# from doc_generator import generate_table_and_columns

# def build_json():

#     print("\n🚀 Running BATCHED Documentation Generator\n")

#     df=fetch_metadata()

#     if df.empty:
#         print("❌ No metadata fetched")
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

#         doc=generate_table_and_columns(full_table,columns_with_types)

#         if not doc:
#             print("❌ LLM generation failed")
#             table_desc="Generation failed"
#             col_entries=[]
#         else:
#             table_desc=doc.get("table_description","Generation failed")

#             col_entries=[]
#             for col in doc.get("columns",[]):
#                 col_entries.append({
#                     "column_name":col.get("column_name",""),
#                     "data_type":"",
#                     "description":col.get("description","")
#                 })

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

#     with open("auto_documentation_batched.json","w",encoding="utf-8") as f:
#         json.dump(final_output,f,indent=2,ensure_ascii=False)

#     print("\n✅ Batched documentation saved!\n")

# if __name__=="__main__":
#     build_json()





#New
import json
from metadata_fetcher import fetch_metadata
from doc_generator import generate_table_and_columns

def build_json():

    print("\n🚀 Running BATCHED Documentation Generator\n")

    df=fetch_metadata()

    if df.empty:
        print("❌ No metadata fetched")
        return

    final_output=[]

    grouped=df.groupby(["table_catalog","table_schema","table_name"])

    for (catalog,schema,table),table_df in grouped:

        full_table=f"{catalog}.{schema}.{table}"

        print(f"\n📘 Processing table: {full_table}")

        # ✅ Build datatype lookup from Databricks metadata
        datatype_lookup={
            row["column_name"]:row["data_type"]
            for _,row in table_df.iterrows()
        }

        columns_with_types=[
            {
                "column_name":row["column_name"],
                "data_type":row["data_type"]
            }
            for _,row in table_df.iterrows()
        ]

        doc=generate_table_and_columns(full_table,columns_with_types)

        if not doc:
            print("❌ LLM generation failed")
            table_desc="Generation failed"
            col_entries=[]
        else:
            table_desc=doc.get("table_description","Generation failed")

            col_entries=[]

            for col in doc.get("columns",[]):

                col_name=col.get("column_name","")

                col_entries.append({
                    "column_name":col_name,
                    "data_type":datatype_lookup.get(col_name,"UNKNOWN"),
                    "description":col.get("description","")
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

    with open("auto_documentation_batched.json","w",encoding="utf-8") as f:
        json.dump(final_output,f,indent=2,ensure_ascii=False)

    print("\n✅ Batched documentation saved!\n")

if __name__=="__main__":
    build_json()
