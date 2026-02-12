# import os
# import json
# from openai import OpenAI
# from dotenv import load_dotenv

# load_dotenv()

# client=OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# def generate_table_and_columns(table,columns_with_types):

#     col_text="\n".join(
#         [f"{c['column_name']} ({c['data_type']})" for c in columns_with_types]
#     )

#     prompt=f"""
# You are a data documentation expert.

# Table: {table}

# Columns:
# {col_text}

# Generate:

# 1. Business-friendly table description
# 2. JSON array of column descriptions

# Return STRICT JSON:

# {{
#   "table_description":"...",
#   "columns":[
#     {{
#       "column_name":"...",
#       "description":"..."
#     }}
#   ]
# }}
# """

#     resp=client.responses.create(
#         model="gpt-5-nano",
#         input=prompt
#     )
#     print(resp)

#     text=resp.output[0].content[0].text
    
#     try:
#         return json.loads(text)
#     except:
#         print("⚠ Model returned non-JSON:\n",text)
#         return {
#             "table_description":"Generation failed",
#             "columns":[]
#         }






# GenAi Version
# import google.generativeai as genai
# import os
# from dotenv import load_dotenv
# from openai import OpenAI

# load_dotenv()
# model=OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
# # genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# # model=genai.GenerativeModel("gemini-2.5-flash")

# def generate_column_desc(table,col,data_type):
#     prompt=f"""
#     Table: {table}
#     Column: {col}
#     Data Type: {data_type}

#     Provide a concise business description explaining
#     the meaning and expected format of this column.
#     """
#     return model.generate_content(prompt).text

# def generate_table_desc(table,columns):
#     col_list=", ".join(columns)

#     prompt=f"""
#     Table Name: {table}
#     Columns: {col_list}

#     Generate a business-friendly description explaining
#     what this table represents and what data it contains.
#     """
#     return model.generate_content(prompt).text




# def generate_column_desc(table,col,data_type):
#     return f"Column {col} in {table}"

# def generate_table_desc(table,columns):
#     return f"Table description for {table}"




#Gaurav Version
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client=OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_column_desc(table,col,data_type):

    prompt=f"""
    Table: {table}
    Column: {col}
    Data Type: {data_type}

    Provide a concise business description explaining
    the meaning and expected format of this column.
    """

    response=client.responses.create(
        model="gpt-5-mini",
        input=prompt
    )

    return response.output_text


def generate_table_desc(table,columns):

    col_list=", ".join(columns)

    prompt=f"""
    Table Name: {table}
    Columns: {col_list}

    Generate a business-friendly description explaining
    what this table represents and what data it contains.
    """

    response=client.responses.create(
        model="gpt-5-mini",
        input=prompt
    )

    return response.output_text
