import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client=OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_column_desc(table, col, data_type):
    prompt = f"""
    Table: {table} | Column: {col} ({data_type})

    Provide a concise business description in Markdown:
    - **Summary**: One sentence on what this is.
    - **Format**: Expected patterns or rules.
    - **Business Rule**: Any logic or constraints.
    """
    response = client.responses.create(model="gpt-5-mini", input=prompt)
    return response.output_text

def generate_table_desc(table, columns):
    col_list = ", ".join(columns)
    prompt = f"""
    Table Name: {table}
    Columns: {col_list}

    Generate a high-level summary in Markdown:
    ### Overview
    (What the table represents)
    
    ### Key Use Cases
    - Bullet 1
    - Bullet 2
    
    ### Data Privacy/Caveats
    - Bullet 1
    """
    response = client.responses.create(model="gpt-5-mini", input=prompt)
    return response.output_text