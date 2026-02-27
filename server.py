from flask import Flask, jsonify, request
from flask_cors import CORS
from metadata_fetcher import fetch_metadata
from doc_generator import generate_column_desc, generate_table_desc

app = Flask(__name__)
CORS(app)


@app.route("/generate", methods=["GET"])
def generate():

    catalog = request.args.get("catalog")
    schema = request.args.get("schema")
    table = request.args.get("table")

    if not catalog or not schema or not table:
        return jsonify({"error": "Missing parameters. Provide catalog, schema, and table."}), 400

    print(f"\n🚀 Request → {catalog}.{schema}.{table}")

    try:
        df = fetch_metadata(catalog, schema, table)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        print(f"❌ Databricks error: {e}")
        return jsonify({"error": "Failed to connect to Databricks or fetch metadata."}), 500

    if df.empty:
        return jsonify({"error": "No metadata found for this table."}), 404

    columns = df["column_name"].tolist()

    try:
        table_desc = generate_table_desc(f"{catalog}.{schema}.{table}", columns)
    except Exception as e:
        print(f"❌ Table description failed: {e}")
        table_desc = "Description generation failed"

    col_entries = []

    for _, row in df.iterrows():
        col_name = row["column_name"]
        data_type = row["data_type"]

        try:
            col_desc = generate_column_desc(
                f"{catalog}.{schema}.{table}",
                col_name,
                data_type
            )
        except Exception as e:
            print(f"❌ Column {col_name} failed: {e}")
            col_desc = "Description generation failed"

        col_entries.append({
            "column_name": col_name,
            "data_type": data_type,
            "description": col_desc
        })

    return jsonify({
        "catalog": catalog,
        "schema": schema,
        "table": {
            "table_name": table,
            "table_description": table_desc,
            "columns": col_entries
        }
    })


if __name__ == "__main__":
    app.run(port=5000)
