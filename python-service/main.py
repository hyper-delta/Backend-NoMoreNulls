from fastapi import FastAPI
import pandas as pd
import os
import uvicorn

app = FastAPI()

# ==============================
# 🤖 AI BUSINESS SUMMARY
# ==============================
@app.post("/generate-summary")
def generate_summary(payload: dict):
    """
    Expected payload:
    {
      "tableName": "users",
      "columns": [
        { "name": "id", "type": "int" }
      ]
    }
    """

    table_name = payload.get("tableName")
    columns = payload.get("columns", [])

    summary = (
        f"This table '{table_name}' stores business data "
        f"with {len(columns)} attributes."
    )

    return {
        "tableName": table_name,
        "businessSummary": summary
    }


# ==============================
# 🔥 TIMESTAMP DETECTOR
# ==============================
def detect_timestamp_column(df: pd.DataFrame):
    timestamp_candidates = [
        "created_at",
        "created_on",
        "updated_at",
        "last_updated",
        "modified_date",
        "modified_on",
        "created",
        "created_date",
        "last_updated_date"
    ]

    for col in df.columns:
        if col.lower() in timestamp_candidates:
            return col

    return None


# ==============================
# 😈 DATA QUALITY + FRESHNESS + RISK
# ==============================
@app.post("/analyze-data")
def analyze_data(payload: dict):
    """
    Expected payload from Node:

    {
      "tableName": "users",
      "rows": [
        { "id": 1, "name": "John", "created_at": "2024-01-01" }
      ]
    }
    """

    table_name = payload.get("tableName")
    rows = payload.get("rows", [])

    # Convert to DataFrame
    df = pd.DataFrame(rows)

    # ==============================
    # Column Metrics
    # ==============================
    column_metrics = []

    if not df.empty:
        for col in df.columns:
            completeness = df[col].notnull().mean() * 100
            uniqueness = df[col].nunique() / len(df) * 100

            column_metrics.append({
                "column": col,
                "completeness": round(completeness, 2),
                "uniqueness": round(uniqueness, 2)
            })

    # ==============================
    # Freshness
    # ==============================
    freshness_info = {
        "lastUpdated": None,
        "status": "UNKNOWN"
    }

    timestamp_column = detect_timestamp_column(df)

    if timestamp_column and not df.empty:
        last_updated = df[timestamp_column].max()
        if pd.notna(last_updated):
            freshness_info["lastUpdated"] = str(last_updated)
            freshness_info["status"] = "ACTIVE"
        else:
            freshness_info["status"] = "NO DATA"
    else:
        freshness_info["status"] = "NO TIMESTAMP"

    # ==============================
    # Risks
    # ==============================
    risks = []

    if df.empty:
        risks.append("Table contains no data → Dataset inactive")

    for metric in column_metrics:
        if metric["completeness"] < 50:
            risks.append(
                f"Column '{metric['column']}' has low completeness "
                f"({metric['completeness']}%)"
            )

        if metric["uniqueness"] < 10:
            risks.append(
                f"Column '{metric['column']}' has very low uniqueness "
                f"({metric['uniqueness']}%)"
            )

    if freshness_info["status"] == "NO TIMESTAMP":
        risks.append("No timestamp column detected → Freshness unavailable")

    # ==============================
    # Final Response
    # ==============================
    return {
        "tableName": table_name,
        "metrics": column_metrics,
        "freshness": freshness_info,
        "risks": risks
    }


# ==============================
# 🚀 RUN
# ==============================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)