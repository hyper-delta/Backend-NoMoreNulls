from fastapi import FastAPI
import pandas as pd
import os
import re
import importlib
import importlib.util
import uvicorn

app = FastAPI()


def normalize_column_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).lower()).strip("_")


def build_fallback_summary(table_name: str, columns: list[dict]) -> str:
    normalized_table = normalize_column_name(table_name)

    primary_keys = [col["name"] for col in columns if col.get("isPrimaryKey")]
    foreign_keys = [col["name"] for col in columns if col.get("isForeignKey")]

    id_columns = [
        col["name"]
        for col in columns
        if normalize_column_name(col["name"]).endswith("_id")
        or normalize_column_name(col["name"]) == "id"
    ]
    timestamp_columns = [
        col["name"]
        for col in columns
        if any(
            token in normalize_column_name(col["name"])
            for token in ["date", "time", "created", "updated", "modified"]
        )
    ]
    amount_columns = [
        col["name"]
        for col in columns
        if any(
            token in normalize_column_name(col["name"])
            for token in ["amount", "price", "total", "balance", "cost", "value"]
        )
    ]
    status_columns = [
        col["name"]
        for col in columns
        if any(
            token in normalize_column_name(col["name"])
            for token in ["status", "state", "flag", "active", "enabled"]
        )
    ]

    relationship_signal = (
        "This table appears to work as a relationship/bridge table because its name and schema suggest links between business entities."
        if "relationship" in normalized_table or "mapping" in normalized_table
        else "This table appears to capture a core business entity or transaction record used by operational workflows."
    )

    key_structure = (
        f"Primary key columns: {', '.join(primary_keys)}. "
        if primary_keys
        else "No explicit primary key was detected in metadata, so uniqueness may depend on a composite business key. "
    )
    key_structure += (
        f"Foreign key columns: {', '.join(foreign_keys)}."
        if foreign_keys
        else "No foreign key constraints were found, so relationships may be managed in application logic."
    )

    analytics_use = (
        "Likely reporting usage includes entity counts over time, status distribution analysis, and relationship integrity checks across linked tables."
    )

    signals = []
    if timestamp_columns:
        signals.append(
            f"Lifecycle tracking is likely available through timestamp-like columns ({', '.join(timestamp_columns[:6])})."
        )
    if amount_columns:
        signals.append(
            f"Financial or numeric performance measures may be present via amount/value columns ({', '.join(amount_columns[:6])})."
        )
    if status_columns:
        signals.append(
            f"Process-state monitoring is likely possible using status/flag columns ({', '.join(status_columns[:6])})."
        )
    if id_columns:
        signals.append(
            f"Entity linkage and drill-down reporting can use identifier columns ({', '.join(id_columns[:8])})."
        )

    column_preview = ", ".join(
        f"{col['name']} ({col.get('type', 'unknown')})" for col in columns[:12]
    )
    if len(columns) > 12:
        column_preview += ", ..."

    summary_parts = [
        f"Table '{table_name}' contains {len(columns)} columns and is likely part of the business data model used for downstream analytics and operational reporting.",
        relationship_signal,
        key_structure,
        f"Schema preview: {column_preview if column_preview else 'No columns were provided in the payload.'}",
        *signals,
        analytics_use,
        "Recommended checks: validate key uniqueness, monitor null rates on business-critical fields, and confirm referential consistency with parent tables before using this table in executive dashboards.",
    ]

    return " ".join(summary_parts)


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
        { "name": "id", "type": "int", "isPrimaryKey": true, "isForeignKey": false }
      ]
    }
    """

    table_name = payload.get("tableName")
    columns = payload.get("columns", [])

    gemini_api_key = os.environ.get("GEMINI_API_KEY")
    gemini_model = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")

    if not gemini_api_key:
        return {
            "tableName": table_name,
            "businessSummary": build_fallback_summary(table_name, columns),
        }

    if importlib.util.find_spec("google.generativeai") is None:
        return {
            "tableName": table_name,
            "businessSummary": build_fallback_summary(table_name, columns),
        }

    genai = importlib.import_module("google.generativeai")
    genai.configure(api_key=gemini_api_key)

    prompt = f"""
You are a senior data architect preparing documentation for business and analytics teams.
Generate a detailed, useful summary for ONE table using only the schema clues below.
Do not invent facts; use cautious language like "likely" or "appears to".

Output requirements:
- 140-220 words.
- Single plain-text paragraph (no markdown bullets).
- Cover: business purpose, key entities, data grain, relationship model, likely reporting use-cases, and data quality risks.
- Mention primary/foreign key implications when present.
- Mention any noticeable time/date, status, amount, or identifier signals from columns.

Table name: {table_name}
Columns (JSON-like): {columns}
"""

    try:
        model = genai.GenerativeModel(gemini_model)
        response = model.generate_content(prompt)

        summary = ""
        if response.candidates:
            parts = response.candidates[0].content.parts
            summary = "".join(
                part.text for part in parts if hasattr(part, "text") and part.text
            ).strip()

        if not summary or len(summary.split()) < 90:
            summary = build_fallback_summary(table_name, columns)

        return {
            "tableName": table_name,
            "businessSummary": summary,
        }
    except Exception as error:
        print(f"Gemini summary error for table {table_name}: {error}")
        return {
            "tableName": table_name,
            "businessSummary": build_fallback_summary(table_name, columns),
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
        "last_updated_date",
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

            column_metrics.append(
                {
                    "column": col,
                    "completeness": round(completeness, 2),
                    "uniqueness": round(uniqueness, 2),
                }
            )

    # ==============================
    # Freshness
    # ==============================
    freshness_info = {"lastUpdated": None, "status": "UNKNOWN"}

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
        "risks": risks,
    }


# ==============================
# 🚀 RUN
# ==============================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
