from fastapi import FastAPI
import pandas as pd
import os
import re
import importlib
import importlib.util
import uvicorn

app = FastAPI()


# ==============================
# 🔧 HELPERS
# ==============================
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
        "This table appears to function as a relationship or bridge table linking business entities."
        if "relationship" in normalized_table or "mapping" in normalized_table
        else "This table appears to capture a core business entity or transactional record."
    )

    key_structure = (
        f"Primary key columns: {', '.join(primary_keys)}. "
        if primary_keys
        else "No explicit primary key detected; uniqueness may rely on composite logic. "
    )

    key_structure += (
        f"Foreign key columns: {', '.join(foreign_keys)}."
        if foreign_keys
        else "No foreign key constraints detected."
    )

    column_preview = ", ".join(
        f"{col['name']} ({col.get('type', 'unknown')})"
        for col in columns[:12]
    )

    if len(columns) > 12:
        column_preview += ", ..."

    signals = []
    if timestamp_columns:
        signals.append(
            f"Timestamp indicators include: {', '.join(timestamp_columns[:6])}."
        )
    if amount_columns:
        signals.append(
            f"Financial indicators include: {', '.join(amount_columns[:6])}."
        )
    if status_columns:
        signals.append(
            f"Status indicators include: {', '.join(status_columns[:6])}."
        )
    if id_columns:
        signals.append(
            f"Identifier columns include: {', '.join(id_columns[:8])}."
        )

    summary_parts = [
        f"Table '{table_name}' contains {len(columns)} columns and is likely part of the operational business data model.",
        relationship_signal,
        key_structure,
        f"Schema preview: {column_preview if column_preview else 'No columns provided.'}",
        *signals,
        "Recommended checks include validating key uniqueness, monitoring null rates, and verifying referential integrity before use in executive reporting.",
    ]

    return " ".join(summary_parts)


# ==============================
# 🤖 AI BUSINESS SUMMARY
# ==============================
@app.post("/generate-summary")
def generate_summary(payload: dict):

    table_name = payload.get("tableName")
    columns = payload.get("columns", [])

    gemini_api_key = os.environ.get("GEMINI_API_KEY")
    gemini_model = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")

    if not gemini_api_key:
        return {
            "tableName": table_name,
            "businessSummary": build_fallback_summary(table_name, columns),
        }

    # Check if Gemini package exists
    if importlib.util.find_spec("google.generativeai") is None:
        return {
            "tableName": table_name,
            "businessSummary": build_fallback_summary(table_name, columns),
        }

    try:
        genai = importlib.import_module("google.generativeai")
        genai.configure(api_key=gemini_api_key)

        prompt = f"""
You are a senior data architect preparing documentation for business and analytics teams.
Generate a detailed but grounded summary for ONE database table.
Use cautious language like "likely" or "appears to".
Do not invent facts beyond schema clues.

Requirements:
- 140–220 words
- Single plain paragraph
- Cover business purpose, key entities, relationships, reporting usage, and data risks

Table name: {table_name}
Columns: {columns}
"""

        model = genai.GenerativeModel(gemini_model)
        response = model.generate_content(prompt)

        summary = ""

        if response.candidates:
            parts = response.candidates[0].content.parts
            summary = "".join(
                part.text for part in parts
                if hasattr(part, "text") and part.text
            ).strip()

        # Fallback if AI returns weak result
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
        "created_at", "created_on", "updated_at",
        "last_updated", "modified_date", "modified_on",
        "created", "created_date", "last_updated_date"
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

    table_name = payload.get("tableName")
    rows = payload.get("rows", [])

    df = pd.DataFrame(rows)

    column_metrics = []

    if not df.empty:
        for col in df.columns:
            completeness = df[col].notnull().mean() * 100
            uniqueness = df[col].nunique() / len(df) * 100

            column_metrics.append({
                "column": col,
                "completeness": round(completeness, 2),
                "uniqueness": round(uniqueness, 2),
            })

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

    risks = []

    if df.empty:
        risks.append("Table contains no data → Dataset inactive")

    for metric in column_metrics:
        if metric["completeness"] < 50:
            risks.append(
                f"Column '{metric['column']}' has low completeness ({metric['completeness']}%)"
            )
        if metric["uniqueness"] < 10:
            risks.append(
                f"Column '{metric['column']}' has very low uniqueness ({metric['uniqueness']}%)"
            )

    if freshness_info["status"] == "NO TIMESTAMP":
        risks.append("No timestamp column detected → Freshness unavailable")

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