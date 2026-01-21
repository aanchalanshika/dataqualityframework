import json
import os
from datetime import datetime

import pandas as pd

from standardizer import standardize_data
from validator import validate_data

# Load schema
with open("config/schema.json", "r", encoding="utf-8") as f:
    schema = json.load(f)

# Load data (correct filename)
df = pd.read_csv("data/rawdata.csv")

# Standardize data (now requires schema)
df_standardized = standardize_data(df, schema)

# Validate data (now requires schema, returns GE validation result dict)
validation_results = validate_data(df_standardized, schema)

# Flag invalid records based on schema
def flag_invalid_rows(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """Add columns 'is_valid' and 'errors' based on schema checks."""
    columns = schema.get("columns", {})
    errors = [[] for _ in range(len(df))]
    is_valid = [True] * len(df)

    for col, spec in columns.items():
        if col not in df.columns:
            for i in range(len(df)):
                errors[i].append(f"missing column: {col}")
                is_valid[i] = False
            continue
        series = df[col]
        # Required
        if spec.get("required"):
            nulls = series.isna()
            for i, is_null in enumerate(nulls.tolist()):
                if is_null:
                    errors[i].append(f"{col} is required")
                    is_valid[i] = False
        # Type-specific checks
        t = spec.get("type")
        if t == "date":
            bad = series.isna()
            for i, b in enumerate(bad.tolist()):
                if b and spec.get("required"):
                    errors[i].append(f"{col} invalid date")
                    is_valid[i] = False
        elif t in {"int", "float"}:
            # min/max constraints
            if "min" in spec:
                bad = series < spec["min"]
                for i, b in enumerate(bad.fillna(False).tolist()):
                    if b:
                        errors[i].append(f"{col} below min {spec['min']}")
                        is_valid[i] = False
            if "max" in spec:
                bad = series > spec["max"]
                for i, b in enumerate(bad.fillna(False).tolist()):
                    if b:
                        errors[i].append(f"{col} above max {spec['max']}")
                        is_valid[i] = False

    flagged = df.copy()
    flagged["is_valid"] = pd.Series(is_valid, dtype=bool)
    flagged["errors"] = pd.Series(["; ".join(err) if err else "" for err in errors], dtype=object)
    return flagged

df_flagged = flag_invalid_rows(df_standardized, schema)

# Save cleaned data
df_standardized.to_csv("data/cleaned_data.csv", index=False)

# Generate HTML report
total = len(df_flagged)
invalid = (~df_flagged["is_valid"]).sum()
valid = total - invalid
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# GE summary
ge_stats = validation_results.get("statistics", {})
ge_success = ge_stats.get("successful_expectations", 0)
ge_total = ge_stats.get("evaluated_expectations", 0)

invalid_rows_html = df_flagged.loc[~df_flagged["is_valid"]]
invalid_table = invalid_rows_html.to_html(index=False, escape=False)

html = f"""
<html>
  <head>
    <meta charset='utf-8'>
    <title>Data Quality Report</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 24px; }}
      .cards {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }}
      .card {{ padding: 12px; border: 1px solid #ddd; border-radius: 8px; }}
      table {{ border-collapse: collapse; width: 100%; }}
      th, td {{ border: 1px solid #ddd; padding: 8px; }}
      th {{ background: #f6f6f6; }}
    </style>
  </head>
  <body>
    <h1>Data Quality Report</h1>
    <p><strong>Source:</strong> data/rawdata.csv | <strong>Generated:</strong> {timestamp}</p>
    <div class="cards">
      <div class="card"><strong>Total Rows:</strong><br/>{total}</div>
      <div class="card"><strong>Valid Rows:</strong><br/>{valid}</div>
      <div class="card"><strong>Invalid Rows:</strong><br/>{invalid}</div>
      <div class="card"><strong>GE Expectations:</strong><br/>{ge_success}/{ge_total} passed</div>
    </div>
    <h2>Invalid Records</h2>
    {invalid_table}
  </body>
</html>
"""

os.makedirs("reports", exist_ok=True)
with open("reports/data_quality_report.html", "w", encoding="utf-8") as f:
    f.write(html)

print("✅ Data Quality Pipeline Executed Successfully")
print(f"   Total: {total} | Valid: {valid} | Invalid: {invalid}")
print(f"   Report: reports/data_quality_report.html")