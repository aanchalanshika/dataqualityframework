import json
import os
from datetime import datetime

import pandas as pd

from standardizer import standardize_data
from validator import validate_data

# Load schema
with open("config/schema.json", "r", encoding="utf-8") as f:
    schema = json.load(f)

# Get CSV filename from user
print("=" * 60)
print("   DATA QUALITY FRAMEWORK")
print("=" * 60)
filename = input("\nEnter CSV file name (inside data folder): ").strip()

# Add .csv extension if not provided
if not filename.endswith('.csv'):
    filename += '.csv'

# Construct full path
csv_path = os.path.join("data", filename)

# Check if file exists
if not os.path.exists(csv_path):
    print(f"\n❌ Error: File '{csv_path}' not found!")
    print(f"   Available files in data folder:")
    if os.path.exists("data"):
        for file in os.listdir("data"):
            if file.endswith('.csv'):
                print(f"   - {file}")
    exit(1)

print(f"\n✓ Loading: {csv_path}")

# Load data
df = pd.read_csv(csv_path)

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
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset='utf-8'>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Report</title>
    <style>
      * {{
        margin: 0;
        padding: 0;
        box-sizing: border-box;
      }}
      
      body {{
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        min-height: 100vh;
      }}
      
      .container {{
        max-width: 1200px;
        margin: 0 auto;
        background: white;
        border-radius: 16px;
        box-shadow: 0 20px 60px rgba(0,0,0,0.3);
        padding: 40px;
      }}
      
      h1 {{
        color: #2d3748;
        font-size: 2.5em;
        margin-bottom: 10px;
        border-bottom: 4px solid #667eea;
        padding-bottom: 15px;
      }}
      
      .meta {{
        color: #718096;
        font-size: 0.95em;
        margin-bottom: 30px;
        padding: 10px;
        background: #f7fafc;
        border-left: 4px solid #667eea;
        border-radius: 4px;
      }}
      
      .cards {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 20px;
        margin-bottom: 40px;
      }}
      
      .card {{
        padding: 25px;
        border-radius: 12px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
      }}
      
      .card:hover {{
        transform: translateY(-5px);
        box-shadow: 0 8px 25px rgba(102, 126, 234, 0.6);
      }}
      
      .card-label {{
        font-size: 0.85em;
        opacity: 0.9;
        margin-bottom: 8px;
        text-transform: uppercase;
        letter-spacing: 1px;
      }}
      
      .card-value {{
        font-size: 2.2em;
        font-weight: bold;
      }}
      
      h2 {{
        color: #2d3748;
        font-size: 1.8em;
        margin: 30px 0 20px 0;
        padding-bottom: 10px;
        border-bottom: 2px solid #e2e8f0;
      }}
      
      table {{
        border-collapse: collapse;
        width: 100%;
        margin-top: 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border-radius: 8px;
        overflow: hidden;
      }}
      
      th {{
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 15px;
        text-align: left;
        font-weight: 600;
        text-transform: uppercase;
        font-size: 0.85em;
        letter-spacing: 0.5px;
      }}
      
      td {{
        padding: 12px 15px;
        border-bottom: 1px solid #e2e8f0;
        color: #4a5568;
      }}
      
      tr:hover {{
        background: #f7fafc;
      }}
      
      tr:last-child td {{
        border-bottom: none;
      }}
      
      .error-cell {{
        color: #e53e3e;
        font-weight: 500;
      }}
      
      @media (max-width: 768px) {{
        .container {{
          padding: 20px;
        }}
        
        h1 {{
          font-size: 1.8em;
        }}
        
        .cards {{
          grid-template-columns: 1fr;
        }}
        
        table {{
          font-size: 0.85em;
        }}
        
        th, td {{
          padding: 8px;
        }}
      }}
      
      .footer {{
        margin-top: 40px;
        text-align: center;
        color: #a0aec0;
        font-size: 0.85em;
        padding-top: 20px;
        border-top: 1px solid #e2e8f0;
      }}
    </style>
  </head>
  <body>
    <div class="container">
      <h1>📊 Data Quality Report</h1>
      <div class="meta">
        <strong>Source:</strong> {csv_path} | <strong>Generated:</strong> {timestamp}
      </div>
      
      <div class="cards">
        <div class="card">
          <div class="card-label">Total Rows</div>
          <div class="card-value">{total}</div>
        </div>
        <div class="card">
          <div class="card-label">Valid Rows</div>
          <div class="card-value">{valid}</div>
        </div>
        <div class="card">
          <div class="card-label">Invalid Rows</div>
          <div class="card-value">{invalid}</div>
        </div>
        <div class="card">
          <div class="card-label">Expectations</div>
          <div class="card-value">{ge_success}/{ge_total}</div>
        </div>
      </div>
      
      <h2>❌ Invalid Records</h2>
      {invalid_table}
      
      <div class="footer">
        Generated by Data Quality Framework | Python & Pandas
      </div>
    </div>
  </body>
</html>
"""

os.makedirs("reports", exist_ok=True)
with open("reports/data_quality_report.html", "w", encoding="utf-8") as f:
    f.write(html)

print("✅ Data Quality Pipeline Executed Successfully")
print(f"   Source: {csv_path}")
print(f"   Total: {total} | Valid: {valid} | Invalid: {invalid}")
print(f"   Report: reports/data_quality_report.html")
print("\n" + "=" * 60)