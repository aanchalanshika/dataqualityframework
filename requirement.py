import json
import os  #use to create forlder or directory which we use to store data 
from datetime import datetime

import pandas as pd

from standardizer import standardize_data # importing our own created function
from validator import validate_data


def load_schema(path: str) -> dict:
	with open(path, "r", encoding="utf-8") as f:  #here utf8 makes sures that all type of characters are read properly plus this function reads the json schema properly+ here the "R" stands for read mode which makes sure that it will only read the file and not write on it 
		return json.load(f)


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
				if b:
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


def generate_html_report(output_path: str, source_csv: str, df_flagged: pd.DataFrame, ge_result: dict):
	total = len(df_flagged)
	invalid = (~df_flagged["is_valid"]).sum()
	valid = total - invalid
	timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

	# GE summary
	ge_stats = ge_result.get("statistics", {}) if isinstance(ge_result, dict) else {}
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
		<p><strong>Source:</strong> {source_csv} | <strong>Generated:</strong> {timestamp}</p>
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
	os.makedirs(os.path.dirname(output_path), exist_ok=True)
	with open(output_path, "w", encoding="utf-8") as f:
		f.write(html)


def run_pipeline(
	input_csv: str = os.path.join("data", "rawdata.csv"),
	schema_path: str = os.path.join("config", "schema.json"),
	standardized_csv: str = os.path.join("data", "standardized.csv"),
	invalid_csv: str = os.path.join("data", "invalid_rows.csv"),
	report_html: str = os.path.join("reports", "report.html"),
):
	schema = load_schema(schema_path)
	df_raw = pd.read_csv(input_csv)
	df_std = standardize_data(df_raw, schema)
	ge_result = validate_data(df_std, schema)
	df_flagged = flag_invalid_rows(df_std, schema)

	# Save outputs
	df_std.to_csv(standardized_csv, index=False)
	df_flagged.loc[~df_flagged["is_valid"]].to_csv(invalid_csv, index=False)
	generate_html_report(report_html, input_csv, df_flagged, ge_result)

	return {
		"standardized_csv": standardized_csv,
		"invalid_csv": invalid_csv,
		"report_html": report_html,
		"ge_result_summary": ge_result.get("statistics", {}) if isinstance(ge_result, dict) else {},
	}


if __name__ == "__main__":
	outputs = run_pipeline()
	print("Outputs:")
	for k, v in outputs.items():
		print(f"- {k}: {v}")
