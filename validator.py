import pandas as pd


def validate_data(df, schema: dict):
    """Build and run validations based on schema (without Great Expectations)."""
    
    results = {
        "success": True,
        "statistics": {
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0
        },
        "results": []
    }
    
    columns = schema.get("columns", {})
    for col, spec in columns.items():
        if col not in df.columns:
            results["success"] = False
            results["statistics"]["evaluated_expectations"] += 1
            results["statistics"]["unsuccessful_expectations"] += 1
            results["results"].append({
                "expectation_type": "column_exists",
                "success": False,
                "column": col
            })
            continue
            
        # Required check
        if spec.get("required"):
            results["statistics"]["evaluated_expectations"] += 1
            nulls = df[col].isna().sum()
            if nulls > 0:
                results["success"] = False
                results["statistics"]["unsuccessful_expectations"] += 1
                results["results"].append({
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "success": False,
                    "column": col,
                    "null_count": int(nulls)
                })
            else:
                results["statistics"]["successful_expectations"] += 1
                results["results"].append({
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "success": True,
                    "column": col
                })
        
        # Type check
        col_type = spec.get("type")
        results["statistics"]["evaluated_expectations"] += 1
        type_valid = True
        
        if col_type == "int":
            type_valid = df[col].dtype in ["int64", "Int64"]
        elif col_type == "float":
            type_valid = df[col].dtype in ["float64", "int64", "Int64"]
        elif col_type == "date":
            type_valid = pd.api.types.is_datetime64_any_dtype(df[col])
        elif col_type == "string":
            type_valid = df[col].dtype == "object"
            
        if type_valid:
            results["statistics"]["successful_expectations"] += 1
        else:
            results["success"] = False
            results["statistics"]["unsuccessful_expectations"] += 1
            
        results["results"].append({
            "expectation_type": "expect_column_type",
            "success": type_valid,
            "column": col,
            "expected_type": col_type
        })
        
        # Numeric constraints
        if col_type in {"int", "float"}:
            if "min" in spec:
                results["statistics"]["evaluated_expectations"] += 1
                below_min = (df[col] < spec["min"]).sum()
                if below_min > 0:
                    results["success"] = False
                    results["statistics"]["unsuccessful_expectations"] += 1
                    results["results"].append({
                        "expectation_type": "expect_column_values_to_be_between",
                        "success": False,
                        "column": col,
                        "min_value": spec["min"],
                        "unexpected_count": int(below_min)
                    })
                else:
                    results["statistics"]["successful_expectations"] += 1
                    results["results"].append({
                        "expectation_type": "expect_column_values_to_be_between",
                        "success": True,
                        "column": col,
                        "min_value": spec["min"]
                    })
    
    return results
