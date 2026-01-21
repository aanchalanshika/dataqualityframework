import re
import pandas as pd


def _clean_numeric_series(series: pd.Series) -> pd.Series:
    """Clean a numeric series by removing thousands separators, currency symbols,
    and coercing to float."""
    # Keep digits, decimal point, and minus sign; replace others with empty
    cleaned = series.astype(str).str.replace(r"[^0-9\.-]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")


def _parse_dates_with_formats(series: pd.Series, formats: list[str]) -> pd.Series:
    """Try parsing dates using a list of strftime formats; fall back to pandas auto-parse."""
    result = pd.to_datetime(series, errors="coerce")
    # Try explicit formats where auto-parse failed
    if formats:
        mask = result.isna() & series.notna()
        if mask.any():
            s = series.copy()
            # Attempt formats in order until parsed
            parsed = pd.Series([pd.NaT] * len(series))
            for fmt in formats:
                try:
                    attempt = pd.to_datetime(s[mask], format=fmt, errors="coerce")
                    parsed.loc[mask] = attempt
                    # Update mask for remaining failures
                    mask = parsed.isna() & s.notna()
                    if not mask.any():
                        break
                except Exception:
                    # Ignore bad format attempts
                    pass
            # Merge successful parsed values back into result
            filled = result.copy()
            fill_mask = parsed.notna()
            filled.loc[fill_mask] = parsed.loc[fill_mask]
            result = filled
    return result


def standardize_data(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """Standardize data based on schema definitions.

    - Dates: parse using provided parse_formats (or auto-parse) to datetime64[ns]
    - Numerics: strip non-numeric chars and coerce to float
    """
    columns = schema.get("columns", {})
    out = df.copy()

    for col, spec in columns.items():
        if col not in out.columns:
            continue
        col_type = spec.get("type")
        if col_type == "date":
            formats = spec.get("parse_formats") or ([spec.get("format")] if spec.get("format") else [])
            out[col] = _parse_dates_with_formats(out[col], formats)
        elif col_type in {"float", "int"}:
            out[col] = _clean_numeric_series(out[col])
            if col_type == "int":
                # Convert to integer if possible
                out[col] = out[col].dropna().astype(int)
        elif col_type == "string":
            out[col] = out[col].astype(str).str.strip()

    return out
