from dataclasses import dataclass

import pandas as pd
import yaml

from pipeline import config
from pipeline.transform import normalize_columns

SYSTEM_COLUMNS = {"sender", "received_at"}


@dataclass
class Issue:
    level: str
    code: str
    message: str


def load_contract(name):
    path = config.ROOT / "contracts" / f"{name}.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def wrong_type(series, expected):
    values = series.dropna().astype(str).str.strip()
    values = values[values != ""]
    if values.empty or expected == "string":
        return False
    if expected == "number":
        parsed = pd.to_numeric(values.str.replace(",", ""), errors="coerce")
    elif expected == "date":
        parsed = pd.to_datetime(values, errors="coerce", format="mixed")
    else:
        return False
    return parsed.isna().mean() > 0.5


def check_schema(df, contract):
    issues = []
    expected = contract["columns"]

    if df is None or len(df) < contract.get("min_rows", 1):
        rows = 0 if df is None else len(df)
        issues.append(Issue("error", "empty_file", f"The file has {rows} row(s), expected at least {contract.get('min_rows', 1)}"))
        if df is None or df.columns.empty:
            return issues

    df = normalize_columns(df)
    found = set(df.columns) - SYSTEM_COLUMNS

    for name, rules in expected.items():
        if name not in found:
            if rules.get("required"):
                issues.append(Issue("error", "missing_column", f"Required column '{name}' is missing"))
            else:
                issues.append(Issue("info", "missing_optional_column", f"Optional column '{name}' is missing"))

    for name in sorted(found - set(expected)):
        issues.append(Issue("warning", "extra_column", f"Unexpected column '{name}' was found"))

    for name, rules in expected.items():
        if name in found and wrong_type(df[name], rules.get("type", "string")):
            issues.append(Issue("warning", "type_changed", f"Column '{name}' should be {rules['type']} but most values are not"))

    return issues


def has_errors(issues):
    return any(i.level == "error" for i in issues)


if __name__ == "__main__":
    from pipeline.sources.email_source import inbox_files, read_attachment

    contract = load_contract("orders")
    for path in inbox_files():
        print(f"\n{path.name}")
        found = check_schema(read_attachment(path), contract)
        for issue in found:
            print(f"  [{issue.level.upper()}] {issue.code}: {issue.message}")
        if not found:
            print("  OK, matches the contract")