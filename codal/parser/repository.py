import unicodedata

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from fuzzywuzzy import process  # type: ignore[import-untyped]
from pandas.core.frame import DataFrame


def convert_to_float(value: str | float) -> float | None:
    if isinstance(value, str):
        is_negative = False
        if value.startswith("(") and value.endswith(")"):
            is_negative = True
            value = value[1:-1]
        value = value.replace(",", "")
        try:
            value = float(value)
            return -value if is_negative else value
        except ValueError:
            return None
    return value


def normalize_text(text: str) -> str:
    return unicodedata.normalize("NFKC", text).strip().lower()


def find_table_index(report_html_content: str, table_name: str) -> int | None:

    soup = BeautifulSoup(report_html_content, "html.parser")

    target_string = normalize_text(table_name)

    th3 = soup.find_all("h3")

    for i, t in enumerate(th3):
        if t and t.text:
            normalized_text = normalize_text(t.text)
            matched, score = process.extractOne(
                target_string, [normalized_text]
            )
            if score > 96:
                return i
    return None


def get_first_table_after_index(
    report_html_content: str, index: int
) -> pd.DataFrame:
    """
    Retrieves the first table in the HTML content that follows
      the <h3> tag at the specified index.
    """

    soup = BeautifulSoup(report_html_content, "html.parser")
    th3 = soup.find_all("h3")

    target_h3 = th3[index]

    next_table = target_h3.find_next("table")
    if not next_table:
        raise ValueError("No table found after the specified <h3> tag.")

    return pd.read_html(str(next_table))[0]


def extract_variables(table: DataFrame, row_names_map: dict) -> DataFrame:
    values = {}
    try:
        for row_name, var_name in row_names_map.items():
            try:
                table_row_names = table.iloc[:, 0].astype(str).tolist()
                matched, score = process.extractOne(row_name, table_row_names)

                if score >= 95:
                    matched_row = table[table.iloc[:, 0] == matched]

                    values[var_name] = (
                        convert_to_float(matched_row.iloc[0, 1])
                        if not matched_row.empty
                        else None
                    )
                else:
                    values[var_name] = None
            except Exception as e:
                print(f"Error processing row '{row_name}': {e}")
    except Exception as e:
        print(f"Error processing table: {e}")

    return pd.DataFrame.from_dict(values, orient="index", columns=["Value"])


def safe_calc(func, args):
    if any(pd.isna(arg) for arg in args):
        return np.nan
    return func(*args)


def calculate_ratios(df: DataFrame, calculations: dict) -> DataFrame:
    ratios = {
        ratio_name: (
            safe_calc(func, [df["Value"].get(arg, np.nan) for arg in args])
        )
        for ratio_name, (func, args) in calculations.items()
    }

    return pd.DataFrame(ratios, index=[0])


def extract_financial_data(
    report_html_content: str,
    table_names_map: dict,
    calculations: dict,
) -> DataFrame:
    values = pd.DataFrame()
    for name, row_names in table_names_map.items():
        idx = find_table_index(report_html_content, name)
        if idx is not None:
            table = get_first_table_after_index(report_html_content, idx)
            extracted_variables = extract_variables(table, row_names)
            values = pd.concat(
                [values, extracted_variables], ignore_index=False
            )

    values.loc["revenue_per_share"] = values.loc["revenue"] / (
        values.loc["capital"] / 1000
    )
    return calculate_ratios(values, calculations)
