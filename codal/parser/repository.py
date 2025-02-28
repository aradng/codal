import logging
import unicodedata
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from fuzzywuzzy import process  # type: ignore[import-untyped]
from jdatetime import date as JalaliDate
from pandas.core.frame import DataFrame

from codal.parser.exceptions import IncompatibleFormatError
from codal.parser.schemas import PriceCollection
from codal.settings import settings

logger = logging.getLogger(__name__)


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
    report_html_content: str, index: int, before98: bool = False
) -> pd.DataFrame:
    """
    Retrieves the first table in the HTML content that follows
      the <h3> tag at the specified index.
    """

    soup = BeautifulSoup(report_html_content, "html.parser")
    th3 = soup.find_all("h3")

    target_h3 = th3[index]

    if before98:
        table_holder = target_h3.find_next("div", class_="table_holder")
        tables = table_holder.find_all("table")
        if len(tables) >= 2:
            next_table = tables[1]
        else:
            next_table = tables[0]
    else:
        next_table = target_h3.find_next("table")
    if not next_table:
        raise ValueError("No table found after the specified <h3> tag.")

    return pd.read_html(str(next_table))[0]


def find_row_for_variable(
    table: DataFrame,
    values: dict[str, Any],
    col: int,
    row_name: str,
    var_name: str,
):
    try:
        table_row_names = table.iloc[:, col].astype(str).tolist()
    except IndexError:
        return
    matched, score = process.extractOne(row_name, table_row_names)

    if values.get(var_name) is None:
        if score >= 95:
            idx = table.iloc[:, col] == matched
            matched_row: DataFrame = table[idx]
            var = matched_row.iloc[0, col + 1]
            if pd.isna(var):
                table.drop(index=matched_row.index[0], inplace=True)
                find_row_for_variable(table, values, col, row_name, var_name)
            else:
                values[var_name] = (
                    convert_to_float(var) if not matched_row.empty else None
                )
        else:
            values[var_name] = None


def extract_variables(
    tables: dict[str, DataFrame], table_names_map: dict
) -> DataFrame:
    values: dict = {}
    unmatched_tables_count = 3
    try:
        for map_key, mapping in table_names_map.items():
            if tables.get(map_key) is not None:
                unmatched_tables_count -= 1
                for row_name, var_name in mapping.items():
                    try:
                        find_row_for_variable(
                            tables[map_key], values, 0, row_name, var_name
                        )
                    except Exception as e:
                        print(f"Error processing row '{row_name}': {e}")
    except Exception as e:
        print(f"Error processing table: {e}")
    if unmatched_tables_count > 1:
        raise IncompatibleFormatError(
            "Report standard format is not compatible with the given mapping"
        )

    return pd.DataFrame.from_dict(values, orient="index", columns=["Value"])


def safe_calc(func, args):
    if any(pd.isna(arg) for arg in args):
        return np.nan
    return func(*args)


def calc_financial_ratios(df: DataFrame, calculations: dict) -> pd.Series:
    ratios = {
        ratio_name: (
            safe_calc(func, [df["Value"].get(arg, np.nan) for arg in args])
        )
        for ratio_name, (func, args) in calculations.items()
    }

    return pd.Series(ratios)


def extract_financial_data(
    tables: dict,
    table_names_map: dict,
    calculations: dict,
    jdate: JalaliDate,
    symbol: str,
) -> pd.Series:

    extracted_variables = extract_variables(tables, table_names_map)

    extracted_variables.loc["revenue_per_share"] = extracted_variables.loc[
        "revenue"
    ] / (extracted_variables.loc["capital"] / 1000)

    price_collection = collect_prices(jdate, symbol)
    extracted_variables = pd.concat(
        [
            extracted_variables,
            pd.DataFrame(price_collection.model_dump(), index=["Value"]).T,
        ]
    )

    return calc_financial_ratios(extracted_variables, calculations)


def get_price(
    prices: DataFrame,
    price_date: JalaliDate,
    column: str,
    date_format: str = "%Y/%m/%d",
    max_lookup_attempts: int = 60,
) -> float:
    """
    Generic function to retrieve stock, gold, oil, or USD prices.
    """
    price = prices.loc[
        prices["jdate"] == price_date.strftime(format=date_format)
    ][column]

    if not price.empty:
        return (
            float(price.values[0].replace(",", ""))
            if isinstance(price.values[0], str)
            else price.values[0]
        )

    for i in range(1, max_lookup_attempts + 1):

        # Check previous date (T - i)
        prev_date = price_date - timedelta(i)
        price = prices.loc[
            prices["jdate"] == prev_date.strftime(format=date_format)
        ][column]
        if not price.empty:
            return (
                float(price.values[0].replace(",", ""))
                if isinstance(price.values[0], str)
                else price.values[0]
            )

        # Check next date (T + i)
        next_date = price_date + timedelta(i)
        price = prices.loc[
            prices["jdate"] == next_date.strftime(format=date_format)
        ][column]
        if not price.empty:
            return (
                float(price.values[0].replace(",", ""))
                if isinstance(price.values[0], str)
                else price.values[0]
            )

    return np.nan


def safe_price_extraction(
    file_path: str,
    jdate: JalaliDate,
    column: str,
    date_format: str = "%Y/%m/%d",
) -> tuple[float, float]:
    """
    Reads a CSV file and retrieves the price for the given date and base year.
    Returns (price, base_price) or (np.nan, np.nan) in case of errors.
    """
    try:
        with open(file_path) as f:
            prices = pd.read_csv(f)
        price = get_price(
            prices, jdate, column=column, date_format=date_format
        )
        base_price = get_price(
            prices,
            JalaliDate(settings.BASE_YEAR, 1, 1),
            column=column,
            date_format=date_format,
        )
        return price, base_price
    except Exception as e:
        logger.warning(f"Error fetching price from {file_path}: {e}")
        return np.nan, np.nan


def collect_prices(jdate: JalaliDate, symbol: str) -> PriceCollection:
    stock_price, base_stock_price = safe_price_extraction(
        settings.TSETMC_STOCKS_CSV_PATH, jdate, symbol, "%Y-%m-%d"
    )
    gold_price, base_gold_price = safe_price_extraction(
        settings.GOLD_PRICE_CSV_PATH, jdate, "close"
    )
    oil_price, base_oil_price = safe_price_extraction(
        settings.OIL_PRICE_CSV_PATH, jdate, "close"
    )
    usd_price, base_usd_price = safe_price_extraction(
        settings.USD_PRICE_CSV_PATH, jdate, "close"
    )

    try:
        with open(settings.GDP_CSV_PATH) as f:
            gdps = pd.read_csv(f)
        gdp = gdps.loc[
            gdps["year"] == jdate.togregorian().year - 1, "gdp_ppp"
        ].values[0]
    except Exception as e:
        logger.warning(f"GDP error: {e}")
        gdp = np.nan

    return PriceCollection(
        GDP=gdp,
        price_per_share=stock_price,
        gold_price=gold_price,
        oil_price=oil_price,
        usd_price=usd_price,
        delta_stock_price=stock_price - base_stock_price,
        delta_gold_price=gold_price - base_gold_price,
        delta_oil_price=oil_price - base_oil_price,
        delta_usd_price=usd_price - base_usd_price,
    )
