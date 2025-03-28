from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from dagster import get_dagster_logger
from fuzzywuzzy import process  # type: ignore[import-untyped]
from pandas.core.frame import DataFrame

from codal.fetcher.utils import sanitize_persian
from codal.parser.schemas import PriceCollection, PriceDFs, Ratios, Report


class IncompatibleFormatError(Exception):
    pass


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
        if score >= 90:
            idx = table.iloc[:, col] == matched
            matched_row: DataFrame = table[idx]
            var = matched_row.iloc[0, col + 1]
            if pd.isna(var):
                table.drop(index=matched_row.index[0], inplace=True)
                find_row_for_variable(table, values, col, row_name, var_name)
            else:
                values[var_name] = (
                    float(var) if not matched_row.empty else np.nan
                )
        else:
            values[var_name] = None
        logger = get_dagster_logger()
        logger.debug(f"values: {values}")


# TODO: this can be simplified more since its a single column dataframe now


def extract_variables(
    tables: dict[str, DataFrame], table_names_map: dict
) -> Report:
    tables.pop("error", False)
    tables = {
        sanitize_persian(k): v.map(
            lambda x: sanitize_persian(x) if isinstance(x, str) else x
        )
        for k, v in tables.items()
    }
    values: dict = {}
    logger = get_dagster_logger()
    unmatched_tables_count = min(len(tables), len(table_names_map))
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
                        logger.error(
                            f"Error finding variable {var_name}"
                            f" in table {map_key}: {e}"
                        )
    except Exception as e:
        logger.error(f"Error extracting variables: {e}")
        raise e
    if unmatched_tables_count > 0:
        raise IncompatibleFormatError(
            "did not match all tables with the mapping provided\n"
            f"mappings : {list(table_names_map.keys())}\n"
            f"tables_headers : {list(tables.keys())}\n"
            f"table_count: {len(tables)}\n"
            f"unmatched_count: {unmatched_tables_count}\n"
        )

    return Report.model_validate(values)


# TODO: this can be simplified more since its a single column dataframe now


def collect_prices(
    date: date,
    timeframe: int,
    symbol: str,
    price_dfs: PriceDFs,
) -> PriceCollection:
    date = pd.Timestamp(date)
    df = {
        k: {
            "curr": v["df"].asof(date)[v["col"]],
            "prev": v["df"].asof(date - pd.Timedelta(days=30 * timeframe))[
                v["col"]
            ],
        }
        for k, v in {
            "stock_price": {"df": price_dfs.TSETMC_STOCKS, "col": symbol},
            "gold_price": {"df": price_dfs.GOLD_PRICES, "col": "close"},
            "usd_price": {"df": price_dfs.USD_PRICES, "col": "close"},
            "oil_price": {"df": price_dfs.OIL_PRICES, "col": "value"},
            "gdp": {"df": price_dfs.GDP, "col": "gdp_ppp"},
        }.items()
    }

    return PriceCollection(
        GDP=df["gdp"]["curr"],
        price_per_share=df["stock_price"]["curr"],
        gold_price=df["gold_price"]["curr"],
        oil_price=df["oil_price"]["curr"],
        usd_price=df["usd_price"]["curr"],
        delta_stock_price=df["stock_price"]["curr"]
        - df["stock_price"]["prev"],
        delta_gold_price=df["gold_price"]["curr"] - df["gold_price"]["prev"],
        delta_oil_price=df["oil_price"]["curr"] - df["oil_price"]["prev"],
        delta_usd_price=df["usd_price"]["curr"] - df["usd_price"]["prev"],
    )


def calc_financial_ratios(
    report: Report, price_collection: PriceCollection
) -> pd.Series:
    return pd.Series(
        Ratios(
            current_ratio=report.current_assets / report.current_liabilities,
            quick_ratio=(report.current_assets - report.inventories)
            / report.current_liabilities,
            debt_ratio=report.total_liabilities / report.total_assets,
            current_assets_ratio=report.current_assets / report.total_assets,
            cash_ratio=(report.cash + report.short_term_investments)
            / report.current_liabilities,
            net_profit_margin=report.net_profit / report.revenue,
            operating_profit_margin=report.operating_income / report.revenue,
            gross_profit_margin=report.gross_profit / report.revenue,
            return_on_equity=report.net_profit / report.equity,
            return_on_assets=report.net_profit / report.total_assets,
            total_debt_to_equity_ratio=report.total_liabilities
            / report.equity,
            current_debt_to_equity_ratio=report.current_liabilities
            / report.equity,
            long_term_debt_to_equity_ratio=report.long_term_liabilities
            / report.equity,
            debt_to_equity_ratio=report.total_liabilities / report.equity,
            equity_ratio=report.equity / report.total_assets,
            total_asset_turnover=report.revenue / report.total_assets,
            pe_ratio=price_collection.price_per_share
            / report.earnings_per_share,
            price_sales_ratio=price_collection.price_per_share
            / (report.revenue / (report.capital / 1000)),
            cash_return_on_assets=report.operating_cash_flow
            / report.total_assets,
            cash_return_on_equity=report.operating_cash_flow / report.equity,
            earnings_quality=report.operating_cash_flow / report.net_profit,
            cash_debt_coverage=report.operating_cash_flow
            / report.total_liabilities,
            current_cash_coverage=report.operating_cash_flow
            / report.current_liabilities,
            revenue_to_GDP=report.revenue / price_collection.GDP,
            price_to_gold=price_collection.price_per_share
            / price_collection.gold_price,
            price_to_oil=price_collection.price_per_share
            / price_collection.oil_price,
            price_to_usd=price_collection.price_per_share
            / price_collection.usd_price,
            delta_price_to_delta_gold=price_collection.delta_stock_price
            / price_collection.delta_gold_price,
            delta_price_to_delta_oil=price_collection.delta_stock_price
            / price_collection.delta_oil_price,
            delta_price_to_delta_usd=price_collection.delta_stock_price
            / price_collection.delta_usd_price,
        ).model_dump()
    )
