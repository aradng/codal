from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from dagster import get_dagster_logger
from fuzzywuzzy import process  # type: ignore[import-untyped]
from pandas.core.frame import DataFrame

from codal.fetcher.utils import sanitize_persian
from codal.parser.schemas import (
    FullReport,
    PriceCollection,
    PriceDFs,
    Ratios,
    Report,
)


class IncompatibleFormatError(Exception):
    pass


def find_row_for_variable(
    table: DataFrame,
    values: dict[str, Any],
    row_name: str,
    var_name: str,
):
    try:
        table_row_names = table.iloc[:, 0].astype(str).tolist()
    except IndexError:
        return
    matched, score = process.extractOne(row_name, table_row_names)
    values[var_name] = (
        (table[table.iloc[:, 0] == matched].iloc[:, 1].dropna().iat[0])
        if score >= 90
        else np.nan
    )


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
    for map_key, mapping in table_names_map.items():
        if tables.get(map_key) is not None:
            unmatched_tables_count -= 1

            for row_name, var_name in mapping.items():
                try:
                    find_row_for_variable(
                        tables[map_key], values, row_name, var_name
                    )
                except Exception as e:
                    logger.debug(
                        f"Error finding variable {var_name}"
                        f" in table {map_key}: {e}"
                    )
    if unmatched_tables_count > 0:
        raise IncompatibleFormatError(
            "did not match all tables with the mapping provided\n"
            f"mappings : {list(table_names_map.keys())}\n"
            f"tables_headers : {list(tables.keys())}\n"
            f"table_count: {len(tables)}\n"
            f"unmatched_count: {unmatched_tables_count}\n"
        )
    return Report.model_validate(values)


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
    prices = PriceCollection(
        GDP=df["gdp"]["curr"],
        price_per_share=df["stock_price"]["curr"],
        gold_price=df["gold_price"]["curr"],
        oil_price=df["oil_price"]["curr"] * df["usd_price"]["curr"],
        usd_price=df["usd_price"]["curr"],
        delta_stock_price=df["stock_price"]["curr"]
        - df["stock_price"]["prev"],
        delta_gold_price=df["gold_price"]["curr"] - df["gold_price"]["prev"],
        delta_oil_price=df["oil_price"]["curr"] * df["usd_price"]["curr"]
        - df["oil_price"]["prev"] * df["usd_price"]["prev"],
        delta_usd_price=df["usd_price"]["curr"] - df["usd_price"]["prev"],
    )

    return PriceCollection.model_validate(prices, from_attributes=True)


def calc_financial_ratios(report: FullReport) -> pd.Series:
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
            pe_ratio=report.price_per_share / report.earnings_per_share,
            price_sales_ratio=report.price_per_share
            / (report.revenue / (report.capital / 1000)),
            cash_return_on_assets=report.operating_cash_flow
            / report.total_assets,
            cash_return_on_equity=report.operating_cash_flow / report.equity,
            earnings_quality=report.operating_cash_flow / report.net_profit,
            cash_debt_coverage=report.operating_cash_flow
            / report.total_liabilities,
            current_cash_coverage=report.operating_cash_flow
            / report.current_liabilities,
            revenue_to_GDP=report.revenue / report.GDP,
            price_to_gold=report.price_per_share / report.gold_price,
            price_to_oil=report.price_per_share / report.oil_price,
            price_to_usd=report.price_per_share / report.usd_price,
            delta_price_to_delta_gold=report.delta_stock_price
            / report.delta_gold_price,
            delta_price_to_delta_oil=report.delta_stock_price
            / report.delta_oil_price,
            delta_price_to_delta_usd=report.delta_stock_price
            / report.delta_usd_price,
            net_cash_flow_operating=report.net_cash_flow_operating,
            net_cash_flow_investing=report.net_cash_flow_investing,
            net_cash_flow_financing=report.net_cash_flow_financing,
            net_increase_decrease_cash=report.net_increase_decrease_cash,
        ).model_dump()
    )


def financial_ratios_of_industries(row: pd.Series) -> pd.Series | None:
    validated = FullReport.model_validate(row.to_dict())
    result = calc_financial_ratios(validated)
    result["industry_group"] = row.name
    return result
