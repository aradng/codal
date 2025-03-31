from typing import Any

import numpy as np
from pandas import DataFrame
from pydantic import BaseModel, field_validator


class PriceCollection(BaseModel):
    GDP: np.float64 = np.float64(np.nan)
    price_per_share: np.float64 = np.float64(np.nan)
    gold_price: np.float64 = np.float64(np.nan)
    oil_price: np.float64 = np.float64(np.nan)
    usd_price: np.float64 = np.float64(np.nan)
    delta_stock_price: np.float64 = np.float64(np.nan)
    delta_gold_price: np.float64 = np.float64(np.nan)
    delta_oil_price: np.float64 = np.float64(np.nan)
    delta_usd_price: np.float64 = np.float64(np.nan)

    @field_validator("*", mode="before")
    @classmethod
    def float_to_np(cls, value: Any) -> np.float64:
        try:
            return np.float64(value)
        except Exception:
            return np.float64(np.nan)

    class Config:
        arbitrary_types_allowed = True


class PriceDFs(BaseModel):
    TSETMC_STOCKS: DataFrame
    GDP: DataFrame
    GOLD_PRICES: DataFrame
    OIL_PRICES: DataFrame
    USD_PRICES: DataFrame

    class Config:
        arbitrary_types_allowed = True


class Report(BaseModel):
    net_profit: np.float64 = np.float64(np.nan)
    gross_profit: np.float64 = np.float64(np.nan)
    operating_income: np.float64 = np.float64(np.nan)
    earnings_per_share: np.float64 = np.float64(np.nan)
    revenue: np.float64 = np.float64(np.nan)
    cost_of_revenue: np.float64 = np.float64(np.nan)

    non_current_assets: np.float64 = np.float64(np.nan)
    current_assets: np.float64 = np.float64(np.nan)
    total_assets: np.float64 = np.float64(np.nan)
    long_term_liabilities: np.float64 = np.float64(np.nan)
    current_liabilities: np.float64 = np.float64(np.nan)
    total_liabilities: np.float64 = np.float64(np.nan)
    equity: np.float64 = np.float64(np.nan)
    total_liabilities_and_equity: np.float64 = np.float64(np.nan)
    capital: np.float64 = np.float64(np.nan)
    cash: np.float64 = np.float64(np.nan)
    short_term_investments: np.float64 = np.float64(np.nan)
    long_term_investments: np.float64 = np.float64(np.nan)
    inventories: np.float64 = np.float64(np.nan)

    operating_cash_flow: np.float64 = np.float64(np.nan)
    cash_taxes_paid: np.float64 = np.float64(np.nan)
    net_cash_flow_operating: np.float64 = np.float64(np.nan)
    net_cash_flow_investing: np.float64 = np.float64(np.nan)
    net_cash_flow_financing: np.float64 = np.float64(np.nan)
    net_increase_decrease_cash: np.float64 = np.float64(np.nan)

    @field_validator("*", mode="before")
    @classmethod
    def float_to_np(cls, value: Any) -> np.float64:
        try:
            return np.float64(value)
        except Exception:
            return np.float64(np.nan)

    class Config:
        arbitrary_types_allowed = True


class FullReport(Report, PriceCollection):  # type: ignore[misc]
    pass


class Ratios(BaseModel):
    current_ratio: np.float64
    quick_ratio: np.float64
    debt_ratio: np.float64
    current_assets_ratio: np.float64
    cash_ratio: np.float64
    net_profit_margin: np.float64
    operating_profit_margin: np.float64
    gross_profit_margin: np.float64
    return_on_equity: np.float64
    return_on_assets: np.float64
    total_debt_to_equity_ratio: np.float64
    current_debt_to_equity_ratio: np.float64
    long_term_debt_to_equity_ratio: np.float64
    debt_to_equity_ratio: np.float64
    equity_ratio: np.float64
    total_asset_turnover: np.float64
    pe_ratio: np.float64
    price_sales_ratio: np.float64
    cash_return_on_assets: np.float64
    cash_return_on_equity: np.float64
    earnings_quality: np.float64
    cash_debt_coverage: np.float64
    current_cash_coverage: np.float64
    revenue_to_GDP: np.float64
    price_to_gold: np.float64
    price_to_oil: np.float64
    price_to_usd: np.float64
    delta_price_to_delta_gold: np.float64
    delta_price_to_delta_oil: np.float64
    delta_price_to_delta_usd: np.float64

    net_cash_flow_operating: np.float64
    net_cash_flow_investing: np.float64
    net_cash_flow_financing: np.float64
    net_increase_decrease_cash: np.float64

    @field_validator("*", mode="before")
    @classmethod
    def float_to_np(cls, value: Any) -> np.float64:
        try:
            return np.float64(value)
        except Exception:
            return np.float64(np.nan)

    class Config:
        arbitrary_types_allowed = True
