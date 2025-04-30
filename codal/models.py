from datetime import datetime
from typing import Literal

from beanie import Document


class Industry(Document):
    class Settings:
        name = "Industries"

    id: int
    name: str


class Company(Document):
    class Settings:
        name = "Companies"

    id: int
    name: str
    symbol: str
    industry_group: int
    industry_name: str
    deleted: bool


class Profile(Document):
    class Settings:
        name = "Profiles"

    name: str
    is_industry: bool
    industry_group: int

    timeframe: Literal[3, 6, 12]
    jdate: str
    date: datetime

    current_ratio: float | None
    quick_ratio: float | None
    debt_ratio: float | None
    current_assets_ratio: float | None
    cash_ratio: float | None
    net_profit_margin: float | None
    operating_profit_margin: float | None
    gross_profit_margin: float | None
    return_on_equity: float | None
    return_on_assets: float | None
    total_debt_to_equity_ratio: float | None
    current_debt_to_equity_ratio: float | None
    long_term_debt_to_equity_ratio: float | None
    debt_to_equity_ratio: float | None
    equity_ratio: float | None
    total_asset_turnover: float | None
    pe_ratio: float | None
    price_sales_ratio: float | None
    cash_return_on_assets: float | None
    cash_return_on_equity: float | None
    earnings_quality: float | None
    cash_debt_coverage: float | None
    current_cash_coverage: float | None
    revenue_to_GDP: float | None
    price_to_gold: float | None
    price_to_oil: float | None
    price_to_usd: float | None
    delta_price_to_delta_gold: float | None
    delta_price_to_delta_oil: float | None
    delta_price_to_delta_usd: float | None

    net_cash_flow_operating: float | None
    net_cash_flow_investing: float | None
    net_cash_flow_financing: float | None
    net_increase_decrease_cash: float | None


class Report(Document):
    class Settings:
        name = "Reports"

    net_profit: float | None
    gross_profit: float | None
    operating_income: float | None
    earnings_per_share: float | None
    revenue: float | None
    cost_of_revenue: float | None
    non_current_assets: float | None
    current_assets: float | None
    total_assets: float | None
    long_term_liabilities: float | None
    current_liabilities: float | None
    total_liabilities: float | None
    equity: float | None
    total_liabilities_and_equity: float | None
    capital: float | None
    cash: float | None
    short_term_investments: float | None
    long_term_investments: float | None
    inventories: float | None
    operating_cash_flow: float | None
    cash_taxes_paid: float | None
    net_cash_flow_operating: float | None
    net_cash_flow_investing: float | None
    net_cash_flow_financing: float | None
    net_increase_decrease_cash: float | None

    name: str
    industry_group: int
    jdate: str
    date: datetime
    timeframe: Literal[3, 6, 12]
