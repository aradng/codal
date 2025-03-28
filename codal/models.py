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


class Report(Document):
    class Settings:
        name = "reports"

    net_profit: float
    gross_profit: float
    operating_income: float
    earnings_per_share: float
    revenue: float
    cost_of_revenue: float

    non_current_assets: float
    current_assets: float
    total_assets: float
    long_term_liabilities: float
    current_liabilities: float
    total_liabilities: float
    equity: float
    total_liabilities_and_equity: float
    capital: float
    cash: float
    short_term_investments: float
    long_term_investments: float
    inventories: float

    operating_cash_flow: float
    cash_taxes_paid: float
    # net_cash_flow_operating: float
    net_cash_flow_investing: float
    net_cash_flow_financing: float
    net_increase_decrease_cash: float


class Profile(Document):
    class Settings:
        name = "Profiles"

    name: str
    is_industry: bool
    industry_group: int

    timeframe: Literal[3, 6, 12]
    year: int
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
