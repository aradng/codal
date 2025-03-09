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

    current_ratio: float
    quick_ratio: float
    debt_ratio: float
    current_assets_ratio: float
    cash_ratio: float
    net_profit_margin: float
    operating_profit_margin: float
    gross_profit_margin: float
    return_on_equity: float
    return_on_assets: float
    total_debt_to_equity_ratio: float
    current_debt_to_equity_ratio: float
    long_term_debt_to_equity_ratio: float
    debt_to_equity_ratio: float
    equity_ratio: float
    total_asset_turnover: float
    pe_ratio: float
    price_sales_ratio: float
    cash_return_on_assets: float
    cash_return_on_equity: float
    earnings_quality: float
    cash_debt_coverage: float
    current_cash_coverage: float
    revenue_to_GDP: float
    price_to_gold: float
    price_to_oil: float
    price_to_usd: float
    delta_price_to_delta_gold: float
    delta_price_to_delta_oil: float
    delta_price_to_delta_usd: float
