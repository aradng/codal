from typing import Callable

from codal.parser.financial_ratios import (
    calc_asset_turnover,
    calc_cash_debt_coverage,
    calc_cash_ratio,
    calc_cash_return_on_assets,
    calc_cash_return_on_equity,
    calc_current_assets_ratio,
    calc_current_cash_coverage,
    calc_current_debt_to_equity_ratio,
    calc_current_ratio,
    calc_debt_ratio,
    calc_debt_to_equity_ratio,
    calc_earnings_quality,
    calc_equity_ratio,
    calc_gross_profit_margin,
    calc_long_term_debt_to_equity_ratio,
    calc_operating_margin,
    calc_pe_ratio,
    calc_price_sales_ratio,
    calc_profit_margin,
    calc_quick_ratio,
    calc_return_on_assets,
    calc_return_on_equity,
    calc_total_debt_to_equity_ratio,
)


# for year after 1398
table_names_map = {
    "صورت سود و زیان": {
        "سود(زيان) خالص": "net_profit",
        "سود(زيان) ناخالص": "gross_profit",
        "سود(زيان) عملياتى": "operating_income",
        "سود (زيان) خالص هر سهم – ريال": "earnings_per_share",
        "درآمدهاي عملياتي": "revenue",
        "بهاى تمام شده درآمدهاي عملياتي": "cost_of_revenue",
    },
    "صورت وضعیت مالی": {
        "جمع دارايي‌هاي غيرجاري": "non_current_assets",
        "جمع دارايي‌هاي جاري": "current_assets",
        "جمع دارايي‌ها": "total_assets",
        "جمع بدهي‌هاي غيرجاري": "long_term_liabilities",  # ##
        "جمع بدهي‌هاي جاري": "current_liabilities",
        "جمع بدهي‌ها": "total_liabilities",
        "جمع حقوق مالکانه": "equity",
        "جمع حقوق مالکانه و بدهي‌ها": "total_liabilities_and_equity",
        "سرمايه": "capital",
        "موجودي نقد": "cash",
        "سرمايه‌گذاري‌هاي کوتاه‌مدت": "short_term_investments",
        "سرمايه‌گذاري‌هاي بلندمدت": "long_term_investments",
        "موجودي مواد و کالا": "inventories",  # سفارشات و پيش‌پرداخت‌ها +
    },
    "صورت جریان های نقدی": {
        "نقد حاصل از عمليات": "operating_cash_flow",
        "پرداخت‌هاي نقدي بابت ماليات بر درآمد": "cash_taxes_paid",
        "جريان ‌خالص ‌ورود‌ (خروج) ‌نقد حاصل از فعاليت‌هاي ‌عملياتي": (
            "net_cash_flow_operating"
        ),
        "جريان خالص ورود (خروج) نقد حاصل از فعاليت‌هاي سرمايه‌گذاري": (
            "net_cash_flow_investing"
        ),
        "جريان خالص ورود (خروج) نقد قبل از فعاليت‌هاي تامين مالي": (
            "net_cash_flow_before_financing"
        ),
        "خالص افزايش (کاهش) در موجودي نقد": "net_increase_decrease_cash",
    },
}


calculations: dict[str, tuple[Callable, list[str]]] = {
    "current_ratio": (
        calc_current_ratio,
        ["current_assets", "current_liabilities"],
    ),
    "quick_ratio": (
        calc_quick_ratio,
        ["current_assets", "inventories", "current_liabilities"],
    ),
    "debt_ratio": (calc_debt_ratio, ["total_liabilities", "total_assets"]),
    "current_assets_ratio": (
        calc_current_assets_ratio,
        ["current_assets", "total_assets"],
    ),
    "cash_ratio": (
        calc_cash_ratio,
        ["cash", "short_term_investments", "current_liabilities"],
    ),
    "net_profit_margin": (calc_profit_margin, ["net_profit", "revenue"]),
    "operating_profit_margin": (
        calc_operating_margin,
        ["operating_income", "revenue"],
    ),
    "gross_profit_margin": (
        calc_gross_profit_margin,
        ["gross_profit", "revenue"],
    ),
    "return_on_equity": (
        calc_return_on_equity,
        ["net_profit", "equity"],
    ),
    "return_on_assets": (
        calc_return_on_assets,
        ["net_profit", "total_assets"],
    ),
    "total_debt_to_equity_ratio": (
        calc_total_debt_to_equity_ratio,
        ["total_liabilities", "equity"],
    ),
    "current_debt_to_equity_ratio": (
        calc_current_debt_to_equity_ratio,
        ["current_liabilities", "equity"],
    ),
    "long_term_debt_to_equity_ratio": (
        calc_long_term_debt_to_equity_ratio,
        ["long_term_liabilities", "equity"],
    ),
    "debt_to_equity_ratio": (
        calc_debt_to_equity_ratio,
        ["total_liabilities", "equity"],
    ),
    "equity_ratio": (calc_equity_ratio, ["equity", "total_assets"]),
    "total_asset_turnover": (
        calc_asset_turnover,
        ["revenue", "total_assets"],
    ),
    "pe_ratio": (calc_pe_ratio, ["price_per_share", "earnings_per_share"]),
    "price_sales_ratio": (
        calc_price_sales_ratio,
        ["price_per_share", "revenue_per_share"],
    ),
    "cash_return_on_assets": (
        calc_cash_return_on_assets,
        ["operating_cash_flow", "total_assets"],
    ),
    "cash_return_on_equity": (
        calc_cash_return_on_equity,
        ["operating_cash_flow", "equity"],
    ),
    "earnings_quality": (
        calc_earnings_quality,
        ["operating_cash_flow", "net_profit"],
    ),
    "cash_debt_coverage": (
        calc_cash_debt_coverage,
        ["operating_cash_flow", "total_liabilities"],
    ),
    "current_cash_coverage": (
        calc_current_cash_coverage,
        ["operating_cash_flow", "current_liabilities"],
    ),
}
