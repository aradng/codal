from collections.abc import Callable

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
    calc_delta_price_to_delta_gold,
    calc_delta_price_to_delta_oil,
    calc_delta_price_to_delta_usd,
    calc_earnings_quality,
    calc_equity_ratio,
    calc_gross_profit_margin,
    calc_long_term_debt_to_equity_ratio,
    calc_operating_margin,
    calc_pe_ratio,
    calc_price_sales_ratio,
    calc_price_to_gold,
    calc_price_to_oil,
    calc_price_to_usd,
    calc_profit_margin,
    calc_quick_ratio,
    calc_return_on_assets,
    calc_return_on_equity,
    calc_revenue_to_GDP,
    calc_total_debt_to_equity_ratio,
)

# for year after 1398
table_names_map = {
    "صورت سود و زیان": {
        "سود (زیان) خالص": "net_profit",
        "سود (زیان) ناخالص": "gross_profit",
        "سود (زیان) عملیاتی": "operating_income",
        "سود (زیان) خالص هر سهم– ریال": "earnings_per_share",
        "درآمدهای عملیاتی": "revenue",
        "بهاى تمام شده درآمدهای عملیاتی": "cost_of_revenue",
    },
    "صورت وضعیت مالی": {
        "جمع دارایی‌های غیرجاری": "non_current_assets",
        "جمع دارایی‌های جاری": "current_assets",
        "جمع دارایی‌ها": "total_assets",
        "جمع بدهی‌های غیرجاری": "long_term_liabilities",  # ##
        "جمع بدهی‌های جاری": "current_liabilities",
        "جمع بدهی‌ها": "total_liabilities",
        "جمع حقوق مالکانه": "equity",
        "جمع حقوق مالکانه و بدهی‌ها": "total_liabilities_and_equity",
        "سرمایه": "capital",
        "موجودی نقد": "cash",
        "سرمایه‌گذاری‌های کوتاه‌مدت": "short_term_investments",
        "سرمایه‌گذاری‌های بلندمدت": "long_term_investments",
        "موجودی مواد و کالا": "inventories",  # سفارشات و پيش‌پرداخت‌ها +
    },
    "صورت جریان های نقدی": {
        "نقد حاصل از عملیات": "operating_cash_flow",
        "پرداخت‌های نقدی بابت مالیات بر درآمد": "cash_taxes_paid",
        "جریان ‌خالص ‌ورود‌ (خروج) ‌نقد حاصل از فعالیت‌های ‌عملیاتی": (
            "net_cash_flow_operating"
        ),
        "جريان خالص ورود (خروج) نقد حاصل از فعاليت‌های سرمایه‌گذاری": (
            "net_cash_flow_investing"
        ),
        "جريان خالص ورود (خروج) نقد حاصل از فعالیت‌های تامين مالی": (
            "net_cash_flow_financing"
        ),
        "خالص افزايش (کاهش) در موجودی نقد": "net_increase_decrease_cash",
    },
}


table_names_map_b98 = {
    "صورت سود و زیان": {
        "سود (زیان) خالص": "net_profit",
        "سود (زیان) ناخالص ": "gross_profit",
        "سود (زیان) عملیاتی": "operating_income",
        "سود (زیان) خالص هر سهم– ریال": "earnings_per_share",
        "درآمدهای عملیاتی": "revenue",
        "بهای تمام ‌شده درآمدهای عملیاتی": "cost_of_revenue",
    },
    "ترازنامه": {
        "جمع دارایی‌های غیرجاری": "non_current_assets",
        "جمع دارایی‌های جاری": "current_assets",
        "جمع دارایی‌ها": "total_assets",
        "جمع بدهی‌های غیرجاری": "long_term_liabilities",  # ##
        "جمع بدهی‌های جاری": "current_liabilities",
        "جمع بدهی‌ها": "total_liabilities",
        "جمع حقوق صاحبان سهام": "equity",
        "جمع بدهی‌ها و حقوق صاحبان سهام": "total_liabilities_and_equity",
        "سرمایه": "capital",
        "موجودی نقد": "cash",
        "سرمایه‌گذاری‌‌های کوتاه مدت": "short_term_investments",
        "سرمایه‌گذاری‌های بلندمدت": "long_term_investments",
        "موجودی مواد و کالا": "inventories",  # سفارشات و پيش‌پرداخت‌ها +
    },
    "جریان وجوه نقد": {
        "جریان خالص ورود (خروج) وجه نقد ناشی از فعالیت‌های عملیاتی": (
            "operating_cash_flow"
        ),
        "مالیات بر درآمد پرداختی": "cash_taxes_paid",
        # "جریان خالص ورود (خروج) وجه نقد ناشی از فعالیت‌های عملیاتی": (
        #     "net_cash_flow_operating"
        # ),
        "جریان خالص ورود (خروج) وجه نقد ناشی از فعالیت‌های سرمایه‌گذاری": (
            "net_cash_flow_investing"
        ),
        "جریان خالص ورود (خروج) وجه نقد ناشی از فعالیت‌های تأمین مالی": (
            "net_cash_flow_before_financing"
        ),
        "خالص افزایش (کاهش) در موجودی نقد": "net_increase_decrease_cash",
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
    "revenue_to_GDP": (
        calc_revenue_to_GDP,
        ["revenue", "GDP"],
    ),
    "price_to_gold": (
        calc_price_to_gold,
        ["price_per_share", "gold_price"],
    ),
    "price_to_oil": (
        calc_price_to_oil,
        ["price_per_share", "oil_price"],
    ),
    "price_to_usd": (
        calc_price_to_usd,
        ["price_per_share", "usd_price"],
    ),
    "delta_price_to_delta_gold": (
        calc_delta_price_to_delta_gold,
        ["delta_stock_price", "delta_gold_price"],
    ),
    "delta_price_to_delta_oil": (
        calc_delta_price_to_delta_oil,
        ["delta_stock_price", "delta_oil_price"],
    ),
    "delta_price_to_delta_usd": (
        calc_delta_price_to_delta_usd,
        ["delta_stock_price", "delta_usd_price"],
    ),
}
