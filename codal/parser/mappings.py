# after 1398
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
        "جمع بدهی‌های غیرجاری": "long_term_liabilities",
        "جمع بدهی‌های جاری": "current_liabilities",
        "جمع بدهی‌ها": "total_liabilities",
        "جمع حقوق مالکانه": "equity",
        "جمع حقوق مالکانه و بدهی‌ها": "total_liabilities_and_equity",
        "سرمایه": "capital",
        "موجودی نقد": "cash",
        "سرمایه‌گذاری‌های کوتاه‌مدت": "short_term_investments",
        "سرمایه‌گذاری‌های بلندمدت": "long_term_investments",
        "موجودی مواد و کالا": "inventories",
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

# before 98
table_names_map_b98 = {
    "صورت سود و زیان": {
        "سود (زیان) خالص": "net_profit",
        "سود (زیان) ناخالص": "gross_profit",
        "سود (زیان) عملیاتی": "operating_income",
        "سود (زیان) خالص هر سهم– ریال": "earnings_per_share",
        "درآمدهای عملیاتی": "revenue",
        "بهای تمام ‌شده درآمدهای عملیاتی": "cost_of_revenue",
    },
    "ترازنامه": {
        "جمع دارایی‌های غیرجاری": "non_current_assets",
        "جمع دارایی‌های جاری": "current_assets",
        "جمع دارایی‌ها": "total_assets",
        "جمع بدهی‌های غیرجاری": "long_term_liabilities",
        "جمع بدهی‌های جاری": "current_liabilities",
        "جمع بدهی‌ها": "total_liabilities",
        "جمع حقوق صاحبان سهام": "equity",
        "جمع بدهی‌ها و حقوق صاحبان سهام": "total_liabilities_and_equity",
        "سرمایه": "capital",
        "موجودی نقد": "cash",
        "سرمایه‌گذاری‌‌های کوتاه مدت": "short_term_investments",
        "سرمایه‌گذاری‌های بلندمدت": "long_term_investments",
        "موجودی مواد و کالا": "inventories",
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
            "net_cash_flow_financing"
        ),
        "خالص افزایش (کاهش) در موجودی نقد": "net_increase_decrease_cash",
    },
}
