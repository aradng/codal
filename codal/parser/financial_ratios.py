"""فعالیت"""  # Activity


# نسبت گردش دارایی
def calc_asset_turnover(net_sales, total_assets):
    return net_sales / total_assets


# دوره وصول مطالبات
def calc_average_collection_period(accounts_receivable, annual_credit_sales):
    return accounts_receivable / (annual_credit_sales / 365)


# چرخه تبدیل نقد
def calc_cash_conversion_cycle(
    inventory_conversion_period,
    receivables_conversion_period,
    payables_conversion_period,
):
    return (
        inventory_conversion_period
        + receivables_conversion_period
        - payables_conversion_period
    )


# دوره گردش موجودی کالا
def calc_inventory_conversion_period(inventory_turnover_ratio):
    return 365 / inventory_turnover_ratio


# نسبت گردش موجودی کالا
def calc_inventory_conversion_ratio(sales, cost_of_goods_sold):
    return (sales * 0.5) / cost_of_goods_sold


# نسبت گردش موجودی کالا
def calc_inventory_turnover(sales, average_inventory):
    return sales / average_inventory


# دوره پرداختنی‌ها
def calc_payables_conversion_period(accounts_payable, purchases):
    return (accounts_payable / purchases) * 365


# دوره تبدیل مطالبات
def calc_receivables_conversion_period(receivables, net_sales):
    return (receivables / net_sales) * 365


# نسبت گردش مطالبات
def calc_receivables_turnover_ratio(net_credit_sales, average_net_receivables):
    return net_credit_sales / average_net_receivables


""" اصول پایه """  # Basic


# دارایی‌ها
def calc_assets(liabilities, equity):
    return liabilities + equity


# سود قبل از بهره و مالیات (EBIT)
def calc_ebit(revenue, operating_expenses):
    return revenue - operating_expenses


# حقوق صاحبان سهام یا ارزش ویژه
def calc_equity(assets, liabilities):
    return assets - liabilities


# سود ناخالص
def calc_gross_profit(revenue, cost_of_goods_sold):
    return revenue - cost_of_goods_sold


# بدهی‌ها
def calc_liabilities(assets, equity):
    return assets - equity


# سود خالص
def calc_net_profit(gross_profit, operating_expenses, taxes, interest):
    return gross_profit - operating_expenses - taxes - interest


# سود عملیاتی
def calc_operating_profit(gross_profit, operating_expenses):
    return gross_profit - operating_expenses


# درآمد فروش
def calc_sales_revenue(gross_sales, sales_of_returns_and_allowances):
    return gross_sales - sales_of_returns_and_allowances


""" بدهی """  # Debt


# نسبت بدهی
def calc_debt_ratio(total_liabilities, total_assets):
    return total_liabilities / total_assets


# نسبت پوشش بدهی
def calc_debt_service_coverage_ratio(net_operating_income, total_debt_service):
    return net_operating_income / total_debt_service


# نسبت کل بدهی به ارزش ویژه
def calc_total_debt_to_equity_ratio(total_debt, equity):
    return total_debt / equity


# نسبت بدهی جاری به ارزش ویژه
def calc_current_debt_to_equity_ratio(current_liabilities, equity):
    return current_liabilities / equity


# نسبت بدهی بلند مدت به ارزش ویژه
def calc_long_term_debt_to_equity_ratio(long_term_liabilities, equity):
    return long_term_liabilities / equity


# نسبت بدهی به حقوق صاحبان سهام
def calc_debt_to_equity_ratio(total_liabilities, equity):
    return total_liabilities / equity


# نسبت مالکانه
def calc_equity_ratio(equity, total_assets):
    return equity / total_assets


""" استهلاک """  # Depreciation


# ارزش دفتری
def calc_book_value(acquisition_cost, depreciation):
    return acquisition_cost - depreciation


# روش نزولی
def calc_declining_balance(depreciation_rate, book_value_at_beginning_of_year):
    return depreciation_rate * book_value_at_beginning_of_year


# روش تولید واحد
def calc_units_of_production(
    cost_of_asset,
    residual_value,
    estimated_total_production,
    actual_production,
):
    return (
        (cost_of_asset - residual_value) / estimated_total_production
    ) * actual_production


# روش خط مستقیم
def calc_straight_line_method(
    cost_of_fixed_asset, residual_value, useful_life_of_asset
):
    return (cost_of_fixed_asset - residual_value) / useful_life_of_asset


""" نقدینگی """  # Liquidity


# نسبت وجه نقد
def calc_cash_ratio(cash, short_term_investments, current_liabilities):
    return (cash + short_term_investments) / current_liabilities


# نسبت جاری
def calc_current_ratio(current_assets, current_liabilities):
    return current_assets / current_liabilities


# نسبت دارایی جاری
def calc_current_assets_ratio(current_assets, total_assets):
    return current_assets / total_assets


# نسبت جریان نقدی عملیاتی
def calc_operating_cash_flow_ratio(operating_cash_flow, total_debts):
    return operating_cash_flow / total_debts


# نسبت سریع
def calc_quick_ratio(current_assets, inventories, current_liabilities):
    return (current_assets - inventories) / current_liabilities


# بازده نقدی دارایی‌ها
def calc_cash_return_on_assets(operating_cash_flow, total_assets):
    return operating_cash_flow / total_assets


# بازده نقدی حقوق صاحبان سهام
def calc_cash_return_on_equity(operating_cash_flow, shareholders_equity):
    return operating_cash_flow / shareholders_equity


# کیفیت سود
def calc_earnings_quality(operating_cash_flow, net_income):
    return operating_cash_flow / net_income


# پوشش نقدی بدهی
def calc_cash_debt_coverage(operating_cash_flow, total_liabilities):
    return operating_cash_flow / total_liabilities


# پوشش نقدی جاری
def calc_current_cash_coverage(operating_cash_flow, current_liabilities):
    return operating_cash_flow / current_liabilities


""" بازار """  # Market


# پوشش سود سهام
def calc_dividend_cover(earnings_per_share, dividends_per_share):
    return earnings_per_share / dividends_per_share


# سود هر سهم
def calc_dividends_per_share(dividends_paid, number_of_shares):
    return dividends_paid / number_of_shares


# بازده سود سهام
def calc_dividend_yield(annual_dividend_per_share, price_per_share):
    return annual_dividend_per_share / price_per_share


# سود هر سهم
def calc_earnings_per_share(net_earnings, number_of_shares):
    return net_earnings / number_of_shares


# نسبت پرداخت سود سهام
def calc_payout_ratio(dividends, earnings):
    return dividends / earnings


# نسبت PEG
def calc_peg_ratio(price_per_earnings, annual_eps_growth):
    return price_per_earnings / annual_eps_growth


# نسبت قیمت به سود (P/E)
def calc_pe_ratio(price_per_share, earnings_per_share):
    return price_per_share / earnings_per_share


# نسبت قیمت به فروش(درامد) (P/S)
def calc_price_sales_ratio(price_per_share, revenue_per_share):
    return price_per_share / revenue_per_share


""" سودآوری """  # Profitability


# نسبت کارایی
def calc_efficiency_ratio(non_interest_expense, revenue):
    return non_interest_expense / revenue


# حاشیه سود ناخالص
def calc_gross_profit_margin(gross_profit, revenue):
    return gross_profit / revenue


# حاشیه سود عملیاتی
def calc_operating_margin(operating_income, revenue):
    return operating_income / revenue


# حاشیه سود خالص
def calc_profit_margin(net_profit, revenue):
    return net_profit / revenue


# بازده دارایی‌ها
def calc_return_on_assets(net_income, total_assets):
    return net_income / total_assets


# بازده سرمایه
def calc_return_on_capital(ebit, tax_rate, invested_capital):
    return ebit * (1 - tax_rate) / invested_capital


# بازده حقوق صاحبان سهام
def calc_return_on_equity(net_income, average_shareholder_equity):
    return net_income / average_shareholder_equity


# بازده دارایی‌های خالص
def calc_return_on_net_assets(net_income, fixed_assets, working_capital):
    return net_income / (fixed_assets + working_capital)


# بازده تعدیل‌شده با ریسک
def calc_risk_adjusted_return_on_capital(expected_return, economic_capital):
    return expected_return / economic_capital


# بازده سرمایه‌گذاری
def calc_return_on_investment(gain, cost):
    return (gain - cost) / cost


# سود قبل از بهره، مالیات، استهلاک و آمورتیزاسیون (EBITDA)
def calc_ebitda(ebit, depreciation, amortization):
    return ebit + depreciation + amortization


"""قیمت"""  # Price


def calc_revenue_to_GDP(revenue, gdp):
    return revenue / gdp


def calc_price_to_gold(stock_price, gold_price):
    return stock_price / gold_price


def calc_price_to_usd(stock_price, usd_price):
    return stock_price / usd_price


def calc_price_to_oil(stock_price, oil_price):
    return stock_price / oil_price


def calc_delta_price_to_delta_gold(delta_stock_price, delta_gold_price):
    return delta_stock_price / delta_gold_price


def calc_delta_price_to_delta_oil(delta_stock_price, delta_oil_price):
    return delta_stock_price / delta_oil_price


def calc_delta_price_to_delta_usd(delta_stock_price, delta_usd_price):
    return delta_stock_price / delta_usd_price
