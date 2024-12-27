from itertools import chain
from pathlib import Path
from urllib.parse import urlencode, urljoin

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    MaterializeResult,
    MetadataValue,
    MultiPartitionKey,
    TimeWindow,
    asset,
)
from pydantic import BaseModel

from codal.fetcher.partitions import company_timeframe_partition
from codal.fetcher.resources import (
    AlphaVantaAPIResource,
    APINinjaResource,
    CodalAPIResource,
    FileStoreCompanyListing,
    FileStoreCompanyReport,
    FileStoreIndustryListing,
    FileStoreTSETMCListing,
    TgjuAPIResource,
    TSEMTMCAPIResource,
)
from codal.fetcher.schemas import (
    CompanyReportLetter,
    CompanyReportOut,
    CompanyReportsIn,
)
from codal.fetcher.utils import sanitize_persian


@asset(
    automation_condition=AutomationCondition.on_cron("@weekly"),
)
def get_industries(
    industries_file: FileStoreIndustryListing,
    codal_api: CodalAPIResource,
) -> pd.DataFrame:
    """
    returns updated industry listings
    """
    return industries_file.write(codal_api.industries)


@asset(
    automation_condition=AutomationCondition.on_cron("@weekly"),
)
def get_companies(
    companies_file: FileStoreCompanyListing,
    codal_api: CodalAPIResource,
) -> pd.DataFrame:
    """
    returns updated company listings
    """
    return companies_file.write(codal_api.companies)


def get_url(params: BaseModel) -> str:
    base_url = "https://search.codal.ir/api/search/v2/q?"
    query_string = urlencode(params.model_dump(exclude_none=True), safe="")
    return urljoin(base_url, f"?{query_string}")


def fetch_reports(
    context: AssetExecutionContext, params: CompanyReportOut
) -> list[CompanyReportLetter]:
    listings: list[CompanyReportsIn] = []
    current_page = 1
    context.log.info(
        f"fetching with params {params.model_dump(exclude_none=True)}"
    )
    context.log.info(f"fetch url: {MetadataValue.url(get_url(params))}")
    while True:
        params.PageNumber = current_page
        response = requests.get(
            "https://search.codal.ir/api/search/v2/q?",
            params=params.model_dump(exclude_none=True),
            headers={
                "User-Agent": "",
            },
        )
        if response.status_code != 200:
            response.raise_for_status()
        current_page += 1
        listings.append(CompanyReportsIn.model_validate(response.json()))
        if listings[-1].IsAttacker:
            raise Exception("Rate Limit Reached")
        if listings[-1].Page < current_page:
            break
    return list(chain.from_iterable([listing.Letters for listing in listings]))


def get_excel_report(context: AssetExecutionContext, url) -> bytes:
    """
    download single excel from url returns bytes
    """
    context.log.info(f"fetching from {url}")
    response = requests.get(
        url,
        headers={
            "User-Agent": "codal",
        },
        timeout=30,
    )
    if response.status_code == 200:
        assert isinstance(response.content, bytes)
        return response.content

    context.log.error(f"download failed for {url}")
    # sometimes files are not found and return internal error
    if response.status_code != 500:
        response.raise_for_status()
    return b""


def get_company_excels(
    context: AssetExecutionContext,
    reports: list[CompanyReportLetter],
    timeframe: int,
    company_report: FileStoreCompanyReport,
    whitelist_symbol: list[str],
) -> pd.DataFrame:
    """
    download codal reports for a specific symbol and timeframe
    returns list of PosixPath for excels in current run
    """
    files: list[Path] = []
    context.log.info(f"fetching {len(reports)} sheets")
    assert isinstance(context.partition_key, MultiPartitionKey)
    company_report._time_frame = timeframe
    for report in reports:
        if (
            not report.HasExcel
            or report.jdate is None
            or report.Symbol not in whitelist_symbol
        ):
            continue
        company_report._symbol = sanitize_persian(report.Symbol.strip())
        company_report._year = report.jdate.year
        company_report._filename = f"{report.jdate.isoformat()}.xls"
        if file := get_excel_report(context, report.ExcelUrl):
            company_report.write(file)
            files.append(company_report.data_path)

    return pd.DataFrame(files)


@asset(
    partitions_def=company_timeframe_partition,
    automation_condition=AutomationCondition.eager(),
)
def fetch_company_reports(
    context: AssetExecutionContext,
    fetch_tsemc_filtered_companies,
    company_report: FileStoreCompanyReport,
) -> MaterializeResult:
    """
    list of new report for a specific symbol and timeframe "
    "since last partition [week]
    """
    assert isinstance(context.partition_key, MultiPartitionKey)
    assert isinstance(context.partition_time_window, TimeWindow)
    time_window = context.partition_time_window
    timeframe = int(context.partition_key.keys_by_dimension["timeframe"])
    company_report._time_frame = timeframe
    params = CompanyReportOut.model_validate(
        dict(
            Length=timeframe,
            Audited=timeframe >= 6,
            NotAudited=not (timeframe >= 6),
            FromDate=time_window.start,
            ToDate=time_window.end,
        )
    )
    files = get_company_excels(
        context,
        fetch_reports(context, params=params),
        timeframe,
        company_report,
        fetch_tsemc_filtered_companies.index.values,
    )
    company_report.update_checkpoint(
        start=time_window.start, end=time_window.end
    )
    return MaterializeResult(
        metadata={
            "url": MetadataValue.url(get_url(params)),
            "num_records": MetadataValue.int(len(files)),
            "paths": MetadataValue.md(files.to_markdown()),
        }
    )


@asset(
    automation_condition=AutomationCondition.on_cron("@weekly"),
)
def fetch_gdp(
    ninja_api: APINinjaResource,
) -> pd.DataFrame:
    """
    historical gdp for iran
    """
    return ninja_api.fetch_gdp(country="iran")


@asset(
    automation_condition=AutomationCondition.on_cron("@weekly"),
)
def fetch_commodity(alpha_vantage_api: AlphaVantaAPIResource) -> pd.DataFrame:
    """
    historical prices for BRENT CRUDE OIL
    """
    return alpha_vantage_api.fetch_history(symbol="BRENT")


@asset(
    automation_condition=AutomationCondition.on_cron("@weekly"),
)
def fetch_usd(tgju_api: TgjuAPIResource) -> pd.DataFrame:
    """
    historical prices for USD/RIAL
    """
    return tgju_api.fetch_history(currency="price_dollar_rl")


@asset(
    automation_condition=AutomationCondition.on_cron("@weekly"),
)
def fetch_gold(tgju_api: TgjuAPIResource) -> pd.DataFrame:
    """
    historical prices for 18k Gold/RIAL
    """
    return tgju_api.fetch_history(currency="geram18")


@asset(
    automation_condition=AutomationCondition.on_cron("@weekly"),
)
async def fetch_tsemc_filtered_companies(
    tsetmc_api: TSEMTMCAPIResource,
    tsetmc_file: FileStoreTSETMCListing,
    get_companies,
    get_industries,
) -> pd.DataFrame:
    """
    list of non deleted codal companies listed in tsetmc,
    excluded in industry group blacklist
    """
    # for now its a mapping might search on industries
    # if industry indexes change over time
    excluded_insustries = {
        2: "اوراق مشارکت و سپرده های بانکی",
        56: "سرمایه گذاریها",
        66: "بیمه وصندوق بازنشستگی به جزتامین اجتماعی",
        67: "فعالیتهای کمکی به نهادهای مالی واسط",
        61: "حمل و نقل آبی",
    }
    # filter symbols with excluded industry groups
    symbols = get_companies[
        ~get_companies["industry_group"].isin(excluded_insustries.keys())
    ].index
    return tsetmc_file.write(await tsetmc_api.fetch_symbols(symbols))


@asset(
    automation_condition=AutomationCondition.on_cron("@weekly"),
)
async def fetch_tsemc_stocks(
    tsetmc_api: TSEMTMCAPIResource, fetch_tsemc_filtered_companies
) -> pd.DataFrame:
    """
    fetch stock price history for filtered companies
    """

    df = await tsetmc_api.fetch_stocks(fetch_tsemc_filtered_companies)
    df.to_csv("./data/tsetmc_stocks.csv")
    return df
