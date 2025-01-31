from itertools import chain
from typing import Any
from urllib.parse import urlencode, urljoin

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    Backoff,
    Jitter,
    MetadataValue,
    MultiPartitionKey,
    Output,
    RetryPolicy,
    TimeWindow,
    asset,
)
from pydantic import BaseModel

from codal.fetcher.partitions import company_timeframe_partition
from codal.fetcher.resources import (
    AlphaVantaAPIResource,
    APINinjaResource,
    CodalAPIResource,
    FileStoreCompanyReport,
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
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    automation_condition=AutomationCondition.on_cron("@weekly"),
    io_manager_key="df",
    metadata={"name": "codal_industries"},
)
def get_industries(
    context: AssetExecutionContext,
    codal_api: CodalAPIResource,
) -> Output[pd.DataFrame]:
    """
    returns updated industry listings
    """
    df = codal_api.industries
    return Output(df, metadata={"records": len(df)})


@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    automation_condition=AutomationCondition.on_cron("@weekly"),
    io_manager_key="df",
    metadata={"name": "codal_companies"},
)
def get_companies(
    context: AssetExecutionContext,
    codal_api: CodalAPIResource,
) -> Output[pd.DataFrame]:
    """
    returns updated company listings
    """
    df = codal_api.companies
    return Output(df, metadata={"records": len(df)})


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
    retry_count = 3
    context.log.info(f"fetching from {url}")
    for i in range(retry_count):
        try:
            response = requests.get(
                url,
                headers={
                    "User-Agent": "codal",
                },
                timeout=10,
            )
            if response.status_code == 200:
                assert isinstance(response.content, bytes)
                return response.content

            # sometimes files are not found and return internal error
            if response.status_code != 500:
                response.raise_for_status()
        except requests.ReadTimeout:
            if i < retry_count - 1:
                context.log.warning("retrying ...")
                pass
    context.log.error(f"download failed for {url}")
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
    files: list[dict[str, Any]] = []
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
        metadata = {
            "symbol": company_report._symbol,
            "year": company_report._year,
            "timeframe": company_report._time_frame,
            "name": company_report._filename,
            "path": company_report.data_path,
            "error": False,
        }
        if file := get_excel_report(context, report.ExcelUrl):
            company_report.write(file)
            files.append(metadata)
        else:
            metadata["error"] = True
            files.append(metadata)

    return pd.DataFrame(
        files, columns=["symbol", "year", "timeframe", "name", "path", "error"]
    )


@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    partitions_def=company_timeframe_partition,
    automation_condition=AutomationCondition.eager(),
)
def fetch_company_reports(
    context: AssetExecutionContext,
    fetch_tsetmc_filtered_companies,
    company_report: FileStoreCompanyReport,
) -> pd.DataFrame:
    """
    list of new report for a specific symbol and timeframe
    since last partition [week]
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
        fetch_tsetmc_filtered_companies["source_symbol"].values,
    )
    company_report.update_checkpoint(
        start=time_window.start, end=time_window.end
    )
    context.add_output_metadata(
        metadata={
            "url": MetadataValue.url(get_url(params)),
            "records": MetadataValue.int(len(files)),
            "errors": MetadataValue.int(len(files[files["error"]])),
            "paths": MetadataValue.md(files.to_markdown()),
        }
    )
    return files


@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    automation_condition=AutomationCondition.on_cron("@weekly"),
    io_manager_key="df",
    metadata={"name": "gdp"},
)
def fetch_gdp(
    ninja_api: APINinjaResource,
) -> pd.DataFrame:
    """
    historical gdp for iran
    """
    return ninja_api.fetch_gdp(country="iran")


@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    automation_condition=AutomationCondition.on_cron("@weekly"),
    io_manager_key="df",
    metadata={"name": "oil"},
)
def fetch_commodity(alpha_vantage_api: AlphaVantaAPIResource) -> pd.DataFrame:
    """
    historical prices for BRENT CRUDE OIL
    """
    return alpha_vantage_api.fetch_history(symbol="BRENT")


@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    automation_condition=AutomationCondition.on_cron("@weekly"),
    io_manager_key="df",
    metadata={"name": "codal_industries"},
)
def fetch_usd(tgju_api: TgjuAPIResource) -> pd.DataFrame:
    """
    historical prices for USD/RIAL
    """
    return tgju_api.fetch_history(currency="price_dollar_rl")


@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    automation_condition=AutomationCondition.on_cron("@weekly"),
    io_manager_key="df",
    metadata={"name": "gold"},
)
def fetch_gold(tgju_api: TgjuAPIResource) -> Output[pd.DataFrame]:
    """
    historical prices for 18k Gold/RIAL
    """
<<<<<<< HEAD
    tgju_api.fetch_history(currency="geram18")
=======
    return Output(
        tgju_api.fetch_history(currency="geram18"),
        metadata={"name": "gold"},
    )
>>>>>>> 152360a (unify file resource to df)


@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    automation_condition=AutomationCondition.eager(),
    io_manager_key="df",
    metadata={"name": "tsetmc_companies"},
)
async def fetch_tsetmc_filtered_companies(
    tsetmc_api: TSEMTMCAPIResource,
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
        46: "تجارت عمده فروشی به جز وسایل نقلیه موتور",
        56: "سرمایه گذاریها",
        66: "بیمه وصندوق بازنشستگی به جزتامین اجتماعی",
        67: "فعالیتهای کمکی به نهادهای مالی واسط",
        61: "حمل و نقل آبی",
    }
    # filter symbols with excluded industry groups
    symbols = get_companies[
        ~get_companies["industry_group"].isin(excluded_insustries.keys())
    ]["symbol"]
    return await tsetmc_api.fetch_symbols(symbols)


@asset(
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    automation_condition=AutomationCondition.on_cron("@weekly")
    | AutomationCondition.eager(),
    io_manager_key="df",
    metadata={"name": "tsetmc_stocks"},
)
async def fetch_tsetmc_stocks(
    tsetmc_api: TSEMTMCAPIResource, fetch_tsetmc_filtered_companies
) -> pd.DataFrame:
    """
    fetch stock price history for filtered companies
    """

    return await tsetmc_api.fetch_stocks(fetch_tsetmc_filtered_companies)


@asset(
    partitions_def=company_timeframe_partition,
    io_manager_key="df",
    metadata={"name": "ata_kek_file"},
)
async def ata_kek(fetch_company_reports, fetch_tsetmc_stocks):
    from jdatetime import date as jdate

    from codal.parser.mappings import calculations, table_names_map
    from codal.parser.repository import extract_financial_data

    for i in fetch_company_reports.itterrows():
        with open(i["path"], "r+") as f:
            a = f.ready()

        yield extract_financial_data(
            a,
            table_names_map,
            calculations,
            jdate.fromisoformat(["name"].split(".")[0]),
            i["symbol"],
        )
