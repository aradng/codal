import asyncio
import traceback
from itertools import chain
from urllib.parse import urlencode, urljoin

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    Backoff,
    Failure,
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
    CodalReportResource,
    FileStoreCompanyReport,
    TgjuAPIResource,
    TSEMTMCAPIResource,
)
from codal.fetcher.schemas import (
    CompanyReportLetter,
    CompanyReportOut,
    CompanyReportsIn,
)
from codal.fetcher.utils import APIError, sanitize_persian


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
    codal_api: CodalAPIResource,
) -> Output[pd.DataFrame]:
    """
    returns updated industry listings
    """
    df = codal_api.industries
    df.drop_duplicates("Id", keep="last", inplace=True)
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


def fetch_reports(params: CompanyReportOut) -> list[CompanyReportLetter]:
    listings: list[CompanyReportsIn] = []
    current_page = 1
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


async def fetch_report_data(
    context: AssetExecutionContext,
    report: CompanyReportLetter,
    company_report: FileStoreCompanyReport,
    codal_report_api: CodalReportResource,
    semaphore: asyncio.Semaphore,
):
    file = {
        "symbol": sanitize_persian(report.Symbol.strip()),
        "year": report.jdate.year,
        "timeframe": report.timeframe,
        "name": report.jdate.isoformat(),
        "error": False,
    }
    file["path"] = company_report.path(**file)
    try:
        context.log.info(f"fetching {report.Url}")
        async with semaphore:
            tables = await codal_report_api.fetch_tables(str(report.Url))
        company_report.write(
            tables,
            symbol=file["symbol"],
            timeframe=file["timeframe"],
            name=file["name"],
        )
    except (TimeoutError, asyncio.CancelledError) as e:
        raise e
    except APIError:
        file["error"] = True
    except Exception as e:
        context.log.error(f"error fetching {report.Url}: {e}")
        raise Failure(
            description=f"error fetching {report.Url}: {e}",
            metadata={
                "url": MetadataValue.url(str(report.Url)),
                "error": MetadataValue.text(str(e)),
                "trace": MetadataValue.text(str(traceback.format_exc())),
            },
            allow_retries=False,
        )
    return file


@asset(
    retry_policy=RetryPolicy(
        max_retries=5,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    partitions_def=company_timeframe_partition,
    automation_condition=AutomationCondition.eager(),
)
async def fetch_company_reports(
    context: AssetExecutionContext,
    fetch_tsetmc_filtered_companies,
    company_report: FileStoreCompanyReport,
    codal_report_api: CodalReportResource,
) -> Output[pd.DataFrame]:
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
    context.log.info(
        f"fetching with params {params.model_dump(exclude_none=True)}"
    )
    context.log.info(f"fetch url: {MetadataValue.url(get_url(params))}")
    filtered_reports = filter(
        lambda x: x.Symbol
        in fetch_tsetmc_filtered_companies["source_symbol"].values,
        fetch_reports(params=params),
    )
    semaphore = asyncio.Semaphore(10)  # Limit concurrency
    tasks = [
        asyncio.create_task(
            fetch_report_data(
                context,
                report,
                company_report,
                codal_report_api,
                semaphore,
            )
        )
        for report in filtered_reports
    ]
    try:
        data = pd.DataFrame(
            await asyncio.gather(*tasks),
            columns=["symbol", "year", "timeframe", "name", "path", "error"],
        )
    except TimeoutError:
        for task in tasks:
            task.cancel()
        raise

    return Output(
        data,
        metadata={
            "url": MetadataValue.url(get_url(params)),
            "records": MetadataValue.int(len(data)),
            "errors": MetadataValue.int(int(data["error"].sum())),
            "paths": MetadataValue.md(data.to_markdown()),
        },
    )


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
    metadata={"name": "usd"},
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
    return Output(
        tgju_api.fetch_history(currency="geram18"),
        metadata={"name": "gold"},
    )


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
    excluding industry group blacklist
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
    companies = get_companies[
        ~get_companies["industry_group"].isin(excluded_insustries.keys())
    ]
    df = await tsetmc_api.fetch_symbols(companies["symbol"])
    return df


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
