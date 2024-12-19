from pathlib import Path
from urllib.parse import urlencode, urljoin
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    MaterializeResult,
    MetadataValue,
    MultiPartitionKey,
    asset,
    op,
)
from itertools import chain
from pydantic import BaseModel
import requests
from codal.dagster_assets.fetcher.resources import (
    FileStoreCompanyListing,
    FileStoreCompanyReport,
    FileStoreIndustryListing,
)
from codal.fetcher.schemas import (
    CompanyIn,
    CompanyReportLetter,
    CompanyReportOut,
    CompanyReportsIn,
    IndustryGroupIn,
)

from codal.dagster_assets.fetcher.partitions import (
    company_timeframe_partition,
)
import pandas as pd

from codal.dagster_assets.fetcher.utils import partition_name_sanitizer


@asset(
    automation_condition=AutomationCondition.on_cron("@daily"),
)
def get_industries(
    industries_file: FileStoreIndustryListing,
):
    response = requests.get(
        "https://search.codal.ir/api/search/v1/IndustryGroup",
        headers={
            "User-Agent": "",
        },
    )

    df = pd.DataFrame(
        [
            IndustryGroupIn.model_validate(industry).model_dump()
            for industry in response.json()
        ]
    )
    df.set_index("Id")
    industries_file.write(df)
    return df


@asset(
    automation_condition=AutomationCondition.on_cron("@daily"),
)
def get_companies(
    context: AssetExecutionContext,
    companies_file: FileStoreCompanyListing,
) -> pd.DataFrame:
    """
    updates company listings and returns new companies
    """
    response: requests.Response = requests.get(
        "https://search.codal.ir/api/search/v1/companies",
        headers={
            "User-Agent": "",
        },
    )

    companies = pd.DataFrame(
        [CompanyIn.model_validate(r).model_dump() for r in response.json()]
    )
    companies.set_index("symbol", inplace=True)
    companies_file.write(companies)
    return companies


def get_url(params: BaseModel) -> str:
    base_url = "https://search.codal.ir/api/search/v2/q?"
    query_string = urlencode(params.model_dump(exclude_none=True), safe="")
    return urljoin(base_url, f"?{query_string}")


def get_company_report_listings(
    context: AssetExecutionContext, params: CompanyReportOut
) -> list[CompanyReportLetter]:
    listings: list[CompanyReportsIn] = []
    current_page = 1
    context.log.info(f"fetching with params {params.model_dump(exclude_none=True)}")
    while True:
        params.PageNumber = current_page
        context.log.info(
            f"fetching {params.Symbol} {params.Length} month time frame: page {params.PageNumber}"
        )
        response = requests.get(
            "https://search.codal.ir/api/search/v2/q?",
            params=params.model_dump(exclude_none=True),
            headers={
                "User-Agent": "",
            },
        )
        if response.status_code != 200:
            context.log.error("failed!")
            response.raise_for_status()
        current_page += 1
        listings.append(CompanyReportsIn.model_validate(response.json()))
        if listings[-1].IsAttacker:
            context.log.warning("rate limit reached please adjust workers or delay")
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
        timeout=10,
    )
    if response.status_code == 200:
        assert isinstance(response.content, bytes)
        return response.content

    context.log.error(f"download failed for {url}")
    response.raise_for_status()
    return b""


def get_company_excels(
    context: AssetExecutionContext,
    reports: list[CompanyReportLetter],
    company_report: FileStoreCompanyReport,
) -> pd.DataFrame:
    """
    download codal reports for a specific symbol and timeframe
    returns list of PosixPath for excels in current run
    """
    files: list[Path] = []
    context.log.info(f"fetching {len(reports)} sheets")
    assert isinstance(context.partition_key, MultiPartitionKey)
    # use sanitized names for better directory naming schemas
    company_report._symbol = context.partition_key.keys_by_dimension["symbol"]
    company_report._time_frame = int(
        context.partition_key.keys_by_dimension["timeframe"]
    )
    for report in reports:
        if not report.HasExcel or report.jdate is None:
            continue
        company_report._year = report.jdate.year
        company_report._filename = f"{report.jdate.isoformat()}.xls"
        company_report.write(get_excel_report(context, report.ExcelUrl))
        files.append(company_report.data_path)

    return pd.DataFrame(files)


@asset(
    partitions_def=company_timeframe_partition,
)
def get_company_reports(
    context: AssetExecutionContext,
    company_report: FileStoreCompanyReport,
) -> MaterializeResult:
    """
    returns list of new report for a specific symbol and timeframe since last
    """
    assert isinstance(context.partition_key, MultiPartitionKey)
    symbol = partition_name_sanitizer(
        context.partition_key.keys_by_dimension["symbol"], reverse=True
    )
    timeframe = int(context.partition_key.keys_by_dimension["timeframe"])
    company_report._symbol = symbol
    company_report._time_frame = timeframe
    params = CompanyReportOut.model_validate(
        dict(
            Symbol=symbol,
            Length=timeframe,
            Audited=timeframe >= 6,
            NotAudited=not (timeframe >= 6),
            FromDate=company_report.last_checkpoint,
        )
    )
    files = get_company_excels(
        context,
        get_company_report_listings(context, params=params),
        company_report,
    )
    company_report.update_checkpoint
    return MaterializeResult(
        metadata={
            "url": MetadataValue.url(get_url(params)),
            "num_records": MetadataValue.int(len(files)),
            "paths": MetadataValue.md(files.to_markdown()),
        }
    )
