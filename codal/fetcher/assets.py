import asyncio
import traceback
from datetime import timedelta
from itertools import chain
from urllib.parse import urlencode, urljoin

import matplotlib.pyplot as plt
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
from jdatetime import date as jdate
from pydantic import BaseModel

from codal.fetcher.partitions import company_multi_partition
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
    df["Id"] = df["Id"].astype(int)
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
    assert report.jdate is not None
    file = {
        "symbol": sanitize_persian(report.Symbol.strip()),
        "year": report.jdate.year,
        "timeframe": report.timeframe,
        "name": report.jdate.isoformat(),
        "date": report.date,
        "error": False,
    }
    file["path"] = company_report.path(**file)
    try:
        async with semaphore:
            context.log.info(f"fetching {report.Url}")
            tables = await codal_report_api.fetch_tables(str(report.Url))
        company_report.write(
            tables,
            symbol=file["symbol"],
            timeframe=file["timeframe"],
            name=file["name"],
        )
        if "error" in tables:
            file["error"] = True
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
    partitions_def=company_multi_partition,
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
        lambda x: x.Symbol in fetch_tsetmc_filtered_companies["symbol"].values
        and x.jdate is not None,
        fetch_reports(params=params),
    )
    semaphore = asyncio.Semaphore(3)  # Limit concurrency
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
            columns=[
                "symbol",
                "year",
                "timeframe",
                "name",
                "date",
                "path",
                "error",
            ],
        )
    except TimeoutError:
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
) -> Output[pd.DataFrame]:
    """
    historical gdp for iran
    """
    df = ninja_api.fetch_gdp(country="iran")
    df.index = pd.to_datetime(df.index)
    df = (
        df.resample("ME")
        .asfreq()
        .interpolate(method="linear", limit_direction="both")
    )
    df["jdate"] = df.index.map(lambda x: jdate.fromgregorian(date=x))
    df = df.bfill().ffill()
    return Output(
        df,
        metadata={"records": MetadataValue.int(len(df))},
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
    metadata={"name": "oil"},
)
def fetch_commodity(
    alpha_vantage_api: AlphaVantaAPIResource,
) -> Output[pd.DataFrame]:
    """
    historical prices for BRENT CRUDE OIL
    """
    df = alpha_vantage_api.fetch_history(symbol="BRENT")
    return Output(
        df,
        metadata={"records": MetadataValue.int(len(df))},
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
    metadata={"name": "usd"},
)
def fetch_usd(tgju_api: TgjuAPIResource) -> Output[pd.DataFrame]:
    """
    historical prices for USD/RIAL
    """
    df = tgju_api.fetch_history(currency="price_dollar_rl")
    return Output(
        df,
        metadata={"records": MetadataValue.int(len(df))},
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
    metadata={"name": "gold"},
)
def fetch_gold(tgju_api: TgjuAPIResource) -> Output[pd.DataFrame]:
    """
    historical prices for 18k Gold/RIAL
    """
    df = tgju_api.fetch_history(currency="geram18")
    return Output(
        df,
        metadata={"records": MetadataValue.int(len(df))},
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
) -> Output[pd.DataFrame]:
    """
    list of non deleted codal companies listed in tsetmc,
    excluding industry group blacklist
    """
    # for now its a mapping might search on industries
    # if industry indexes change over time
    excluded_industries = pd.DataFrame(
        {
            2: "اوراق مشارکت و سپرده های بانکی",
            39: "شرکتهای چند رشته ای صنعتی",
            46: "تجارت عمده فروشی به جز وسایل نقلیه موتور",
            56: "سرمایه گذاریها",
            57: "بانکها و موسسات اعتباری",
            66: "بیمه وصندوق بازنشستگی به جزتامین اجتماعی",
            67: "فعالیتهای کمکی به نهادهای مالی واسط",
            61: "حمل و نقل آبی",
        }.items(),
        columns=["Id", "Name"],
    ).set_index("Id")
    # filter symbols with excluded industry groups
    companies = get_companies[
        ~get_companies["industry_group"].isin(excluded_industries.index)
    ]
    excluded_companies = get_companies[
        get_companies["industry_group"].isin(excluded_industries.index)
    ]["symbol"].values
    df = await tsetmc_api.fetch_symbols(companies["symbol"])
    return Output(
        df,
        metadata={
            "records": MetadataValue.int(len(df)),
            "included_records": MetadataValue.int(len(companies)),
            "excluded_records": MetadataValue.int(len(excluded_companies)),
            "excluded_industries": MetadataValue.md(
                excluded_industries.to_markdown(),
            ),
        },
    )


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
) -> Output[pd.DataFrame]:
    """
    fetch stock price history for filtered companies
    """
    df = await tsetmc_api.fetch_stocks(fetch_tsetmc_filtered_companies)
    df["jdate"] = (df.index.to_series().astype("int64") // 10**9).map(
        lambda x: jdate.fromtimestamp(x)
    )
    df["jdate_next"] = df["jdate"].shift().bfill()
    df["eom"] = df.apply(
        lambda x: ((x["jdate"] + timedelta(days=1)).month != x["jdate"].month)
        or x["jdate"].month != x["jdate_next"].month,
        axis=1,
    )
    df = df.drop(columns=["jdate_next"]).sort_index(ascending=True)
    # just hold end of jmonth data
    # df = df[df["eom"]]
    return Output(
        df,
        metadata={
            "records": MetadataValue.int(len(df)),
        },
    )


@asset(automation_condition=AutomationCondition.eager())
def company_report_summary(
    fetch_company_reports: dict[str, pd.DataFrame],
) -> Output[pd.DataFrame]:
    """
    Generate a summary of reports, including an image visualization.
    """

    df = pd.concat(fetch_company_reports.values())
    df["jdate"] = df["name"].apply(jdate.fromisoformat)
    df["date"] = pd.to_datetime(df["jdate"].apply(lambda x: x.togregorian()))
    summary_df = (
        df.groupby(["date", "timeframe"])
        .agg(
            num_records=("error", "count"),
            total_errors=("error", "sum"),
        )
        .reset_index()
    )
    summary_df["total_errors"] = summary_df["total_errors"].astype(int)
    summary_df["num_records"] = summary_df["num_records"].astype(int)
    timeframes = sorted(summary_df["timeframe"].unique())

    plt.style.use("dark_background")
    plt.tight_layout()
    fig, axes = plt.subplots(len(timeframes), 1, figsize=(30, 20))

    for i, timeframe in enumerate(timeframes):
        subset = summary_df[summary_df["timeframe"] == timeframe]

        axes[i].plot(
            subset["date"],
            subset["num_records"],
            label=f"Records ({timeframe} months)",
            marker="o",
            linewidth=2,
        )
        axes[i].plot(
            subset["date"],
            subset["total_errors"],
            label=f"Errors ({timeframe} months)",
            marker="x",
            linestyle="dashed",
            linewidth=2,
        )
        axes[i].fill_between(subset["date"], subset["num_records"], alpha=0.2)
        axes[i].fill_between(subset["date"], subset["total_errors"], alpha=0.4)
        axes[i].set_ylabel("Counts", fontsize=14, color="white")
        axes[i].set_title(
            f"Records & Errors Over Time: {timeframe} Months",
            fontsize=16,
            color="white",
        )
        axes[i].legend(loc="upper left", fontsize=12)
        axes[i].grid(color="gray", linestyle="dashed", alpha=0.5)

        total_records = subset["num_records"].sum()
        total_errors = subset["total_errors"].sum()
        relative_error = (
            (total_errors / total_records) * 100 if total_records > 0 else 0
        )

        textstr = (
            f"Total Records: {total_records:,}\n"
            f"Total Errors: {total_errors:,}\n"
            f"Error %: {relative_error:.2f}%"
        )
        props = dict(boxstyle="round", facecolor="black", alpha=0.5)
        axes[i].text(
            0.005,
            0.55,
            textstr,
            transform=axes[i].transAxes,
            fontsize=14,
            color="white",
            bbox=props,
        )

    plt.xticks(rotation=45, fontsize=12, color="white")
    plt.yticks(fontsize=12, color="white")

    fig.suptitle(
        "Company Report Summary: Records & Errors Over Time",
        fontsize=18,
        color="white",
    )
    img_path = "./data/company_report_summary.png"
    plt.savefig(img_path, bbox_inches="tight", dpi=300)
    plt.close()

    summary = summary_df.groupby("timeframe").agg(
        num_records=("num_records", "sum"), errors=("total_errors", "sum")
    )
    summary["error_percentage"] = (
        summary["errors"] / summary["num_records"]
    ) * 100

    summary.loc["total"] = summary.sum(numeric_only=True)
    return Output(
        summary_df,
        metadata={
            "summary": MetadataValue.md(summary.to_markdown()),
            "image": MetadataValue.path(img_path),
            "path": MetadataValue.path(img_path),
        },
    )
