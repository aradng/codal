from datetime import datetime
from pathlib import Path

import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetIn,
    AutomationCondition,
    DimensionPartitionMapping,
    MultiPartitionKey,
    MultiPartitionMapping,
    Output,
    SpecificPartitionsPartitionMapping,
    TimeWindow,
    TimeWindowPartitionMapping,
    asset,
)
from jdatetime import date as jdate

from codal.fetcher.partitions import (
    company_multi_partition,
    report_multi_partition,
    timeframes,
)
from codal.parser.mappings import table_names_map, table_names_map_b98
from codal.parser.repository import (
    IncompatibleFormatError,
    calc_financial_ratios,
    collect_prices,
    extract_variables,
    financial_ratios_of_industries,
)
from codal.parser.schemas import PriceCollection, PriceDFs
from codal.parser.utils import fetch_chronological_report


@asset(
    automation_condition=AutomationCondition.eager(),
    io_manager_key="mongo",
    metadata={"collection": "Industries"},
    ins={
        "get_industries": AssetIn(key="get_industries", input_manager_key="df")
    },
)
def industries(get_industries: pd.DataFrame) -> Output[pd.DataFrame]:
    get_industries.rename(columns={"Id": "_id", "Name": "name"}, inplace=True)
    return Output(get_industries, metadata={"records": len(get_industries)})


@asset(
    automation_condition=AutomationCondition.eager(),
    io_manager_key="mongo",
    metadata={"collection": "Companies"},
    ins={
        "get_companies": AssetIn(key="get_companies", input_manager_key="df"),
        "get_industries": AssetIn(
            key="get_industries", input_manager_key="df"
        ),
        "fetch_tsetmc_filtered_companies": AssetIn(
            key="fetch_tsetmc_filtered_companies", input_manager_key="df"
        ),
    },
)
def companies(
    get_companies: pd.DataFrame,
    get_industries: pd.DataFrame,
    fetch_tsetmc_filtered_companies: pd.DataFrame,
) -> Output[pd.DataFrame]:
    df = fetch_tsetmc_filtered_companies.drop(
        columns=["market_title", "market_type"]
    ).copy()
    df = df.merge(
        get_companies[["symbol", "industry_group"]],
        left_on="symbol",
        right_on="symbol",
        how="inner",
    )
    df = (
        df.merge(
            get_industries[["Id", "Name"]],
            left_on="industry_group",
            right_on="Id",
            how="left",
        )
        .drop(columns=["Id"])
        .rename(columns={"Name": "industry_name"})
    )
    df.loc[df["industry_name"].isna(), "industry_name"] = df[
        "industry_group"
    ].astype(str)
    df.rename(columns={"instrument_code": "_id"}, inplace=True)
    return Output(df, metadata={"records": len(df)})


@asset(
    partitions_def=company_multi_partition,
    io_manager_key="mongo",
    metadata={"collection": "Profiles"},
    ins={
        "fetch_company_reports": AssetIn(
            key="fetch_company_reports", input_manager_key="io_manager"
        )
    }
    | {
        asset: AssetIn(key=asset, input_manager_key="df")
        for asset in [
            "fetch_tsetmc_stocks",
            "fetch_gdp",
            "fetch_gold",
            "fetch_usd",
            "fetch_commodity",
            "get_companies",
        ]
    },
)
async def profiles(
    context: AssetExecutionContext,
    fetch_company_reports: pd.DataFrame,
    fetch_tsetmc_stocks: pd.DataFrame,
    fetch_gdp: pd.DataFrame,
    fetch_gold: pd.DataFrame,
    fetch_usd: pd.DataFrame,
    fetch_commodity: pd.DataFrame,
    get_companies: pd.DataFrame,
) -> Output[pd.DataFrame]:

    answer = []
    assert isinstance(context.partition_key, MultiPartitionKey)
    assert isinstance(context.partition_time_window, TimeWindow)
    timeframe = int(context.partition_key.keys_by_dimension["timeframe"])

    for _, company in fetch_company_reports.iterrows():
        data = pd.read_pickle(rf"{company["path"]}")
        report_jdate = jdate.fromisoformat(company["name"].split(".")[0])

        price_collection = collect_prices(
            report_jdate.togregorian(),
            timeframe,
            company["symbol"],
            PriceDFs(
                TSETMC_STOCKS=fetch_tsetmc_stocks,
                GDP=fetch_gdp,
                GOLD_PRICES=fetch_gold,
                OIL_PRICES=fetch_commodity,
                USD_PRICES=fetch_usd,
            ),
        )

        try:
            report = extract_variables(
                data,
                table_names_map,
            )
        except IncompatibleFormatError:
            report = extract_variables(
                data,
                table_names_map_b98,
            )
        extracted_data = calc_financial_ratios(
            pd.Series(report.model_dump() | price_collection.model_dump())
        )

        extracted_data["name"] = company["symbol"]
        extracted_data["is_industry"] = False
        extracted_data["industry_group"] = get_companies.loc[
            get_companies["symbol"] == company["symbol"],
            "industry_group",
        ].iat[0]
        extracted_data["jdate"] = report_jdate.isoformat()
        extracted_data["date"] = datetime.combine(
            report_jdate.togregorian(), datetime.min.time()
        )
        extracted_data["timeframe"] = timeframe

        answer.append(extracted_data)

        context.log.info(
            f"Successfully processed {company['symbol']} - {company["name"]}"
        )
    result_df = pd.DataFrame(answer)
    return Output(result_df, metadata={"records": len(result_df)})


async def industry_profiles(
    context: AssetExecutionContext,
    fetch_gdp: pd.DataFrame,
    get_companies: pd.DataFrame,
    get_industries: pd.DataFrame,
) -> Output[pd.DataFrame]:

    all_raw_data = []
    assert isinstance(context.partition_key, MultiPartitionKey)
    assert isinstance(context.partition_time_window, TimeWindow)
    time_window = context.partition_time_window
    timeframe = int(context.partition_key.keys_by_dimension["timeframe"])
    context.log.info(
        f"Processing reports for timeframe: {timeframe}"
        f" from {time_window.start} to {time_window.end}"
    )

    for report_date, company in fetch_chronological_report(
        Path("./data/companies/"),
        timeframe=timeframe,
        from_date=time_window.start,
        to_date=time_window.end,
    ).iterrows():

        context.log.info(
            f"Processing report: {company['path']}"
            f" for {company['symbol']} at"
            f"{jdate.fromgregorian(date=report_date).isoformat()}"
        )

        try:
            data = pd.read_pickle(rf"{company["path"]}")
            if not data:
                context.log.warning(
                    f"Empty data for {company['symbol']} at {company['path']}"
                )
                continue
            try:
                report = extract_variables(
                    data,
                    table_names_map,
                )
            except IncompatibleFormatError:
                report = extract_variables(
                    data,
                    table_names_map_b98,
                )

            industry_group = get_companies.loc[
                get_companies["symbol"] == company["symbol"],
                "industry_group",
            ].iat[0]

            raw_data = pd.Series(
                report.model_dump()
                | PriceCollection(
                    GDP=fetch_gdp.asof(pd.Timestamp(report_date))["gdp_ppp"]
                ).model_dump()
                | dict(industry_group=industry_group)
            )

            all_raw_data.append(raw_data)

            context.log.info(
                f"Successfulfy processed report: {company['path']}"
                f" for {company['symbol']} at"
                f"{jdate.fromgregorian(date=report_date).isoformat()}"
            )

        except Exception as e:
            context.log.error(
                f"Error processing {company['symbol']}"
                f" at {company['path']}: {e}",
                exc_info=True,
            )
    context.log.info(
        f"Calculating ratios of reports for timeframe: {timeframe}"
        f" from {time_window.start} to {time_window.end}"
    )

    industry_reports = (
        pd.DataFrame(all_raw_data).groupby(["industry_group"]).sum()
    )
    result_df: pd.DataFrame = industry_reports.apply(
        financial_ratios_of_industries, axis=1
    )
    result_df["is_industry"] = True
    result_df["name"] = result_df["industry_group"].apply(
        lambda group: (
            get_industries.loc[get_industries["Id"] == group, "Name"].iat[0]
        )
    )
    result_df["date"] = time_window.end
    result_df["jdate"] = jdate.fromgregorian(date=time_window.end).isoformat()
    result_df["timeframe"] = timeframe

    context.log.info(
        f"Processed reports for timeframe: {timeframe}"
        f" from {time_window.start} to {time_window.end}"
    )

    return Output(result_df, metadata={"records": len(result_df)})


industry_profiles_assets = [
    asset(
        name=f"industry_profiles_{timeframes[timeframe]}",
        partitions_def=partition_def,
        io_manager_key="mongo",
        metadata={"collection": "Profiles"},
        ins={
            asset: AssetIn(key=asset, input_manager_key="df")
            for asset in [
                "fetch_gdp",
                "get_companies",
                "get_industries",
            ]
        },
        deps=[
            AssetDep(
                "fetch_company_reports",
                partition_mapping=MultiPartitionMapping(
                    {
                        "timeframe": DimensionPartitionMapping(
                            "timeframe",
                            SpecificPartitionsPartitionMapping([timeframe]),
                        ),
                        "timewindow": DimensionPartitionMapping(
                            "timewindow",
                            partition_mapping=TimeWindowPartitionMapping(
                                allow_nonexistent_upstream_partitions=True,
                            ),
                        ),
                    }
                ),
            )
        ],
    )(industry_profiles)
    for timeframe, partition_def in report_multi_partition.items()
]
