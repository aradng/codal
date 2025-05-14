from datetime import datetime
from pathlib import Path

import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetIn,
    AutomationCondition,
    DimensionPartitionMapping,
    HookContext,
    MetadataValue,
    MultiPartitionKey,
    MultiPartitionMapping,
    Output,
    SpecificPartitionsPartitionMapping,
    TimeWindow,
    TimeWindowPartitionMapping,
    asset,
    success_hook,
)
from jdatetime import date as jdate
from pymongo.collection import Collection

from codal.fetcher.partitions import (
    company_multi_partition,
    report_multi_partition,
    timeframes,
)
from codal.fetcher.resources import MongoResource
from codal.parser.mappings import table_names_map, table_names_map_b98
from codal.parser.prediction import (
    IQR_filter,
    add_lag_features,
    normalize,
    plot_feature_importance,
    plot_tain_test_errors,
    predict,
    train_list,
)
from codal.parser.repository import (
    IncompatibleFormatError,
    calc_financial_ratios,
    collect_prices,
    extract_variables,
    financial_ratios_of_industries,
)
from codal.parser.schemas import PriceCollection, PriceDFs
from codal.parser.utils import (
    fetch_chronological_report,
    mongo_dedup_reports_pipeline,
)


@asset(
    automation_condition=AutomationCondition.eager(),
    io_manager_key="mongodf",
    metadata={"collection": "Industries"},
    ins={
        "get_industries": AssetIn(key="get_industries", input_manager_key="df")
    },
)
def industries(get_industries: pd.DataFrame) -> Output[pd.DataFrame]:
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
    get_industries.rename(columns={"Id": "_id", "Name": "name"}, inplace=True)
    get_industries = get_industries[
        ~get_industries["_id"].isin(excluded_industries.index)
    ]
    return Output(get_industries, metadata={"records": len(get_industries)})


@asset(
    automation_condition=AutomationCondition.eager(),
    io_manager_key="mongodf",
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
    automation_condition=AutomationCondition.eager(),
    partitions_def=company_multi_partition,
    io_manager_key="mongodf",
    metadata={"collection": "Reports"},
    ins={
        "fetch_company_reports": AssetIn(
            key="fetch_company_reports", input_manager_key="io_manager"
        )
    }
    | {
        asset: AssetIn(key=asset, input_manager_key="df")
        for asset in [
            "fetch_tsetmc_stocks",
            "get_companies",
        ]
    },
)
async def reports(
    context: AssetExecutionContext,
    fetch_company_reports: pd.DataFrame,
    fetch_tsetmc_stocks: pd.DataFrame,
    get_companies: pd.DataFrame,
) -> Output[pd.DataFrame]:

    answer = []
    assert isinstance(context.partition_key, MultiPartitionKey)
    assert isinstance(context.partition_time_window, TimeWindow)
    timeframe = int(context.partition_key.keys_by_dimension["timeframe"])

    for _, company in fetch_company_reports.iterrows():
        if company["symbol"] not in fetch_tsetmc_stocks.columns:
            context.log.warning(
                f"Company {company['symbol']} not in TSETMC stocks"
            )
            continue
        if not company["path"].exists() and company["error"]:
            context.log.warning(
                f"Company {company['symbol']} path does not exist"
            )
            continue

        data = pd.read_pickle(company["path"])
        report_jdate = jdate.fromisoformat(company["name"].split(".")[0])

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
        extracted_data = pd.Series(report.model_dump())

        extracted_data["name"] = company["symbol"]
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


@asset(
    automation_condition=AutomationCondition.eager(),
    partitions_def=company_multi_partition,
    io_manager_key="mongodf",
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
        if company["symbol"] not in fetch_tsetmc_stocks.columns:
            context.log.warning(
                f"Company {company['symbol']} not in TSETMC stocks"
            )
            continue
        if not company["path"].exists() and company["error"]:
            context.log.warning(
                f"Company {company['symbol']} path does not exist"
            )
            continue

        data = pd.read_pickle(company["path"])
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
    late_published_dates = pd.Series()
    if not result_df.empty:
        late_published_dates = result_df[
            (
                result_df["date"]
                > pd.Timestamp(
                    context.partition_time_window.end
                ).to_datetime64()
            )
            | (
                result_df["date"]
                < pd.Timestamp(
                    context.partition_time_window.start
                ).to_datetime64()
            )
        ]["date"].drop_duplicates()
    context.log.info(f"Late importpublished dates: {late_published_dates}")
    return Output(
        result_df,
        metadata={
            "records": len(result_df),
            "late_published_dates": late_published_dates.to_json(
                orient="records"
            ),
        },
    )


@success_hook(required_resource_keys={"mongo"})
def deduplicate_reports(
    context: HookContext,
) -> int:
    collection: Collection = context.resources.mongo.db["Profiles"]
    collection.aggregate(mongo_dedup_reports_pipeline())
    count = collection.delete_many({"delete": True}).deleted_count
    context.log.info(f"Deleted {count} duplicate reports")
    context.log.info(context.op)
    context.log.info(context.hook_def)
    context.log.info(context.step_key)
    context.log.info(context.resources)
    return count


async def industry_profiles(
    context: AssetExecutionContext,
    fetch_gdp: pd.DataFrame,
    fetch_usd: pd.DataFrame,
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
                GDP=fetch_gdp.asof(pd.Timestamp(report_date))["gdp_nominal"]
                * fetch_usd.asof(pd.Timestamp(report_date))["close"]
                * 1e3,
            ).model_dump()
            | dict(industry_group=industry_group)
        )

        all_raw_data.append(raw_data)

    industry_reports = (
        pd.DataFrame(all_raw_data)
        .groupby(["industry_group"])
        .agg(
            {
                col: "sum" if col != "GDP" else "first"
                for col in pd.DataFrame(all_raw_data).columns
            }
        )
    )
    result_df: pd.DataFrame = industry_reports.apply(
        financial_ratios_of_industries, axis=1
    ).reset_index()
    result_df["is_industry"] = True
    result_df["name"] = result_df.merge(
        get_industries[["Id", "Name"]],
        how="left",
        left_on="industry_group",
        right_on="Id",
    )["Name"]
    result_df["name"] = result_df["name"].fillna(
        result_df["industry_group"].astype(int).astype(str)
    )
    result_df["date"] = time_window.end
    result_df["jdate"] = jdate.fromgregorian(date=time_window.end).isoformat()
    result_df["timeframe"] = timeframe

    return Output(result_df, metadata={"records": len(result_df)})


industry_profiles_assets = [
    asset(
        name=f"industry_profiles_{timeframes[timeframe]}",
        automation_condition=AutomationCondition.eager(),
        partitions_def=partition_def,
        io_manager_key="mongodf",
        metadata={"collection": "Profiles"},
        ins={
            asset: AssetIn(key=asset, input_manager_key="df")
            for asset in [
                "fetch_gdp",
                "fetch_usd",
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


@asset(
    deps=[profiles],
    automation_condition=AutomationCondition.eager(),
    io_manager_key="mongodf",
    metadata={"collection": "Predictions"},
)
def ranking_predictions(
    context: AssetExecutionContext, mongo: MongoResource
) -> Output[pd.DataFrame]:
    df = pd.DataFrame.from_records(
        mongo.db["Profiles"]
        .find({"is_industry": False, "timeframe": 12})
        .sort("date")
        .to_list()
    )
    context.log.info(f"Fetched {len(df)} records")
    weight_list = [
        {
            "return_on_assets": 1,
            "gross_profit_margin": 1,
        },
        {
            "return_on_assets": 1,
            "current_ratio": 1,
        },
        {
            "gross_profit_margin": 7,
            "net_profit_margin": 10,
            "total_asset_turnover": 6,
            "earnings_quality": 3,
            "current_ratio": 7,
            "debt_to_equity_ratio": 2,
        },
    ]
    df = df.drop(
        columns=["_id", "is_industry", "jdate", "partition_key", "asset_key"]
    )
    df.set_index("date", inplace=True)
    df["time"] = df.index
    df.sort_index(inplace=True)
    df = df[
        ~df.duplicated(subset=["name", "time"], keep="last").drop(
            columns=["time"]
        )
    ]
    df_list = []

    for weights in weight_list:
        sf = df.copy()
        for col in weights.keys():
            sf = IQR_filter(sf, col)
        sf["score"] = (
            normalize(sf[weights.keys()]).multiply(weights).sum(axis=1)
        )
        df_list.append(sf)

    context.log.info(f"{len(df_list)} dataframes created")

    df_w_lag = []
    for sf in df_list:
        sf = add_lag_features(
            sf,
            group_col="name",
            lags=[1, 2, 3],
            excluded_cols=["time", "date", "industry_group"],
        )
        df_w_lag.append(sf)
    context.log.info(f"added lag features to {len(df_w_lag)} dataframes")
    context.log.info("Training models")
    results = train_list(df_w_lag, weight_list)
    plot_tain_test_errors(results)
    plot_feature_importance(results)
    context.log.info("running predictions on best model")
    predictions = predict(results)
    from_date = pd.to_datetime(
        jdate(year=jdate.today().year - 1, month=1, day=1).togregorian(),
        utc=True,
    )
    to_date = pd.to_datetime(
        jdate(year=jdate.today().year, month=1, day=1).togregorian(), utc=True
    )
    predictions = predictions[
        (predictions.index >= from_date) & (predictions.index < to_date)
    ].reset_index()
    result_df = pd.DataFrame(
        [
            {
                "best_iteration": result["model"].best_iteration,
                "best_score": result["model"].best_score,
                "samples": len(result["df"].dropna(subset=["target"])),
                "error_to_std": (
                    result["model"].best_score / result["df"]["score"].std()
                ),
                "weights": result["weights"],
            }
            for result in results
        ]
    )
    return Output(
        predictions,
        metadata={
            "records": MetadataValue.int(len(predictions)),
            "results": MetadataValue.md(result_df.to_markdown()),
        },
    )
