import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AutomationCondition,
    Output,
    asset,
)

from codal.fetcher.partitions import company_timeframe_partition
from codal.parser.exceptions import IncompatibleFormatError


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
    df = fetch_tsetmc_filtered_companies.copy()
    df = (
        df.merge(
            get_companies[["symbol", "industry_group"]],
            left_on="source_symbol",
            right_on="symbol",
            how="inner",
        )
        .drop(columns=["symbol_y"])
        .rename(columns={"symbol_x": "symbol"})
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
    partitions_def=company_timeframe_partition,
    io_manager_key="mongo",
    metadata={"collection": "Profiles"},
    ins={
        "fetch_company_reports": AssetIn(
            key="fetch_company_reports", input_manager_key="io_manager"
        ),
        "fetch_tsetmc_stocks": AssetIn(
            key="fetch_tsetmc_stocks", input_manager_key="df"
        ),
        "fetch_gold": AssetIn(key="fetch_gold", input_manager_key="df"),
        "fetch_usd": AssetIn(key="fetch_usd", input_manager_key="df"),
        "fetch_commodity": AssetIn(
            key="fetch_commodity", input_manager_key="df"
        ),
        "get_companies": AssetIn(key="get_companies", input_manager_key="df"),
    },
)
async def ata_kek(
    context: AssetExecutionContext,
    fetch_company_reports: pd.DataFrame,
    fetch_tsetmc_stocks: pd.DataFrame,
    fetch_gold: pd.DataFrame,
    fetch_usd: pd.DataFrame,
    fetch_commodity: pd.DataFrame,
    get_companies: pd.DataFrame,
) -> Output[pd.DataFrame]:

    # context.log.info("Processing ata_kek")
    # logger = context.log

    import logging
    import os

    from jdatetime import date as jdate

    from codal.parser.mappings import (
        calculations,
        table_names_map,
        table_names_map_b98,
    )
    from codal.parser.repository import collect_prices, extract_financial_data
    from codal.parser.schemas import PriceDFs

    logging.basicConfig(
        filename=os.path.join(os.getcwd(), "ata_kek.log"),  # Log file
        level=logging.INFO,  # Log everything (INFO and above)
        format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    )

    logger = logging.getLogger(__name__)  # Get logger instance

    answer = []
    try:
        logger.info(
            f"Processing {len(fetch_company_reports)} company reports."
        )

        for _, company in fetch_company_reports.iterrows():
            try:
                data = pd.read_pickle(rf"{company["path"]}")

                logger.info(
                    f"Processing report: {company['path']}"
                    f" for {company['symbol']}"
                )
                # d2 = {"name": ["e", "f", "g", "h"], "age": [50, 60, 70, 80]}
                # ddf = pd.DataFrame(d2)

                jdate = jdate.fromisoformat(company["name"].split(".")[0])

                price_collection = collect_prices(
                    jdate,
                    company["symbol"],
                    PriceDFs(
                        TSETMC_STOCKS=fetch_tsetmc_stocks,
                        GOLD_PRICES=fetch_gold,
                        OIL_PRICES=fetch_commodity,
                        USD_PRICES=fetch_usd,
                    ),
                    # PriceDFs(
                    #     TSETMC_STOCKS=ddf,
                    #     GOLD_PRICES=ddf,
                    #     OIL_PRICES=ddf,
                    #     USD_PRICES=ddf,
                    # ),
                )

                try:
                    extracted_data = extract_financial_data(
                        data,
                        (
                            table_names_map_b98
                            if jdate.year <= 1398
                            else table_names_map
                        ),
                        calculations,
                        jdate,
                        company["symbol"],
                        price_collection,
                    )
                except IncompatibleFormatError:
                    extracted_data = extract_financial_data(
                        data,
                        table_names_map_b98,
                        calculations,
                        jdate,
                        company["symbol"],
                        price_collection,
                    )

                extracted_data["name"] = company["symbol"]
                extracted_data["is_industry"] = False
                extracted_data["industry_group"] = get_companies.loc[
                    get_companies["symbol"] == company["symbol"],
                    "industry_group",
                ].values[0]

                extracted_data["timeframe"] = company["timeframe"]
                extracted_data["year"] = company["year"]
                jdate_value = jdate.fromisoformat(
                    company["name"].split(".")[0]
                )
                extracted_data["jdate"] = jdate_value.isoformat()
                extracted_data["date"] = jdate_value.togregorian()

                answer.append(extracted_data)
                logger.info(f"Successfully processed {company['symbol']}")
                logger.info(f"Answer Length: {len(answer)}")

            except Exception as e:
                logger.error(
                    f"Error processing {company['symbol']}"
                    f" at {company['path']}: {e}",
                    exc_info=True,
                )

    except Exception as e:
        logger.critical(f"Unexpected error: {e}")

    # Concatenate results
    if answer:
        # result_df = pd.concat(answer)
        result_df = pd.DataFrame(answer)
        logger.info("Successfully concatenated all data.")
        result_df.to_csv(os.path.join(os.getcwd(), "ata_kek.csv"), mode="a")
        return Output(result_df, metadata={"records": len(result_df)})
    else:
        logger.warning("No data was processed successfully.")
        return Output(pd.DataFrame(), metadata={"records": 0})
