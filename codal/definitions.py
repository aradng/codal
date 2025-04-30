from dagster import Definitions, EnvVar, load_assets_from_modules

from codal.fetcher import assets as fetcher_asset_module
from codal.fetcher.resources import (
    AlphaVantaAPIResource,
    APINinjaResource,
    CodalAPIResource,
    CodalReportResource,
    DataFrameIOManager,
    FileStoreCompanyReport,
    MongoIOManager,
    MongoResource,
    TgjuAPIResource,
    TSEMTMCAPIResource,
)
from codal.fetcher.sensors import (
    fetcher_freshness_checks,
    fetcher_freshness_sensor,
)
from codal.parser import assets as parser_asset_module
from codal.parser.sensors import (
    parser_freshness_checks,
    parser_freshness_sensor,
    profile_late_submit_sensor,
)

fetcher_assets = load_assets_from_modules(
    [fetcher_asset_module], group_name="fetcher", include_specs=True
)
parser_assets = load_assets_from_modules(
    [parser_asset_module], group_name="parser", include_specs=True
)

mongo_resource = MongoResource(
    MONGO_USERNAME=EnvVar("MONGO_USERNAME"),
    MONGO_PASSWORD=EnvVar("MONGO_PASSWORD"),
    MONGO_HOSTNAME=EnvVar("MONGO_HOSTNAME"),
    MONGO_PORT=EnvVar("MONGO_PORT"),
    DB_NAME="codal",
)

defs = Definitions(
    assets=[
        *fetcher_assets,
        *parser_assets,
    ],  # type: ignore
    resources={
        "company_report": FileStoreCompanyReport(),
        "codal_api": CodalAPIResource(),
        "codal_report_api": CodalReportResource(),
        "tgju_api": TgjuAPIResource(),
        "ninja_api": APINinjaResource(API_KEY=EnvVar("NINJA_API_KEY")),
        "alpha_vantage_api": AlphaVantaAPIResource(
            API_KEY=EnvVar("ALPHA_VANTAGE_API_KEY")
        ),
        "tsetmc_api": TSEMTMCAPIResource(RETRY_LIMIT=3, INITIAL_RETRY_DELAY=5),
        "df": DataFrameIOManager(),
        "mongodf": MongoIOManager(client=mongo_resource),
        "mongo": mongo_resource,
    },
    asset_checks=[*parser_freshness_checks, *fetcher_freshness_checks],
    sensors=[
        fetcher_freshness_sensor,
        parser_freshness_sensor,
        profile_late_submit_sensor,
    ],
)
