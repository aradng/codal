from dagster import Definitions, EnvVar, load_assets_from_modules

from codal.fetcher import assets as fetcher_assets
from codal.fetcher.resources import (
    AlphaVantaAPIResource,
    APINinjaResource,
    CodalAPIResource,
    DataFrameIOManager,
    FileStoreCompanyReport,
    MongoIOManager,
    TgjuAPIResource,
    TSEMTMCAPIResource,
)
from codal.fetcher.sensors import freshness_checks, freshness_sensor
from codal.parser import assets as parser_assets

fetcher_assets = load_assets_from_modules(
    [fetcher_assets], group_name="fetcher"
)
parser_assets = load_assets_from_modules([parser_assets], group_name="parser")

defs = Definitions(
    assets=[*fetcher_assets, *parser_assets],  # type: ignore
    resources={
        "company_report": FileStoreCompanyReport(),
        "codal_api": CodalAPIResource(),
        "tgju_api": TgjuAPIResource(),
        "ninja_api": APINinjaResource(API_KEY=EnvVar("NINJA_API_KEY")),
        "alpha_vantage_api": AlphaVantaAPIResource(
            API_KEY=EnvVar("ALPHA_VANTAGE_API_KEY")
        ),
        "tsetmc_api": TSEMTMCAPIResource(RETRY_LIMIT=3, INITIAL_RETRY_DELAY=1),
        "df": DataFrameIOManager(),
        "mongo": MongoIOManager(
            MONGO_USERNAME=EnvVar("MONGO_USERNAME"),
            MONGO_PASSWORD=EnvVar("MONGO_PASSWORD"),
            MONGO_HOSTNAME=EnvVar("MONGO_HOSTNAME"),
            MONGO_PORT=EnvVar("MONGO_PORT"),
            DB_NAME="codal",
        ),
    },
    asset_checks=[*freshness_checks],
    sensors=[
        freshness_sensor,
    ],
)
