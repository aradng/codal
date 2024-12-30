from dagster import Definitions, EnvVar, load_assets_from_modules

from codal.fetcher import assets
from codal.fetcher.resources import (
    AlphaVantaAPIResource,
    APINinjaResource,
    CodalAPIResource,
    DataFrameIOManager,
    FileStoreCompanyReport,
    TgjuAPIResource,
    TSEMTMCAPIResource,
)
from codal.fetcher.sensors import (
    fetcher_sources_freshness_checks,
    fetcher_sources_freshness_sensor,
)

fetcher_assets = load_assets_from_modules([assets], group_name="fetcher")

defs = Definitions(
    assets=fetcher_assets,
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
    },
    asset_checks=[*fetcher_sources_freshness_checks],
    sensors=[
        fetcher_sources_freshness_sensor,
    ],
)
