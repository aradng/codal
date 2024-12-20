from dagster import EnvVar, repository

from dagster import Definitions, load_assets_from_modules

from codal.fetcher import assets
from codal.fetcher.resources import (
    APINinjaResource,
    AlphaVantaAPIResource,
    CodalAPIResource,
    FileStoreCompanyListing,
    FileStoreCompanyReport,
    FileStoreIndustryListing,
    TgjuAPIResource,
)
from codal.fetcher.sensors import (
    fetcher_sources_freshness_checks,
    fetcher_sources_freshness_sensor,
)

from codal.fetcher.jobs import fetch_company_reports_job
from codal.fetcher.schedules import fetch_codal_reports_schedule

fetcher_assets = load_assets_from_modules([assets], group_name="fetcher")

defs = Definitions(
    assets=fetcher_assets,
    resources={
        "industries_file": FileStoreIndustryListing(),
        "companies_file": FileStoreCompanyListing(),
        "company_report": FileStoreCompanyReport(),
        "codal_api": CodalAPIResource(),
        "tgju_api": TgjuAPIResource(),
        "ninja_api": APINinjaResource(API_KEY=EnvVar("NINJA_API_KEY")),
        "alpha_vantage_api": AlphaVantaAPIResource(
            API_KEY=EnvVar("ALPHA_VANTAGE_API_KEY")
        ),
    },
    asset_checks=[*fetcher_sources_freshness_checks],
    sensors=[
        fetcher_sources_freshness_sensor,
    ],
    jobs=[fetch_company_reports_job],
    schedules=[fetch_codal_reports_schedule],
)
