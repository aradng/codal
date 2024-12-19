from dagster import EnvVar, repository

from dagster import Definitions, load_assets_from_modules

from codal.dagster_assets.fetcher import assets
from codal.dagster_assets.fetcher.resources import (
    FileStoreCompanyListing,
    FileStoreCompanyReport,
    FileStoreIndustryListing,
)
from codal.dagster_assets.fetcher.sensors import (
    get_companies_sensor,
    codal_sources_freshness_checks,
    codal_sources_freshness_sensor,
    codal_reports_freshness_checks,
    codal_reports_freshness_sensor,
)

from codal.dagster_assets.fetcher.jobs import fetch_company_reports_job
from codal.dagster_assets.fetcher.schedules import fetch_codal_reports_schedule

fetcher_assets = load_assets_from_modules([assets], group_name="fetcher")

defs = Definitions(
    assets=fetcher_assets,
    resources={
        "industries_file": FileStoreIndustryListing(),
        "companies_file": FileStoreCompanyListing(),
        "company_report": FileStoreCompanyReport(start_date="2011-03-21"),  # 1390
    },
    asset_checks=[*codal_sources_freshness_checks, *codal_reports_freshness_checks],
    sensors=[
        get_companies_sensor,
        codal_sources_freshness_sensor,
        codal_reports_freshness_sensor,
    ],
    jobs=[fetch_company_reports_job],
    schedules=[fetch_codal_reports_schedule],
)
