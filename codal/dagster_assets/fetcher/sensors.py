from datetime import timedelta
from dagster import (
    AssetKey,
    DefaultSensorStatus,
    EventLogEntry,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    asset_sensor,
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
)

from codal.dagster_assets.fetcher.resources import FileStoreCompanyListing
from codal.dagster_assets.fetcher.partitions import company_partitions
from codal.dagster_assets.fetcher.utils import partition_name_sanitizer
import pandas as pd

from codal.dagster_assets.fetcher.assets import (
    get_companies,
    get_company_reports,
    get_company_excels,
    get_industries,
)


@asset_sensor(
    asset_key=get_companies.key,
    default_status=DefaultSensorStatus.RUNNING,
)
def get_companies_sensor(
    context: SensorEvaluationContext,
    asset_event: EventLogEntry,
    companies_file: FileStoreCompanyListing,
):
    """
    update companies dynamic partition from companies.csv
    """
    df = companies_file.read().index.drop_duplicates()
    df = df.map(partition_name_sanitizer)
    current_partitions = pd.Index(
        company_partitions.get_partition_keys(dynamic_partitions_store=context.instance)
    )
    df = df[~df.isin(current_partitions)]
    if df.empty:
        return SkipReason(f"no new companies.")
    yield SensorResult(
        dynamic_partitions_requests=[company_partitions.build_add_request(list(df))],
    )


codal_sources_freshness_checks = build_last_update_freshness_checks(
    assets=[get_companies, get_industries], lower_bound_delta=timedelta(days=2)
)

codal_sources_freshness_sensor = build_sensor_for_freshness_checks(
    name="codal_sources_freshness_sensor",
    freshness_checks=codal_sources_freshness_checks,
    default_status=DefaultSensorStatus.RUNNING,
)

codal_reports_freshness_checks = build_last_update_freshness_checks(
    assets=[get_company_reports],
    lower_bound_delta=timedelta(days=10),
)

codal_reports_freshness_sensor = build_sensor_for_freshness_checks(
    name="codal_reports_freshness_sensor",
    freshness_checks=codal_reports_freshness_checks,
    default_status=DefaultSensorStatus.RUNNING,
)
