from datetime import timedelta
from dagster import (
    DefaultSensorStatus,
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
    load_assets_from_modules,
)

from codal.fetcher import assets


fetcher_sources_freshness_checks = build_last_update_freshness_checks(
    assets=load_assets_from_modules([assets]),  # type: ignore[reportArgumentType]
    lower_bound_delta=timedelta(days=7),
)

fetcher_sources_freshness_sensor = build_sensor_for_freshness_checks(
    name="fetcher_sources_freshness_sensor",
    freshness_checks=fetcher_sources_freshness_checks,
    default_status=DefaultSensorStatus.RUNNING,
)
