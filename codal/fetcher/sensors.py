from datetime import timedelta

from dagster import (
    DefaultSensorStatus,
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
    load_assets_from_modules,
)

from codal.fetcher import assets as fetcher_assets

fetcher_assets = load_assets_from_modules(
    [fetcher_assets], group_name="fetcher"
)

fetcher_freshness_checks = build_last_update_freshness_checks(
    assets=fetcher_assets,
    lower_bound_delta=timedelta(days=7),
)

fetcher_freshness_sensor = build_sensor_for_freshness_checks(
    name="fetcher_freshness_sensor",
    minimum_interval_seconds=60 * 10,
    freshness_checks=fetcher_freshness_checks,
    default_status=DefaultSensorStatus.RUNNING,
)
