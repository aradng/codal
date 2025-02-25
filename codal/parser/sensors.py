from datetime import timedelta

from dagster import (
    DefaultSensorStatus,
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
    load_assets_from_modules,
)

from codal.parser import assets as parser_assets

parser_assets = load_assets_from_modules([parser_assets], group_name="parser")

parser_freshness_checks = build_last_update_freshness_checks(
    assets=parser_assets,
    lower_bound_delta=timedelta(days=7),
)

parser_freshness_sensor = build_sensor_for_freshness_checks(
    name="parser_freshness_sensor",
    minimum_interval_seconds=60 * 10,
    freshness_checks=parser_freshness_checks,
    default_status=DefaultSensorStatus.RUNNING,
)
