from datetime import timedelta

from dagster import (
    DefaultSensorStatus,
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
    load_assets_from_modules,
)

from codal.fetcher import assets as fetcher_assets
from codal.parser import assets as parser_assets

fetcher_assets = load_assets_from_modules(
    [fetcher_assets], group_name="fetcher"
)
parser_assets = load_assets_from_modules([parser_assets], group_name="parser")

freshness_checks = build_last_update_freshness_checks(
    assets=[*fetcher_assets, *parser_assets],  # type: ignore
    lower_bound_delta=timedelta(days=7),
)

freshness_sensor = build_sensor_for_freshness_checks(
    name="freshness_sensor",
    freshness_checks=freshness_checks,
    default_status=DefaultSensorStatus.RUNNING,
)
