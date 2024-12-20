from dagster import define_asset_job
from codal.fetcher.partitions import (
    company_timeframe_partition,
)
from codal.fetcher.assets import fetch_company_reports

fetch_company_reports_job = define_asset_job(
    name="fetch_company_reports_job",
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1,
                },
            }
        }
    },
    selection=[fetch_company_reports],
    partitions_def=company_timeframe_partition,
)
