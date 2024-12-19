from dagster import DefaultScheduleStatus, ScheduleDefinition
from codal.dagster_assets.fetcher.jobs import (
    fetch_company_reports_job,
)


fetch_codal_reports_schedule = ScheduleDefinition(
    job=fetch_company_reports_job,
    cron_schedule="0 0 * * 5",  # every friday
    default_status=DefaultScheduleStatus.RUNNING,
)
