import datetime

from dagster import (
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    TimeWindowPartitionsDefinition,
    WeeklyPartitionsDefinition,
)

start_date = datetime.datetime(2011, 3, 21)

timeframe_partition = StaticPartitionsDefinition(["3", "6", "12"])

company_time_partition = WeeklyPartitionsDefinition(
    name="timewindow", start_date=start_date, end_offset=0
)

company_multi_partition = MultiPartitionsDefinition(
    {
        "timewindow": company_time_partition,
        "timeframe": timeframe_partition,
    }
)

report_multi_partition = {
    timeframe: MultiPartitionsDefinition(
        {
            "timewindow": TimeWindowPartitionsDefinition(
                start=start_date,
                fmt="%Y-%m-%d",
                cron_schedule=f"0 0 5 4/{timeframe} *",
            ),
            "timeframe": StaticPartitionsDefinition([timeframe]),
        }
    )
    for timeframe in timeframe_partition.get_partition_keys()
}
