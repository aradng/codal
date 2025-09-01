import datetime

from dagster import (
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    TimeWindowPartitionsDefinition,
    WeeklyPartitionsDefinition,
)

start_date = datetime.datetime(2011, 3, 21)
# Mapping of month lengths to human labels used in asset naming
timeframes = {"3": "quarterly", "6": "semi-annually", "12": "annually"}

timeframe_partition = StaticPartitionsDefinition(list(timeframes.keys()))

# Weekly time window partitions for report fetching windows
company_time_partition = WeeklyPartitionsDefinition(
    name="timewindow", start_date=start_date, end_offset=0
)

# Cross-partition across time window and report timeframe (3/6/12 months)
company_multi_partition = MultiPartitionsDefinition(
    {
        "timewindow": company_time_partition,
        "timeframe": timeframe_partition,
    }
)

# Per-timeframe multi-partition with cron-aligned time windows for industry profiles # noqa: E501
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
