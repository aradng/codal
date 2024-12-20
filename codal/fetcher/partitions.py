from dagster import (
    MultiPartitionsDefinition,
    WeeklyPartitionsDefinition,
    StaticPartitionsDefinition,
)


time_partition = WeeklyPartitionsDefinition(
    name="timewindow", start_date="2011-03-21", end_offset=0
)

company_timeframe_partition = MultiPartitionsDefinition(
    {
        "timewindow": time_partition,
        "timeframe": StaticPartitionsDefinition(["3", "6", "12"]),
    }
)
