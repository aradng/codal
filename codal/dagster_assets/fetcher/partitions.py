from dagster import (
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)


company_partitions = DynamicPartitionsDefinition(name="companies")

company_timeframe_partition = MultiPartitionsDefinition(
    {
        "symbol": company_partitions,
        "timeframe": StaticPartitionsDefinition(["3", "6", "12"]),
    }
)
