from datetime import timedelta

import pandas as pd
from dagster import (
    AssetKey,
    DefaultSensorStatus,
    EventLogEntry,
    MultiPartitionsDefinition,
    RepositoryDefinition,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    asset_sensor,
    build_last_update_freshness_checks,
    build_sensor_for_freshness_checks,
    define_asset_job,
    load_assets_from_modules,
)
from pymongo.collection import Collection

from codal.fetcher.partitions import timeframes
from codal.parser import assets as parser_assets_module
from codal.parser.utils import mongo_dedup_reports_pipeline

parser_assets = load_assets_from_modules(
    [parser_assets_module], group_name="parser"
)


parser_freshness_checks = build_last_update_freshness_checks(
    assets=parser_assets,  # type: ignore[arg-type]
    lower_bound_delta=timedelta(days=7),
)

parser_freshness_sensor = build_sensor_for_freshness_checks(
    name="parser_freshness_sensor",
    minimum_interval_seconds=60 * 10,
    freshness_checks=parser_freshness_checks,
    default_status=DefaultSensorStatus.RUNNING,
)


profile_late_submit_jobs = {
    asset.key.to_user_string(): define_asset_job(
        f"late_submit_{asset.key.to_user_string().replace("-", "_")}_job",
        [asset],
    )
    for asset in parser_assets_module.industry_profiles_assets
}


@asset_sensor(
    asset_key=AssetKey("profiles"),
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
    jobs=list(profile_late_submit_jobs.values()),
    required_resource_keys={"mongo"},
)
def profile_late_submit_sensor(
    context: SensorEvaluationContext, asset_event: EventLogEntry
):
    """Sensor to check if the fetch_company_reports asset has been
    updated with exclusive reports in partition window."""
    collection: Collection = context.resources.mongo.db["Profiles"]
    collection.aggregate(mongo_dedup_reports_pipeline())
    count = collection.delete_many({"delete": True}).deleted_count
    context.log.info(f"Deleted {count} duplicate reports")

    dagster_event = asset_event.get_dagster_event()
    materialization = dagster_event.step_materialization_data.materialization
    repo = context.repository_def
    assert isinstance(repo, RepositoryDefinition)
    assert isinstance(materialization.partition, str)
    timeframe = materialization.partition.split("|")[0]
    partition = materialization.partition.split("|")[1]
    dates = pd.to_datetime(
        pd.read_json(materialization.metadata["late_published_dates"].value)[
            0
        ],
        unit="ms",
        utc=True,
    )
    target_asset = repo.assets_defs_by_key[
        AssetKey(f"industry_profiles_{timeframes[str(timeframe)]}")
    ]
    materialized_partitions = context.instance.get_materialized_partitions(
        target_asset.key
    )
    assert isinstance(target_asset.partitions_def, MultiPartitionsDefinition)
    target_partition_def = target_asset.partitions_def

    target_partition_df = pd.DataFrame(
        [
            {
                "start": target_partition_def.time_window_for_partition_key(
                    p
                ).start,
                "end": target_partition_def.time_window_for_partition_key(
                    p
                ).end,
                "partition_key": p,
            }
            for p in materialized_partitions
        ]
    )
    matching_partition_keys = set(
        {
            row["partition_key"]
            for date in dates
            for _, row in target_partition_df[
                (target_partition_df["start"] <= date)
                & (date < target_partition_df["end"])
            ].iterrows()
        }
    )

    context.log.info(f"Source partition: {target_partition_def}")
    context.log.info(f"Source partition key: {partition}")
    context.log.info(f"Target asset: {target_asset}")
    context.log.info(
        f"late publish dates: {dates.dt.strftime("%Y-%m-%d").to_list()}"
    )
    context.log.info(
        f"matched materliazied partition keys: {matching_partition_keys}"
    )

    if not matching_partition_keys:
        yield SkipReason(
            f"No matching partition keys found for {target_asset.key}"
        )

    for partition_key in matching_partition_keys:
        context.log.info(
            f"Submitting job for {target_asset.key} "
            f"with partition key {partition_key}"
        )
        yield RunRequest(
            asset_selection=[target_asset.key],
            partition_key=partition_key,
            job_name=profile_late_submit_jobs[
                target_asset.key.to_user_string()
            ].name,
        )
