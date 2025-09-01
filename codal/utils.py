from typing import Any

import requests
from beanie.odm.queries.find import FindMany


def normalize_field(field: str, data_max: dict[str, float]) -> dict:
    """Return a MongoDB expression to normalize a numeric field by its max magnitude."""  # noqa: E501

    return {"$divide": [f"${field}", data_max[field]]}


async def field_max(query: FindMany, fields: list[str]) -> dict[str, float]:
    """Return max absolute values for given fields from the query (defaults to 1 for zeros)."""  # noqa: E501
    data_max = await query.aggregate(
        [
            {
                "$group": {"_id": None}
                | {field: {"$max": {"$abs": f"${field}"}} for field in fields}
            },
        ]
    ).to_list()
    if not data_max:
        return {field: 0 for field in fields}
    return {
        field: value if value != 0 else 1
        for field, value in data_max[0].items()
        if field != "_id"
    }


def debug_query(
    fields: dict[str, float], data_max: dict[str, float]
) -> list[dict]:
    """Build a Mongo pipeline to expose normalization and weighted score steps for debugging."""  # noqa: E501
    return [
        {
            "$addFields": {
                f"{field}_norm": (
                    normalize_field(field, data_max),
                    weight,
                )
                for field, weight in fields.items()
            }
        },
        {
            "$addFields": {
                f"{field}_mul": {
                    "$multiply": [
                        f"${field}_norm",
                        weight,
                    ]
                }
                for field, weight in fields.items()
            }
        },
        {
            "$addFields": {
                "score": {"$sum": [f"${field}_mul" for field in fields.keys()]}
            }
        },
    ]


def mongo_unique_reports_pipeline():
    """Deduplicate reports by name/date/timeframe keeping the most recent partition."""  # noqa: E501
    return [
        {"$sort": {"date": -1, "partition_key": -1}},
        {
            "$group": {
                "_id": {
                    "name": "$name",
                    "date": "$date",
                    "timeframe": "$timeframe",
                },
                "docs": {"$first": "$$ROOT"},
            }
        },
        {"$replaceRoot": {"newRoot": "$docs"}},
    ]


def mongo_fiscal_year_field():
    """Add derived fiscal year integer from Jalali date string (jdate)."""  # noqa: E501
    return {"$addFields": {"year": {"$toInt": {"$substr": ["$jdate", 0, 4]}}}}


def mongo_total_revenue_by_year_pipeline():
    """Aggregate total revenue per year across matched documents."""
    return [
        mongo_fiscal_year_field(),
        {"$match": {"revenue": {"$ne": None}}},
        {
            "$group": {
                "_id": {"year": "$year"},
                "industry_revenue": {"$sum": "$revenue"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "year": "$_id.year",
                "revenue": "$industry_revenue",
            }
        },
        {"$sort": {"year": -1}},
    ]


def mongo_field_groupby_pipeline(field: str, groupby_field: str):
    """Aggregate yearly sums for a field grouped by another field (e.g., industry_group)."""  # noqa: E501
    return [
        mongo_fiscal_year_field(),
        {"$match": {field: {"$ne": None}}},
        {
            "$group": {
                "_id": {"year": "$year", groupby_field: f"${groupby_field}"},
                field: {"$sum": f"${field}"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "year": "$_id.year",
                groupby_field: f"$_id.{groupby_field}",
                field: 1,
            }
        },
        {"$sort": {"year": -1}},
    ]


def mongo_total_field_by_year_pipeline(field: str):
    """Aggregate total of a numeric field per year across matched documents."""  # noqa: E501
    return [
        mongo_fiscal_year_field(),
        {"$match": {field: {"$ne": None}}},
        {
            "$group": {
                "_id": {"year": "$year"},
                field: {"$sum": f"${field}"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "year": "$_id.year",
                field: f"${field}",
            }
        },
        {"$sort": {"year": -1}},
    ]


def dagster_fetch_assets(url):
    """Query Dagster GraphQL API to fetch asset keys present in the graph."""  # noqa: E501
    query = """
    query {
        assetNodes {
            assetKey {
                path
            }
        }
    }
    """
    return {
        "assetKeys": [
            asset["assetKey"]
            for asset in requests.post(
                url,
                json={
                    "query": query,
                },
            ).json()[
                "data"
            ]["assetNodes"]
        ]
    }


def dagster_status_graphql(url, variables):
    """Query Dagster GraphQL for live asset status given asset key variables."""  # noqa: E501
    query = """
    query AssetGraphLiveQuery($assetKeys: [AssetKeyInput!]!) {
    assetNodes(assetKeys: $assetKeys, loadMaterializations: true) {
        id
        assetKey {
        path
        }
        assetMaterializations(limit: 1) {
        timestamp
        metadataEntries {
            label
            ... on MarkdownMetadataEntry {
            mdStr
            label
            }
            ... on TextMetadataEntry {
            label
            text
            }
            ... on PathMetadataEntry {
            label
            path
            }
            ... on FloatMetadataEntry {
            floatValue
            label
            }
            ... on IntMetadataEntry {
            intValue
            label
            }
            ... on UrlMetadataEntry {
            label
            url
            }
        }
        }
        assetChecksOrError {
        ... on AssetChecks {
            checks {
            name
            canExecuteIndividually
            executionForLatestMaterialization {
                status
                timestamp
                evaluation {
                severity
                }
            }
            }
        }
        }
        partitionStats {
        numMaterialized
        numMaterializing
        numPartitions
        numFailed
        }
        groupName
    }
    assetsLatestInfo(assetKeys: $assetKeys) {
        latestRun {
        endTime
        status
        }
        assetKey {
        path
        }
    }
    }
    """
    return requests.post(
        url,
        json={
            "operationName": "AssetGraphLiveQuery",
            "query": query,
            "variables": variables,
        },
    ).json()


def dagster_metadata_parser(input: list[dict[str, Any]]):
    """Flatten Dagster metadata entries into a simple label->value dict."""  # noqa: E501
    result = {}
    for entry in input:
        if not entry:
            continue
        key = entry.pop("label")
        value = entry[next(iter(entry))]
        result[key] = value

    return result
