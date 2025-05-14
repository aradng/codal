import requests
from beanie.odm.queries.find import FindMany


def normalize_field(field: str, data_max: dict[str, float]) -> dict:
    """Returns a MongoDB expression to normalize a field between -1 and 1."""

    return {"$divide": [f"${field}", data_max[field]]}


async def field_max(query: FindMany, fields: list[str]) -> dict[str, float]:
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
    return {"$addFields": {"year": {"$toInt": {"$substr": ["$jdate", 0, 4]}}}}


def mongo_total_revenue_by_year_pipeline():
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
    query = """
    query AssetGraphLiveQuery($assetKeys: [AssetKeyInput!]!) {
    assetNodes(assetKeys: $assetKeys, loadMaterializations: true) {
    id
    assetKey {
        path
    }
    assetMaterializations(limit: 1) {
        timestamp
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
