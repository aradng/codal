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
