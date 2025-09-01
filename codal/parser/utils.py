from datetime import date
from pathlib import Path

import pandas as pd
from jdatetime import date as jdate


def fetch_chronological_report(
    path: Path, timeframe: int, from_date: date, to_date: date
) -> pd.DataFrame:
    """Return dataframe of available report files between dates, indexed by Gregorian date."""  # noqa: E501
    _from_date = pd.Timestamp(from_date).replace(tzinfo=None)
    _to_date = pd.Timestamp(to_date).replace(tzinfo=None)
    return (
        pd.DataFrame(
            [
                {
                    "date": pd.Timestamp(
                        jdate.fromisoformat(file.name).togregorian()
                    ),
                    "symbol": file.parent.parent.name,
                    "path": file,
                }
                for file in path.glob(f"./*/{timeframe}/*")
            ]
        )
        .set_index("date")
        .sort_index(ascending=True)
        .loc[_from_date:_to_date]
    )


def mongo_dedup_reports_pipeline():
    """Pipeline to mark older duplicate profile documents for deletion via merge."""  # noqa: E501
    return [
        {"$sort": {"date": -1, "partition_key": -1}},
        {
            "$group": {
                "_id": {
                    "name": "$name",
                    "date": "$date",
                    "timeframe": "$timeframe",
                },
                "docs": {"$push": "$$ROOT"},
                "count": {"$sum": 1},
            }
        },
        # {"$match": {"count": {"$gt": 1}}},
        {
            "$project": {
                "docs": {"$slice": ["$docs", 1, {"$size": "$docs"}]},
                "count": 1,
            }
        },
        {"$unwind": {"path": "$docs"}},
        {"$replaceRoot": {"newRoot": "$docs"}},
        {"$set": {"delete": True}},
        {
            "$merge": {
                "into": "Profiles",
                "on": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "discard",
            }
        },
    ]
