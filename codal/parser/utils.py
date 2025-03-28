from datetime import date
from pathlib import Path

import pandas as pd
from jdatetime import date as jdate


def fetch_chronological_report(
    path: Path, timeframe: int, from_date: date, to_date: date
) -> pd.DataFrame:
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
