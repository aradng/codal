from beanie import SortDirection
from fastapi import APIRouter
from pydantic import TypeAdapter

from codal.models import Company, Industry, Prediction, Profile
from codal.schemas import (
    PaginatedMixin,
    PredictionOutWithTotal,
    ProfileIn,
    ProfileOut,
    RankOut,
    RankOutWithTotal,
)
from codal.utils import (
    dagster_fetch_assets,
    dagster_status_graphql,
    field_max,
    normalize_field,
)

router = APIRouter()


@router.get("/companies")
async def get_companies() -> list[Company]:
    return await Company.find().to_list()


@router.get("/industries")
async def get_industries() -> list[Industry]:
    return await Industry.find().to_list()


@router.get("/profile/{name}", response_model=list[ProfileOut])
async def get_profile(name: str):
    return (
        await Profile.find(Profile.name == name)
        .sort((Profile.date, SortDirection.DESCENDING))
        .to_list()
    )


@router.post("/score")
async def get_rankings(profile_in: ProfileIn) -> RankOutWithTotal:
    query = Profile.find()
    if profile_in.industry_only is not None:
        query = query.find(Profile.is_industry == profile_in.industry_only)
    if profile_in.industry_group is not None:
        query = query.find(Profile.industry_group == profile_in.industry_group)
    if profile_in.timeframe:
        query = query.find(
            Profile.timeframe == profile_in.timeframe,
        )
    if profile_in.from_date is not None and profile_in.to_date is not None:
        query = query.find(
            Profile.date >= profile_in.from_date,
            Profile.date <= profile_in.to_date,
        )
    data_max = await field_max(
        query, list(profile_in.weights.model_dump(exclude_none=True).keys())
    )
    data = await query.aggregate(
        [
            {
                "$addFields": {
                    "score": {
                        "$sum": [
                            {
                                "$multiply": [
                                    weight,
                                    normalize_field(field, data_max),
                                ]
                            }
                            for field, weight in profile_in.weights.model_dump(
                                exclude_none=True
                            ).items()
                        ]
                    }
                }
            },
            {"$sort": {"score": -1 if profile_in.descending else 1}},
            {"$skip": profile_in.skip},
            {"$limit": profile_in.limit},
        ]
    ).to_list()

    return RankOutWithTotal(
        data=TypeAdapter(list[RankOut]).validate_python(data),
        page=profile_in.offset or 0,
        total=await query.count(),
    )


@router.post("/predict")
async def get_predictions(
    pagination: PaginatedMixin,
) -> PredictionOutWithTotal:
    query = (
        Prediction.find()
        .skip(pagination.skip)
        .limit(
            pagination.limit,
        )
    )

    return PredictionOutWithTotal(
        data=await query.to_list(),
        page=pagination.offset or 0,
        total=await query.count(),
    )


@router.get("/status")
def get_dagster_status():
    url = "http://dagster-webserver:3000/graphql"
    assets = dagster_fetch_assets(url)

    return sorted(
        [
            {
                "asset": asset["assetKey"]["path"][-1],
                "latest_materialization": asset["assetMaterializations"][-1][
                    "timestamp"
                ],
                "status": (
                    asset["assetChecksOrError"]["checks"][-1][
                        "executionForLatestMaterialization"
                    ]
                    or {"status": None}
                )["status"],
                "partitions": (
                    {
                        k.lower().replace("num", ""): v
                        for k, v in asset["partitionStats"].items()
                    }
                    if asset["partitionStats"]
                    else None
                ),
            }
            for asset in dagster_status_graphql(url, assets)["data"][
                "assetNodes"
            ]
        ],
        key=lambda x: x["asset"],
    )
