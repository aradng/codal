from typing import Annotated

from beanie.operators import Eq, RegEx
from fastapi import APIRouter
from pydantic import Field

from codal.models import Company, Industry, Profile
from codal.schemas import ProfileIn, ProfileOutWithTotal
from codal.utils import normalize_field

router = APIRouter()


@router.get("/companies")
async def get_companies(
    q: Annotated[str, Field(min_length=3)],
) -> list[Company]:
    return await Company.find(
        RegEx(Company.symbol, pattern=q, options="i")
    ).to_list()


@router.get("/inustries")
async def get_industries() -> list[Industry]:
    return await Industry.find().to_list()


@router.get("/profile/{asset_id}")
async def get_profile(asset_id: int) -> ProfileOutWithTotal:
    return await Profile.find(Profile.asset_id == asset_id)


@router.get("/score")
async def get_rankings(profile_in: ProfileIn):
    query = Profile.find(
        fetch_links=True
    )  # test this for aggregate and chaining
    if profile_in.industry_only:
        query = query.find(Eq(Profile.is_industry, True))
    if profile_in.industry_group:
        query = query.find(Profile.industry_group == profile_in.industry_group)
    data_query = query.aggregate(
        [
            {
                "$addFields": {
                    "score": {
                        "$sum": [
                            {"$multiply": [weight, normalize_field(field)]}
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
    return ProfileOutWithTotal(
        data=await data_query,
        page=profile_in.offset or 0,
        total=await query.count(),
    )
