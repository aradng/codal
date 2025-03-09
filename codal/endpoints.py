from typing import Annotated

from beanie.operators import Eq, RegEx
from fastapi import APIRouter
from pydantic import Field

from codal.models import Company, Industry, Profile
from codal.schemas import ProfileIn, ProfileOut, RankOutWithTotal
from codal.utils import normalize_field

router = APIRouter()


@router.get("/companies")
async def get_companies(
    q: Annotated[str, Field(min_length=3)],
) -> list[Company]:
    return await Company.find(
        RegEx(Company.symbol, pattern=f"{q}*", options="i")
    ).to_list()


@router.get("/industries")
async def get_industries() -> list[Industry]:
    return await Industry.find().to_list()


@router.get("/profile/{name}")
async def get_profile(name: str) -> list[ProfileOut]:
    return await Profile.find(Profile.name == name).to_list()


@router.get("/score")
async def get_rankings(profile_in: ProfileIn):
    query = Profile.find()
    if profile_in.industry_only:
        query = query.find(Eq(Profile.is_industry, True))
    if profile_in.industry_group:
        query = query.find(Profile.industry_group == profile_in.industry_group)
    data = await query.aggregate(
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
    return RankOutWithTotal(
        data=data,
        page=profile_in.offset or 0,
        total=await query.count(),
    )
