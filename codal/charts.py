import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException

from codal.models import Report
from codal.utils import (
    mongo_total_revenue_by_year_pipeline,
    mongo_unique_reports_pipeline,
)

router = APIRouter()


@router.get("/report", response_model=list[Report])
async def get_reports(symbol: str, timeframe: int):
    return (
        await Report.find(Report.name == symbol, Report.timeframe == timeframe)
        .aggregate(mongo_unique_reports_pipeline())  # TODO: fix and remove
        .to_list()
    )


@router.get("/company_revenue", response_model=dict[str | int, float | None])
async def get_company_revenue_to_total(symbol: str):
    industry_group = (
        report.industry_group
        if (report := await Report.find_one(Report.name == symbol)) is not None
        else None
    )
    if industry_group is None:
        raise HTTPException(
            status_code=404,
            detail=f"Company with name {symbol} not found",
        )
    company_revenue = pd.DataFrame(
        await Report.find(Report.timeframe == 12, Report.name == symbol)
        .aggregate(
            mongo_unique_reports_pipeline()
            + mongo_total_revenue_by_year_pipeline()
        )
        .to_list()
    )
    industry_revenue = pd.DataFrame(
        await Report.find(
            Report.timeframe == 12, Report.industry_group == industry_group
        )
        .aggregate(
            mongo_unique_reports_pipeline()
            + mongo_total_revenue_by_year_pipeline()
        )
        .to_list()
    )
    df = pd.merge(
        company_revenue,
        industry_revenue,
        left_on="year",
        right_on="year",
        how="outer",
        suffixes=("_company", "_industry"),
    ).set_index("year")
    df = (
        df.loc[first_valid:]
        if (first_valid := df.dropna(how="any").first_valid_index())
        is not None
        else df
    )
    return (
        (df["revenue_company"] / df["revenue_industry"])
        .replace(np.nan, None)
        .to_dict()
    )


@router.get("/industry_revenue", response_model=dict[str | int, float | None])
async def get_industry_revenue_to_total(industry_group: int):
    industry_revenue = pd.DataFrame(
        await Report.find(
            Report.timeframe == 12, Report.industry_group == industry_group
        )
        .aggregate(
            mongo_unique_reports_pipeline()
            + mongo_total_revenue_by_year_pipeline()
        )
        .to_list()
    )
    total_revenue = pd.DataFrame(
        await Report.find(Report.timeframe == 12)
        .aggregate(
            mongo_unique_reports_pipeline()
            + mongo_total_revenue_by_year_pipeline()
        )
        .to_list()
    )
    df = pd.merge(
        industry_revenue,
        total_revenue,
        left_on="year",
        right_on="year",
        how="outer",
        suffixes=("_industry", "_total"),
    ).set_index("year")
    df = (
        df.loc[first_valid:]
        if (first_valid := df.dropna(how="any").first_valid_index())
        is not None
        else df
    )
    return (
        (df["revenue_industry"] / df["revenue_total"])
        .replace(np.nan, None)
        .to_dict()
    )
