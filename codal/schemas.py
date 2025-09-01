from datetime import datetime
from typing import Literal, Self

from pydantic import (
    BaseModel,
    Field,
    computed_field,
    field_validator,
    model_validator,
)

from codal.models import Prediction


# API-level validation error with HTTP status code
class FilterError(Exception):
    status_code: int

    def __init__(self, message: str, status_code: int):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


# Common pagination fields and helpers (offset/limit -> skip)
class PaginatedMixin(BaseModel):
    offset: int | None = Field(None, ge=0, examples=[0])
    limit: int | None = Field(None, ge=0, examples=[10])

    @field_validator("limit", mode="after")
    def convert_zero_limit(cls, v: int | None):
        return v or None

    @computed_field  # type: ignore[misc]
    @property
    def skip(self) -> int | None:
        if self.offset is None:
            return None
        if self.limit is None:
            return self.offset
        return self.limit * self.offset


# Weights for scoring/ranking; non-null entries are included in score
class Weights(BaseModel):
    current_ratio: float | None = None
    quick_ratio: float | None = None
    debt_ratio: float | None = None
    current_assets_ratio: float | None = None
    cash_ratio: float | None = None
    net_profit_margin: float | None = None
    operating_profit_margin: float | None = None
    gross_profit_margin: float | None = None
    return_on_equity: float | None = None
    return_on_assets: float | None = None
    total_debt_to_equity_ratio: float | None = None
    current_debt_to_equity_ratio: float | None = None
    long_term_debt_to_equity_ratio: float | None = None
    debt_to_equity_ratio: float | None = None
    equity_ratio: float | None = None
    total_asset_turnover: float | None = None
    pe_ratio: float | None = None
    price_sales_ratio: float | None = None
    cash_return_on_assets: float | None = None
    cash_return_on_equity: float | None = None
    earnings_quality: float | None = None
    cash_debt_coverage: float | None = None
    current_cash_coverage: float | None = None
    revenue_to_GDP: float | None = None
    price_to_gold: float | None = None
    price_to_oil: float | None = None
    price_to_usd: float | None = None
    delta_price_to_delta_gold: float | None = None
    delta_price_to_delta_oil: float | None = None
    delta_price_to_delta_usd: float | None = None


# Request model for filtering/sorting profiles and computing weighted scores
class ProfileIn(PaginatedMixin):
    industry_group: int | None = None
    industry_only: bool | None = None
    descending: bool = True
    timeframe: Literal[3, 6, 12]

    from_date: datetime | None = None
    to_date: datetime | None = None

    weights: Weights

    @model_validator(mode="after")
    def validate_date_range(self) -> Self:
        if (self.from_date is None) ^ (self.to_date is None):
            raise FilterError(
                "Both start date and end date must be present", status_code=400
            )
        if (
            self.from_date is not None
            and self.to_date is not None
            and self.from_date >= self.to_date
        ):
            raise FilterError(
                "Start date must be earlier than end date", status_code=400
            )
        return self

    @model_validator(mode="after")
    def validate_industry_grouping(self) -> Self:
        if self.industry_group is not None and self.industry_only is True:
            raise FilterError(
                "Industry cannot be specified in 'Industry/Company' mode",
                status_code=400,
            )
        return self


# Response model for a profile row, extends Weights for convenience
class ProfileOut(Weights):
    name: str
    is_industry: bool
    industry_group: int

    timeframe: Literal[3, 6, 12]
    jdate: str
    date: datetime


# ProfileOut with computed score field
class RankOut(ProfileOut):
    score: float


# Paged response for rankings
class RankOutWithTotal(BaseModel):
    page: int
    total: int
    data: list[RankOut]


# Paged response for predictions collection
class PredictionOutWithTotal(BaseModel):
    page: int
    total: int
    data: list[Prediction]


# Request model to fetch a specific report by name/timeframe
class ReportIn(BaseModel):
    name: str
    timeframe: Literal[1, 2, 3]
