from datetime import datetime
from typing import Literal, Self

from pydantic import (
    BaseModel,
    Field,
    computed_field,
    field_validator,
    model_validator,
)

from codal.models import Company, Industry


class FilterError(Exception):
    status_code: int

    def __init__(self, message: str, status_code: int):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


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


class Weights(BaseModel):
    current_ratio: float | None
    quick_ratio: float | None
    debt_ratio: float | None
    current_assets_ratio: float | None
    cash_ratio: float | None
    net_profit_margin: float | None
    operating_profit_margin: float | None
    gross_profit_margin: float | None
    return_on_equity: float | None
    return_on_assets: float | None
    total_debt_to_equity_ratio: float | None
    current_debt_to_equity_ratio: float | None
    long_term_debt_to_equity_ratio: float | None
    debt_to_equity_ratio: float | None
    equity_ratio: float | None
    total_asset_turnover: float | None
    pe_ratio: float | None
    price_sales_ratio: float | None
    cash_return_on_assets: float | None
    cash_return_on_equity: float | None
    earnings_quality: float | None
    cash_debt_coverage: float | None
    current_cash_coverage: float | None
    revenue_to_GDP: float | None
    price_to_gold: float | None
    price_to_oil: float | None
    price_to_usd: float | None
    delta_price_to_delta_gold: float | None
    delta_price_to_delta_oil: float | None
    delta_price_to_delta_usd: float | None


class ProfileIn(PaginatedMixin):
    industry_group: int | None
    industry_only: bool
    descending: bool = True
    timeframe: Literal[3, 6, 12]

    from_date: datetime | None = None
    to_date: datetime | None = None

    weights: Weights

    @model_validator(mode="after")
    def validate_date_range(self) -> Self:
        if self.from_date is None or self.to_date is None:
            self.from_date = None
            self.to_date = None
            return self
        if self.from_date >= self.to_date:
            FilterError(
                "Start date must be earlier than end date", status_code=400
            )
        return self

    @model_validator(mode="after")
    def validate_industry_grouping(self) -> Self:
        if self.industry_group is not None and self.industry_only:
            raise FilterError(
                "Industry cannot be specified in 'Industry Only' mode",
                status_code=400,
            )
        return self


class ProfileOut(BaseModel):
    asset: Company | Industry | None = None
    is_industry: bool

    score: float


class ProfileOutWithTotal(BaseModel):
    page: int
    total: int
    data: list[ProfileOut]
