import re
from datetime import date, datetime, timedelta

from jdatetime import date as jdate
from jdatetime import datetime as jdatetime
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    computed_field,
    field_validator,
)

from codal.fetcher.utils import sanitize_persian


class CompanyReportOut(BaseModel):
    Symbol: str | None = None
    Audited: bool = True
    NotAudited: bool = True
    AuditorRef: int = -1
    Category: int = 1
    Childs: bool = False  # no
    CompanyState: int = -1
    CompanyType: int = -1
    Consolidatable: bool = True
    Length: int = -1
    LetterType: int = 6
    Mains: bool = True
    NotConsolidatable: bool = True
    PageNumber: int = 1
    Publisher: bool = False
    ReportingType: int = -1
    TracingNo: int = -1
    search: bool = True
    FromDate: str | None = None
    ToDate: str | None = None

    @field_validator("FromDate", "ToDate", mode="before")
    @classmethod
    def date_to_jdate(cls, value: datetime | date | None) -> None | str:
        if value is None:
            return None
        return jdate.fromgregorian(date=value).isoformat().replace("-", "/")


class SuperVision(BaseModel):
    UnderSupervision: int
    AdditionalInfo: str
    Reasons: list


class CompanyReportLetter(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    SuperVision: SuperVision
    TracingNo: int
    Symbol: str
    CompanyName: str
    UnderSupervision: int
    Title: str
    LetterCode: str
    SentDateTime: datetime
    PublishDateTime: datetime
    HasHtml: bool
    IsEstimate: bool
    Url: HttpUrl
    HasExcel: bool
    HasAttachment: bool
    AttachmentUrl: str
    ExcelUrl: HttpUrl

    @field_validator("SentDateTime", "PublishDateTime", mode="before")
    @classmethod
    def jalali_to_date(cls, v: str) -> datetime:
        return jdatetime.fromisoformat(
            sanitize_persian(v).replace("/", "-")
        ).togregorian()

    @field_validator("Url", mode="before")
    @classmethod
    def add_domain_report(cls, v: str) -> str:
        return str(f"https://codal.ir{v}")

    @computed_field  # type: ignore[misc]
    @property
    def jdate(self) -> jdate | None:
        if match := re.search(r"\d{4}/\d{2}/\d{2}", self.Title):
            res = match.group().replace("/", "-")
            try:
                return jdate.fromisoformat(res)
            except ValueError as e:
                if str(e) == "day is out of range for month":
                    year, month, day = map(int, res.split("-"))
                    return jdate(year=year, month=month, day=day - 1)
                raise ValueError(f"could not correct jdate range {res}")
        return None

    @computed_field  # type: ignore[misc]
    @property
    def timeframe(self) -> int:
        if (match := re.search(r"دوره (\d) ماهه", self.Title)) is not None:
            return int(match.group(1))
        if (match := re.search(r"سال مالی", self.Title)) is not None:
            return 12
        raise ValueError(f"could not find timeframe in {self.Title}")

    @field_validator("Symbol", "CompanyName", mode="after")
    @classmethod
    def sanitize_persian(cls, v: str) -> str:
        return sanitize_persian(v)


class CompanyReportsIn(BaseModel):
    Total: int
    Page: int
    Letters: list[CompanyReportLetter]
    IsAttacker: bool


class IndustryGroupIn(BaseModel):
    Id: int
    Name: str

    @field_validator("Name", mode="after")
    @classmethod
    def sanitize_persian(cls, v: str) -> str:
        return sanitize_persian(v)


class CompanyIn(BaseModel):
    symbol: str = Field(validation_alias="sy")
    name: str = Field(validation_alias="n")
    index: str = Field(validation_alias="i")
    company_type: int = Field(validation_alias="t")
    company_state: int = Field(validation_alias="st")
    industry_group: int = Field(validation_alias="IG")
    report_type: int = Field(validation_alias="RT")

    @field_validator("symbol", "name", mode="after")
    @classmethod
    def sanitize_persian(cls, v: str) -> str:
        return sanitize_persian(v)


class GDPIn(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    country: str
    year: int
    gdp_growth: float
    gdp_nominal: float
    gdp_per_capita_nominal: float
    gdp_ppp: float
    gdp_per_capita_ppp: float
    gdp_ppp_share: float

    @computed_field  # type: ignore[misc]
    @property
    def jdate(self) -> jdate:
        return jdate.fromgregorian(
            date=date(year=self.year + 1, month=1, day=1) - timedelta(days=1)
        )


class TSETMCSymbolIn(BaseModel):
    symbol: str = Field(validation_alias="lVal18AFC")
    name: str = Field(validation_alias="lVal30")
    instrument_code: str = Field(validation_alias="insCode")
    market_title: str = Field(validation_alias="flowTitle")
    market_type: int = Field(validation_alias="flow")
    lastDate: int = Field(exclude=True)

    @computed_field  # type: ignore[misc]
    @property
    def deleted(self) -> bool:
        # has last date means not deleted
        return self.lastDate == 0

    @field_validator("symbol", "name", "market_title", mode="after")
    @classmethod
    def sanitize_persian(cls, v: str) -> str:
        return sanitize_persian(v)


class TSETMCSearchIn(BaseModel):
    instrumentSearch: list[TSETMCSymbolIn]
