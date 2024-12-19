import re
import time
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    computed_field,
    field_validator,
)
from datetime import date, datetime
from jdatetime import date as jdate
from jdatetime import datetime as jdatetime


def persian_to_english(input: str) -> str:
    trans = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
    return input.translate(trans)


class CompanyReportOut(BaseModel):
    Symbol: str | None = None
    Audited: bool = True
    NotAudited: bool = True
    AuditorRef: int = -1
    Category: int = 1
    Childs: bool = False
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
            persian_to_english(v).replace("/", "-")
        ).togregorian()

    # @field_validator("ExcelUrl", mode="before")
    # @classmethod
    # def add_domain_excel(cls, v: str) -> str:
    #     return str(f"https://excel.codal.ir{v}")

    @field_validator("Url", mode="before")
    @classmethod
    def add_domain_report(cls, v: str) -> str:
        return str(f"https://search.codal.ir{v}")

    @computed_field
    @property  # type: ignore[misc]
    def jdate(self) -> jdate | None:
        if match := re.search(r"\d{4}/\d{2}/\d{2}", self.Title):
            return jdate.fromisoformat(match.group().replace("/", "-"))
        return None


class CompanyReportsIn(BaseModel):
    Total: int
    Page: int
    Letters: list[CompanyReportLetter]
    IsAttacker: bool


class IndustryGroupIn(BaseModel):
    Id: int
    Name: str


class CompanyIn(BaseModel):
    symbol: str = Field(validation_alias="sy")
    name: str = Field(validation_alias="n")
    index: str = Field(validation_alias="i")
    company_type: int = Field(validation_alias="t")
    company_state: int = Field(validation_alias="st")
    industry_group: int = Field(validation_alias="IG")
    report_type: int = Field(validation_alias="RT")
