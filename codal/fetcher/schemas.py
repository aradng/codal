from pydantic import BaseModel, HttpUrl, field_validator
from persiantools.jdatetime import JalaliDateTime


class SuperVision(BaseModel):
    undersupervision: int
    additionalinfo: str
    reasons: list


class CompanyReportLetter(BaseModel):
    supervision: SuperVision
    tracingno: int
    symbol: str
    companyname: str
    undersupervision: int
    title: str
    lettercode: str
    # sentdatetime: JalaliDateTime
    # publishdatetime: JalaliDateTime
    hashtml: bool
    isestimate: bool
    url: HttpUrl
    hasexcel: bool
    hasattachment: bool
    attachmenturl: str
    excelurl: HttpUrl

    # @field_validator("sentdatetime", "publishdatetime", mode="before")
    # @classmethod
    # def date_to_jalali(cls, v: str) -> JalaliDateTime:
    #     return JalaliDateTime(v.replace("/", "-"))

    @field_validator("url", mode="before")
    @classmethod
    def add_domain(cls, v: str) -> str:
        return f"codal.ir{v}"


class CompanyReportOut(BaseModel):
    Symbol: str
    Audited: bool = True
    AuditorRef: int = -1
    Category: int = -1
    Childs: bool = True
    CompanyState: int = -1
    CompanyType: int = -1
    Consolidatable: bool = True
    IsNotAudited: bool = False
    Length: int = -1
    LetterType: int = -1
    Mains: bool = False
    NotAudited: bool = True
    NotConsolidatable: bool = False
    PageNumber: int = 1
    Publisher: bool = True
    ReportingType: int = -1
    TracingNo: int = -1
    search: bool = True


class CompanyReportsIn(BaseModel):
    total: int
    page: int
    Letters: list
