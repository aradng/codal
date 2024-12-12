import httpx
from asyncio import run

from codal.fetcher.schemas import CompanyReportsIn

search_api = "https://search.codal.ir/api/search/v2/"


async def get_company_limits(symbol: str):
    async with httpx.AsyncClient() as client:
        response: httpx.Response = await client.get(
            "https://search.codal.ir/api/search/v1/financialYears",
            params={"symbol": symbol},
            headers={
                "User-Agent": "",
            },
        )

        return response.json()


async def get_company_reports(symbol: str):
    params = {
        "Audited": "true",
        "AuditorRef": "-1",
        "Category": "-1",
        "Childs": "true",
        "CompanyState": "-1",
        "CompanyType": "-1",
        "Consolidatable": "true",
        "IsNotAudited": "false",
        "Length": "-1",
        "LetterType": "-1",
        "Mains": "true",
        "NotAudited": "true",
        "NotConsolidatable": "false",
        "PageNumber": "1",
        "Publisher": "false",
        "ReportingType": "-1",
        "Symbol": symbol,
        "TracingNo": "-1",
        "search": "true",
    }
    async with httpx.AsyncClient() as client:

        response: httpx.Response = await client.get(
            "https://search.codal.ir/api/search/v2/q?",
            params=params,
            headers={
                "User-Agent": "",
            },
        )
    return response.json()
    return CompanyReportsIn.model_validate(response.json())


print(run(get_company_reports("پردیس")))
