import asyncio
from datetime import datetime, timedelta
from enum import StrEnum, auto
from io import StringIO
from pathlib import Path
from typing import Literal

import aiohttp
import pandas as pd
import requests
from dagster import ConfigurableResource
from jdatetime import date as jdate
from pydantic import BaseModel, PrivateAttr

from codal.fetcher.schemas import (
    CompanyIn,
    CompanyReportsIn,
    GDPIn,
    IndustryGroupIn,
    TSETMCSearchIn,
    TSETMCSymbolIn,
)
from codal.fetcher.utils import sanitize_persian


class ResponseType(StrEnum):
    json = auto()
    text = auto()


class FileStoreCompanyListing(ConfigurableResource):
    _companies_filename: str = PrivateAttr(default="codal_companies.csv")
    _base_dir: Path = PrivateAttr(default=Path("./data"))

    @property
    def path(self):
        return self._base_dir / self._companies_filename

    def check_dir_exists(self):
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, df: pd.DataFrame):
        self.check_dir_exists()
        df.to_csv(str(self.path), mode="w")
        return df

    def read(self):
        self.check_dir_exists()
        if not self.path.exists():
            df = pd.DataFrame(columns=list(CompanyIn.model_fields.keys()))
            df.set_index("symbol")
            self.write(df)
        return pd.read_csv(self.path, index_col="symbol")


class FileStoreIndustryListing(ConfigurableResource):
    _companies_filename: str = PrivateAttr(default="codal_industries.csv")
    _base_dir: Path = PrivateAttr(default=Path("./data"))

    @property
    def path(self):
        return self._base_dir / self._companies_filename

    def check_dir_exists(self):
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, df: pd.DataFrame):
        self.check_dir_exists()
        df.to_csv(str(self.path), mode="w")
        return df

    def read(self):
        self.check_dir_exists()
        if not self.path.exists():
            df = pd.DataFrame(columns=list(CompanyIn.model_fields.keys()))
            df.set_index("Id", inplace=True)
            self.write(df)
        return pd.read_csv(self.path, index_col="Id")


class FileStoreCompanyReport(ConfigurableResource):
    _base_dir: Path = PrivateAttr(default=Path("./data/companies"))
    _symbol: str = PrivateAttr()
    _year: int = PrivateAttr()
    _time_frame: int = PrivateAttr()
    _filename: str = PrivateAttr()

    @property
    def update_filename(self) -> str:
        return f"updated_at_{self._time_frame}.txt"

    @property
    def path(self) -> Path:
        return self._base_dir / self._symbol

    @property
    def data_path(self) -> Path:
        return (
            self.path
            / str(self._year)
            / str(self._time_frame)
            / self._filename
        )

    @property
    def update_path(self):
        return self._base_dir / self.update_filename

    def check_path_exists(self, path: Path):
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

    def update_checkpoint(self, start: datetime, end: datetime):
        self.check_path_exists(self.update_path)
        with open(self.update_path, "a") as f:
            f.write(f"\n{start.date()}->{end.date()}")

    def write(self, file_content: bytes):
        self.check_path_exists(self.data_path)
        with open(str(self.data_path), "wb") as f:
            f.write(file_content)


class CodalAPIResource(ConfigurableResource):
    _base_url: str = PrivateAttr(default="https://search.codal.ir/api/search")
    _report_path: str = PrivateAttr(default="/v2/q?")
    _company_path: str = PrivateAttr(default="/v1/companies")
    _industry_path: str = PrivateAttr(default="/v1/IndustryGroup")
    _params: dict = PrivateAttr(default_factory=dict)

    @property
    def params(self):
        return self._params

    @params.setter
    def params(self, params: BaseModel | dict | None = None):
        if isinstance(params, BaseModel):
            self._params = params.model_dump(exclude_none=True)
            return
        if isinstance(params, dict):
            self._params = params
            return
        if params is None:
            self._params = dict()
            return
        raise Exception(f"Incompatible params type {type(params)}")

    def _get(self, url):
        response = requests.get(
            url,
            params=self._params,
            headers={"User-Agent": "codal"},
        )
        response.raise_for_status()
        return response

    @property
    def get_reports(self):
        ret = CompanyReportsIn.model_validate(
            self._get(f"{self._base_url}{self._report_path}").json()
        )
        if ret.IsAttacker:
            raise Exception("Codal Rate Limit Reached")
        return ret

    @property
    def companies(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                CompanyIn.model_validate(r).model_dump()
                for r in self._get(
                    f"{self._base_url}{self._company_path}"
                ).json()
            ]
        ).set_index("symbol")

    @property
    def industries(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                IndustryGroupIn.model_validate(industry).model_dump()
                for industry in self._get(
                    f"{self._base_url}{self._industry_path}"
                ).json()
            ]
        ).set_index("Id")


class APINinjaResource(ConfigurableResource):
    API_KEY: str
    _gdp_api: str = PrivateAttr(
        default="https://api.api-ninjas.com/v1/gdp?country={country}"
    )

    def _get(self, url):
        response = requests.get(url, headers={"X-Api-Key": self.API_KEY})
        response.raise_for_status()
        return response.json()

    def fetch_gdp(self, country: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                GDPIn.model_validate(gdp).model_dump()
                for gdp in self._get(self._gdp_api.format(country=country))
            ]
        ).set_index("year")


class AlphaVantaAPIResource(ConfigurableResource):
    API_KEY: str
    _url: str = PrivateAttr(
        default="https://www.alphavantage.co/query?function={symbol}"
        "&interval={interval}&apikey={apikey}"
    )

    def _get(self, url: str, params: dict):
        response = requests.get(url.format(**params, apikey=self.API_KEY))
        response.raise_for_status()
        return response.json()["data"]

    def fetch_history(
        self,
        symbol: str,
        interval: Literal["monthly", "weekly", "daily"] = "monthly",
    ) -> pd.DataFrame:
        return pd.DataFrame(
            self._get(self._url, params=dict(symbol=symbol, interval=interval))
        ).set_index("date")


class TgjuAPIResource(ConfigurableResource):
    _url: str = PrivateAttr(
        default="https://api.tgju.org/v1/market/indicator"
        "/summary-table-data/{currency}"
    )

    def _get(self, url: str):
        response = requests.get(url)
        response.raise_for_status()
        return response.json()["data"]

    def fetch_history(self, currency: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "open": row[0],
                "low": row[1],
                "high": row[2],
                "close": row[3],
                "date": row[6],
                "jdate": row[7],
            }
            for row in self._get(self._url.format(currency=currency))
        ).set_index("date")


class TSEMTMCAPIResource(ConfigurableResource):
    _source_name: str = PrivateAttr(default="source_symbol")
    _search_symbol_url: str = PrivateAttr(
        default="https://cdn.tsetmc.com/api/Instrument"
        "/GetInstrumentSearch/{symbol}"
    )
    _ohlcv_url: str = PrivateAttr(
        default="https://cdn.tsetmc.com/api/ClosingPrice"
        "/GetClosingPriceDailyListCSV/{instrument_code}/{symbol}"
    )
    RETRY_LIMIT: int = 3
    INITIAL_RETRY_DELAY: int = 1  # Seconds

    async def _fetch_with_retries(
        self,
        url: str,
        session: aiohttp.ClientSession,
        response_type: ResponseType = ResponseType.json,
    ) -> dict | str:
        """Fetch data with retries on failure."""
        delay = self.INITIAL_RETRY_DELAY
        err = ""
        for attempt in range(self.RETRY_LIMIT + 1):
            try:
                async with session.get(url) as response:
                    response.raise_for_status()

                    return await getattr(response, response_type)()
            except (
                aiohttp.ClientError,
                aiohttp.ServerTimeoutError,
                aiohttp.ClientResponseError,
            ) as e:
                err = str(e)
                await asyncio.sleep(delay)
                delay *= 2  # Exponential backoff
        raise Exception(
            "retries limit reached adjust RETRY_LIMIT/INITIAL_RETRY_DELAY"
            f" or check network settings : {err}"
        )  # Exhausted retries, re-raise the exception

    async def match_symbol(self, symbol: str, session: aiohttp.ClientSession):
        """Fetches instrument data for a given symbol name"""
        response_data = await self._fetch_with_retries(
            self._search_symbol_url.format(symbol=symbol), session
        )
        assert isinstance(response_data, dict)
        # last date in tsmec means symbol has been deleted
        instruments = TSETMCSearchIn.model_validate(
            response_data
        ).instrumentSearch
        matches = [
            i
            for i in instruments
            if sanitize_persian(i.symbol.strip())
            == sanitize_persian(symbol.strip())
            and not i.deleted
        ]
        if len(matches) == 0:
            return {self._source_name: symbol} | {
                column: None
                for column in list(TSETMCSymbolIn.model_fields.keys())
            }
        return {
            self._source_name: symbol,
        } | matches[0].model_dump()

    async def fetch_symbols(self, symbols: list[str]) -> pd.DataFrame:
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.match_symbol(sanitize_persian(symbol.strip()), session)
                for symbol in symbols
            ]
            return (
                pd.DataFrame(await asyncio.gather(*tasks))
                .dropna()
                .set_index(self._source_name)
            )

    async def fetch_ohlcv(
        self, instrument_code: str, symbol: str, session: aiohttp.ClientSession
    ):
        response_data = await self._fetch_with_retries(
            self._ohlcv_url.format(
                symbol=symbol, instrument_code=instrument_code
            ),
            response_type=ResponseType.text,
            session=session,
        )
        assert isinstance(response_data, str)
        df = pd.read_csv(StringIO(response_data))
        df.rename(
            columns={
                "<TICKER>": "symbol",
                "<DTYYYYMMDD>": "date",
                "<OPEN>": "open",
                "<HIGH>": "high",
                "<LOW>": "low",
                "<CLOSE>": "close",
                "<VOL>": "volume",
            },
            inplace=True,
        )
        df = df.loc[
            :, ["symbol", "open", "close", "high", "low", "volume", "date"]
        ]
        df.date = df.date.apply(lambda x: datetime.strptime(str(x), "%Y%m%d"))
        df.set_index("date", inplace=True)
        return df

    async def fetch_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch_ohlcv(
                    instrument_code=row.instrument_code,
                    symbol=row.symbol,
                    session=session,
                )
                for _, row in df.iterrows()
            ]
            df = (
                pd.DataFrame(
                    [
                        pd.Series(
                            sf["close"],
                            index=sf.index,
                            name=sf["symbol"].iloc[0],
                        )
                        for sf in await asyncio.gather(*tasks)
                    ]
                )
                .T.sort_index(ascending=False)
                .bfill()
            )
            df["jdate"] = (df.index.astype("int64") // 10**9).map(
                lambda x: jdate.fromtimestamp(x)
            )
            df["jdate_next"] = df["jdate"].shift().bfill()
            # just hold end of jmonth data
            df["eom"] = df.apply(
                lambda x: (
                    (x["jdate"] + timedelta(days=1)).month != x["jdate"].month
                )
                or x["jdate"].month != x["jdate_next"].month,
                axis=1,
            )
            return df[df["eom"]].drop(columns=["eom", "jdate_next"])


class FileStoreTSETMCListing(ConfigurableResource):
    _source_name: str = PrivateAttr(default="source_symbol")
    _companies_filename: str = PrivateAttr(default="tsetmc_companies.csv")
    _base_dir: Path = PrivateAttr(default=Path("./data"))

    @property
    def path(self):
        return self._base_dir / self._companies_filename

    def check_dir_exists(self):
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, df: pd.DataFrame) -> pd.DataFrame:
        self.check_dir_exists()
        df.to_csv(str(self.path), mode="w")
        return df

    def read(self):
        self.check_dir_exists()
        if not self.path.exists():
            df = pd.DataFrame(
                columns=list(CompanyIn.model_fields.keys())
                + [self._source_name]
            )
            df.set_index(self._source_name)
            self.write(df)
        return pd.read_csv(self.path, index_col=self._source_name)
