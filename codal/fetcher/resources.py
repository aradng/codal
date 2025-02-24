import asyncio
import json
import pickle
import re
from datetime import date, datetime, timedelta
from enum import StrEnum, auto
from io import StringIO
from pathlib import Path
from typing import Literal

import aiohttp
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from bson.datetime_ms import DatetimeMS
from dagster import (
    ConfigurableIOManager,
    ConfigurableResource,
    InputContext,
    OutputContext,
)
from jdatetime import date as jdate
from jdatetime import datetime as jdatetime
from pydantic import BaseModel, PrivateAttr
from pymongo import AsyncMongoClient, MongoClient
from pymongo.asynchronous.database import AsyncDatabase

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


class FileStoreCompanyReport(ConfigurableResource):
    _base_dir: Path = PrivateAttr(default=Path("./data/companies"))
    _symbol: str = PrivateAttr()
    _time_frame: int = PrivateAttr()
    _filename: str = PrivateAttr()

    @property
    def path(self) -> Path:
        return (
            self._base_dir
            / self._symbol
            / str(self._time_frame)
            / self._filename
        )

    def check_path_exists(self, path: Path):
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, ddf: dict[str, pd.DataFrame]):
        self.check_path_exists(self.path)
        with open(str(self.path), "wb") as f:
            pickle.dump(ddf, f)

    def read(self):
        with open(str(self.path), "rb") as f:
            return pickle.load(f)


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
        )

    @property
    def industries(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                IndustryGroupIn.model_validate(industry).model_dump()
                for industry in self._get(
                    f"{self._base_url}{self._industry_path}"
                ).json()
            ]
        )


class CodalReportResource(ConfigurableResource):
    _viewstates = PrivateAttr(
        default={
            "__VIEWSTATE": None,
            "__VIEWSTATEGENERATOR": None,
            "__VIEWSTATEENCRYPTED": None,
            "__EVENTVALIDATION": None,
        }
    )
    _sheets: dict[str, dict] = PrivateAttr(default_factory=dict)
    _tables: dict[str, pd.DataFrame] = PrivateAttr(default_factory=dict)
    _url: str = PrivateAttr()
    table_names: list[str] = [
        "ترازنامه",
        "جریان وجوه نقد",
        "صورت جریان های نقدی",
        "صورت سود و زیان",
        "صورت وضعیت مالی",
    ]

    @property
    def url(self) -> str:
        return self._url

    @url.setter
    def url(self, url: str):
        self._url = url

    @property
    def sheets(self):
        return self._sheets

    @property
    def tables(self):
        return self._tables

    def fetch(self, table_id: str | None = None):
        data = self._viewstates | {"ctl00$ddlTable": table_id}
        if table_id:
            self.response = requests.post(
                self.url, data=data if table_id else None, timeout=10
            )
        else:
            self.response = requests.get(self.url, timeout=10)
        self.soup = BeautifulSoup(self.response.text, "html.parser")

    def parse_sheet(self):
        pattern = re.compile(
            r"var datasource = (\{.*\});", re.MULTILINE | re.DOTALL
        )
        script = self.soup.find("script", string=pattern)
        text = pattern.search(script.text).group(1)
        data_in = json.loads(text)
        sheet = data_in["sheets"][0]
        title = sheet["title_Fa"]
        # self.sheets[title] = sheet
        table_idx = max(
            [(i, len(j["cells"])) for i, j in enumerate(sheet["tables"])],
            key=lambda x: x[1],
        )[0]
        df = pd.DataFrame(sheet["tables"][table_idx]["cells"])
        df = (
            df.pivot(index="rowCode", columns="columnCode", values="value")
            .reset_index(drop=True)
            .T.reset_index(drop=True)
            .T
        )
        self._tables[title] = self.clean_df(df)

    @staticmethod
    def clean_df(df: pd.DataFrame):
        all_columns = df.iloc[0]
        sf = []
        idx = all_columns[all_columns == "شرح"].index.tolist() + [
            len(all_columns)
        ]
        columns = [
            i.replace("\n", " ")
            for i in df.iloc[0, range(idx[1] - idx[0])].tolist()
        ]
        for i, j in zip(idx[:-1], idx[1:]):
            tmp = df[range(i, j)].reset_index(drop=True).iloc[1:]
            tmp.columns = range(j - i)
            sf.append(tmp)
        kf: pd.DataFrame = pd.concat(sf, axis=0, ignore_index=True)
        kf.columns = columns
        kf.replace("", np.nan, inplace=True)
        kf.dropna(how="all", inplace=True)
        return kf

    def set_viewstates(self):
        for k in self._viewstates.keys():
            self._viewstates[k] = self.soup.find("input", {"id": k})["value"]

    def parse_table_ids(self):
        selector = self.soup.find("select", {"id": "ctl00_ddlTable"})
        return [
            option["value"]
            for option in selector.find_all("option")
            if option.text in self.table_names
        ]

    def fetch_tables(self):
        self.fetch()
        self.set_viewstates()
        for table_id in reversed(self.parse_table_ids()):
            self.fetch(table_id)
            self.set_viewstates()


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
        )


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
        df = pd.DataFrame(
            self._get(self._url, params=dict(symbol=symbol, interval=interval))
        )
        df["jdate"] = df["date"].apply(
            lambda x: jdate.fromgregorian(date=date.fromisoformat(x))
        )
        return df


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
                "date": row[6].replace("/", "-"),
                "jdate": row[7].replace("/", "-"),
            }
            for row in self._get(self._url.format(currency=currency))
        )


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
        src_symbol = symbol
        symbol = sanitize_persian(symbol.strip())
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
            if sanitize_persian(i.symbol.strip()) == symbol and not i.deleted
        ]
        if len(matches) == 0:
            return {self._source_name: src_symbol} | {
                column: None
                for column in list(TSETMCSymbolIn.model_fields.keys())
            }
        return {
            self._source_name: src_symbol,
        } | matches[0].model_dump()

    async def fetch_symbols(self, symbols: list[str]) -> pd.DataFrame:
        async with aiohttp.ClientSession() as session:
            tasks = [self.match_symbol(symbol, session) for symbol in symbols]
            return pd.DataFrame(await asyncio.gather(*tasks)).dropna()

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
        df = df.loc[:, ["open", "close", "high", "low", "volume", "date"]]
        df["date"] = df["date"].apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d")
        )
        df.set_index("date", inplace=True)
        return symbol, df

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
                            name=symbol,
                        )
                        for symbol, sf in await asyncio.gather(*tasks)
                    ]
                )
                .T.sort_index(ascending=False)
                .bfill()
            )
            df["jdate"] = (
                df.index.to_series().astype("int64") // 10**9
            ).map(lambda x: jdate.fromtimestamp(x))
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


class DataFrameIOManager(ConfigurableIOManager):
    _filename: str = PrivateAttr()
    _base_dir: Path = PrivateAttr(default=Path("./data"))

    @property
    def path(self):
        return self._base_dir / self._filename

    def check_dir_exists(self):
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def handle_output(self, context: OutputContext, df: pd.DataFrame):
        self._filename = context.metadata["name"] + ".csv"
        self.check_dir_exists()
        df.to_csv(str(self.path), mode="w", index=False)

    def load_input(self, context: InputContext):
        self._filename = (
            context.upstream_output.metadata.get("name")
            or context.upstream_output.definition_metadata["name"]
        ) + ".csv"
        self.check_dir_exists()
        if not self.path.exists():
            df = pd.DataFrame(columns=list)
            self.write(df)
        return pd.read_csv(self.path)


class MongoIOManager(ConfigurableIOManager):
    MONGO_USERNAME: str = "root"
    MONGO_PASSWORD: str = "root"
    MONGO_HOSTNAME: str = "localhost"
    MONGO_PORT: str = "27017"
    DB_NAME: str
    _client: AsyncMongoClient = PrivateAttr()
    _db: AsyncDatabase = PrivateAttr()
    _inited: bool = PrivateAttr(False)

    def init_db(self):
        self._client = MongoClient(
            f"mongodb://{self.MONGO_USERNAME}:{self.MONGO_PASSWORD}"
            f"@{self.MONGO_HOSTNAME}:{self.MONGO_PORT}",
            tz_aware=True,
        )

        self._db = self._client[self.DB_NAME]
        self._inited = True

    def jdate_to_date(self, x):
        return DatetimeMS(
            int((jdatetime.fromisoformat(x).timestamp() - 19603900800) * 1000)
        )

    def handle_output(self, context: OutputContext, df: pd.DataFrame):
        if not self._inited:
            self.init_db()
        for col in filter(lambda x: "date" == x, df.columns):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass
        collection = self._db[context.metadata["collection"]]
        collection.delete_many(
            {"partition_key": context.partition_key}
            if context.has_partition_key
            else {}
        )
        if context.has_partition_key:
            df["partition_key"] = context.partition_key
        collection.insert_many(list(df.T.to_dict().values()), ordered=False)

    def load_input(self, context: InputContext): ...
