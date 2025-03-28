import asyncio
import json
import logging
import pickle
import re
from datetime import date, datetime
from enum import StrEnum, auto
from functools import reduce
from io import StringIO
from pathlib import Path
from typing import Literal
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp
import numpy as np
import pandas as pd
import requests
import tenacity
from bs4 import BeautifulSoup
from bson.datetime_ms import DatetimeMS
from dagster import (
    ConfigurableIOManager,
    ConfigurableResource,
    InputContext,
    OutputContext,
    get_dagster_logger,
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
from codal.fetcher.utils import APIError, eval_formula, is_float


class ResponseType(StrEnum):
    json = auto()
    text = auto()


class FileStoreCompanyReport(ConfigurableResource):
    BASE_DIR: str = "./data/companies"

    def path(self, symbol: str, timeframe: int, name: str, **kwargs) -> Path:
        return Path(self.BASE_DIR) / symbol / str(timeframe) / name

    def _check_path_exists(self, path: Path):
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

    def write(
        self,
        ddf: dict[str, pd.DataFrame],
        symbol: str,
        timeframe: int,
        name: str,
    ):
        path = self.path(symbol, timeframe, name)
        self._check_path_exists(path)
        with open(str(path), "wb") as f:
            pickle.dump(ddf, f)

    def read(self, symbol: str, timeframe: int, name: str) -> dict:
        path = self.path(symbol, timeframe, name)
        with open(str(path), "rb") as f:
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

    _logger: logging.Logger = PrivateAttr(default_factory=get_dagster_logger)

    async def fetch(
        self, session: aiohttp.ClientSession, url: str, table_id: str = ""
    ) -> BeautifulSoup:
        parsed_url = urlparse(url)
        query = parse_qs(parsed_url.query)
        query["sheetId"] = [table_id]
        parsed_url = parsed_url._replace(query=urlencode(query, doseq=True))

        async with session.get(
            urlunparse(parsed_url),
            timeout=240,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/133.0.0.0 Safari/537.36"
            },
        ) as response:
            if response.status != 200:
                raise APIError(
                    f"Failed to fetch data from {url} "
                    f"for {table_id}: {response.status}",
                    status_code=response.status,
                )
            text = await response.text()
            soup = BeautifulSoup(text, "html.parser")
            errors = soup.find(
                "span", {"id": re.compile(r"ctl00[_$]lblError")}
            )
            if (
                errors is not None
                and "عملیات با اشکال مواجه گردید" in errors.text
            ):
                raise APIError(
                    f"Failed to fetch data from {url} "
                    f"for {table_id}: {errors.text}",
                    status_code=response.status,
                )
            return soup

    @classmethod
    def parse_sheet(cls, soup: BeautifulSoup):
        pd.set_option("future.no_silent_downcasting", True)
        pattern = re.compile(
            r"var datasource = (\{.*\});", re.MULTILINE | re.DOTALL
        )
        script_tag = soup.find("script", string=pattern)
        if script_tag is None:
            raise APIError(
                "datasource",
                status_code=200,
            )
        script = pattern.search(script_tag.text)
        assert isinstance(script, re.Match)
        sheet = json.loads(script.group(1))["sheets"][0]
        title = sheet["title_Fa"]
        try:
            table_idx = max(
                [(i, len(j["cells"])) for i, j in enumerate(sheet["tables"])],
                key=lambda x: x[1],
            )[0]
        except ValueError as e:
            if "iterable argument is empty" in str(e):
                return {}
            raise
        df = pd.DataFrame(sheet["tables"][table_idx]["cells"])
        df.drop_duplicates(["columnSequence", "rowSequence"], inplace=True)
        df = cls.calculate_formulas(df)
        df = (
            df.pivot(
                index="rowSequence", columns="columnSequence", values="value"
            )
            .reset_index(drop=True)
            .T.reset_index(drop=True)
            .T
        )
        return {title: cls.clean_df(df)}

    @staticmethod
    def calculate_formulas(df: pd.DataFrame):
        values = {
            i["address"]: float(i["value"])
            for i in df.to_dict(orient="records")
            if is_float(i["value"])
        }
        df.loc[df["formula"] != "", "value"] = df.loc[
            df["formula"] != "", ["formula", "address"]
        ].apply(
            lambda x: eval_formula(x["formula"], x["address"], values), axis=1
        )
        return df

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

    @staticmethod
    def parse_table_ids(soup: BeautifulSoup) -> list[str]:
        selector = soup.find(
            "select", {"id": re.compile(r"^ctl00[_$]ddlTable$|^ddlTable$")}
        ) or soup.find(
            "select", {"name": re.compile(r"^ctl00[_$]ddlSheet$|^ddlSheet$")}
        )
        if selector is None:
            logger = get_dagster_logger()
            logger.error("Failed to find table selector")
            logger.error(str(soup.prettify()))
        return [
            option["value"]
            for option in selector.find_all("option")
            if option["value"] in ["0", "1", "9"]
        ]

    async def fetch_table(self, session, url: str, table_id: str) -> dict:
        @tenacity.retry(
            wait=tenacity.wait_exponential(
                multiplier=5,
                min=5,
                max=30,
            ),
            stop=tenacity.stop_after_attempt(5),
            reraise=True,
        )
        async def _inner():
            soup = await self.fetch(session, url, table_id)
            return self.parse_sheet(soup)

        try:
            return await _inner()
        except APIError as e:
            if str(e) == "datasource":
                self._logger.error(
                    f"Failed to find datasource from {url} for {table_id}"
                )
            else:
                self._logger.error(f"{type(e)}: {e}")
            return {"error": True}
        except TimeoutError as e:
            self._logger.error(
                f"Timeout while fetching {url} - table {table_id}: {e}"
            )
            raise

    @staticmethod
    def concat_dict(x: dict, y: dict) -> dict:
        return {**x, **y}

    async def fetch_tables(self, url: str):
        self._logger = get_dagster_logger()
        url = url.replace("Decision.aspx", "InterimStatement.aspx")
        async with aiohttp.ClientSession() as session:
            soup = await self.fetch(session, url)
            table_ids = self.parse_table_ids(soup)
            tasks = [
                asyncio.create_task(self.fetch_table(session, url, table_id))
                for table_id in table_ids
            ]
            return reduce(self.concat_dict, await asyncio.gather(*tasks), {})


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
        return (
            pd.DataFrame(
                [
                    GDPIn.model_validate(gdp).model_dump()
                    for gdp in self._get(self._gdp_api.format(country=country))
                ]
            )
            .set_index("date")
            .sort_index(ascending=True)
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
        df["value"] = df["value"].astype(float)
        df["date"] = pd.to_datetime(df["date"])
        return df.set_index("date").sort_index(ascending=True)


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
        df = pd.DataFrame(
            {
                "open": float(row[0].replace(",", "")),
                "low": float(row[1].replace(",", "")),
                "high": float(row[2].replace(",", "")),
                "close": float(row[3].replace(",", "")),
                "date": datetime.strptime(row[6], "%Y/%m/%d"),
                "jdate": jdatetime.strptime(row[7], "%Y/%m/%d"),
            }
            for row in self._get(self._url.format(currency=currency))
        )
        return df.set_index("date").sort_index(ascending=True)


class TSEMTMCAPIResource(ConfigurableResource):
    _search_symbol_url: str = PrivateAttr(
        default="https://cdn.tsetmc.com/api/Instrument"
        "/GetInstrumentSearch/{symbol}"
    )
    _ohlcv_url: str = PrivateAttr(
        default="https://cdn.tsetmc.com/api/ClosingPrice"
        "/GetClosingPriceDailyListCSV/{instrument_code}/{symbol}"
    )
    RETRY_LIMIT: int = 3
    INITIAL_RETRY_DELAY: int = 5  # Seconds

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
                async with session.get(url, timeout=120) as response:
                    response.raise_for_status()

                    return await getattr(response, response_type)()
            except aiohttp.ClientError as e:
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
            instrument
            for instrument in instruments
            if instrument.symbol == symbol and not instrument.deleted
        ]

        if len(matches) == 0:
            return {
                column: None
                for column, field_info in TSETMCSymbolIn.model_fields.items()
                if not field_info.exclude
            }
        return matches[0].model_dump()

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
        df = df.set_index("date").sort_index(ascending=True)
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
                .T.sort_index(ascending=True)
                .bfill()
            )
            return df


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
        self._filename = context.metadata["name"] + ".pkl"
        self.check_dir_exists()
        df.to_pickle(self.path)

    def load_input(self, context: InputContext):
        self._filename = (
            context.upstream_output.metadata.get("name")
            or context.upstream_output.definition_metadata["name"]
        ) + ".pkl"
        self.check_dir_exists()
        if not self.path.exists():
            df = pd.DataFrame(columns=list)
            self.write(df)
        return pd.read_pickle(self.path)


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
        if df.empty:
            return
        if not self._inited:
            self.init_db()
        for col in filter(lambda x: "date" == x, df.columns):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass
        df.replace(pd.NA, None, inplace=True)
        df.replace(np.nan, None, inplace=True)
        df.replace(np.inf, None, inplace=True)
        df.replace(-np.inf, None, inplace=True)
        collection = self._db[context.metadata["collection"]]
        collection.delete_many(
            (
                {
                    "partition_key": context.partition_key,
                }
                if context.has_partition_key
                else {}
            )
            | {"asset_key": context.asset_key.to_user_string()}
        )
        df["asset_key"] = context.asset_key.to_user_string()
        if context.has_partition_key:
            df["partition_key"] = context.partition_key
        collection.insert_many(list(df.T.to_dict().values()), ordered=False)

    def load_input(self, context: InputContext): ...
