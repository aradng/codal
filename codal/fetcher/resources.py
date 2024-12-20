from pathlib import Path
from typing import Literal
from dagster import ConfigurableResource
import pandas as pd
import requests
from codal.fetcher.schemas import CompanyIn, CompanyReportsIn, GDPIn, IndustryGroupIn
from pydantic import BaseModel, PrivateAttr
from datetime import datetime


class FileStoreCompanyListing(ConfigurableResource):
    _companies_filename: str = PrivateAttr(default="companies.csv")
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
    _companies_filename: str = PrivateAttr(default="industries.csv")
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
        return self.path / str(self._year) / str(self._time_frame) / self._filename

    @property
    def update_path(self):
        return self._base_dir / self.update_filename

    def check_path_exists(self, path: Path):
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

    def update_checkpoint(self, start: datetime, end: datetime):
        self.check_path_exists(self.update_path)
        with open(self.update_path, "a") as f:
            f.write(f"{start}->{end}")

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
            c = params.model_dump(exclude_none=True)
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
                for r in self._get(f"{self._base_url}{self._company_path}").json()
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
        default="https://www.alphavantage.co/query?function={symbol}&interval={interval}&apikey={apikey}"
    )

    def _get(self, url: str, params: dict):
        response = requests.get(url.format(**params, apikey=self.API_KEY))
        response.raise_for_status()
        return response.json()["data"]

    def fetch_history(
        self, symbol: str, interval: Literal["monthly", "weekly", "daily"] = "monthly"
    ) -> pd.DataFrame:
        return pd.DataFrame(
            self._get(self._url, params=dict(symbol=symbol, interval=interval))
        ).set_index("date")


class TgjuAPIResource(ConfigurableResource):
    _url: str = PrivateAttr(
        default="https://api.tgju.org/v1/market/indicator/summary-table-data/{currency}"
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
