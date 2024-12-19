from pathlib import Path
from dagster import Config, ConfigurableResource
import pandas as pd
from codal.fetcher.schemas import CompanyIn
from pydantic import PrivateAttr
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

    def read(self):
        self.check_dir_exists()
        if not self.path.exists():
            df = pd.DataFrame(columns=list(CompanyIn.model_fields.keys()))
            df.set_index("Id")
            self.write(df)
        return pd.read_csv(self.path, index_col="Id")


class FileStoreCompanyReport(ConfigurableResource):
    start_date: str
    _base_dir: Path = PrivateAttr(default=Path("./data/companies"))
    _symbol: str = PrivateAttr()
    _year: int = PrivateAttr()
    _time_frame: int = PrivateAttr()
    _filename: str = PrivateAttr()

    @property
    def update_filename(self) -> str:
        return f"updated_at_{self._time_frame}.txt"

    @property
    def _start_date(self) -> datetime:
        return datetime.fromisoformat(self.start_date)

    @property
    def path(self) -> Path:
        return self._base_dir / self._symbol

    @property
    def data_path(self) -> Path:
        return self.path / str(self._year) / str(self._time_frame) / self._filename

    @property
    def update_path(self):
        return self.path / self.update_filename

    def check_path_exists(self, path: Path):
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def update_checkpoint(self):
        self.check_path_exists(self.update_path)
        with open(self.update_path, "a") as f:
            f.write(f"\n{datetime.now()}")

    @property
    def last_checkpoint(self) -> datetime:
        self.check_path_exists(self.update_path)
        if not self.update_path.exists():
            with open(self.update_path, "w") as f:
                f.write(f"{self._start_date}")
            return self._start_date

        with open(self.update_path, "r") as f:
            return datetime.fromisoformat(f.read().splitlines()[-1])

    def write(self, file_content: bytes):
        self.check_path_exists(self.data_path)
        with open(str(self.data_path), "wb") as f:
            f.write(file_content)
