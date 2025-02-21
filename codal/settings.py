from pydantic import computed_field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    MONGO_USERNAME: str
    MONGO_PASSWORD: str
    MONGO_HOSTNAME: str
    MONGO_PORT: int
    MONGO_DB: str

    @computed_field  # type: ignore[misc]
    @property
    def MONGO_URI(self) -> str:
        return (
            f"mongodb://{self.MONGO_USERNAME}:{self.MONGO_PASSWORD}"
            f"@{self.MONGO_HOSTNAME}:{self.MONGO_PORT}"
        )

    GDP_CSV_PATH: str = "data/gdp.csv"
    GOLD_PRICE_CSV_PATH: str = "data/gold.csv"
    OIL_PRICE_CSV_PATH: str = "data/oil.csv"
    USD_PRICE_CSV_PATH: str = "data/usd.csv"
    TSETMC_STOCKS_CSV_PATH: str = "data/tsetmc_stocks.csv"
    BASE_YEAR: int = 1390


settings = Settings()
settings.MONGO_HOSTNAME = "localhost"
