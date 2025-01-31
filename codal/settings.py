from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    GDP_CSV_PATH: str = "data/gdp.csv"
    GOLD_PRICE_CSV_PATH: str = "data/gold.csv"
    OIL_PRICE_CSV_PATH: str = "data/oil.csv"
    USD_PRICE_CSV_PATH: str = "data/usd.csv"
    TSETMC_STOCKS_CSV_PATH: str = "data/tsetmc_stocks.csv"
    BASE_YEAR: int = 1390


settings = Settings()
