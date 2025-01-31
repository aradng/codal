from pydantic import BaseModel


class PriceCollection(BaseModel):
    GDP: float
    price_per_share: float
    gold_price: float
    oil_price: float
    usd_price: float
    delta_stock_price: float
    delta_gold_price: float
    delta_oil_price: float
    delta_usd_price: float
