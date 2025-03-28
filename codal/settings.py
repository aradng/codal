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


settings = Settings()
