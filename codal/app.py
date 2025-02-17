import asyncio

from beanie import init_beanie
from fastapi import FastAPI, HTTPException, Request
from motor.core import AgnosticClient
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.middleware.cors import CORSMiddleware

from codal.endpoints import router
from codal.models import Company, Industry, Profile
from codal.schemas import FilterError
from codal.settings import settings

models = [Company, Industry, Profile]


async def lifespan(app: FastAPI):
    client: AgnosticClient = AsyncIOMotorClient(
        settings.MONGO_URI, tz_aware=True
    )
    client.get_io_loop = asyncio.get_event_loop  # type: ignore[method-assign]
    db = client[settings.MONGO_DB]
    await init_beanie(db, document_models=models)  # type: ignore[arg-type]
    yield


app = FastAPI(
    lifespan=lifespan,
    debug=True,
    title="codal",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router=router, prefix="/api")


@app.exception_handler(FilterError)
async def exception_handler(
    request: Request, exc: FilterError
) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.message)
