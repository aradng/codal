import asyncio

from beanie import init_beanie
from fastapi import FastAPI, HTTPException, Request
from motor.core import AgnosticClient
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.middleware.cors import CORSMiddleware

from codal.charts import router as chart_router
from codal.endpoints import router as main_router
from codal.models import Company, Industry, Prediction, Profile, Report
from codal.schemas import FilterError
from codal.settings import settings

models = [Company, Industry, Profile, Report, Prediction]


async def lifespan(app: FastAPI):
    """Initialize Mongo connection and
    Beanie ODM models for the app lifecycle.
    """
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
    docs_url="/api/codal/docs",
    openapi_url="/api/codal/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router=main_router, prefix="/api/codal")
app.include_router(router=chart_router, prefix="/api/codal/charts")


@app.exception_handler(FilterError)
async def exception_handler(
    request: Request, exc: FilterError
) -> HTTPException:
    """Transform custom FilterError into an HTTPException for FastAPI."""
    raise HTTPException(status_code=exc.status_code, detail=exc.message)
