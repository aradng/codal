from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

app = FastAPI(
    #    lifespan=lifespan,
    debug=True,
    title="codal",
    docs_url="/api/docs",
    openapi_url="api/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
