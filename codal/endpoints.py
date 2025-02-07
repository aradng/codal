from fastapi import APIRouter

router = APIRouter(prefix="/api")


@router.get("/companies")
def get_companies(): ...


@router.get("/inustries")
def get_industries(): ...


@router.get("/profile/{id}")
def get_profile(): ...


@router.get("/score")
def get_rankings(): ...
