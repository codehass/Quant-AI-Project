from fastapi import APIRouter


router = APIRouter(prefix="/api/v1/auth", tags=["Authentication routes"])


@router.get("/")
def test_auth_routers():

    return {"message": "hello from auth routers"}
