from fastapi import FastAPI
from .db.database import engine, Base
from fastapi.middleware.cors import CORSMiddleware
from .api.routers import auth
from .config import settings

app = FastAPI(
    title="RetentionAI API",
    description=("DEscription Project"),
)


origins = [settings.FRONTEND_URL]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)

app.include_router(auth.router)


@app.get("/", tags=["Home route"])
def get_home():
    return {"message": "Hello to Quant AI Project API"}
