from fastapi import FastAPI
from .db.database import engine, Base
from fastapi.middleware.cors import CORSMiddleware
from .api.routers import auth, predict
from .config import settings

app = FastAPI(
    title="Quant-AI API",
    description=(
        "This API provides access to real-time high-frequency price predictions for the BTC/USDT pair. The underlying system utilizes a distributed infrastructure to process raw Binance market data and transform it into actionable insights using Machine Learning."
    ),
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
app.include_router(predict.router)


@app.get("/", tags=["Home route"])
def get_home():
    return {"message": "Hello to Quant AI Project API"}
