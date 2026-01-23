from sqlalchemy import Column, Integer, String, Boolean, Float
from sqlalchemy.sql import func
from sqlalchemy.types import DateTime
from ..db.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)


class BTCPrediction(Base):
    __tablename__ = "btc_predictions"

    id = Column(Integer, primary_key=True, index=True)
    predicted_close_t_plus_10 = Column(Float, nullable=False)
    model_version = Column(Float, nullable=False, server_default="1.1")
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
