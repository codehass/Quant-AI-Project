from fastapi import APIRouter, Depends

from app.authentication.auth import get_current_user
from ...schemas.user_schema import BTCFeaturesRequest, UserSchema
from ml.scripts.predictor import BTCPredictor
from ...db.database import get_db
from sqlalchemy.orm import Session
from sqlalchemy import text

from ...models.user_model import BTCPrediction

router = APIRouter(prefix="/api/v1/predict", tags=["BTC Prediction routes"])


@router.post("/")
async def predict_btc_price(
    features: BTCFeaturesRequest, db: Session = Depends(get_db)
):
    predictor = BTCPredictor()
    result = predictor.predict(features)
    new_prediction = BTCPrediction(predicted_close_t_plus_10=result)
    db.add(new_prediction)
    db.commit()
    db.refresh(new_prediction)
    return {"predicted_close_t_plus_10": result}
