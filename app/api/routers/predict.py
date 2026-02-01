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
    features: BTCFeaturesRequest,
    db: Session = Depends(get_db),
    current_user: UserSchema = Depends(get_current_user),
):
    predictor = BTCPredictor()
    result = predictor.predict(features)
    new_prediction = BTCPrediction(predicted_close_t_plus_10=result)
    db.add(new_prediction)
    db.commit()
    db.refresh(new_prediction)
    return {"predicted_close_t_plus_10": result}


@router.get("/analytics/btc-predictions-summary")
async def get_btc_prediction_analytics(
    db: Session = Depends(get_db), current_user: UserSchema = Depends(get_current_user)
):
    query = text(
        """
        SELECT 
            AVG(predicted_close_t_plus_10) AS average_predicted_close,
            MIN(predicted_close_t_plus_10) AS min_predicted_close,
            MAX(predicted_close_t_plus_10) AS max_predicted_close,
            COUNT(*) AS total_predictions
        FROM btc_predictions;
        """
    )
    result = db.execute(query).fetchone()
    return {
        "average_predicted_close": result.average_predicted_close,
        "min_predicted_close": result.min_predicted_close,
        "max_predicted_close": result.max_predicted_close,
        "total_predictions": result.total_predictions,
    }


@router.get("/analytics/summary")
def get_market_summary(
    db: Session = Depends(get_db), current_user: UserSchema = Depends(get_current_user)
):
    query = text(
        """
        SELECT 
            COUNT(*) as total_records,
            AVG(close) as avg_price,
            MAX(high) as session_high,
            MIN(low) as session_low,
            SUM(volume) as total_volume
        FROM silver_data
    """
    )
    result = db.execute(query).mappings().first()
    return result
