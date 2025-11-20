# app/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from .recommender import init_mftool, recommend_funds_for_profile, list_sample_funds

app = FastAPI(title="Mutual Fund Advisor (India) - Phase 1")

# initialize mftool client once
init_mftool()

class UserProfile(BaseModel):
    monthly_sip: int = Field(..., gt=0)
    horizon_years: int = Field(..., gt=0)
    risk_profile: str = Field(..., pattern="^(low|moderate|high)$")
    preferences: Optional[List[str]] = None

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/sample_funds")
def sample_funds():
    try:
        return list_sample_funds()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/recommend")
def recommend(profile: UserProfile, top_k: int = 5):
    try:
        recs, projection = recommend_funds_for_profile(profile.dict(), top_k=top_k)
        return {"recommended": recs, "sip_projection_inr": projection}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))