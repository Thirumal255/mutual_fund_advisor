from fastapi import APIRouter
from pydantic import BaseModel
from app.agent.orchestrator import recommend_schemes

router = APIRouter(prefix="/api/chat", tags=["Chat"])


class ChatPayload(BaseModel):
    message: str
    session_id: str


@router.post("")
def chat(payload: ChatPayload):
    return recommend_schemes(
        user_question=payload.message,
        session_id=payload.session_id
    )
