# app/agent/orchestrator.py

from app.agent.llm.profile_extraction import extract_profile_update
from app.agent.llm.explainer import explain_chat_with_llm
from app.agent.llm.memory import get_or_create_state
from app.agent.data.loader import load_metrics_ui
from app.agent.data.scorer import score_schemes
from app.agent.data.builder import build_recommendation_payload
from app.vector_qdrant.retriever import retrieve_sid_chunks
import logging
import json
import os

ENABLE_CHAT_DEBUG_LOGS = os.getenv(
    "ENABLE_CHAT_DEBUG_LOGS", "true"
).lower() == "false"


logger = logging.getLogger("mf_advisor.chat")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)



def _merge_profile(existing, incoming):
    """
    Safely merge UserProfile dataclasses
    """
    for field, value in incoming.__dict__.items():
        if value is not None:
            setattr(existing, field, value)

def _log_chat_response(
    *,
    response_type: str,
    intent: str,
    user_question: str,
    message: str,
    profile,
    conversation,
    profile_update,
    recommended_schemes,
    sid_evidence,
    scheme_codes,
    profile_changed

):
    
    if not ENABLE_CHAT_DEBUG_LOGS:
        return  # 🔕 Logging disabled
    
    log_payload = {
        "type": response_type,
        "intent": intent,
        "user_question": user_question,
        "message": message,
        "profile": profile,
        "profile_update": (
            profile_update.profile.__dict__
            if profile_update else None
        ),
        "state.recommended_schemes": recommended_schemes,
        "sid_evidence": sid_evidence,
        "conversation": conversation,
        "scheme_codes": scheme_codes,
        "profile_changed": profile_changed
    }

    logger.info(
        "CHAT_RESPONSE\n%s",
        json.dumps(log_payload, indent=2, default=str)
    )



def recommend_schemes(
    user_question: str,
    session_id: str
):
    state = get_or_create_state(session_id)

    # 🟢 0️⃣ First interaction → Greeting
    if not state.conversation:
        greeting = explain_chat_with_llm(
            user_question="",
            profile=state.profile.__dict__,
            schemes=None,
            conversation=[],
            intent="greeting",
            sid_evidence=None
        )

        state.conversation.append({
            "role": "assistant",
            "content": greeting
        })
        _log_chat_response(
            response_type="greeting",
            intent="greeting",
            user_question=user_question,
            message=greeting,            
            profile=state.profile.__dict__,
            conversation=state.conversation,
            profile_update=None,
            recommended_schemes=state.recommended_schemes,
            sid_evidence=None,
            scheme_codes=None,
            profile_changed=None
                        )
        return {
            "type": "greeting",
            "message": greeting
        }

    # 1️⃣ Save user message
    state.conversation.append({
        "role": "user",
        "content": user_question
    })

    # 2️⃣ Extract profile update
    profile_update = extract_profile_update(
        user_message=user_question,
        current_profile=state.profile
    )

    # 3️⃣ Merge profile safely
    _merge_profile(state.profile, profile_update.profile)

    # 4️⃣ Profile incomplete → clarification
    if not profile_update.is_complete:
        response = explain_chat_with_llm(
            user_question=user_question,
            profile=state.profile.__dict__,
            schemes=None,
            conversation=state.conversation,
            intent="clarification",
            sid_evidence=None
        )

        state.conversation.append({
            "role": "assistant",
            "content": response
        })
        _log_chat_response(
            response_type="clarification",
            intent="clarification",
            user_question=user_question,
            message=response,            
            profile=state.profile.__dict__,
            conversation=state.conversation,
            profile_update=profile_update,
            recommended_schemes=state.recommended_schemes,
            sid_evidence=None,
            scheme_codes=None,
            profile_changed=profile_update.profile_changed
                            )
        return {
            "type": "clarification",
            "message": response
        }
    
    is_followup = bool(state.recommended_schemes)
    sid_evidence = None

    # 5️⃣ Build recommendations (only once or on profile change)
    if not state.recommended_schemes or profile_update.profile_changed:
        schemes = load_metrics_ui("data/metrics_ui.json")
        scored = score_schemes(schemes, state.profile)
        payload = build_recommendation_payload(scored)
        state.recommended_schemes = payload["schemes"]
        intent="recommendation"
        scheme_codes=None
    # 🔵 6️⃣ Detect FOLLOW-UP (after recommendations already exist)
  
    else:
        intent="followup"
        scheme_codes = {
            str(s.get("scheme_code") or s.get("code"))
            for s in state.recommended_schemes
            if s.get("scheme_code") or s.get("code")
        }

        sid_evidence = retrieve_sid_chunks(
            query=user_question,
            allowed_scheme_codes=scheme_codes
        )


    # 6️⃣ Recommendation / follow-up
    response = explain_chat_with_llm(
        user_question=user_question,
        profile=state.profile.__dict__,
        schemes=state.recommended_schemes,
        conversation=state.conversation,
        intent=intent,
        sid_evidence=sid_evidence
    )

    state.conversation.append({
        "role": "assistant",
        "content": response
    })
    _log_chat_response(
        response_type="answer",
        intent=intent,
        user_question=user_question,
        message=response,        
        profile={
            k: v for k, v in state.profile.__dict__.items()
            if v is not None
        },
        conversation=state.conversation,
        profile_update=profile_update,
        recommended_schemes=state.recommended_schemes,
        sid_evidence=sid_evidence,
        scheme_codes=scheme_codes,
        profile_changed=profile_update.profile_changed
                    )
    return {
        "type": "answer",
        "message": response,
        "schemes": state.recommended_schemes,
        "profile": {
        k: v for k, v in state.profile.__dict__.items()
        if v is not None
    }
    }
