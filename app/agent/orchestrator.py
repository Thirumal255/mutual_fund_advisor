# app/agent/orchestrator.py

from app.agent.llm.profile_extraction import extract_profile_update
from app.agent.llm.explainer import explain_chat_with_llm
from app.agent.llm.memory import get_or_create_state
from app.agent.data.loader import load_metrics_ui
from app.agent.data.scorer import score_schemes
from app.agent.data.builder import build_recommendation_payload


def _merge_profile(existing, incoming):
    """
    Safely merge UserProfile dataclasses
    """
    for field, value in incoming.__dict__.items():
        if value is not None:
            setattr(existing, field, value)


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
            intent="greeting"
        )

        state.conversation.append({
            "role": "assistant",
            "content": greeting
        })

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
            intent="clarification"
        )

        state.conversation.append({
            "role": "assistant",
            "content": response
        })

        return {
            "type": "clarification",
            "message": response
        }

    # 5️⃣ Build recommendations (only once or on profile change)
    if not state.recommended_schemes or profile_update.profile_changed:
        schemes = load_metrics_ui("data/metrics_ui.json")
        scored = score_schemes(schemes, state.profile)
        payload = build_recommendation_payload(scored)
        state.recommended_schemes = payload["schemes"]

    # 6️⃣ Recommendation / follow-up
    response = explain_chat_with_llm(
        user_question=user_question,
        profile=state.profile.__dict__,
        schemes=state.recommended_schemes,
        conversation=state.conversation,
        intent="recommendation"
    )

    state.conversation.append({
        "role": "assistant",
        "content": response
    })

    return {
        "type": "answer",
        "message": response,
        "schemes": state.recommended_schemes
    }
