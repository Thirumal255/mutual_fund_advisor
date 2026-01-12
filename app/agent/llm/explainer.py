# app/agent/llm/explainer.py

from typing import List, Optional
from app.agent.llm.openai_client import OpenAIClient
from app.agent.llm.prompts import (
    CHAT_AGENT_SYSTEM_PROMPT,
    PROFILE_CLARIFICATION_PROMPT,
    RECOMMENDATION_EXPLANATION_PROMPT,
    FOLLOWUP_QA_PROMPT,
    SID_FOLLOWUP_QA_PROMPT,
    RESPONSE_STYLE_GUIDELINES,
    GREETING_PROMPT,   # 🆕 import
)

_llm_client: Optional[OpenAIClient] = None


def _get_llm():
    global _llm_client
    if _llm_client is None:
        _llm_client = OpenAIClient()
    return _llm_client


def explain_chat_with_llm(
    user_question: str,
    profile: dict,
    schemes: Optional[List[dict]],
    conversation: List[dict],
    intent: str,
    sid_evidence: Optional[list] = None
) -> str:
    llm = _get_llm()

    # -------------------------------
    # Prompt selection
    # -------------------------------
    if intent == "greeting":
        prompt = GREETING_PROMPT

    elif intent == "clarification":
        prompt = PROFILE_CLARIFICATION_PROMPT.format(
            missing_fields=", ".join(
                k for k, v in profile.items() if v is None
            )
        )

    elif intent == "recommendation":
        prompt = RECOMMENDATION_EXPLANATION_PROMPT.format(
            conversation=conversation,
            user_question=user_question,
            schemes=schemes
        )

    else:  # FOLLOW-UP
        if sid_evidence:
            sid_text = "\n\n".join(
                f"[Scheme {c['scheme_code']} | Page {c.get('page')}]\n{c['text']}"
                for c in sid_evidence
            )

            prompt = SID_FOLLOWUP_QA_PROMPT.format(
                conversation=conversation,
                user_question=user_question,
                schemes=schemes,
                sid_evidence=sid_text
            )
        else:
            prompt = FOLLOWUP_QA_PROMPT.format(
                conversation=conversation,
                user_question=user_question
            )
    # -------------------------------
    # Final LLM call
    # -------------------------------
    messages = [
        {"role": "system", "content": CHAT_AGENT_SYSTEM_PROMPT},
        {"role": "system", "content": RESPONSE_STYLE_GUIDELINES},
        {"role": "user", "content": prompt}
    ]

    response = llm.complete(messages)
    return response

