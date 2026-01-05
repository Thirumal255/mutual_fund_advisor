from app.agent.llm.profile_schema import UserProfile
from app.agent.llm.openai_client import OpenAIClient
from app.agent.llm.prompts import PROFILE_EXTRACTION_PROMPT
import json


class ProfileUpdateResult:
    def __init__(self, profile, profile_changed, missing_fields):
        self.profile = profile
        self.profile_changed = profile_changed
        self.missing_fields = missing_fields
        self.is_complete = len(missing_fields) == 0


_client = OpenAIClient()


def extract_profile_update(user_message: str, current_profile: UserProfile):
    """
    Uses LLM to extract / update user investment profile from conversation.
    Returns:
      - updated UserProfile
      - whether profile changed
      - missing required fields
    """

    prompt = PROFILE_EXTRACTION_PROMPT.format(
        user_message=user_message,
        current_profile=current_profile.__dict__
    )

    # IMPORTANT: complete() returns raw text
    response_text = _client.complete(prompt)

    try:
        parsed = json.loads(response_text)
    except json.JSONDecodeError:
        # Fail safe: do NOT break chat
        return ProfileUpdateResult(
            profile=current_profile,
            profile_changed=False,
            missing_fields=current_profile.missing_fields()
        )

    updated_profile = UserProfile(**parsed.get("profile", current_profile.__dict__))

    return ProfileUpdateResult(
        profile=updated_profile,
        profile_changed=parsed.get("profile_changed", False),
        missing_fields=parsed.get("missing_fields", [])
    )
