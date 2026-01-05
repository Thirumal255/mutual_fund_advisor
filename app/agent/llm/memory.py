from dataclasses import dataclass, field
from typing import List, Dict, Optional
from app.agent.llm.profile_schema import UserProfile

@dataclass
class ConversationState:
    profile: UserProfile = field(default_factory=UserProfile)
    recommended_schemes: List[Dict] = field(default_factory=list)
    conversation: List[Dict[str, str]] = field(default_factory=list)

_SESSION_STORE: Dict[str, ConversationState] = {}

def get_or_create_state(session_id: str) -> ConversationState:
    if session_id not in _SESSION_STORE:
        _SESSION_STORE[session_id] = ConversationState()
    return _SESSION_STORE[session_id]
