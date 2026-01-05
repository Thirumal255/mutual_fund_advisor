from app.agent.orchestrator import handle_chat_turn

SESSION = "test-session-1"


def run():
    print("\n--- TURN 1 ---")
    r1 = handle_chat_turn(
        "Suggest low risk mutual funds for 5 years SIP",
        session_id=SESSION
    )
    print(r1)

    print("\n--- TURN 2 (FOLLOW-UP) ---")
    r2 = handle_chat_turn(
        "Why did you choose these funds?",
        session_id=SESSION
    )
    print(r2)

    print("\n--- TURN 3 (PROFILE CHANGE) ---")
    r3 = handle_chat_turn(
        "Make it high risk instead",
        session_id=SESSION
    )
    print(r3)


if __name__ == "__main__":
    run()
