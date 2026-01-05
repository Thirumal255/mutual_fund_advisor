from app.agent.orchestrator import recommend_schemes

def main():
    print("\n🚀 Interactive Orchestrator Memory Test")
    print("Type 'exit' anytime to stop\n")

    session_id = "manual-test-session"

    # -------------------------------
    # Initial turn
    # -------------------------------
    while True:
        user_input = input("🧑 You: ").strip()

        if user_input.lower() == "exit":
            print("\n👋 Exiting test")
            break

        result = recommend_schemes(
            user_question=user_input,
            session_id=session_id,
            top_n=5
        )

        if result["type"] == "clarification":
            print(f"\n🤖 Clarification needed: {result['message']}")
            continue

        print("\n🤖 Assistant:")
        print(result["response"])

        print("\n📌 Recommended Schemes:")
        for idx, scheme in enumerate(result["recommended_schemes"], start=1):
            print(
                f"{idx}. {scheme['scheme_name']} "
                f"(Score: {scheme.get('score', 'N/A')})"
            )

        print("\n--- Ask a follow-up or type 'exit' ---\n")


if __name__ == "__main__":
    main()
