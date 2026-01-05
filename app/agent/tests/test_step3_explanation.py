from app.agent.data.loader import load_metrics_ui
from app.agent.data.scorer import score_schemes
from app.agent.data.builder import build_recommendation_payload
from app.agent.llm.explainer import explain_scheme_with_llm


class DummyLLM:
    def generate(self, prompt: str) -> str:
        print("\n================ PROMPT SENT TO LLM ================\n")
        print(prompt)
        print("\n====================================================\n")
        return "[LLM RESPONSE PLACEHOLDER]"


def test_step3():
    schemes = load_metrics_ui("data/metrics_ui.json")
    scored = score_schemes(schemes)
    payload = build_recommendation_payload(scored, top_n=1)

    top_scheme = payload["schemes"][0]

    response = explain_scheme_with_llm(
        ranked_scheme=top_scheme,
        user_question="What is the exit load and is this suitable for long term?",
        llm_client=DummyLLM()
    )

    print("\n================ LLM RESPONSE ======================\n")
    print(response)
    print("\n====================================================\n")


if __name__ == "__main__":
    test_step3()
