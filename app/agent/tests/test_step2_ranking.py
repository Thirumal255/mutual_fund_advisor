from app.agent.data.loader import load_metrics_ui
from app.agent.data.scorer import score_schemes
from app.agent.data.builder import build_recommendation_payload


def test_step2():
    schemes = load_metrics_ui("data/metrics_ui.json")
    scored = score_schemes(schemes)
    payload = build_recommendation_payload(scored, top_n=5)

    assert payload["top_n"] == 5
    assert len(payload["schemes"]) == 5
    assert payload["schemes"][0]["rank"] == 1

    print("\nSTEP 2 VERIFIED — Recommendation payload\n")
    for s in payload["schemes"]:
        print(f"{s['rank']}. {s['scheme_name']} ({s['score']})")

    print(f"First and last Schemes with Rank \n\n: {payload['schemes'][0]} \n\n {payload['schemes'][-1]}")


if __name__ == "__main__":
    test_step2()
