from app.agent.data.loader import load_metrics_ui
from app.agent.data.scorer import score_schemes


def test_step1():
    schemes = load_metrics_ui("data/metrics_ui.json")
    scored = score_schemes(schemes)

    assert len(scored) == len(schemes)
    assert "score" in scored[0]

    print("\nTOP 5 SCHEMES:\n")
    for s in scored[:5]:
        print(s["scheme_name"], "→", s["score"])

    print("\nSTEP 1 VERIFIED — scoring successful")

    print(f"First and last Schemes with Score \n\n: {scored[0]} \n\n {scored[-1]}")


if __name__ == "__main__":
    test_step1()
