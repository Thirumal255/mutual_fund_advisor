from app.agent.data.loader import load_metrics_ui


def test_step0():
    schemes = load_metrics_ui("data/metrics_ui.json")

    assert len(schemes) > 0

    for s in schemes:
        assert s["scheme_code"]
        assert s["scheme_name"]
        assert isinstance(s["metrics"], dict)

    print(f"STEP 0 VERIFIED — Loaded {len(schemes)} schemes")
    print(f"First and last Schemes \n\n: {schemes[0]} \n\n {schemes[-1]}")


if __name__ == "__main__":
    test_step0()
