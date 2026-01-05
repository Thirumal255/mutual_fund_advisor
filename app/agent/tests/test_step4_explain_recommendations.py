from app.agent.orchestrator import recommend_schemes


if __name__ == "__main__":
    result = recommend_schemes(
        user_question="Suggest low volatility debt funds for 3 years",
        llm_client=None,
        top_n=3,
        explain=False  # test deterministic path first
        )

    print("Top Recommendations:")
    for r in result["recommendations"]:
        print(
            r["scheme_name"],
            "| Score:",
            round(r["score"], 2)
        )

    print("\nMetadata:", result["metadata"])
