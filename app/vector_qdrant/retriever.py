from pathlib import Path
from openai import OpenAI

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchAny

# ---------------- OpenAI Client ----------------

def get_openai_client() -> OpenAI:
    key_path = Path(__file__).resolve().parents[2] / "OPENAI_API_KEY.txt"
    api_key = key_path.read_text(encoding="utf-8").strip()
    return OpenAI(api_key=api_key)

llm = get_openai_client()

# ---------------- Qdrant ----------------

COLLECTION = "sid_chunks"
qdrant = QdrantClient(url="http://localhost:6333")

# ---------------- Retrieval ----------------

def retrieve_sid_chunks(
    query: str,
    allowed_scheme_codes: set[str],
    top_k: int = 8
) -> list[dict]:
    """
    Qdrant v1.16.x compliant retrieval.
    Hard-scoped to allowed_scheme_codes.
    """

    # 🔒 HARD SAFETY GUARD
    if not allowed_scheme_codes:
        return []

    # Create embedding
    query_vector = llm.embeddings.create(
        model="text-embedding-3-small",
        input=query
    ).data[0].embedding

    # Execute vector search
    response = qdrant.query_points(
        collection_name=COLLECTION,
        query=query_vector,
        query_filter=Filter(
            must=[
                FieldCondition(
                    key="scheme_code",
                    match=MatchAny(any=list(allowed_scheme_codes))
                )
            ]
        ),
        limit=top_k,
    )

    # 🔑 IMPORTANT: access response.points
    points = response.points or []

    return [
        {
            "scheme_code": p.payload.get("scheme_code"),
            "page": p.payload.get("page"),
            "text": p.payload.get("text")
        }
        for p in points
        if p.payload is not None
    ]
