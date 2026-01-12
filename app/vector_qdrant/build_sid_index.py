import json
import uuid
from pathlib import Path

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from openai import OpenAI

from app.doc_loader import extract_paragraphs

# ---------------- OpenAI Client ----------------

def get_openai_client() -> OpenAI:
    key_path = Path(__file__).resolve().parents[2] / "OPENAI_API_KEY.txt"
    api_key = key_path.read_text(encoding="utf-8").strip()
    return OpenAI(api_key=api_key)

llm = get_openai_client()

# ---------------- Config ----------------

DATA_DIR = "data"
SID_INDEX_PATH = f"{DATA_DIR}/sid_index.json"
COLLECTION = "sid_chunks"

qdrant = QdrantClient("localhost", port=6333)

# ---------------- Embedding ----------------

def embed(text: str):
    return llm.embeddings.create(
        model="text-embedding-3-small",
        input=text
    ).data[0].embedding

# ---------------- Main ----------------

def main():
    with open(SID_INDEX_PATH, "r", encoding="utf-8") as f:
        sid_index = json.load(f)

    batch = []
    total_chunks = 0
    total_files = 0

    print("\n📄 Starting SID indexing...\n")

    for scheme_code, pdf_path in sid_index.items():
        total_files += 1
        file_chunks = 0

        try:
            paragraphs = extract_paragraphs(pdf_path)
        except Exception as e:
            print(f"❌ Failed to read PDF: {pdf_path} | Error: {e}")
            continue

        for p in paragraphs:
            text = p["text"].strip()
            if len(text) < 40:
                continue

            batch.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=embed(text),
                    payload={
                        "scheme_code": str(scheme_code),
                        "page": p.get("page"),
                        "text": text,
                        "source_file": Path(pdf_path).name
                    }
                )
            )

            file_chunks += 1
            total_chunks += 1

            if len(batch) >= 200:
                qdrant.upsert(collection_name=COLLECTION, points=batch)
                batch.clear()

        print(
            f"✅ Indexed file: {Path(pdf_path).name} "
            f"| Scheme: {scheme_code} "
            f"| Chunks: {file_chunks}"
        )

    if batch:
        qdrant.upsert(collection_name=COLLECTION, points=batch)

    print("\n" + "-" * 50)
    print(f"📊 TOTAL FILES INDEXED : {total_files}")
    print(f"📊 TOTAL CHUNKS INDEXED: {total_chunks}")
    print("-" * 50)
    print("✅ SID indexing completed successfully\n")

if __name__ == "__main__":
    main()
