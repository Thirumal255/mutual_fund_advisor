from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PayloadSchemaType

COLLECTION = "sid_chunks"

def main():
    client = QdrantClient(url="http://localhost:6333")

    # 1. Delete collection if it already exists (explicit, safe)
    if client.collection_exists(COLLECTION):
        print(f"Collection '{COLLECTION}' already exists. Deleting...")
        client.delete_collection(collection_name=COLLECTION)

    # 2. Create collection
    client.create_collection(
        collection_name=COLLECTION,
        vectors_config=VectorParams(
            size=1536,               # OpenAI text-embedding-3-small
            distance=Distance.COSINE
        )
    )

    # 3. Create payload index for filtering (critical)
    client.create_payload_index(
        collection_name=COLLECTION,
        field_name="scheme_code",
        field_schema=PayloadSchemaType.KEYWORD
    )

    print(f"✅ Qdrant collection '{COLLECTION}' created successfully")

if __name__ == "__main__":
    main()
