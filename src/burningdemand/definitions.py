import os
from pathlib import Path
from dotenv import load_dotenv
from dagster import Definitions

# Load .env file from project root
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)

# Import assets directly
from .assets import raw_items, embeddings, clusters, issues, live_issues

from .resources.llm_resource import LLMResource
from .resources.collectors.collectors_resource import CollectorsResource
from .resources.duckdb_resource import DuckDBResource
from .resources.embedding_resource import EmbeddingResource
from .resources.pocketbase_resource import PocketBaseResource
from .resources.redis_resource import RedisResource

# Combine all assets
# Assets use AssetKey with prefixes (bronze, silver, gold) in their dependencies
# Dagster will automatically handle the key prefixes based on the AssetKey references
all_assets = [
    raw_items,
    embeddings,
    clusters,
    issues,
    live_issues,
]

llm_model = os.getenv("LLM_MODEL", "groq/llama-3.1-70b-versatile")

defs = Definitions(
    assets=all_assets,
    resources={
        "db": DuckDBResource(),
        "embedding": EmbeddingResource(),
        "collector": CollectorsResource(),
        "llm": LLMResource(model=llm_model),
        "pb": PocketBaseResource.from_env(),
        "redis": RedisResource(),
    },
)
