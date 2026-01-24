# burningdemand_dagster/definitions.py
from pathlib import Path

from dotenv import load_dotenv
from dagster import Definitions, load_assets_from_package_module

# Load .env file from project root
# override=True ensures .env values take precedence over existing env vars
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)

from . import assets as assets_pkg

from .resources.anthropic_resource import AnthropicResource
from .resources.collectors.collectors_resource import CollectorsResource
from .resources.duckdb_resource import DuckDBResource
from .resources.embedding_resource import EmbeddingResource
from .resources.pocketbase_resource import PocketBaseResource

all_assets = load_assets_from_package_module(assets_pkg)

defs = Definitions(
    assets=all_assets,
    resources={
        "db": DuckDBResource(),
        "embedding": EmbeddingResource(),
        "collector": CollectorsResource(),
        "anthropic_api": AnthropicResource(),
        "pb": PocketBaseResource.from_env(),
    },
)
