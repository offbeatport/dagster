# burningdemand_dagster/definitions.py
from dagster import Definitions, load_assets_from_package_module

from . import assets as assets_pkg

from .resources.duckdb_resource import DuckDBResource
from .resources.embedding_resource import EmbeddingResource
from .resources.app_config_resource import AppConfigResource
from .resources.http_clients_resource import HTTPClientsResource

all_assets = load_assets_from_package_module(assets_pkg)

defs = Definitions(
    assets=all_assets,
    resources={
        "db": DuckDBResource(),
        "embedding": EmbeddingResource(),
        "apis": AppConfigResource(),
        "http": HTTPClientsResource(),
    },
)
