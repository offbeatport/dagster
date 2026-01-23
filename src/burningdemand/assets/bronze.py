import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset

from ..partitions import source_day_partitions
from ..resources.duckdb_resource import DuckDBResource
from ..resources.app_config_resource import AppConfigResource
from ..resources.http_clients_resource import HTTPClientsResource
from ..utils.collectors import collect_source_async
from ..utils.url import normalize_url, url_hash


@asset(partitions_def=source_day_partitions, compute_kind="io", group_name="bronze")
async def bronze_raw_items(
    context: AssetExecutionContext,
    db: DuckDBResource,
    apis: AppConfigResource,
    http: HTTPClientsResource,
) -> MaterializeResult:
    source = context.partition_key.keys_by_dimension["source"]
    date = context.partition_key.keys_by_dimension["date"]

    items, meta = await collect_source_async(source, date, apis, http.aclient)

    if not items:
        return MaterializeResult(metadata={"source": source, "date": date, "collected": 0, "insert_attempted": 0, "collector": meta})

    df = pd.DataFrame(items)
    df["url"] = df["url"].map(normalize_url)
    df["url_hash"] = df["url"].map(url_hash)
    df["source"] = source
    df["collection_date"] = date

    # enforce expected cols & sizes
    df["title"] = df["title"].fillna("").astype(str)
    df["body"] = df["body"].fillna("").astype(str).str.slice(0, 500)

    inserted_attempt = db.insert_df(
        "bronze_raw_items",
        df,
        ["url_hash", "source", "collection_date",
            "url", "title", "body", "created_at"],
    )

    return MaterializeResult(
        metadata={
            "source": source,
            "date": date,
            "collected": int(len(df)),
            "insert_attempted": int(inserted_attempt),
            "collector": meta,
        }
    )
