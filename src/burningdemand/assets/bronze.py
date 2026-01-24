import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset

from ..partitions import source_day_partitions
from ..resources.collectors.collectors_resource import CollectorsResource
from ..resources.duckdb_resource import DuckDBResource
from ..utils.url import normalize_url, url_hash


@asset(partitions_def=source_day_partitions, compute_kind="io", group_name="bronze")
async def bronze_raw_items(
    context: AssetExecutionContext,
    db: DuckDBResource,
    collector: CollectorsResource,
) -> MaterializeResult:
    source = context.partition_key.keys_by_dimension["source"]
    date = context.partition_key.keys_by_dimension["date"]

    items, meta = await collector.collect(source, date)

    if not items:
        return MaterializeResult(
            metadata={
                "source": source,
                "date": date,
                "collected": 0,
                "insert_attempted": 0,
                "collector": meta,
            }
        )

    df = pd.DataFrame(items)
    df["url"] = df["url"].map(normalize_url)
    df["url_hash"] = df["url"].map(url_hash)
    df["source"] = source
    df["collection_date"] = date

    # enforce expected cols & sizes - convert to object dtype for DuckDB compatibility
    df["title"] = df["title"].fillna("").astype("object")
    df["body"] = df["body"].fillna("").astype("object").str.slice(0, 500)
    df["url"] = df["url"].astype("object")
    df["url_hash"] = df["url_hash"].astype("object")
    df["source"] = df["source"].astype("object")
    df["collection_date"] = df["collection_date"].astype("object")
    df["created_at"] = df["created_at"].fillna("").astype("object")

    inserted_attempt = db.insert_df(
        "bronze_raw_items",
        df,
        ["url_hash", "source", "collection_date", "url", "title", "body", "created_at"],
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
