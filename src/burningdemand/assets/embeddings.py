# burningdemand_dagster/assets/embeddings.py
import pandas as pd
import numpy as np
from dagster import AssetExecutionContext, MaterializeResult, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.embedding_resource import EmbeddingResource
from dagster import Config


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    deps=["raw_items"],
    description="Generate vector embeddings for raw items using sentence transformers. Converts title+body text into 384-dimensional vectors for semantic similarity and clustering.",
)
def embeddings(
    context: AssetExecutionContext,
    db: DuckDBResource,
    embedding: EmbeddingResource,
) -> MaterializeResult:
    date = context.partition_key

    items = db.query_df(
        f"""
        SELECT b.url_hash, b.title, b.body
        FROM bronze.raw_items b
        WHERE b.collection_date = ?
        """,
        [date],
    )

    if len(items) == 0:
        context.log.info(f"No new items to process for {date}")
        return MaterializeResult(metadata={"enriched": 0})

    total = 0
    batch_size = 1000
    total_batches = (len(items) + batch_size - 1) // batch_size

    context.log.info(
        f"Processing {len(items)} items in {total_batches} batches (batch size: {batch_size})"
    )

    for i in range(0, len(items), batch_size):
        batch_num = (i // batch_size) + 1
        batch = items.iloc[i : i + batch_size].copy()

        context.log.info(
            f"Batch {batch_num}/{total_batches}: Generating embeddings for {len(batch)} items..."
        )

        texts = (
            (
                batch["title"].fillna("").astype(str)
                + " "
                + batch["body"].fillna("").astype(str)
            )
            .str.strip()
            .tolist()
        )

        embs = embedding.encode(texts)  # ndarray (n, 384)
        # store embedding as list[float] per row (DuckDB FLOAT[384])
        batch["embedding"] = [e.tolist() for e in embs]
        batch["embedding_date"] = date

        silver_df = pd.DataFrame(
            {
                "url_hash": batch["url_hash"],
                "embedding": batch["embedding"],
                "embedding_date": batch["embedding_date"],
            }
        )

        # Convert string columns to object dtype for DuckDB compatibility
        silver_df["url_hash"] = silver_df["url_hash"].astype("object")
        silver_df["embedding_date"] = silver_df["embedding_date"].astype("object")

        context.log.info(
            f"Batch {batch_num}/{total_batches}: Inserting {len(silver_df)} items into database..."
        )

        inserted_attempt = db.upsert_df(
            "silver",
            "embeddings",
            silver_df,
            ["url_hash", "embedding", "embedding_date"],
        )
        total += int(inserted_attempt)

        context.log.info(
            f"Batch {batch_num}/{total_batches}: Completed ({inserted_attempt} upserted, {total} total embeddings)"
        )

    return MaterializeResult(
        metadata={"embeddings": int(total), "batch_size": int(batch_size)}
    )
