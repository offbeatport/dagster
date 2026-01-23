# burningdemand_dagster/assets/silver.py
import pandas as pd
import numpy as np
from dagster import AssetExecutionContext, MaterializeResult, asset

from ..partitions import daily_partitions
from ..resources.duckdb_resource import DuckDBResource
from ..resources.embedding_resource import EmbeddingResource
from ..assets.bronze import bronze_raw_items


@asset(
    partitions_def=daily_partitions,
    compute_kind="compute",
    group_name="silver",
    deps=[bronze_raw_items],
)
def silver_items_with_embeddings(
    context: AssetExecutionContext,
    db: DuckDBResource,
    embedding: EmbeddingResource,
) -> MaterializeResult:
    date = context.partition_key

    items = db.query_df(
        """
        SELECT b.url_hash, b.source, b.collection_date, b.url,
               b.title, b.body, b.created_at
        FROM bronze_raw_items b
        WHERE b.collection_date = ?
          AND NOT EXISTS (
              SELECT 1
              FROM silver_items_with_embeddings s
              WHERE s.url_hash = b.url_hash
          )
        """,
        [date],
    )

    if len(items) == 0:
        return MaterializeResult(metadata={"enriched": 0})

    enriched = 0
    batch_size = 1000

    for i in range(0, len(items), batch_size):
        batch = items.iloc[i : i + batch_size].copy()
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

        inserted_attempt = db.insert_df(
            "silver_items_with_embeddings",
            batch,
            [
                "url_hash",
                "source",
                "collection_date",
                "url",
                "title",
                "body",
                "created_at",
                "embedding",
                "embedding_date",
            ],
        )
        enriched += int(inserted_attempt)

    return MaterializeResult(
        metadata={"enriched": int(enriched), "batch_size": int(batch_size)}
    )


@asset(
    partitions_def=daily_partitions,
    compute_kind="compute",
    group_name="silver",
    deps=[silver_items_with_embeddings],
)
def silver_clusters(
    context: AssetExecutionContext,
    db: DuckDBResource,
) -> MaterializeResult:
    from sklearn.cluster import MiniBatchKMeans

    date = context.partition_key

    data = db.query_df(
        """
        SELECT url_hash, embedding
        FROM silver_items_with_embeddings
        WHERE embedding_date = ?
        """,
        [date],
    )

    if len(data) == 0:
        return MaterializeResult(metadata={"clusters": 0, "items": 0})

    embeddings = np.array(data["embedding"].tolist(), dtype=np.float32)
    n_clusters = max(10, len(embeddings) // 100)

    kmeans = MiniBatchKMeans(n_clusters=n_clusters, batch_size=256, random_state=42)
    labels = kmeans.fit_predict(embeddings)

    unique, counts = np.unique(labels, return_counts=True)
    size_by_cluster = dict(zip(unique.tolist(), counts.tolist()))

    # write clusters + membership (bulk for membership)
    created_clusters = 0
    for cid in range(n_clusters):
        size = int(size_by_cluster.get(cid, 0))
        if size == 0:
            continue
        db.execute(
            """
            INSERT INTO silver_clusters (cluster_date, cluster_id, cluster_size)
            VALUES (?, ?, ?)
            ON CONFLICT DO UPDATE SET cluster_size = EXCLUDED.cluster_size
            """,
            [date, cid, size],
        )
        created_clusters += 1

    members_df = pd.DataFrame(
        {
            "cluster_date": date,
            "cluster_id": [int(c) for c in labels.tolist()],
            "url_hash": data["url_hash"].tolist(),
        }
    )

    db.insert_df(
        "silver_cluster_members", members_df, ["cluster_date", "cluster_id", "url_hash"]
    )

    sizes = list(size_by_cluster.values()) if size_by_cluster else [0]
    sizes_sorted = sorted(sizes)
    p50 = sizes_sorted[len(sizes_sorted) // 2]
    p90 = (
        sizes_sorted[int(len(sizes_sorted) * 0.9) - 1]
        if len(sizes_sorted) >= 10
        else sizes_sorted[-1]
    )

    return MaterializeResult(
        metadata={
            "items": int(len(data)),
            "clusters": int(created_clusters),
            "cluster_size_min": int(min(sizes)),
            "cluster_size_p50": int(p50),
            "cluster_size_p90": int(p90),
            "cluster_size_max": int(max(sizes)),
        }
    )
