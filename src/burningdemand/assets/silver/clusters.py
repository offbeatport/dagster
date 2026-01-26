# burningdemand_dagster/assets/clusters.py
import pandas as pd
import numpy as np
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from sklearn.cluster import MiniBatchKMeans

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource


@asset(
    partitions_def=daily_partitions,
    deps=[AssetKey(["silver", "embeddings"])],
)
def clusters(
    context: AssetExecutionContext,
    db: DuckDBResource,
) -> MaterializeResult:

    date = context.partition_key

    data = db.query_df(
        """
        SELECT url_hash, embedding
        FROM silver.embeddings
        WHERE embedding_date = ?
        """,
        [date],
    )

    if len(data) == 0:
        return MaterializeResult(metadata={"clusters": 0, "items": 0})

    embeddings_array = np.array(data["embedding"].tolist(), dtype=np.float32)
    n_clusters = max(10, len(embeddings_array) // 100)

    kmeans = MiniBatchKMeans(n_clusters=n_clusters, batch_size=256, random_state=42)
    labels = kmeans.fit_predict(embeddings_array)

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
            INSERT INTO silver.clusters (cluster_date, cluster_id, cluster_size)
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

    # Convert string columns to object dtype for DuckDB compatibility
    members_df["url_hash"] = members_df["url_hash"].astype("object")
    members_df["cluster_date"] = members_df["cluster_date"].astype("object")

    db.upsert_df(
        "silver",
        "cluster_members",
        members_df,
        ["cluster_date", "cluster_id", "url_hash"],
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
