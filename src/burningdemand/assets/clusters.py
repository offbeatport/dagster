# burningdemand_dagster/assets/clusters.py
import pandas as pd
import numpy as np
import hdbscan

from dagster import AssetExecutionContext, MaterializeResult, asset
from datetime import datetime, timedelta
from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource


@asset(
    partitions_def=daily_partitions,
    group_name="silver",
    deps=["embeddings"],
    description="Cluster items by similarity using HDBSCAN on stored embeddings. Uses rolling window (last 7 days) to allow clusters to grow day-over-day and detect heat signals. Groups similar issues together, allows noise/unclustered items, and stores quality metrics (outlier_ratio, mean_distance, authority_score). Authority score = sum of votes + 0.5 * comments to distinguish high-engagement signals from noise.",
)
def clusters(
    context: AssetExecutionContext,
    db: DuckDBResource,
) -> MaterializeResult:

    date = context.partition_key

    # Rolling window: cluster last 7 days of data

    date_obj = datetime.strptime(date, "%Y-%m-%d")
    window_start = (date_obj - timedelta(days=6)).strftime(
        "%Y-%m-%d"
    )  # Last 7 days inclusive

    context.log.info(f"Clustering rolling window: {window_start} to {date} (7 days)")

    # Get items with embeddings from the rolling window that haven't been clustered for this date
    data = db.query_df(
        """
        SELECT 
            e.url_hash, 
            e.embedding,
            b.vote_count, 
            b.comment_count
        FROM silver.embeddings e
        JOIN bronze.raw_items b ON e.url_hash = b.url_hash
        WHERE b.collection_date >= ?
          AND b.collection_date <= ?
          AND NOT EXISTS (
              SELECT 1 FROM silver.cluster_assignments ca
              WHERE ca.url_hash = e.url_hash
                AND ca.cluster_date = ?
          )
        """,
        [window_start, date, date],
    )

    if len(data) == 0:
        # Check if items were already clustered for this date
        existing = db.query_df(
            """
            SELECT COUNT(*) as cnt
            FROM silver.clusters
            WHERE cluster_date = ?
            """,
            [date],
        )
        if existing.iloc[0]["cnt"] > 0:
            return MaterializeResult(
                metadata={
                    "clusters": 0,
                    "items": 0,
                    "noise": 0,
                    "status": "already_clustered",
                }
            )
        return MaterializeResult(metadata={"clusters": 0, "items": 0, "noise": 0})

    context.log.info(f"Clustering {len(data)} items with stored embeddings...")

    # Convert stored embeddings to numpy array
    embeddings_array = np.array(data["embedding"].tolist(), dtype=np.float32)

    # HDBSCAN parameters: min_cluster_size and min_samples
    # min_cluster_size: minimum size of clusters
    # min_samples: conservative estimate of number of samples in cluster
    min_cluster_size = max(
        5, len(embeddings_array) // 200
    )  # At least 5, or 0.5% of data
    min_samples = max(3, min_cluster_size // 2)

    context.log.info(
        f"Running HDBSCAN with min_cluster_size={min_cluster_size}, min_samples={min_samples} on {len(embeddings_array)} items"
    )

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric="euclidean",
        cluster_selection_method="eom",
    )
    labels = clusterer.fit_predict(embeddings_array)

    # Compute quality metrics
    # outlier_ratio: fraction of points marked as noise (label == -1)
    noise_count = int(np.sum(labels == -1))
    outlier_ratio = float(noise_count / len(labels)) if len(labels) > 0 else 0.0

    # Get unique cluster IDs (excluding noise)
    unique_labels = np.unique(labels)
    cluster_ids = unique_labels[unique_labels != -1]

    # Compute metrics for each cluster: mean_distance and authority_score
    cluster_metrics = {}
    for cid in cluster_ids:
        cluster_mask = labels == cid
        cluster_points = embeddings_array[cluster_mask]
        cluster_data = data[cluster_mask]

        # Compute centroid
        centroid = np.mean(cluster_points, axis=0)

        # Compute distances from points to centroid
        distances = np.linalg.norm(cluster_points - centroid, axis=1)
        mean_distance = float(np.mean(distances))

        # Compute Authority Score: sum of votes + comments (weighted by engagement)
        # This distinguishes high-engagement signals from noise
        vote_sum = int(cluster_data["vote_count"].sum())
        comment_sum = int(cluster_data["comment_count"].sum())
        authority_score = vote_sum + (
            comment_sum * 0.5
        )  # Comments weighted less than votes

        cluster_metrics[int(cid)] = {
            "size": int(np.sum(cluster_mask)),
            "mean_distance": mean_distance,
            "authority_score": float(authority_score),
            "vote_sum": vote_sum,
            "comment_sum": comment_sum,
        }

    # Store cluster assignments in a mapping table (url_hash -> cluster_id)
    # We'll create a simple mapping table for lookups
    cluster_assignments = []
    for idx, url_hash in enumerate(data["url_hash"]):
        cluster_assignments.append(
            {
                "url_hash": url_hash,
                "cluster_date": date,
                "cluster_id": int(labels[idx]),
            }
        )

    # Store assignments (upsert to handle re-runs)
    if cluster_assignments:
        assignments_df = pd.DataFrame(cluster_assignments)
        db.upsert_df(
            "silver",
            "cluster_assignments",
            assignments_df,
            ["url_hash", "cluster_date", "cluster_id"],
        )

    # Write cluster metadata with quality metrics and authority score
    created_clusters = 0
    for cid, metrics in cluster_metrics.items():
        db.execute(
            """
            INSERT INTO silver.clusters (
                cluster_date, cluster_id, cluster_size, 
                outlier_ratio, mean_distance, authority_score
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (cluster_date, cluster_id) DO UPDATE SET 
                cluster_size = EXCLUDED.cluster_size,
                outlier_ratio = EXCLUDED.outlier_ratio,
                mean_distance = EXCLUDED.mean_distance,
                authority_score = EXCLUDED.authority_score
            """,
            [
                date,
                cid,
                metrics["size"],
                outlier_ratio,  # Store global outlier ratio for reference
                metrics["mean_distance"],
                metrics["authority_score"],
            ],
        )
        created_clusters += 1

    # Store global outlier ratio in a special record or metadata
    # For now, we'll include it in the return metadata

    sizes = [m["size"] for m in cluster_metrics.values()] if cluster_metrics else [0]
    sizes_sorted = sorted(sizes) if sizes else [0]
    p50 = sizes_sorted[len(sizes_sorted) // 2] if sizes_sorted else 0
    p90 = (
        sizes_sorted[int(len(sizes_sorted) * 0.9) - 1]
        if len(sizes_sorted) >= 10
        else sizes_sorted[-1] if sizes_sorted else 0
    )

    authority_scores = (
        [m["authority_score"] for m in cluster_metrics.values()]
        if cluster_metrics
        else [0]
    )

    return MaterializeResult(
        metadata={
            "items": int(len(data)),
            "clusters": int(created_clusters),
            "noise": int(noise_count),
            "outlier_ratio": float(outlier_ratio),
            "cluster_size_min": int(min(sizes)) if sizes else 0,
            "cluster_size_p50": int(p50),
            "cluster_size_p90": int(p90),
            "cluster_size_max": int(max(sizes)) if sizes else 0,
            "authority_score_max": (
                float(max(authority_scores)) if authority_scores else 0.0
            ),
            "authority_score_avg": (
                float(np.mean(authority_scores)) if authority_scores else 0.0
            ),
            "window_start": window_start,
            "window_end": date,
        }
    )
