# burningdemand_dagster/assets/issues.py
import asyncio
from typing import Dict, List, Tuple
import pandas as pd
from litellm import acompletion

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    MetadataValue,
    asset,
)

from burningdemand.partitions import daily_partitions
from burningdemand.resources.llm_resource import LLMResource
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.utils.llm_schema import IssueLabel, extract_first_json_obj


def get_unlabeled_clusters(db: DuckDBResource, date: str) -> pd.DataFrame:
    """Get clusters that haven't been labeled yet."""
    return db.query_df(
        """
        SELECT sc.cluster_id, sc.cluster_size
        FROM silver.clusters sc
        WHERE sc.cluster_date = ?
          AND NOT EXISTS (
              SELECT 1 FROM gold.issues gi
              WHERE gi.cluster_date = sc.cluster_date
                AND gi.cluster_id = sc.cluster_id
          )
        """,
        [date],
    )


def prepare_clusters(
    db: DuckDBResource, date: str, cluster_ids: List[int]
) -> Tuple[Dict[int, List[str]], Dict[int, str]]:
    """Get titles and summaries for each cluster from stored data."""
    # Get titles
    items_df = db.query_df(
        f"""
        SELECT cm.cluster_id, b.title
        FROM silver.cluster_members cm
        JOIN silver.embeddings s ON cm.url_hash = s.url_hash
        JOIN bronze.raw_items b ON s.url_hash = b.url_hash
        WHERE cm.cluster_date = ?
          AND cm.cluster_id IN ({",".join(["?"] * len(cluster_ids))})
        """,
        [date, *cluster_ids],
    )

    # Get stored summaries
    summaries_df = db.query_df(
        f"""
        SELECT cluster_id, summary
        FROM silver.cluster_summaries
        WHERE cluster_date = ?
          AND cluster_id IN ({",".join(["?"] * len(cluster_ids))})
        """,
        [date, *cluster_ids],
    )

    titles_by_cluster = {}
    summaries_by_cluster = {}

    for cid in cluster_ids:
        cluster_items = items_df[items_df["cluster_id"] == cid].head(10)
        titles = cluster_items["title"].fillna("").astype(str).tolist()
        titles_by_cluster[cid] = titles

        # Get stored summary
        cluster_summary = summaries_df[summaries_df["cluster_id"] == cid]
        if len(cluster_summary) > 0:
            summaries_by_cluster[cid] = cluster_summary.iloc[0]["summary"] or ""
        else:
            summaries_by_cluster[cid] = ""

    return titles_by_cluster, summaries_by_cluster


async def label_cluster_with_llm(
    cid: int,
    size: int,
    titles: List[str],
    summary: str,
    date: str,
    llm: LLMResource,
    sem: asyncio.Semaphore,
    results: List[dict],
    errors: List[str],
) -> None:
    """Label a single cluster using LLM."""
    async with sem:
        titles_str = "\n".join([f"- {t}" for t in titles if t])
        summary_section = f"\n\nIssue summaries:\n{summary}" if summary else ""

        prompt = f"""Analyze these {len(titles)} issues (sampled from a cluster of {size}):

Titles:
{titles_str}{summary_section}

Return ONLY valid JSON:
{{
  "canonical_title": "Generic problem (max 120 chars)",
  "category": "ai|finance|compliance|logistics|healthtech|devtools|ecommerce|other",
  "description": "Root problem in 1-2 sentences",
  "would_pay_": number of would_pay signals,
  "impact_level": "low|medium|high"
}}"""

        try:
            response = await acompletion(
                model=llm.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,
            )
            raw = response.choices[0].message.content
            data = extract_first_json_obj(raw)
            label = IssueLabel.model_validate(data)

            results.append(
                {
                    "cluster_date": date,
                    "cluster_id": int(cid),
                    "canonical_title": label.canonical_title,
                    "category": label.category,
                    "description": label.description,
                    "would_pay_signal": bool(label.would_pay_signal),
                    "impact_level": label.impact_level,
                    "cluster_size": int(size),
                }
            )
        except Exception as e:
            errors.append(f"cluster_id={cid}: {type(e).__name__}: {e}")


def save_results(db: DuckDBResource, results: List[dict], date: str) -> None:
    """Save labeled issues and evidence to database."""
    df = pd.DataFrame(results)

    def _tx():
        db.upsert_df(
            "gold",
            "issues",
            df,
            [
                "cluster_date",
                "cluster_id",
                "canonical_title",
                "category",
                "description",
                "would_pay_signal",
                "impact_level",
                "cluster_size",
            ],
        )

        # Evidence denorm in SQL (fast)
        db.execute(
            """
            INSERT INTO gold.issue_evidence (cluster_date, cluster_id, url_hash, source, url, title, body, posted_at)
            SELECT cm.cluster_date,
                   cm.cluster_id,
                   s.url_hash,
                   b.source,
                   b.url,
                   b.title,
                   b.body,
                   b.created_at
            FROM silver.cluster_members cm
            JOIN silver.embeddings s
              ON s.url_hash = cm.url_hash
            JOIN bronze.raw_items b
              ON s.url_hash = b.url_hash
            WHERE cm.cluster_date = ?
              AND EXISTS (
                  SELECT 1 FROM gold.issues gi
                  WHERE gi.cluster_date = cm.cluster_date
                    AND gi.cluster_id = cm.cluster_id
              )
            ON CONFLICT DO NOTHING
            """,
            [date],
        )

    db.transaction(_tx)


@asset(partitions_def=daily_partitions, deps=[AssetKey(["silver", "summaries"])])
async def issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
    llm: LLMResource,
) -> MaterializeResult:
    date = context.partition_key

    unlabeled = get_unlabeled_clusters(db, date)

    if len(unlabeled) == 0:
        return MaterializeResult(metadata={"labeled": 0})

    cluster_ids = unlabeled["cluster_id"].astype(int).tolist()
    titles_by_cluster, summaries_by_cluster = prepare_clusters(db, date, cluster_ids)

    sem = asyncio.Semaphore(8)
    errors = []
    results = []

    await asyncio.gather(
        *[
            label_cluster_with_llm(
                int(r["cluster_id"]),
                int(r["cluster_size"]),
                titles_by_cluster.get(int(r["cluster_id"]), [])[:10],
                summaries_by_cluster.get(int(r["cluster_id"]), ""),
                date,
                llm,
                sem,
                results,
                errors,
            )
            for _, r in unlabeled.iterrows()
        ]
    )

    if results:
        save_results(db, results, date)

    md = {
        "attempted": int(len(unlabeled)),
        "labeled": int(len(results)),
        "errors": int(len(errors)),
    }
    if errors:
        md["error_samples"] = MetadataValue.json(errors[:5])

    return MaterializeResult(metadata=md)
