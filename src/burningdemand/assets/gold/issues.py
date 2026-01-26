# burningdemand_dagster/assets/issues.py
import asyncio
import os
from pprint import pprint
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


@asset(partitions_def=daily_partitions, deps=[AssetKey(["silver", "clusters"])])
async def issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
    llm: LLMResource,
) -> MaterializeResult:

    date = context.partition_key

    # Set API keys in environment for LiteLLM
    if llm.groq_api_key:
        os.environ["GROQ_API_KEY"] = llm.groq_api_key
    if llm.anthropic_api_key:
        os.environ["ANTHROPIC_API_KEY"] = llm.anthropic_api_key
    if llm.openai_api_key:
        os.environ["OPENAI_API_KEY"] = llm.openai_api_key

    unlabeled = db.query_df(
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

    if len(unlabeled) == 0:
        return MaterializeResult(metadata={"labeled": 0})

    cluster_ids = unlabeled["cluster_id"].astype(int).tolist()

    titles_df = db.query_df(
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

    titles_by_cluster = {}
    for cid in cluster_ids:
        titles_by_cluster[cid] = (
            titles_df[titles_df["cluster_id"] == cid]["title"]
            .head(10)
            .fillna("")
            .astype(str)
            .tolist()
        )

    sem = asyncio.Semaphore(8)
    errors = []
    results = []

    # context.log.info("titles_by_cluster:", pprint(titles_by_cluster))

    async def label_one(cid: int, size: int):
        async with sem:
            titles = titles_by_cluster.get(cid, [])[:10]
            titles_str = "\n".join([f"- {t}" for t in titles if t])

            prompt = f"""Analyze these {len(titles)} issues (sampled from a cluster of {size}):

{titles_str}

Return ONLY valid JSON:
{{
  "canonical_title": "Generic problem (max 120 chars)",
  "category": "ai|finance|compliance|logistics|healthtech|devtools|ecommerce|other",
  "description": "Root problem in 1-2 sentences",
  "would_pay_": number of would_pay signals,
  "impact_level": "low|medium|high"
}}"""

            try:
                # print(prompt)
                # # exit()
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

    await asyncio.gather(
        *[
            label_one(int(r["cluster_id"]), int(r["cluster_size"]))
            for _, r in unlabeled.iterrows()
        ]
    )

    if results:
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

    md = {
        "attempted": int(len(unlabeled)),
        "labeled": int(len(results)),
        "errors": int(len(errors)),
    }
    if errors:
        md["error_samples"] = MetadataValue.json(errors[:5])

    return MaterializeResult(metadata=md)
