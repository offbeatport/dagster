# burningdemand_dagster/assets/gold.py
import asyncio
import pandas as pd
import anthropic
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from ..partitions import daily_partitions
from ..resources.duckdb_resource import DuckDBResource
from ..resources.app_config_resource import AppConfigResource
from ..resources.http_clients_resource import HTTPClientsResource
from ..utils.llm_schema import IssueLabel, extract_first_json_obj
from ..utils.retries import request_with_retry_async

_SOURCE_TYPE_MAP = {
    "github": "github_issue",
    "stackoverflow": "stackoverflow_question",
    "reddit": "reddit_thread",
    "hackernews": "other",
}


@asset(partitions_def=daily_partitions, compute_kind="ai", group_name="gold", deps=[])
async def gold_issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
    apis: AppConfigResource,
) -> MaterializeResult:
    date = context.partition_key
    client = anthropic.AsyncAnthropic(api_key=apis.anthropic_api_key)

    unlabeled = db.query_df(
        """
        SELECT sc.cluster_id, sc.cluster_size
        FROM silver_clusters sc
        WHERE sc.cluster_date = ?
          AND NOT EXISTS (
              SELECT 1 FROM gold_issues gi
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
        SELECT cm.cluster_id, s.title
        FROM silver_cluster_members cm
        JOIN silver_items_with_embeddings s ON cm.url_hash = s.url_hash
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

    async def label_one(cid: int, size: int):
        async with sem:
            titles = titles_by_cluster.get(cid, [])[:10]
            titles_str = "\n".join([f"- {t}" for t in titles if t])

            prompt = f"""Analyze these {len(titles)} issues (sampled from a cluster of {size}):

{titles_str}

Return ONLY valid JSON:
{{
  "canonical_title": "Generic problem (max 80 chars)",
  "category": "ai|finance|compliance|logistics|healthtech|devtools|ecommerce|other",
  "description": "Root problem in 1-2 sentences",
  "would_pay_signal": true/false,
  "impact_level": "low|medium|high"
}}"""

            try:
                msg = await client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=500,
                    messages=[{"role": "user", "content": prompt}],
                )
                raw = msg.content[0].text
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
            db.insert_df(
                "gold_issues",
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
                INSERT INTO gold_issue_evidence (cluster_date, cluster_id, url_hash, source, url, title, body, posted_at)
                SELECT cm.cluster_date,
                       cm.cluster_id,
                       s.url_hash,
                       s.source,
                       s.url,
                       s.title,
                       s.body,
                       s.created_at
                FROM silver_cluster_members cm
                JOIN silver_items_with_embeddings s
                  ON s.url_hash = cm.url_hash
                WHERE cm.cluster_date = ?
                  AND EXISTS (
                      SELECT 1 FROM gold_issues gi
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


@asset(
    partitions_def=daily_partitions,
    compute_kind="io",
    group_name="gold",
    deps=[gold_issues],
)
async def pocketbase_synced_issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
    apis: AppConfigResource,
    http: HTTPClientsResource,
) -> MaterializeResult:
    date = context.partition_key

    issues = db.query_df(
        """
        SELECT g.*
        FROM gold_issues g
        WHERE g.cluster_date = ?
          AND NOT EXISTS (
              SELECT 1 FROM pocketbase_sync ps
              WHERE ps.cluster_date = g.cluster_date
                AND ps.cluster_id = g.cluster_id
          )
    
        """,
        [date],
    )

    if len(issues) == 0:
        return MaterializeResult(metadata={"synced": 0})

    headers = {"Authorization": apis.pocketbase_admin_token}
    aclient = http.aclient

    synced = 0
    ev_posted = 0
    ev_errors = 0
    ev_sem = asyncio.Semaphore(20)

    async def post_evidence(issue_id: str, ev: dict):
        nonlocal ev_posted, ev_errors
        async with ev_sem:
            try:
                await request_with_retry_async(
                    aclient,
                    "POST",
                    f"{apis.pocketbase_url}/api/collections/evidence/records",
                    json={
                        "issue": issue_id,
                        "url": ev["url"],
                        "source_type": _SOURCE_TYPE_MAP.get(ev["source"], "other"),
                        "content_snippet": (ev.get("body") or "")[:500],
                        "posted_at": ev["posted_at"],
                    },
                    headers=headers,
                    timeout=10,
                )
                ev_posted += 1
            except Exception:
                ev_errors += 1

    for _, issue in issues.iterrows():
        # Create issue
        try:
            resp = await request_with_retry_async(
                aclient,
                "POST",
                f"{apis.pocketbase_url}/api/collections/issues/records",
                json={
                    "title": issue["canonical_title"],
                    "description": issue["description"],
                    "category": issue["category"],
                    "status": "open",
                    "origin": "collected",
                },
                headers=headers,
                timeout=30,
            )
            issue_id = resp.json()["id"]
        except Exception:
            continue

        evidence = db.query_df(
            """
            SELECT source, url, body, posted_at
            FROM gold_issue_evidence
            WHERE cluster_date = ? AND cluster_id = ?
            """,
            [date, int(issue["cluster_id"])],
        ).to_dict(orient="records")

        await asyncio.gather(*[post_evidence(issue_id, ev) for ev in evidence])

        # Track sync
        db.execute(
            "INSERT INTO pocketbase_sync (cluster_date, cluster_id, issue_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
            [date, int(issue["cluster_id"]), issue_id],
        )
        synced += 1

    return MaterializeResult(
        metadata={
            "synced": int(synced),
            "issues_considered": int(len(issues)),
            "evidence_posted": int(ev_posted),
            "evidence_errors": int(ev_errors),
        }
    )
