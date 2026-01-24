# burningdemand_dagster/assets/gold.py
import asyncio
import pandas as pd
import anthropic
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from ..partitions import daily_partitions
from ..resources.anthropic_resource import AnthropicResource
from ..resources.duckdb_resource import DuckDBResource
from ..resources.pocketbase_resource import PocketBaseResource
from ..utils.llm_schema import IssueLabel, extract_first_json_obj

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
    anthropic_api: AnthropicResource,
) -> MaterializeResult:
    date = context.partition_key
    client = anthropic.AsyncAnthropic(api_key=anthropic_api.api_key)

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
    pb: PocketBaseResource,
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

    synced = 0
    ev_posted = 0
    ev_errors = 0
    skipped = 0

    # Run PocketBase operations in executor since they're synchronous
    loop = asyncio.get_event_loop()

    def check_issue(pb_client, filter_expr):
        return pb_client.get_one_by_filter("issues", filter_expr)

    def create_issue(pb_client, payload):
        return pb_client.create("issues", payload)

    def check_evidence(pb_client, filter_expr):
        return pb_client.get_one_by_filter("evidence", filter_expr)

    def create_evidence(pb_client, payload):
        return pb_client.create("evidence", payload)

    for _, issue in issues.iterrows():
        # Check if issue already exists (might have been created manually)
        # Use a unique identifier based on cluster_date and cluster_id
        cluster_id = int(issue["cluster_id"])
        issue_title = str(issue["canonical_title"])
        issue_desc = str(issue["description"])
        issue_cat = str(issue["category"])
        
        # Check for existing issue by cluster_date and cluster_id
        # Escape quotes in filter expression for PocketBase
        date_escaped = str(date).replace('"', '\\"')
        filter_expr = f'origin="collected" && cluster_date="{date_escaped}" && cluster_id={cluster_id}'
        existing_issue = await loop.run_in_executor(
            None,
            check_issue,
            pb,
            filter_expr,
        )

        if existing_issue:
            issue_id = existing_issue["id"]
            skipped += 1
        else:
            # Create new issue
            try:
                issue_payload = {
                    "title": issue_title,
                    "description": issue_desc,
                    "category": issue_cat,
                    "status": "open",
                    "origin": "collected",
                    "cluster_date": date,
                    "cluster_id": cluster_id,
                }
                issue_record = await loop.run_in_executor(
                    None,
                    create_issue,
                    pb,
                    issue_payload,
                )
                issue_id = issue_record["id"]
            except Exception as e:
                context.log.warning(f"Failed to create issue for cluster {cluster_id}: {e}")
                continue

        # Get evidence for this issue
        evidence = db.query_df(
            """
            SELECT source, url, body, posted_at
            FROM gold_issue_evidence
            WHERE cluster_date = ? AND cluster_id = ?
            """,
            [date, cluster_id],
        ).to_dict(orient="records")

        # Create evidence records (check for duplicates by URL to avoid conflicts with manual entries)
        for ev in evidence:
            try:
                ev_url = str(ev["url"])
                ev_source = str(ev.get("source", ""))
                ev_body = str(ev.get("body", ""))[:500]
                ev_posted_at = str(ev.get("posted_at", ""))
                
                # Check if evidence already exists for this issue and URL
                # Escape quotes in URL for PocketBase filter (PocketBase uses \" for escaped quotes)
                ev_url_escaped = ev_url.replace('"', '\\"')
                evidence_filter = f'issue="{issue_id}" && url="{ev_url_escaped}"'
                existing_evidence = await loop.run_in_executor(
                    None,
                    check_evidence,
                    pb,
                    evidence_filter,
                )

                if not existing_evidence:
                    evidence_payload = {
                        "issue": issue_id,
                        "url": ev_url,
                        "source_type": _SOURCE_TYPE_MAP.get(ev_source, "other"),
                        "content_snippet": ev_body,
                        "posted_at": ev_posted_at,
                    }
                    await loop.run_in_executor(
                        None,
                        create_evidence,
                        pb,
                        evidence_payload,
                    )
                    ev_posted += 1
            except Exception as e:
                context.log.warning(f"Failed to create evidence for issue {issue_id}: {e}")
                ev_errors += 1

        # Track sync
        db.execute(
            "INSERT INTO pocketbase_sync (cluster_date, cluster_id, issue_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
            [date, cluster_id, issue_id],
        )
        synced += 1

    return MaterializeResult(
        metadata={
            "synced": int(synced),
            "skipped": int(skipped),
            "issues_considered": int(len(issues)),
            "evidence_posted": int(ev_posted),
            "evidence_errors": int(ev_errors),
        }
    )
