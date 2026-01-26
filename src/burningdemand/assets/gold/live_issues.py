# burningdemand_dagster/assets/gold.py
import asyncio
import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, AssetKey, asset

from burningdemand.partitions import daily_partitions
from burningdemand.resources.duckdb_resource import DuckDBResource
from burningdemand.resources.pocketbase_resource import PocketBaseResource
from burningdemand.assets.gold.issues import issues

_SOURCE_TYPE_MAP = {
    "github": "github_issue",
    "stackoverflow": "stackoverflow_question",
    "reddit": "reddit_thread",
    "hackernews": "other",
}


@asset(
    partitions_def=daily_partitions,
    compute_kind="io",
    deps=[AssetKey(["gold", "issues"])],
)
async def live_issues(
    context: AssetExecutionContext,
    db: DuckDBResource,
    pb: PocketBaseResource,
) -> MaterializeResult:
    date = context.partition_key

    issues = db.query_df(
        """
        SELECT g.*
        FROM gold.issues g
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
                context.log.warning(
                    f"Failed to create issue for cluster {cluster_id}: {e}"
                )
                continue

        # Get evidence for this issue
        evidence = db.query_df(
            """
            SELECT source, url, body, posted_at
            FROM gold.issue_evidence
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
                context.log.warning(
                    f"Failed to create evidence for issue {issue_id}: {e}"
                )
                ev_errors += 1

        # Track sync
        db.execute(
            "INSERT INTO gold.live_issues (cluster_date, cluster_id, issue_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
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
