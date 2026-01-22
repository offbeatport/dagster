import os
from typing import Any, Dict

from dagster import AssetExecutionContext, Config, asset, get_dagster_logger

from .common import COMMON_RETRY
from .utils import (
    http_get_json,
    make_content_hash_with_updated,
    normalize_text,
)
from ..resources import PocketBaseResource


class GitHubCollectorConfig(Config):
    queries: list[str] = [
        "workaround in:body is:issue is:open",
        '"this blocks" in:body is:issue',
    ]
    per_page: int = 30
    max_total: int = 100


@asset(
    retry_policy=COMMON_RETRY,
    description="Collects GitHub issues matching pain point queries",
)
def github_issues(
    context: AssetExecutionContext,
    config: GitHubCollectorConfig,
    pb: PocketBaseResource,
) -> Dict[str, Any]:
    """
    Uses GitHub Search Issues API. You can include repo scoping in queries like:
      'repo:ORG/REPO workaround in:body is:issue'
    """
    log = get_dagster_logger()
    log.info("Starting GitHub collector")
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError(
            "Missing GITHUB_TOKEN env var for GitHub collector.")

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "BurningDemand-Dagster",
    }

    queries = config.queries
    per_page = config.per_page
    max_total = config.max_total

    stats = {"fetched": 0, "created": 0, "updated": 0, "skipped": 0}
    for q in queries:
        if stats["fetched"] >= max_total:
            break

        # Sort by updated so you see fresh pain
        params = {"q": q, "sort": "updated",
                  "order": "desc", "per_page": min(per_page, 100)}
        data = http_get_json(
            "https://api.github.com/search/issues", headers=headers, params=params)
        items = data.get("items") or []

        for it in items:
            if stats["fetched"] >= max_total:
                break

            # Filter out PRs if desired (PRs have pull_request field)
            if it.get("pull_request"):
                continue

            title = normalize_text(it.get("title") or "")
            body = normalize_text(it.get("body") or "")
            url = it.get("html_url") or ""
            posted_at = it.get("created_at")  # already ISO
            updated_at = it.get("updated_at")

            tags = []
            if it.get("labels"):
                tags.extend([lbl.get("name")
                            for lbl in it.get("labels") if lbl.get("name")])

            # content hash includes updated_at to catch edits
            content_hash = make_content_hash_with_updated(
                title, body, url, posted_at, updated_at)

            ing = {
                "source": "github",
                "source_id": str(it.get("id")),  # stable numeric id
                "url": url,
                "title": title[:200],
                "body": body[:5000],
                "author": (it.get("user") or {}).get("login") or "",
                "tags": tags,
                "score": it.get("score"),  # relevance score from search
                "comments_count": it.get("comments"),
                "posted_at": posted_at,
                "raw": it,
                "content_hash": content_hash,
            }

            action, _ = pb.upsert_ing_item(ing)
            stats["fetched"] += 1
            stats[action] += 1

    log.info(f"GitHub done: {stats}")
    return stats
