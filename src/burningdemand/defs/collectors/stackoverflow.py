import os
import time
from typing import Any, Dict

from dagster import AssetExecutionContext, Config, asset, get_dagster_logger

from burningdemand.defs.common import COMMON_RETRY
from burningdemand.defs.utils import (
    http_get_json,
    iso_from_unix,
    keyword_hit,
    make_content_hash,
    normalize_text,
)
from burningdemand.resources import PocketBaseResource


class StackOverflowCollectorConfig(Config):
    tags: list[str] = ["jira", "oauth", "payments"]
    pagesize: int = 30
    max_pages: int = 2
    sort: str = "activity"  # activity|creation|votes
    keywords: list[str] = ["blocked", "workaround", "can't", "unable"]


@asset(
    retry_policy=COMMON_RETRY,
    description="Collects StackOverflow questions matching pain point tags and keywords",
)
def stackoverflow_questions(
    context: AssetExecutionContext,
    config: StackOverflowCollectorConfig,
    pb: PocketBaseResource,
) -> Dict[str, Any]:
    """
    Stack Exchange API. Optional env STACKEXCHANGE_KEY to increase quota.
    """
    log = get_dagster_logger()

    key = os.getenv("STACKEXCHANGE_KEY")
    tagged = ";".join(config.tags)
    pagesize = min(config.pagesize, 100)
    max_pages = config.max_pages
    sort = config.sort
    keywords = config.keywords or []

    stats = {"fetched": 0, "matched": 0,
             "created": 0, "updated": 0, "skipped": 0}

    for page in range(1, max_pages + 1):
        params = {
            "site": "stackoverflow",
            "tagged": tagged,
            "pagesize": pagesize,
            "page": page,
            "order": "desc",
            "sort": sort,
            "filter": "withbody",
        }
        if key:
            params["key"] = key

        data = http_get_json(
            "https://api.stackexchange.com/2.3/questions", params=params)
        items = data.get("items") or []

        for it in items:
            stats["fetched"] += 1

            title = normalize_text(it.get("title") or "")
            body = normalize_text(it.get("body") or "")
            url = it.get("link") or ""
            combined = f"{title}\n{body}"

            if not keyword_hit(combined, keywords):
                continue

            stats["matched"] += 1
            posted_at = iso_from_unix(int(it["creation_date"])) if it.get(
                "creation_date") else None

            content_hash = make_content_hash(title, body, url, posted_at)

            ing = {
                "source": "stack_overflow",
                "source_id": str(it.get("question_id")),
                "url": url,
                "title": title[:200],
                "body": body[:5000],
                "author": ((it.get("owner") or {}).get("display_name")) or "",
                "tags": it.get("tags") or [],
                "score": it.get("score"),
                "comments_count": it.get("answer_count"),
                "posted_at": posted_at,
                "raw": it,
                "content_hash": content_hash,
            }

            action, _ = pb.upsert_ing_item(ing)
            stats[action] += 1

        # Respect backoff if provided
        backoff = data.get("backoff")
        if backoff:
            log.warning(f"StackExchange backoff={backoff}s")
            time.sleep(int(backoff))

        if not data.get("has_more"):
            break

    log.info(f"StackOverflow done: {stats}")
    return stats
