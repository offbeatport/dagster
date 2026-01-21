import os
import time
from typing import Any, Dict

from dagster import Array, Field, Int, String, get_dagster_logger, op

from .common import COMMON_RETRY
from .utils import (
    http_get_json,
    iso_from_unix,
    keyword_hit,
    make_content_hash,
    normalize_text,
)
from burningdemand.pb import pb_client_from_env


@op(
    retry_policy=COMMON_RETRY,
    config_schema={
        "tags": Field(Array(String), default_value=["jira", "oauth", "payments"]),
        "pagesize": Field(Int, default_value=30),
        "max_pages": Field(Int, default_value=2),
        "sort": Field(String, default_value="activity", description="activity|creation|votes"),
        "keywords": Field(Array(String), default_value=["blocked", "workaround", "can't", "unable"], is_required=False),
    },
)
def collect_stackoverflow() -> Dict[str, Any]:
    """
    Stack Exchange API. Optional env STACKEXCHANGE_KEY to increase quota.
    """
    log = get_dagster_logger()
    cfg = collect_stackoverflow.config
    pb = pb_client_from_env()

    key = os.getenv("STACKEXCHANGE_KEY")
    tagged = ";".join(cfg["tags"])
    pagesize = min(int(cfg["pagesize"]), 100)
    max_pages = int(cfg["max_pages"])
    sort = cfg["sort"]
    keywords = cfg.get("keywords") or []

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
