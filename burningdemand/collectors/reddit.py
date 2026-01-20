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
        "subreddits": Field(Array(String), default_value=["startups", "saas"]),
        "query": Field(String, default_value='blocked OR workaround OR "does anyone"'),
        "limit": Field(Int, default_value=25),
        "keywords": Field(Array(String), default_value=["blocked", "workaround", "pain"], is_required=False),
    },
)
def collect_reddit() -> Dict[str, Any]:
    """
    Simple public Reddit search.json. For production reliability, switch to OAuth.
    This is still useful for early-stage signal mining.
    """
    log = get_dagster_logger()
    cfg = collect_reddit.config
    pb = pb_client_from_env()

    subreddits = cfg["subreddits"]
    query = cfg["query"]
    limit = min(int(cfg["limit"]), 100)
    keywords = cfg.get("keywords") or []

    headers = {
        "User-Agent": os.getenv("REDDIT_USER_AGENT", "BurningDemand/1.0")}

    stats = {"fetched": 0, "matched": 0,
             "created": 0, "updated": 0, "skipped": 0}

    for sub in subreddits:
        url = f"https://www.reddit.com/r/{sub}/search.json"
        params = {
            "q": query,
            "restrict_sr": 1,
            "sort": "new",
            "t": "month",
            "limit": limit,
        }
        data = http_get_json(url, headers=headers, params=params, timeout_s=25)
        children = ((data.get("data") or {}).get("children")) or []

        for ch in children:
            post = (ch.get("data") or {})
            stats["fetched"] += 1

            title = normalize_text(post.get("title") or "")
            body = normalize_text(post.get("selftext") or "")
            combined = f"{title}\n{body}"
            if not keyword_hit(combined, keywords):
                continue

            stats["matched"] += 1

            permalink = post.get("permalink") or ""
            url = f"https://www.reddit.com{permalink}" if permalink else (
                post.get("url") or "")
            posted_at = iso_from_unix(int(post["created_utc"])) if post.get(
                "created_utc") else None
            source_id = post.get("name") or str(post.get("id") or "")

            content_hash = make_content_hash(title, body, url, posted_at)

            ing = {
                "source": "reddit",
                "source_id": source_id,
                "url": url,
                "title": title[:200],
                "body": body[:5000],
                "author": post.get("author") or "",
                "tags": ["r/" + sub],
                "score": post.get("score"),
                "comments_count": post.get("num_comments"),
                "posted_at": posted_at,
                "raw": post,
                "content_hash": content_hash,
            }

            action, _ = pb.upsert_ing_item(ing)
            stats[action] += 1

        # gentle throttle to avoid hammering reddit
        time.sleep(1)

    log.info(f"Reddit done: {stats}")
    return stats
