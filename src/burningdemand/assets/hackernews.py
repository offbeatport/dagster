import time
from typing import Any, Dict

from dagster import AssetExecutionContext, Config, asset, get_dagster_logger

from .common import COMMON_RETRY
from .utils import (
    http_get_json,
    iso_from_unix,
    keyword_hit,
    make_content_hash,
    normalize_text,
    strip_hn_html,
)
from burningdemand.resources import PocketBaseResource


class HackerNewsCollectorConfig(Config):
    max_items: int = 200
    keywords: list[str] = ["blocked", "workaround", "pain", "does anyone", "we had to"]
    mode: str = "newstories"  # newstories|topstories|askstories
    sleep_ms: int = 0


@asset(
    retry_policy=COMMON_RETRY,
    description="Collects HackerNews stories matching pain point keywords",
)
def hackernews_stories(
    context: AssetExecutionContext,
    config: HackerNewsCollectorConfig,
    pb: PocketBaseResource,
) -> Dict[str, Any]:
    """
    Fetch HN story items from Firebase API and upsert into PocketBase ing_items.
    """
    log = get_dagster_logger()

    mode = config.mode
    max_items = config.max_items
    keywords = config.keywords or []
    sleep_ms = config.sleep_ms

    ids_url = f"https://hacker-news.firebaseio.com/v0/{mode}.json"
    ids = http_get_json(ids_url)
    if not isinstance(ids, list):
        raise RuntimeError(f"Unexpected HN ids response: {type(ids)}")

    ids = ids[:max_items]

    stats = {"scanned": 0, "matched": 0,
             "created": 0, "updated": 0, "skipped": 0}
    for hn_id in ids:
        stats["scanned"] += 1
        item_url = f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json"
        item = http_get_json(item_url)
        if not item or item.get("dead") or item.get("deleted"):
            continue
        if item.get("type") != "story":
            continue

        title = normalize_text(item.get("title") or "")
        body = strip_hn_html(item.get("text") or "")
        url = item.get(
            "url") or f"https://news.ycombinator.com/item?id={item.get('id')}"
        combined = f"{title}\n{body}"

        if not keyword_hit(combined, keywords):
            continue

        posted_at = iso_from_unix(
            int(item["time"])) if item.get("time") else None
        content_hash = make_content_hash(title, body, url, posted_at)

        ing = {
            "source": "hacker_news",
            "source_id": str(item.get("id")),
            "url": url,
            "title": title[:200],
            "body": body[:5000],
            "author": item.get("by") or "",
            "tags": ["hn", mode],
            "score": item.get("score"),
            "comments_count": item.get("descendants"),
            "posted_at": posted_at,
            "raw": item,
            "content_hash": content_hash,
        }

        action, _rid = pb.upsert_ing_item(ing)
        stats["matched"] += 1
        stats[action] += 1

        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

    log.info(f"HN done: {stats}")
    return stats
