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
    strip_hn_html,
)
from burningdemand.pb import pb_client_from_env


@op(
    retry_policy=COMMON_RETRY,
    config_schema={
        "max_items": Field(Int, default_value=200, description="Max items to scan per run"),
        "keywords": Field(
            Array(String),
            default_value=["blocked", "workaround",
                           "pain", "does anyone", "we had to"],
            is_required=False,
        ),
        "mode": Field(String, default_value="newstories", description="HN feed: newstories|topstories|askstories"),
        "sleep_ms": Field(Int, default_value=0, description="Optional throttle between item fetches"),
    },
)
def collect_hackernews() -> Dict[str, Any]:
    """
    Fetch HN story items from Firebase API and upsert into PocketBase ing_items.
    """
    log = get_dagster_logger()
    cfg = collect_hackernews.config
    pb = pb_client_from_env()

    mode = cfg["mode"]
    max_items = int(cfg["max_items"])
    keywords = cfg.get("keywords") or []
    sleep_ms = int(cfg.get("sleep_ms") or 0)

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
