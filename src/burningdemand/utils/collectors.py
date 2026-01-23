import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from ..utils.keywords import build_stackoverflow_tags, matches_keywords
from ..utils.url import iso_date_to_utc_bounds

BODY_MAX_LENGTH = 500
REDDIT_SUBREDDITS = ["entrepreneur", "SaaS", "startups"]


async def collect_source_async(
    source: str, date: str, apis, http, context
) -> Tuple[List[Dict], Dict]:
    """Collect items for a single (source, date) partition."""
    collectors = {
        "github": _collect_github,
        "stackoverflow": _collect_stackoverflow,
        "reddit": _collect_reddit,
        "hackernews": _collect_hackernews,
    }

    collector = collectors.get(source)
    if not collector:
        return [], {"requests": 0, "note": f"unknown source={source}"}

    return await collector(date, apis, http, context)


# GitHub
async def _collect_github(date: str, apis, http, context) -> Tuple[List[Dict], Dict]:
    from ..utils.keywords import get_source_keywords

    headers = {"Authorization": f"token {apis.github_token}"}
    keywords = get_source_keywords("github")

    # Split into chunks of 6 keywords (5 OR operators max)
    queries = []
    for i in range(0, len(keywords), 6):
        chunk = keywords[i : i + 6]
        keyword_query = " OR ".join([f'"{kw}"' for kw in chunk])
        queries.append(f"is:issue created:{date} ({keyword_query})")

    # Build all request specs
    specs = [
        {
            "method": "GET",
            "url": "https://api.github.com/search/issues",
            "params": {"q": q, "per_page": 100, "page": p},
            "headers": headers,
        }
        for q in queries
        for p in range(1, 11)
    ]

    responses = await http.batch_requests(context, specs, "api.github.com")

    seen = set()
    items = []

    for resp in responses:
        for it in resp.json().get("items", []):
            url = it.get("html_url")
            if not url or url in seen:
                continue

            title = it.get("title") or ""
            body = it.get("body") or ""

            if matches_keywords(f"{title} {body}".lower(), source="github"):
                seen.add(url)
                items.append(
                    {
                        "url": url,
                        "title": title,
                        "body": body[:BODY_MAX_LENGTH],
                        "created_at": it.get("created_at") or "",
                    }
                )

    context.log.info(f"GitHub: {len(responses)} requests, {len(items)} items")
    return items, {"requests": len(responses), "queries": len(queries)}


# StackOverflow
async def _collect_stackoverflow(
    date: str, apis, http, context
) -> Tuple[List[Dict], Dict]:
    from_ts, to_ts = iso_date_to_utc_bounds(date)
    tags = build_stackoverflow_tags()
    key = getattr(apis, "stackexchange_key", None)

    context.log.info("StackOverflow: 10 pages")

    async def fetch_page(page: int):
        params = {
            "fromdate": from_ts,
            "todate": to_ts,
            "site": "stackoverflow",
            "pagesize": 100,
            "page": page,
            "filter": "withbody",
        }
        if tags:
            params["tagged"] = ";".join(tags)
        if key:
            params["key"] = key

        resp = await http.request_with_retry_async(
            "GET", "https://api.stackexchange.com/2.3/questions", params=params
        )
        return resp.json().get("items", [])

    pages = await asyncio.gather(*[fetch_page(p) for p in range(1, 11)])

    items = []
    for page_items in pages:
        for it in page_items:
            title = it.get("title") or ""
            body = it.get("body_markdown") or it.get("body") or ""

            if matches_keywords(f"{title} {body}".lower(), source="stackoverflow"):
                created = it.get("creation_date")
                items.append(
                    {
                        "url": it.get("link") or "",
                        "title": title,
                        "body": body[:BODY_MAX_LENGTH],
                        "created_at": (
                            datetime.fromtimestamp(
                                int(created), tz=timezone.utc
                            ).isoformat()
                            if created
                            else ""
                        ),
                    }
                )

    context.log.info(f"StackOverflow: 10 requests, {len(items)} items")
    return items, {"requests": 10, "used_key": bool(key)}


# Reddit
async def _collect_reddit(date: str, apis, http, context) -> Tuple[List[Dict], Dict]:
    from_ts, to_ts = iso_date_to_utc_bounds(date)
    client_id = getattr(apis, "reddit_client_id", None)
    client_secret = getattr(apis, "reddit_client_secret", None)
    user_agent = getattr(apis, "reddit_user_agent", "BurningDemand/0.1")

    # Get OAuth token if credentials provided
    token = None
    req_count = 0

    if client_id and client_secret:
        req_count += 1
        resp = await http.request_with_retry_async(
            "POST",
            "https://www.reddit.com/api/v1/access_token",
            auth=(client_id, client_secret),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": user_agent},
        )
        token = resp.json().get("access_token")

    base = "https://oauth.reddit.com" if token else "https://api.reddit.com"
    headers = {"User-Agent": user_agent}
    if token:
        headers["Authorization"] = f"bearer {token}"

    context.log.info(
        f"Reddit: {len(REDDIT_SUBREDDITS)} subreddits, {'OAuth' if token else 'public'}"
    )

    async def fetch_sub(sub: str):
        nonlocal req_count
        req_count += 1
        resp = await http.request_with_retry_async(
            "GET", f"{base}/r/{sub}/new", params={"limit": 100}, headers=headers
        )
        return resp.json().get("data", {}).get("children", [])

    results = await asyncio.gather(*[fetch_sub(s) for s in REDDIT_SUBREDDITS])

    items = []
    for children in results:
        for ch in children:
            d = ch.get("data") or {}
            created = int(d.get("created_utc") or 0)

            if from_ts <= created < to_ts:
                title = d.get("title") or ""
                body = d.get("selftext") or ""

                if matches_keywords(f"{title} {body}".lower(), source="reddit"):
                    items.append(
                        {
                            "url": f"https://reddit.com{d.get('permalink','')}",
                            "title": title,
                            "body": body[:BODY_MAX_LENGTH],
                            "created_at": datetime.fromtimestamp(
                                created, tz=timezone.utc
                            ).isoformat(),
                        }
                    )

    context.log.info(f"Reddit: {req_count} requests, {len(items)} items")
    return items, {
        "requests": req_count,
        "subs": REDDIT_SUBREDDITS,
        "used_oauth": bool(token),
    }


# HackerNews
async def _collect_hackernews(
    date: str, apis, http, context
) -> Tuple[List[Dict], Dict]:
    from_ts, to_ts = iso_date_to_utc_bounds(date)

    context.log.info("HackerNews: 10 pages")

    async def fetch_page(page: int):
        resp = await http.request_with_retry_async(
            "GET",
            "https://hn.algolia.com/api/v1/search_by_date",
            params={
                "tags": "story",
                "numericFilters": f"created_at_i>{from_ts},created_at_i<{to_ts}",
                "hitsPerPage": 100,
                "page": page,
            },
        )
        return resp.json().get("hits", [])

    pages = await asyncio.gather(*[fetch_page(p) for p in range(10)])

    items = []
    for hits in pages:
        for it in hits:
            title = it.get("title") or ""
            body = it.get("story_text") or ""

            if matches_keywords(f"{title} {body}".lower(), source="hackernews"):
                created_i = int(it.get("created_at_i") or 0)
                items.append(
                    {
                        "url": it.get("url")
                        or f"https://news.ycombinator.com/item?id={it.get('objectID')}",
                        "title": title,
                        "body": body[:BODY_MAX_LENGTH],
                        "created_at": (
                            datetime.fromtimestamp(
                                created_i, tz=timezone.utc
                            ).isoformat()
                            if created_i
                            else ""
                        ),
                    }
                )

    context.log.info(f"HackerNews: 10 requests, {len(items)} items")
    return items, {"requests": 10}
