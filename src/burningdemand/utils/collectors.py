import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import httpx

from ..utils.keywords import (
    build_github_query,
    build_stackoverflow_tags,
    matches_keywords,
)
from ..utils.retries import request_with_retry_async
from ..utils.url import iso_date_to_utc_bounds


async def collect_source_async(
    source: str,
    date: str,
    apis,
    client: httpx.AsyncClient,
) -> Tuple[List[Dict], Dict]:
    """
    Collect items for a single (source, date) partition.
    Supports: github, stackoverflow, reddit, hackernews

    Returns: (items, metadata)

    Item schema:
      {
        "url": str,
        "title": str,
        "body": str,
        "created_at": str (iso8601)
      }
    """
    if source == "github":
        return await _collect_github(
            date=date,
            apis=apis,
            client=client,
        )

    if source == "stackoverflow":
        return await _collect_stackoverflow(
            date=date,
            apis=apis,
            client=client,
        )

    if source == "reddit":
        return await _collect_reddit(
            date=date,
            apis=apis,
            client=client,
        )

    if source == "hackernews":
        return await _collect_hackernews(date=date, apis=apis, client=client)

    return [], {"requests": 0, "note": f"unknown source={source}"}


# -----------------------------------------------------------------------------
# GitHub (token required)
# -----------------------------------------------------------------------------

async def _collect_github(
    date: str,
    apis,
    client: httpx.AsyncClient,
) -> Tuple[List[Dict], Dict]:
    headers = {"Authorization": f"token {apis.github_token}"}
    req_count = 0

    # Build query using keywords
    query = build_github_query(date)

    per_page = min(apis.github_per_page, 100)  # GitHub max is 100
    max_pages = apis.github_max_pages
    body_max_length = apis.body_max_length

    async def fetch_page(page: int):
        nonlocal req_count
        req_count += 1
        resp = await request_with_retry_async(
            client,
            "GET",
            "https://api.github.com/search/issues",
            params={"q": query, "per_page": per_page, "page": page},
            headers=headers,
        )
        return resp.json().get("items", [])

    pages = await asyncio.gather(*[fetch_page(p) for p in range(1, max_pages + 1)])
    items: List[Dict] = []
    for page_items in pages:
        for it in page_items:
            title = it.get("title") or ""
            body = it.get("body") or ""
            combined_text = f"{title} {body}".lower()

            # Filter by keywords (GitHub query already includes keywords, but double-check)
            if not matches_keywords(combined_text, source="github"):
                continue

            items.append(
                {
                    "url": it["html_url"],
                    "title": title,
                    "body": body[:body_max_length],
                    "created_at": it.get("created_at") or "",
                }
            )
    return items, {"requests": req_count}


# -----------------------------------------------------------------------------
# StackOverflow / StackExchange (key optional but recommended)
# -----------------------------------------------------------------------------

async def _collect_stackoverflow(
    date: str,
    apis,
    client: httpx.AsyncClient,
) -> Tuple[List[Dict], Dict]:
    from_ts, to_ts = iso_date_to_utc_bounds(date)
    req_count = 0

    # Get tags from keywords
    tags = build_stackoverflow_tags()

    # StackExchange max is 100
    pagesize = min(apis.stackoverflow_pagesize, 100)
    max_pages = apis.stackoverflow_max_pages
    body_max_length = apis.body_max_length
    stackexchange_key = getattr(apis, "stackexchange_key", None)

    async def fetch_page(page: int):
        nonlocal req_count
        req_count += 1

        params = {
            "fromdate": from_ts,
            "todate": to_ts,
            "site": "stackoverflow",
            "pagesize": pagesize,
            "page": page,
            # includes body (as HTML)
            "filter": "withbody",
        }
        if tags:
            params["tagged"] = ";".join(tags)
        if stackexchange_key:
            params["key"] = stackexchange_key

        resp = await request_with_retry_async(
            client,
            "GET",
            "https://api.stackexchange.com/2.3/questions",
            params=params,
        )
        return resp.json().get("items", [])

    pages = await asyncio.gather(*[fetch_page(p) for p in range(1, max_pages + 1)])
    items: List[Dict] = []
    for page_items in pages:
        for it in page_items:
            title = it.get("title") or ""
            body = it.get("body_markdown") or it.get("body") or ""
            combined_text = f"{title} {body}".lower()

            # Filter by keywords
            if not matches_keywords(combined_text, source="stackoverflow"):
                continue

            created = it.get("creation_date")
            created_iso = (
                datetime.fromtimestamp(
                    int(created), tz=timezone.utc).isoformat() if created else ""
            )
            items.append(
                {
                    "url": it.get("link") or "",
                    "title": title,
                    # withbody uses "body" (HTML). Some responses include body_markdown depending on filters.
                    "body": body[:body_max_length],
                    "created_at": created_iso,
                }
            )

    meta = {"requests": req_count, "used_key": bool(stackexchange_key)}
    return items, meta


# -----------------------------------------------------------------------------
# Reddit (prefer OAuth if credentials provided; fallback to public endpoints)
# -----------------------------------------------------------------------------

async def _collect_reddit(
    date: str,
    apis,
    client: httpx.AsyncClient,
) -> Tuple[List[Dict], Dict]:
    from_ts, to_ts = iso_date_to_utc_bounds(date)
    req_count = 0
    subs = apis.reddit_subreddits
    reddit_client_id = getattr(apis, "reddit_client_id", None)
    reddit_client_secret = getattr(apis, "reddit_client_secret", None)
    reddit_user_agent = getattr(apis, "reddit_user_agent", "BurningDemand/0.1")
    limit = apis.reddit_limit
    body_max_length = apis.body_max_length

    async def get_oauth_token() -> Optional[str]:
        """
        If client_id/secret are set, use installed-app script flow to fetch an app token.
        """
        nonlocal req_count
        if not reddit_client_id or not reddit_client_secret:
            return None

        req_count += 1
        auth = (reddit_client_id, reddit_client_secret)
        resp = await request_with_retry_async(
            client,
            "POST",
            "https://www.reddit.com/api/v1/access_token",
            auth=auth,
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": reddit_user_agent},
        )
        data = resp.json()
        return data.get("access_token")

    token = await get_oauth_token()

    # Choose endpoint + headers
    if token:
        base = "https://oauth.reddit.com"
        headers = {"Authorization": f"bearer {token}",
                   "User-Agent": reddit_user_agent}
        auth_mode = "oauth"
    else:
        base = "https://api.reddit.com"
        headers = {"User-Agent": reddit_user_agent}
        auth_mode = "public"

    async def fetch_sub(sub: str):
        nonlocal req_count
        req_count += 1
        resp = await request_with_retry_async(
            client,
            "GET",
            f"{base}/r/{sub}/new",
            params={"limit": limit},
            headers=headers,
        )
        return resp.json().get("data", {}).get("children", [])

    results = await asyncio.gather(*[fetch_sub(s) for s in subs])

    items: List[Dict] = []
    for children in results:
        for ch in children:
            d = ch.get("data") or {}
            created = int(d.get("created_utc") or 0)
            if from_ts <= created < to_ts:
                title = d.get("title") or ""
                body = d.get("selftext") or ""
                combined_text = f"{title} {body}".lower()

                # Filter by keywords
                if not matches_keywords(combined_text, source="reddit"):
                    continue

                items.append(
                    {
                        "url": f"https://reddit.com{d.get('permalink','')}",
                        "title": title,
                        "body": body[:body_max_length],
                        "created_at": datetime.fromtimestamp(created, tz=timezone.utc).isoformat(),
                    }
                )

    meta = {
        "requests": req_count,
        "subs": subs,
        "auth_mode": auth_mode,
        "used_oauth": bool(token),
    }
    return items, meta


# -----------------------------------------------------------------------------
# HackerNews (Algolia API; no token)
# -----------------------------------------------------------------------------

async def _collect_hackernews(
    date: str,
    apis,
    client: httpx.AsyncClient,
) -> Tuple[List[Dict], Dict]:
    from_ts, to_ts = iso_date_to_utc_bounds(date)
    req_count = 0

    hits_per_page = apis.hackernews_hits_per_page
    max_pages = apis.hackernews_max_pages
    body_max_length = apis.body_max_length

    async def fetch_page(page: int):
        nonlocal req_count
        req_count += 1
        resp = await request_with_retry_async(
            client,
            "GET",
            "https://hn.algolia.com/api/v1/search_by_date",
            params={
                "tags": "story",
                "numericFilters": f"created_at_i>{from_ts},created_at_i<{to_ts}",
                "hitsPerPage": hits_per_page,
                "page": page,
            },
        )
        return resp.json().get("hits", [])

    pages = await asyncio.gather(*[fetch_page(p) for p in range(0, max_pages)])

    items: List[Dict] = []
    for hits in pages:
        for it in hits:
            title = it.get("title") or ""
            body = it.get("story_text") or ""
            combined_text = f"{title} {body}".lower()

            # Filter by keywords
            if not matches_keywords(combined_text, source="hackernews"):
                continue

            url = it.get(
                "url") or f"https://news.ycombinator.com/item?id={it.get('objectID')}"
            created_i = int(it.get("created_at_i") or 0)
            items.append(
                {
                    "url": url,
                    "title": title,
                    "body": body[:body_max_length],
                    "created_at": datetime.fromtimestamp(created_i, tz=timezone.utc).isoformat()
                    if created_i
                    else "",
                }
            )

    return items, {"requests": req_count}
