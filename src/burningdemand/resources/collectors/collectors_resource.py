import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from dagster import ConfigurableResource, EnvVar

from .collector_queries import (
    get_body_max_length,
    get_max_queries,
    get_query_keywords,
    get_query_subreddits,
    get_query_tags,
    matches_query_keywords,
)
from burningdemand.utils.http import batch_requests, create_async_client, request_async
from burningdemand.utils.url import iso_date_to_utc_bounds


class CollectorsResource(ConfigurableResource):
    """Resource for collecting data from various sources."""

    # GitHub
    github_token: str = EnvVar("GITHUB_TOKEN")

    # StackExchange
    stackexchange_key: Optional[str] = os.getenv("STACKEXCHANGE_KEY")

    # Reddit
    reddit_client_id: Optional[str] = os.getenv("REDDIT_CLIENT_ID")
    reddit_client_secret: Optional[str] = os.getenv("REDDIT_CLIENT_SECRET")

    def setup_for_execution(self, context) -> None:
        self._context = context
        # HTTP client configuration
        self._client = create_async_client(
            timeout=30.0,
            user_agent="BurningDemand/0.1",
        )

    def teardown_after_execution(self, context) -> None:
        try:
            # Properly await the async close
            if hasattr(self, "_client"):
                asyncio.run(self._client.aclose())
        except:
            pass

    async def collect(
        self,
        source: str,
        date: str,
    ) -> Tuple[List[Dict], Dict]:
        """Collect items for a single (source, date) partition."""
        collectors = {
            "github": self._collect_github,
            "stackoverflow": self._collect_stackoverflow,
            "reddit": self._collect_reddit,
            "hackernews": self._collect_hackernews,
        }

        collector = collectors.get(source)
        if not collector:
            return [], {"requests": 0, "note": f"unknown source={source}"}

        return await collector(date)

    async def _collect_github(self, date: str) -> Tuple[List[Dict], Dict]:
        headers = {"Authorization": f"token {self.github_token}"}
        keywords = get_query_keywords("github")

        # Split into chunks of 6 keywords (5 OR operators max)
        queries = []
        for i in range(0, len(keywords), 6):
            chunk = keywords[i : i + 6]
            keyword_query = " OR ".join([f'"{kw}"' for kw in chunk])
            queries.append(f"is:issue created:{date} ({keyword_query})")

        # Apply max_queries limit if configured
        max_queries = get_max_queries("github")
        if max_queries is not None:
            queries = queries[:max_queries]

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

        responses = await batch_requests(
            self._client,
            self._context,
            specs,
        )

        seen = set()
        items = []

        for resp in responses:
            for it in resp.json().get("items", []):
                url = it.get("html_url")
                if not url or url in seen:
                    continue

                title = it.get("title") or ""
                body = it.get("body") or ""

                if matches_query_keywords(f"{title} {body}", "github"):
                    seen.add(url)
                    items.append(
                        {
                            "url": url,
                            "title": title,
                            "body": body[: get_body_max_length()],
                            "created_at": it.get("created_at") or "",
                        }
                    )

        self._context.log.info(f"GitHub: {len(responses)} requests, {len(items)} items")
        return items, {"requests": len(responses), "queries": len(queries)}

    async def _collect_stackoverflow(self, date: str) -> Tuple[List[Dict], Dict]:
        from_ts, to_ts = iso_date_to_utc_bounds(date)
        tags = get_query_tags("stackoverflow")
        key = self.stackexchange_key

        self._context.log.info("StackOverflow: 10 pages")

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

            resp = await request_async(
                self._client,
                self._context,
                "GET",
                "https://api.stackexchange.com/2.3/questions",
                params=params,
            )
            return resp.json().get("items", [])

        pages = await asyncio.gather(*[fetch_page(p) for p in range(1, 11)])

        items = []
        for page_items in pages:
            for it in page_items:
                title = it.get("title") or ""
                body = it.get("body_markdown") or it.get("body") or ""

                if matches_query_keywords(f"{title} {body}", "stackoverflow"):
                    created = it.get("creation_date")
                    items.append(
                        {
                            "url": it.get("link") or "",
                            "title": title,
                            "body": body[: get_body_max_length()],
                            "created_at": (
                                datetime.fromtimestamp(
                                    int(created), tz=timezone.utc
                                ).isoformat()
                                if created
                                else ""
                            ),
                        }
                    )

        self._context.log.info(f"StackOverflow: 10 requests, {len(items)} items")
        return items, {"requests": 10, "used_key": bool(key)}

    async def _collect_reddit(self, date: str) -> Tuple[List[Dict], Dict]:
        from_ts, to_ts = iso_date_to_utc_bounds(date)
        client_id = self.reddit_client_id
        client_secret = self.reddit_client_secret
        user_agent = "BurningDemand/0.1"
        subreddits = get_query_subreddits()

        # Get OAuth token if credentials provided
        token = None
        req_count = 0

        if client_id and client_secret:
            req_count += 1
            resp = await request_async(
                self._client,
                self._context,
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

        self._context.log.info(
            f"Reddit: {len(subreddits)} subreddits, {'OAuth' if token else 'public'}"
        )

        async def fetch_sub(sub: str):
            nonlocal req_count
            req_count += 1
            resp = await request_async(
                self._client,
                self._context,
                "GET",
                f"{base}/r/{sub}/new",
                params={"limit": 100},
                headers=headers,
            )
            return resp.json().get("data", {}).get("children", [])

        results = await asyncio.gather(*[fetch_sub(s) for s in subreddits])

        items = []
        for children in results:
            for ch in children:
                d = ch.get("data") or {}
                created = int(d.get("created_utc") or 0)

                if from_ts <= created < to_ts:
                    title = d.get("title") or ""
                    body = d.get("selftext") or ""

                    if matches_query_keywords(f"{title} {body}", "reddit"):
                        items.append(
                            {
                                "url": f"https://reddit.com{d.get('permalink','')}",
                                "title": title,
                                "body": body[: get_body_max_length()],
                                "created_at": datetime.fromtimestamp(
                                    created, tz=timezone.utc
                                ).isoformat(),
                            }
                        )

        self._context.log.info(f"Reddit: {req_count} requests, {len(items)} items")
        return items, {
            "requests": req_count,
            "subs": subreddits,
            "used_oauth": bool(token),
        }

    async def _collect_hackernews(self, date: str) -> Tuple[List[Dict], Dict]:
        from_ts, to_ts = iso_date_to_utc_bounds(date)

        self._context.log.info("HackerNews: 10 pages")

        async def fetch_page(page: int):
            resp = await request_async(
                self._client,
                self._context,
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

                if matches_query_keywords(f"{title} {body}", "hackernews"):
                    created_i = int(it.get("created_at_i") or 0)
                    items.append(
                        {
                            "url": it.get("url")
                            or f"https://news.ycombinator.com/item?id={it.get('objectID')}",
                            "title": title,
                            "body": body[: get_body_max_length()],
                            "created_at": (
                                datetime.fromtimestamp(
                                    created_i, tz=timezone.utc
                                ).isoformat()
                                if created_i
                                else ""
                            ),
                        }
                    )

        self._context.log.info(f"HackerNews: 10 requests, {len(items)} items")
        return items, {"requests": 10}
