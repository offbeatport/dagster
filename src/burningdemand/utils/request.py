import asyncio
from collections import defaultdict
from urllib.parse import urlparse
from typing import Any

import httpx
from dagster import AssetExecutionContext

# Rate limit configuration: (max_requests, time_period_seconds)
RATE_LIMITS: dict[str, tuple[int, float]] = {
    "api.github.com": (30, 60.0),
    "api.stackexchange.com": (30, 60.0),
    "www.reddit.com": (60, 60.0),
    "oauth.reddit.com": (60, 60.0),
    "api.reddit.com": (60, 60.0),
    "hn.algolia.com": (100, 60.0),
}


def create_async_client(
    timeout: float,
    user_agent: str = "BurningDemand/0.1",
) -> httpx.AsyncClient:
    """Create an async httpx client with connection pooling."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout),
        headers={"User-Agent": user_agent},
        limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
    )


async def request_async(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs: Any,
) -> httpx.Response:
    """Make an async HTTP request (rate limiting handled at batch level)."""
    resp = await client.request(method, url, **kwargs)
    resp.raise_for_status()
    return resp


async def batch_requests(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    requests: list[dict],
) -> list[httpx.Response]:
    """Execute requests with distributed rate limiting per domain.

    For domains with rate limits, uses the Redis-backed RedisResource
    (available as ``context.resources.redis``) to coordinate limits across
    multiple processes. RedisResource is responsible for failing if Redis
    is not reachable or misconfigured.
    """
    if not requests:
        return []

    context.log.info(f"Executing {len(requests)} requests with rate limiting")

    # Group requests by domain (single pass)
    requests_by_domain: dict[str, list[tuple[int, dict]]] = defaultdict(list)
    for idx, req in enumerate(requests):
        domain = urlparse(req.get("url", "")).netloc or ""
        requests_by_domain[domain].append((idx, req))

    # Access Redis-based rate limiter once (Definitions should provide it)
    redis = getattr(context.resources, "redis", None)

    # Process all domains concurrently
    async def process_domain(
        domain: str, domain_requests: list[tuple[int, dict]]
    ) -> list[tuple[int, httpx.Response]]:
        """Process all requests for a single domain."""
        rate_limit_config = RATE_LIMITS.get(domain)

        if rate_limit_config:
            rate_limit, time_period = rate_limit_config

            async def run_with_limit(idx: int, req: dict) -> tuple[int, httpx.Response]:
                await redis.acquire_rate_limit_slot(
                    domain=domain,
                    max_requests=rate_limit,
                    period_seconds=time_period,
                )
                resp = await request_async(client, **req)
                return idx, resp

            return await asyncio.gather(
                *[run_with_limit(idx, req) for idx, req in domain_requests]
            )
        else:
            # No rate limit: process all concurrently
            results = await asyncio.gather(
                *[request_async(client, **req) for _, req in domain_requests]
            )
            return [(idx, resp) for (idx, _), resp in zip(domain_requests, results)]

    # Execute all domains in parallel
    domain_results = await asyncio.gather(
        *[
            process_domain(domain, domain_requests)
            for domain, domain_requests in requests_by_domain.items()
        ]
    )

    # Flatten and reconstruct results in original order
    all_results = {}
    for domain_result in domain_results:
        for idx, resp in domain_result:
            all_results[idx] = resp

    results = [all_results[i] for i in range(len(requests))]
    context.log.info(f"Completed all {len(results)} requests")
    return results
