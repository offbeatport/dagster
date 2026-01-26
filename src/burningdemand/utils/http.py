import asyncio
from collections import defaultdict
from urllib.parse import urlparse

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
    timeout: float = 30.0,
    user_agent: str = "BurningDemand/0.1",
) -> httpx.AsyncClient:
    """Create an async httpx client."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout),
        headers={"User-Agent": user_agent},
    )


async def request_async(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    method: str,
    url: str,
    **kwargs,
) -> httpx.Response:
    """Make an async HTTP request (rate limiting handled at batch level)."""
    domain = urlparse(url).netloc or ""

    resp = await client.request(method, url, **kwargs)
    resp.raise_for_status()
    return resp


async def batch_requests(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    requests: list[dict],
) -> list[httpx.Response]:
    """Execute requests in strict batches to enforce rate limits.

    For domains with rate limits, ensures no more than rate_limit requests
    per time_period by batching: send rate_limit requests, wait time_period, repeat.
    """
    if not requests:
        return []

    context.log.info(f"Executing {len(requests)} requests with rate limiting")

    # Group requests by domain
    requests_by_domain = defaultdict(list)

    for idx, req in enumerate(requests):
        url = req.get("url", "")
        domain = urlparse(url).netloc or ""
        requests_by_domain[domain].append((idx, req))

    # Process each domain's requests
    all_results = {}

    for domain, domain_requests in requests_by_domain.items():
        rate_limit_config = RATE_LIMITS.get(domain)

        if rate_limit_config:
            rate_limit, time_period = rate_limit_config
            # Strict batching: send rate_limit requests, wait time_period, repeat
            total = len(domain_requests)
            batch_num = 0

            for batch_start in range(0, total, rate_limit):
                batch_end = min(batch_start + rate_limit, total)
                batch = domain_requests[batch_start:batch_end]
                batch_num += 1

                context.log.info(
                    f"{domain}: Batch {batch_num} of {len(batch)} requests "
                    f"({batch_start + 1}-{batch_end} of {total})"
                )

                # Execute batch concurrently
                tasks = [request_async(client, context, **req) for idx, req in batch]
                batch_results = await asyncio.gather(*tasks)

                # Store results
                for (idx, _), result in zip(batch, batch_results):
                    all_results[idx] = result

                # Wait time_period seconds before next batch (unless last batch)
                if batch_end < total:
                    context.log.info(
                        f"{domain}: Waiting {time_period}s before next batch..."
                    )
                    await asyncio.sleep(time_period)
        else:
            # No rate limit, process all concurrently
            tasks = [
                request_async(client, context, **req) for idx, req in domain_requests
            ]
            results = await asyncio.gather(*tasks)
            for (idx, _), result in zip(domain_requests, results):
                all_results[idx] = result

    # Reconstruct results in original order
    results = [all_results[i] for i in range(len(requests))]

    context.log.info(f"Completed all {len(results)} requests")
    return results
