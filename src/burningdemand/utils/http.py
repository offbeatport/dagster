import asyncio
from urllib.parse import urlparse
from typing import Optional

import httpx
from aiolimiter import AsyncLimiter
from dagster import AssetExecutionContext

# Default HTTP configuration
_RATE_LIMITS: dict[str, int] = {"api.github.com": 30}
_RATE_LIMIT_PERIOD = 60.0
_rate_limiters: dict[str, AsyncLimiter] = {}


def _get_limiter(domain: str, rate_limits: dict[str, int]) -> Optional[AsyncLimiter]:
    """Get or create rate limiter for domain.

    AsyncLimiter(max_rate, time_period) allows max_rate requests per time_period seconds.
    For 30 requests/minute, use AsyncLimiter(30, 60.0).
    """
    if domain not in rate_limits:
        return None
    if domain not in _rate_limiters:
        max_rate = rate_limits[domain]
        # Allow max_rate requests per 60 seconds
        _rate_limiters[domain] = AsyncLimiter(max_rate, _RATE_LIMIT_PERIOD)
    return _rate_limiters[domain]


def create_async_client(
    timeout: float = 30.0,
    user_agent: str = "BurningDemand/0.1",
) -> httpx.AsyncClient:
    """Create an async httpx client."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout),
        headers={"User-Agent": user_agent},
    )


async def _request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    rate_limits: dict[str, int],
    **kwargs,
) -> httpx.Response:
    """Internal request function with automatic rate limiting."""
    domain = urlparse(url).netloc or ""
    limiter = _get_limiter(domain, rate_limits)

    if limiter:
        async with limiter:
            resp = await client.request(method, url, **kwargs)
    else:
        resp = await client.request(method, url, **kwargs)

    resp.raise_for_status()
    return resp


async def request_async(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    method: str,
    url: str,
    rate_limits: Optional[dict[str, int]] = None,
    **kwargs,
) -> httpx.Response:
    """Make an async HTTP request with automatic rate limiting."""
    rate_limits = rate_limits or _RATE_LIMITS

    try:
        return await _request(client, method, url, rate_limits, **kwargs)
    except httpx.HTTPStatusError as e:
        # Log response body and headers for debugging (especially for 403 rate limit errors)
        if e.response is not None:
            try:
                response_body = e.response.text
                response_headers = dict(e.response.headers)
                context.log.error(
                    f"HTTP {e.response.status_code} {method} {url}\n"
                    f"Headers: {response_headers}\n"
                    f"Body: {response_body}"
                )
            except Exception as ex:
                context.log.error(
                    f"HTTP {e.response.status_code} {method} {url}: Could not read response (error: {ex})"
                )
        raise
    except Exception as e:
        context.log.error(f"Failed {method} {url}: {e}")
        raise


async def batch_requests(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    requests: list[dict],
    rate_limits: Optional[dict[str, int]] = None,
) -> list[httpx.Response]:
    """Execute all requests concurrently with automatic rate limiting per domain."""
    if not requests:
        return []

    rate_limits = rate_limits or _RATE_LIMITS
    total = len(requests)

    context.log.info(f"Executing {total} requests with automatic rate limiting")

    # Process requests in batches to respect rate limits
    # Use a semaphore to limit concurrent requests per domain
    max_rate = max(rate_limits.values()) if rate_limits else 30
    context.log.info(
        f"Max concurrent requests {max_rate} per {_RATE_LIMIT_PERIOD} seconds"
    )
    semaphore = asyncio.Semaphore(max_rate)

    # Process in batches to show progress and respect rate limits
    batch_size = max_rate  # Process one rate limit's worth at a time
    num_batches = (total + batch_size - 1) // batch_size

    results = []
    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, total)
        batch_requests_list = requests[start_idx:end_idx]

        context.log.info(
            f"Executing batch {batch_num + 1} of {num_batches} ({len(batch_requests_list)} requests)"
        )

        async def bounded_request(r: dict) -> httpx.Response:
            async with semaphore:
                return await request_async(
                    client, context, **r, rate_limits=rate_limits
                )

        batch_results = await asyncio.gather(
            *[bounded_request(r) for r in batch_requests_list]
        )
        results.extend(batch_results)

        # Wait for the rate limit period before starting the next batch (except for the last batch)
        if batch_num < num_batches - 1:
            context.log.info(
                f"Waiting {_RATE_LIMIT_PERIOD} seconds before next batch to respect rate limit..."
            )
            await asyncio.sleep(_RATE_LIMIT_PERIOD)

    context.log.info(f"Completed all {len(results)} requests")
    return results
