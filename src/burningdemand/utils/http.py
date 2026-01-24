import asyncio
from urllib.parse import urlparse
from typing import Optional

import httpx
from aiolimiter import AsyncLimiter
from dagster import AssetExecutionContext
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# Default HTTP configuration
_RETRYABLE = {429, 500, 502, 503, 504}
_RATE_LIMITS: dict[str, int] = {"api.github.com": 30}
_rate_limiters: dict[str, AsyncLimiter] = {}


def _get_limiter(domain: str, rate_limits: dict[str, int]) -> Optional[AsyncLimiter]:
    """Get or create rate limiter for domain."""
    if domain not in rate_limits:
        return None
    if domain not in _rate_limiters:
        _rate_limiters[domain] = AsyncLimiter(rate_limits[domain] / 60.0, 1.0)
    return _rate_limiters[domain]


class RetryableHTTPError(Exception):
    """Exception for retryable HTTP errors."""

    pass


def create_async_client(
    timeout: float = 30.0,
    user_agent: str = "BurningDemand/0.1",
) -> httpx.AsyncClient:
    """Create an async httpx client."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout),
        headers={"User-Agent": user_agent},
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type((RetryableHTTPError, httpx.RequestError)),
    reraise=True,
)
async def _request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    rate_limits: dict[str, int],
    retryable: set[int],
    **kwargs,
) -> httpx.Response:
    """Internal request function with automatic rate limiting and retry logic."""
    domain = urlparse(url).netloc or ""
    limiter = _get_limiter(domain, rate_limits)

    if limiter:
        async with limiter:
            resp = await client.request(method, url, **kwargs)
    else:
        resp = await client.request(method, url, **kwargs)

    if resp.status_code in retryable:
        raise RetryableHTTPError(f"HTTP {resp.status_code}")

    resp.raise_for_status()
    return resp


async def request_async(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    method: str,
    url: str,
    rate_limits: Optional[dict[str, int]] = None,
    retryable: Optional[set[int]] = None,
    **kwargs,
) -> httpx.Response:
    """Make an async HTTP request with automatic rate limiting and retry."""
    rate_limits = rate_limits or _RATE_LIMITS
    retryable = retryable or _RETRYABLE

    try:
        return await _request(client, method, url, rate_limits, retryable, **kwargs)
    except Exception as e:
        context.log.error(f"Failed {method} {url}: {e}")
        raise


async def batch_requests(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    requests: list[dict],
    rate_limits: Optional[dict[str, int]] = None,
    retryable: Optional[set[int]] = None,
) -> list[httpx.Response]:
    """Execute all requests concurrently with automatic rate limiting per domain."""
    if not requests:
        return []

    rate_limits = rate_limits or _RATE_LIMITS
    retryable = retryable or _RETRYABLE
    total = len(requests)

    context.log.info(f"Executing {total} requests with automatic rate limiting")

    results = await asyncio.gather(
        *[
            request_async(
                client, context, **r, rate_limits=rate_limits, retryable=retryable
            )
            for r in requests
        ]
    )

    context.log.info(f"Completed all {len(results)} requests")
    return results
