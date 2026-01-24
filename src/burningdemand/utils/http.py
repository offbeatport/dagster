import asyncio
from dataclasses import dataclass, field
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


@dataclass
class HTTPConfig:
    """Configuration for HTTP client behavior."""
    retryable_status_codes: set[int] = field(default_factory=lambda: {429, 500, 502, 503, 504})
    rate_limits: dict[str, int] = field(default_factory=lambda: {"api.github.com": 30})
    rate_reset_seconds: float = 60.0
    timeout: float = 30.0
    user_agent: str = "BurningDemand/0.1"

    def __post_init__(self):
        self._rate_limiters: dict[str, AsyncLimiter] = {}


class RetryableHTTPError(Exception):
    """Exception for retryable HTTP errors."""
    pass


def _get_limiter(config: HTTPConfig, domain: str) -> Optional[AsyncLimiter]:
    if domain not in config.rate_limits:
        return None
    if domain not in config._rate_limiters:
        config._rate_limiters[domain] = AsyncLimiter(
            config.rate_limits[domain] / 60.0, 1.0
        )
    return config._rate_limiters[domain]


def create_async_client(config: Optional[HTTPConfig] = None) -> httpx.AsyncClient:
    """Create an async httpx client."""
    if config is None:
        config = HTTPConfig()
    return httpx.AsyncClient(
        timeout=httpx.Timeout(config.timeout),
        headers={"User-Agent": config.user_agent}
    )


def create_sync_client(config: Optional[HTTPConfig] = None) -> httpx.Client:
    """Create a sync httpx client."""
    if config is None:
        config = HTTPConfig()
    return httpx.Client(
        timeout=httpx.Timeout(config.timeout),
        headers={"User-Agent": config.user_agent}
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type((RetryableHTTPError, httpx.RequestError)),
    reraise=True,
)
async def _request(
    client: httpx.AsyncClient,
    config: HTTPConfig,
    method: str,
    url: str,
    limiter: Optional[AsyncLimiter],
    **kwargs
) -> httpx.Response:
    if limiter:
        async with limiter:
            resp = await client.request(method, url, **kwargs)
    else:
        resp = await client.request(method, url, **kwargs)

    if resp.status_code in config.retryable_status_codes:
        raise RetryableHTTPError(f"HTTP {resp.status_code}")

    resp.raise_for_status()
    return resp


async def request_with_retry_async(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    method: str,
    url: str,
    config: Optional[HTTPConfig] = None,
    use_rate_limiter: bool = True,
    **kwargs
) -> httpx.Response:
    """Make an async HTTP request with retry and rate limiting."""
    if config is None:
        config = HTTPConfig()
    domain = urlparse(url).netloc or ""
    limiter = _get_limiter(config, domain) if use_rate_limiter else None
    try:
        return await _request(client, config, method, url, limiter, **kwargs)
    except Exception as e:
        context.log.error(f"Failed {method} {url}: {e}")
        raise


async def batch_requests(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    requests: list,
    domain: str,
) -> list:
    """Run batch requests split by rate limit per minute."""
    if not requests:
        return []

    rate_limit = _RATE_LIMITS.get(domain)
    total_requests = len(requests)

    if not rate_limit:
        context.log.info(f"Executing {total_requests} requests (no rate limit)")
        return await asyncio.gather(
            *[request_with_retry_async(client, context, **r) for r in requests]
        )

    total_batches = (total_requests + rate_limit - 1) // rate_limit
    context.log.info(
        f"Executing {total_requests} requests in {total_batches} batches of {rate_limit}"
    )

    results = []
    for batch_num, i in enumerate(range(0, total_requests, rate_limit), 1):
        batch = requests[i : i + rate_limit]
        batch_size = len(batch)
        context.log.info(
            f"Processing batch {batch_num}/{total_batches} ({batch_size} requests)"
        )

        # Disable rate limiter in request_with_retry_async since we're batching here
        batch_coroutines = [
            request_with_retry_async(
                client, context, **{**r, "use_rate_limiter": False}
            )
            for r in batch
        ]
        batch_results = await asyncio.gather(*batch_coroutines)
        results.extend(batch_results)

        context.log.info(
            f"Completed batch {batch_num}/{total_batches} ({len(batch_results)} responses)"
        )

        if i + rate_limit < total_requests:
            context.log.info(
                f"Waiting {_RATE_RESET_SECONDS} seconds before next batch..."
            )
            await asyncio.sleep(_RATE_RESET_SECONDS)

    context.log.info(f"All batches completed: {len(results)} total responses")
    return results
