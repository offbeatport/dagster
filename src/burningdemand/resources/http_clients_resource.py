import asyncio
from urllib.parse import urlparse
from typing import Optional

import httpx
from aiolimiter import AsyncLimiter
from dagster import ConfigurableResource
from pydantic import Field
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

_RETRYABLE = {429, 500, 502, 503, 504}
_RATE_LIMITS = {"api.github.com": 30}
_RATE_RESET_SECONDS = 60.0
_rate_limiters: dict[str, AsyncLimiter] = {}


class RetryableHTTPError(Exception):
    """Exception for retryable HTTP errors."""

    pass


def _get_limiter(domain: str) -> Optional[AsyncLimiter]:
    if domain not in _RATE_LIMITS:
        return None
    if domain not in _rate_limiters:
        _rate_limiters[domain] = AsyncLimiter(_RATE_LIMITS[domain] / 60.0, 1.0)
    return _rate_limiters[domain]


class HTTPClientsResource(ConfigurableResource):
    user_agent: str = Field(default="BurningDemand/0.1")

    def setup_for_execution(self, context) -> None:
        cfg = {"timeout": 30.0, "headers": {"User-Agent": self.user_agent}}
        self._client = httpx.Client(**cfg)
        self._aclient = httpx.AsyncClient(**cfg)
        self._context = context

    def teardown_after_execution(self, context) -> None:
        try:
            self._client.close()
        except:
            pass

    @property
    def client(self) -> httpx.Client:
        return self._client

    @property
    def aclient(self) -> httpx.AsyncClient:
        return self._aclient

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type((RetryableHTTPError, httpx.RequestError)),
        reraise=True,
    )
    async def _request(
        self, method: str, url: str, limiter: Optional[AsyncLimiter], **kwargs
    ):
        if limiter:
            async with limiter:
                resp = await self._aclient.request(method, url, **kwargs)
        else:
            resp = await self._aclient.request(method, url, **kwargs)

        if resp.status_code in _RETRYABLE:
            raise RetryableHTTPError(f"HTTP {resp.status_code}")

        resp.raise_for_status()
        return resp

    async def request_with_retry_async(
        self, method: str, url: str, use_rate_limiter: bool = True, **kwargs
    ) -> httpx.Response:
        domain = urlparse(url).netloc or ""
        limiter = _get_limiter(domain) if use_rate_limiter else None
        try:
            return await self._request(method, url, limiter, **kwargs)
        except Exception as e:
            self._context.log.error(f"Failed {method} {url}: {e}")
            raise

    async def batch_requests(self, context, requests: list, domain: str) -> list:
        if not requests:
            return []

        rate_limit = _RATE_LIMITS.get(domain)
        total_requests = len(requests)

        if not rate_limit:
            context.log.info(f"Executing {total_requests} requests (no rate limit)")
            return await asyncio.gather(
                *[self.request_with_retry_async(**r) for r in requests]
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
                self.request_with_retry_async(**{**r, "use_rate_limiter": False})
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
