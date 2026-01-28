import asyncio
import os

import redis.asyncio as redis
from dagster import ConfigurableResource
from pydantic import Field


class RedisResource(ConfigurableResource):
    """Async Redis client used for cross-process coordination (e.g. rate limiting)."""

    url: str = Field(
        default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        description="Redis connection URL, e.g. redis://localhost:6379/0",
    )

    def setup_for_execution(self, context) -> None:
        """Initialise Redis client and fail fast if it's not reachable."""
        self._client = redis.from_url(self.url, encoding="utf-8", decode_responses=True)
        self._context = context

        # Proactively verify connection so runs fail at startup if Redis is down
        async def _ping() -> None:
            await self._client.ping()

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(_ping())
        except Exception as e:
            raise RuntimeError(
                f"Failed to connect to Redis at '{self.url}'. "
                "Ensure REDIS_URL is correct and Redis is running."
            ) from e

    async def acquire_rate_limit_slot(
        self,
        domain: str,
        max_requests: int,
        period_seconds: float,
    ) -> None:
        """Distributed fixed-window rate limiter.

        Uses a simple INCR + EXPIRE pattern:
        - Key: rate:<domain>
        - Window: period_seconds
        - If count <= max_requests: request is allowed immediately.
        - If count > max_requests: wait until the window TTL elapses, then retry.

        If Redis is unreachable or misconfigured, this raises a RuntimeError
        so callers know the rate limiter is not functioning.
        """
        key = f"rate:{domain}"
        ttl_ms = int(period_seconds * 1000)

        try:
            while True:
                # Increment the counter atomically
                count = await self._client.incr(key)

                # On first increment, set the window TTL
                if count == 1:
                    await self._client.pexpire(key, ttl_ms)

                if count <= max_requests:
                    # Within limit for this window
                    return

                # Over the limit: wait for the current window to expire
                pttl = await self._client.pttl(key)
                # If key has no TTL for some reason, default to full window
                if pttl is None or pttl < 0:
                    pttl = ttl_ms

                wait_for = max(pttl / 1000.0, 0.1)
                if getattr(self, "_context", None) is not None:
                    self._context.log.info(
                        f"{domain}: rate limit hit (count={count}/{max_requests}), "
                        f"waiting {wait_for:.2f}s for window reset..."
                    )

                await asyncio.sleep(wait_for)
        except Exception as e:
            raise RuntimeError(
                f"Redis rate limiter failed for domain '{domain}'. "
                "Ensure REDIS_URL is correct and Redis is reachable."
            ) from e
