# burningdemand_dagster/utils/retries.py
import random
from typing import Optional

import httpx

_RETRYABLE = {429, 500, 502, 503, 504}


async def request_with_retry_async(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    max_attempts: int = 6,
    **kwargs,
) -> httpx.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(max_attempts):
        try:
            resp = await client.request(method, url, **kwargs)
            if resp.status_code in _RETRYABLE:
                wait = min(30.0, (2**attempt) + random.random())
                import asyncio

                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            wait = min(30.0, (2**attempt) + random.random())
            import asyncio

            await asyncio.sleep(wait)
    raise last_exc or RuntimeError("request_with_retry_async failed")


def request_with_retry(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    max_attempts: int = 6,
    **kwargs,
) -> httpx.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(max_attempts):
        try:
            resp = client.request(method, url, **kwargs)
            if resp.status_code in _RETRYABLE:
                wait = min(30.0, (2**attempt) + random.random())
                import time

                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            import time

            wait = min(30.0, (2**attempt) + random.random())
            time.sleep(wait)
    raise last_exc or RuntimeError("request_with_retry failed")
