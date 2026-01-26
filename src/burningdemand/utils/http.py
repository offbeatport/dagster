import asyncio
import time
from collections import defaultdict
from urllib.parse import urlparse
from typing import Optional

import httpx
from aiolimiter import AsyncLimiter
from dagster import AssetExecutionContext

# Rate limit configuration per host
# Each entry has 'rate' (requests) and 'period' (seconds)
# These are automatically applied to all requests to these hosts
_RATE_LIMITS: dict[str, dict[str, float]] = {
    "api.github.com": {"rate": 25, "period": 60.0},  # Very conservative: 25 to account for AsyncLimiter burst behavior
    "api.stackexchange.com": {"rate": 30, "period": 60.0},
    "www.reddit.com": {"rate": 60, "period": 60.0},
    "oauth.reddit.com": {"rate": 60, "period": 60.0},
    "api.reddit.com": {"rate": 60, "period": 60.0},
    "hn.algolia.com": {"rate": 100, "period": 60.0},
}
_rate_limiters: dict[str, AsyncLimiter] = {}
_limiter_lock = asyncio.Lock()  # Protect limiter creation from race conditions

# Track request timestamps per domain for debugging
_request_timestamps: dict[str, list[float]] = {}
_request_counts: dict[str, int] = {}  # Count requests per domain
_timestamps_lock = asyncio.Lock()


async def _get_limiter(
    domain: str, rate_limits: dict[str, dict[str, float]]
) -> Optional[AsyncLimiter]:
    """Get or create rate limiter for domain (thread-safe).

    AsyncLimiter(max_rate, time_period) allows max_rate requests per time_period seconds.
    For 30 requests/minute, use AsyncLimiter(30, 60.0).

    This function is async and uses a lock to ensure only one limiter is created per domain,
    even when called concurrently from multiple partitions/assets.
    """
    if domain not in rate_limits:
        return None

    # Check if limiter already exists (fast path, no lock needed for read)
    if domain in _rate_limiters:
        return _rate_limiters[domain]

    # Use lock to prevent race condition when creating new limiter
    async with _limiter_lock:
        # Double-check after acquiring lock (another coroutine might have created it)
        if domain not in _rate_limiters:
            config = rate_limits[domain]
            max_rate = int(config["rate"])
            period = config["period"]
            _rate_limiters[domain] = AsyncLimiter(max_rate, period)
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


async def request_async(
    client: httpx.AsyncClient,
    context: AssetExecutionContext,
    method: str,
    url: str,
    rate_limits: Optional[dict[str, dict[str, float]]] = None,
    **kwargs,
) -> httpx.Response:
    """Make an async HTTP request with automatic rate limiting."""
    rate_limits = rate_limits or _RATE_LIMITS

    try:
        domain = urlparse(url).netloc or ""
        limiter = await _get_limiter(domain, rate_limits)
        
        # Track request timing for debugging
        request_start_time = time.time()
        if limiter:
            # Log before entering limiter (shows when request is queued)
            context.log.debug(f"[{domain}] Request queued: {method} {url}")
            
            async with limiter:
                # Log when request actually goes through (after limiter)
                request_exec_time = time.time()
                wait_time = request_exec_time - request_start_time
                
                async with _timestamps_lock:
                    if domain not in _request_timestamps:
                        _request_timestamps[domain] = []
                    _request_timestamps[domain].append(request_exec_time)
                    # Keep only last 100 timestamps
                    if len(_request_timestamps[domain]) > 100:
                        _request_timestamps[domain] = _request_timestamps[domain][-100:]
                    
                    _request_counts[domain] = _request_counts.get(domain, 0) + 1
                    count = _request_counts[domain]
                    
                    # Count requests in last period
                    config = rate_limits.get(domain, {})
                    period = config.get("period", 60.0)
                    recent_in_period = [
                        t for t in _request_timestamps[domain] 
                        if request_exec_time - t < period
                    ]
                    recent_count = len(recent_in_period)
                    limit = int(config.get("rate", 0)) if config else 0
                
                context.log.info(
                    f"[{domain}] Request #{count} executing (waited {wait_time:.2f}s, "
                    f"{recent_count}/{limit} in last {int(period)}s): {method} {url}"
                )
                
                resp = await client.request(method, url, **kwargs)
                
                # Log rate limit info from response headers (if available)
                if domain == "api.github.com" and resp.headers.get("x-ratelimit-remaining"):
                    remaining = resp.headers.get("x-ratelimit-remaining")
                    used = resp.headers.get("x-ratelimit-used")
                    limit_header = resp.headers.get("x-ratelimit-limit")
                    context.log.info(
                        f"[{domain}] Rate limit status: {used}/{limit_header} used, {remaining} remaining"
                    )
        else:
            resp = await client.request(method, url, **kwargs)

        resp.raise_for_status()
        return resp
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
    rate_limits: Optional[dict[str, dict[str, float]]] = None,
) -> list[httpx.Response]:
    """Execute all requests concurrently with automatic rate limiting per domain.

    Respects _RATE_LIMITS for each domain. The AsyncLimiter ensures
    we never exceed the rate limit (e.g., 29 requests per 60 seconds for api.github.com).

    Args:
        client: HTTP client
        context: Dagster context for logging
        requests: List of request dicts (method, url, params, headers, etc.)
        rate_limits: Optional rate limits dict (uses _RATE_LIMITS if None)
    """
    if not requests:
        return []

    rate_limits = rate_limits or _RATE_LIMITS
    total = len(requests)

    # Group requests by domain to understand the workload
    by_domain = defaultdict(int)
    for r in requests:
        domain = urlparse(r.get("url", "")).netloc or ""
        by_domain[domain] += 1

    context.log.info(
        f"Executing {total} requests across {len(by_domain)} domains with rate limiting"
    )
    
    # Reset request counts for this batch
    async with _timestamps_lock:
        for domain in by_domain.keys():
            _request_counts[domain] = 0
    
    for domain, count in by_domain.items():
        config = rate_limits.get(domain)
        if config:
            limit_str = f"{int(config['rate'])}/{int(config['period'])}s"
            # Check recent request rate for this domain
            async with _timestamps_lock:
                recent = _request_timestamps.get(domain, [])
                if recent:
                    # Count requests in last period
                    now = time.time()
                    period = config["period"]
                    recent_in_period = [t for t in recent if now - t < period]
                    if recent_in_period:
                        context.log.warning(
                            f"  {domain}: {count} new requests queued, but {len(recent_in_period)} requests "
                            f"already made in last {int(period)}s (limit: {limit_str})"
                        )
        else:
            limit_str = "unlimited"
        context.log.info(f"  {domain}: {count} requests queued (limit: {limit_str})")

    # Create tasks - AsyncLimiter will automatically throttle to respect rate limits
    tasks = [
        request_async(client, context, **r, rate_limits=rate_limits) for r in requests
    ]
    results = await asyncio.gather(*tasks)
    
    # Log final counts per domain
    async with _timestamps_lock:
        for domain in by_domain.keys():
            total_made = _request_counts.get(domain, 0)
            config = rate_limits.get(domain)
            if config:
                period = config["period"]
                now = time.time()
                recent = _request_timestamps.get(domain, [])
                recent_in_period = [t for t in recent if now - t < period]
                context.log.info(
                    f"[{domain}] Completed: {total_made} total requests made, "
                    f"{len(recent_in_period)} in last {int(period)}s"
                )
    
    context.log.info(f"Completed all {len(results)} requests")
    return results
