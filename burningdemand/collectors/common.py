from dagster import RetryPolicy

COMMON_RETRY = RetryPolicy(max_retries=3, delay=2)
