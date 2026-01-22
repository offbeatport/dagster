from dagster import RetryPolicy

COMMON_RETRY = RetryPolicy(max_retries=0, delay=2)
