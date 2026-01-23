from typing import List, Optional

from dagster import ConfigurableResource, EnvVar


class AppConfigResource(ConfigurableResource):
    """
    Reads secrets/config from environment variables.
    Missing required vars will cause an immediate validation error on startup.
    """

    # --- GitHub ---
    github_token: str = EnvVar("GITHUB_TOKEN")

    # --- StackExchange ---
    stackexchange_key: Optional[str] = EnvVar.external("STACKEXCHANGE_KEY")

    # --- Reddit ---
    reddit_client_id: Optional[str] = EnvVar.external("REDDIT_CLIENT_ID")
    reddit_client_secret: Optional[str] = EnvVar.external(
        "REDDIT_CLIENT_SECRET")
    reddit_user_agent: str = EnvVar.get(
        "REDDIT_USER_AGENT") or "BurningDemand/0.1"

    # --- Anthropic ---
    anthropic_api_key: str = EnvVar("ANTHROPIC_API_KEY")

    # --- PocketBase ---
    pocketbase_url: str = EnvVar("POCKETBASE_URL")
    pocketbase_admin_email: str = EnvVar("POCKETBASE_ADMIN_EMAIL")
    pocketbase_admin_password: str = EnvVar("POCKETBASE_ADMIN_PASSWORD")

    # --- Collection Configuration ---
    # Common settings
    body_max_length: int = 500  # Max length for body text
    max_pages: int = 10  # Max number of pages to fetch per source

    # GitHub settings
    github_per_page: int = 100  # Items per page (max 100)
    github_max_pages: int = 10  # Max pages to fetch

    # StackOverflow settings
    stackoverflow_pagesize: int = 100  # Items per page (max 100)
    stackoverflow_max_pages: int = 10  # Max pages to fetch

    # Reddit settings
    reddit_limit: int = 100  # Items per request
    reddit_subreddits: List[str] = ["entrepreneur", "SaaS", "startups"]  # Subreddits to monitor

    # HackerNews settings
    hackernews_hits_per_page: int = 100  # Items per page
    hackernews_max_pages: int = 10  # Max pages to fetch
