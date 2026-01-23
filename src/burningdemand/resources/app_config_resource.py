from typing import Optional

from dagster import ConfigurableResource, EnvVar


class AppConfigResource(ConfigurableResource):
    """
    Reads secrets/config from environment variables.
    Missing required vars will cause an immediate validation error on startup.
    """

    # --- GitHub ---
    github_token: str = EnvVar("GITHUB_TOKEN")

    # --- StackExchange ---
    stackexchange_key: Optional[str] = EnvVar.get_value("STACKEXCHANGE_KEY")

    # --- Reddit ---
    reddit_client_id: Optional[str] = EnvVar.get_value("REDDIT_CLIENT_ID")
    reddit_client_secret: Optional[str] = EnvVar.get_value("REDDIT_CLIENT_SECRET")
    reddit_user_agent: str = (
        EnvVar.get_value("REDDIT_USER_AGENT") or "BurningDemand/0.1"
    )

    # --- Anthropic ---
    anthropic_api_key: str = EnvVar("ANTHROPIC_API_KEY")

    # --- PocketBase ---
    pocketbase_url: str = EnvVar("POCKETBASE_URL")
    pocketbase_admin_email: str = EnvVar("POCKETBASE_ADMIN_EMAIL")
    pocketbase_admin_password: str = EnvVar("POCKETBASE_ADMIN_PASSWORD")
