from dagster import ConfigurableResource, EnvVar


class AnthropicResource(ConfigurableResource):
    """Resource for Anthropic API access."""

    api_key: str = EnvVar("ANTHROPIC_API_KEY")
