"""Configuration for collectors query keywords, tags, and subreddits."""

# Core keywords shared across all sources
CORE_KEYWORDS = [
    # ---- Strong buying / switching intent ----
    "looking for",
    "any recommendations",
    "recommend a tool",
    "what tool do you use",
    "alternatives to",
    "alternative to",
    "replacement for",
    "switching from",
    "migrating from",
    "best tool for",
    "best software for",
    "is there a tool",
    "is there software",
    "saas for",
    "platform for",
    # ---- Pricing / willingness to pay ----
    "pricing",
    "cost",
    "expensive",
    "cheaper",
    "worth paying",
    "subscription",
    "license",
    "enterprise plan",
    "free tier",
    "trial",
    "paid version",
    # ---- Pain / friction ----
    "too slow",
    "slow",
    "frustrating",
    "annoying",
    "broken",
    "doesn't work",
    "hard to use",
    "hard to manage",
    "manual",
    "time-consuming",
    "tedious",
    "error prone",
    "unreliable",
    "scaling issues",
    "performance issues",
    # ---- Workflow / ops context ----
    "workflow",
    "automation",
    "monitoring",
    "reporting",
    "tracking",
    "dashboard",
    "alerting",
    "compliance",
    "audit",
    "integration",
    "sync",
    "migration",
    "data pipeline",
    # ---- Business / team context ----
    "at work",
    "at my company",
    "in production",
    "in our team",
    "for clients",
    "for customers",
    "enterprise",
    "internal tool",
    "ops",
    "devops",
]

# Source-specific configuration
GITHUB_EXTRA_KEYWORDS = [
    "feature request",
    "enhancement",
    "performance",
    "scalability",
    "timeout",
    "memory leak",
    "integration",
]

STACKOVERFLOW_TAGS = [
    "python",
    "javascript",
    "typescript",
    "java",
    "go",
    "docker",
    "kubernetes",
    "aws",
    "postgresql",
]

STACKOVERFLOW_EXTRA_KEYWORDS = [
    "best practice",
    "production",
    "scaling",
    "monitoring",
]

REDDIT_EXTRA_KEYWORDS = [
    "entrepreneur",
    "SaaS",
    "startups",
]

REDDIT_SUBREDDITS = [
    "entrepreneur",
    "SaaS",
    "startups",
]

HACKERNEWS_EXTRA_KEYWORDS = [
    "startup",
    "saas",
    "tool",
    "platform",
    "automation",
    "analytics",
]


def get_keywords(source: str) -> list[str]:
    """Get all keywords for a source (core + source-specific)."""
    source_keywords = {
        "github": GITHUB_EXTRA_KEYWORDS,
        "stackoverflow": STACKOVERFLOW_EXTRA_KEYWORDS,
        "reddit": REDDIT_EXTRA_KEYWORDS,
        "hackernews": HACKERNEWS_EXTRA_KEYWORDS,
    }.get(source, [])
    return CORE_KEYWORDS + source_keywords


def get_tags(source: str) -> list[str]:
    """Get tags for a source."""
    tags = {
        "stackoverflow": STACKOVERFLOW_TAGS,
    }.get(source, [])
    return tags


def get_subreddits() -> list[str]:
    """Get Reddit subreddits to monitor."""
    return REDDIT_SUBREDDITS


def matches_keywords(text: str, source: str) -> bool:
    """Check if text matches any keywords for the given source."""
    if not text:
        return False
    text_lower = text.lower()
    keywords = get_keywords(source)
    return any(keyword.lower() in text_lower for keyword in keywords)
