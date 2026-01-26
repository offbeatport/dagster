"""Query configuration for collectors: keywords, tags, subreddits, and body size limits."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class SourceQueryConfig:
    """Query configuration for a single data source."""

    extra_keywords: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    subreddits: List[str] = field(default_factory=list)
    max_queries: Optional[int] = None  # Limit number of queries to execute


# Core keywords shared across all sources
CORE_KEYWORDS = [
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

# Source-specific query configuration
SOURCE_QUERIES: Dict[str, SourceQueryConfig] = {
    "github": SourceQueryConfig(
        extra_keywords=[
            "feature request",
            "enhancement",
            "performance",
            "scalability",
            "timeout",
            "memory leak",
            "integration",
        ],
    ),
    "stackoverflow": SourceQueryConfig(
        tags=[
            "python",
            "javascript",
            "typescript",
            "java",
            "go",
            "docker",
            "kubernetes",
            "aws",
            "postgresql",
        ],
        extra_keywords=["best practice", "production", "scaling", "monitoring"],
    ),
    "reddit": SourceQueryConfig(
        extra_keywords=[
            "entrepreneur",
            "SaaS",
            "startups",
        ],
        subreddits=[
            "entrepreneur",
            "SaaS",
            "startups",
        ],
    ),
    "hackernews": SourceQueryConfig(
        extra_keywords=[
            "startup",
            "saas",
            "tool",
            "platform",
            "automation",
            "analytics",
        ],
    ),
}


def get_query_keywords(source: str) -> List[str]:
    """Get all keywords for a source (core + source-specific)."""
    return (
        CORE_KEYWORDS + SOURCE_QUERIES.get(source, SourceQueryConfig()).extra_keywords
    )


def get_query_tags(source: str) -> List[str]:
    """Get tags for a source."""
    return SOURCE_QUERIES.get(source, SourceQueryConfig()).tags


def get_query_subreddits() -> List[str]:
    """Get Reddit subreddits to monitor."""
    return SOURCE_QUERIES.get("reddit", SourceQueryConfig()).subreddits


def matches_query_keywords(text: str, source: str) -> bool:
    """Check if text matches any keywords for the given source."""
    if not text:
        return False
    text_lower = text.lower()
    keywords = get_query_keywords(source)
    return any(keyword.lower() in text_lower for keyword in keywords)


def get_body_max_length() -> int:
    """Get maximum body length for collected items."""
    return 10 * 1000  # 10KB


def get_max_queries(source: str) -> Optional[int]:
    """Get maximum number of queries to execute for a source."""
    return SOURCE_QUERIES.get(source, SourceQueryConfig()).max_queries
