from pathlib import Path
from typing import Dict, List, Optional

import yaml

# Module-level cache for keywords
_keywords_cache: Optional[Dict] = None
_keywords_file_path: Optional[Path] = None


def _get_keywords_file_path(keywords_file: Optional[str] = None) -> Path:
    """Get the path to the keywords YAML file."""
    if keywords_file:
        return Path(keywords_file)

    # Default to keywords.yaml in the package directory
    package_dir = Path(__file__).parent.parent
    return package_dir / "keywords.yaml"


def _load_keywords(keywords_file: Optional[str] = None) -> Dict:
    """Load keywords from YAML file with caching."""
    global _keywords_cache, _keywords_file_path

    file_path = _get_keywords_file_path(keywords_file)

    # Return cached if same file
    if _keywords_cache is not None and _keywords_file_path == file_path:
        return _keywords_cache

    if not file_path.exists():
        raise FileNotFoundError(f"Keywords file not found: {file_path}")

    with open(file_path, "r") as f:
        _keywords_cache = yaml.safe_load(f)

    _keywords_file_path = file_path
    return _keywords_cache


def get_core_keywords(keywords_file: Optional[str] = None) -> List[str]:
    """Get all core keywords."""
    keywords = _load_keywords(keywords_file)
    return keywords.get("core", [])


def get_source_keywords(source: str, keywords_file: Optional[str] = None) -> List[str]:
    """Get keywords for a specific source (combines core + source-specific)."""
    keywords = _load_keywords(keywords_file)
    core = keywords.get("core", [])
    source_config = keywords.get("sources", {}).get(source, {})
    extra = source_config.get("extra", [])
    return core + extra


def get_source_tags(source: str, keywords_file: Optional[str] = None) -> List[str]:
    """Get tags for a specific source (e.g., StackOverflow tags)."""
    keywords = _load_keywords(keywords_file)
    source_config = keywords.get("sources", {}).get(source, {})
    return source_config.get("tags", [])


def build_github_query(date: str, keywords_file: Optional[str] = None) -> str:
    """Build GitHub search query using keywords."""
    source_keywords = get_source_keywords("github", keywords_file)
    # Build OR query from keywords
    keyword_query = " OR ".join(
        [f'"{kw}"' for kw in source_keywords[:10]]
    )  # Limit to avoid query too long
    return f"is:issue created:{date} ({keyword_query})"


def build_stackoverflow_tags(keywords_file: Optional[str] = None) -> List[str]:
    """Get StackOverflow tags to filter by."""
    return get_source_tags("stackoverflow", keywords_file)


def matches_keywords(
    text: str, source: Optional[str] = None, keywords_file: Optional[str] = None
) -> bool:
    """Check if text matches any keywords for the given source."""
    if not text:
        return False

    text_lower = text.lower()
    keywords = (
        get_source_keywords(source, keywords_file)
        if source
        else get_core_keywords(keywords_file)
    )

    return any(keyword.lower() in text_lower for keyword in keywords)
