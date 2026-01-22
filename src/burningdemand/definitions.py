from dagster import Definitions

from .assets.github import github_issues
from .assets.hackernews import hackernews_stories
from .assets.reddit import reddit_posts
from .assets.stackoverflow import stackoverflow_questions
from .resources.pocketbase import PocketBaseResource


defs = Definitions(
    assets=[github_issues, hackernews_stories, reddit_posts, stackoverflow_questions],
    resources={
        "pb": PocketBaseResource.from_env(),
    },
)
