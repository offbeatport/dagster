from dagster import Definitions

from burningdemand.defs.collectors.github import github_issues
from burningdemand.defs.collectors.hackernews import hackernews_stories
from burningdemand.defs.collectors.reddit import reddit_posts
from burningdemand.defs.collectors.stackoverflow import stackoverflow_questions
from burningdemand.resources import PocketBaseResource


defs = Definitions(
    assets=[github_issues, hackernews_stories, reddit_posts, stackoverflow_questions],
    resources={
        "pb": PocketBaseResource.from_env(),
    },
)
