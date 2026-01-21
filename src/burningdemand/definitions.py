from dagster import Definitions, job

from burningdemand.collectors.github import collect_github
from burningdemand.collectors.hackernews import collect_hackernews
from burningdemand.collectors.reddit import collect_reddit
from burningdemand.collectors.stackoverflow import collect_stackoverflow


# ----------------------------
# Jobs (one per collector)
# ----------------------------

@job
def hn_collect_job():
    collect_hackernews()


@job
def github_collect_job():
    collect_github()


@job
def so_collect_job():
    collect_stackoverflow()


@job
def reddit_collect_job():
    collect_reddit()


defs = Definitions(
    jobs=[hn_collect_job, github_collect_job,
          so_collect_job, reddit_collect_job],
)
