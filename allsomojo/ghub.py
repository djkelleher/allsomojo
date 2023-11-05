from datetime import date, datetime, timedelta, timezone
from itertools import count
from time import sleep, time
from typing import Any, Dict, Generator, Optional

import sqlalchemy as sa
from github import (
    Auth,
    Github,
    GithubException,
    RateLimitExceededException,
    UnknownObjectException,
)
from github.Repository import Repository
from tqdm import tqdm

from .common import config, logger
from .db import engine, repos_table, save_repo_metadata, save_repo_query


def save_github_repos_metadata(
    start_search_at_last_crawl: bool = True,
    include_blacklisted: bool = False,
):
    """Save metadata for new repos and update existing."""
    client = Github(auth=Auth.Token(config.gh_token))
    start_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    search_for_repos(client, start_from_last_crawl=start_search_at_last_crawl)
    # update all repo entries that did not just get updated from search.
    update_saved_repos(client, include_blacklisted, last_crawled_before=start_time)


def search_for_repos(client: Github, start_from_last_crawl: bool = True) -> int:
    """Save metadata for all repos that have been created since last run."""
    search_start = None
    if start_from_last_crawl:
        with engine.begin() as conn:
            search_start = conn.execute(
                sa.select(sa.func.max(repos_table.c.updated_at))
            ).scalar()
    if search_start is None:
        # start at Mojo release date.
        search_start = date(2023, 5, 1)
    else:
        if isinstance(search_start, datetime):
            search_start = search_start.date()
        search_start -= timedelta(days=1)
    logger.info("Searching for repos. Starting from %s.", search_start)
    counter = count(1)
    for is_fork in (True, None, False):
        query = "mojo in:name,description,readme,topics"
        if is_fork is not None:
            query += " fork:" + str(is_fork).lower()
        for repo in rate_limited_repo_search(client, query, search_start, date.today()):
            logger.info(
                "[%i] %s created at %s",
                next(counter),
                repo.full_name,
                repo.created_at,
            )
            repo_metadata = extract_repo_metadata(repo)
            save_repo_query(repo_metadata["full_name"], query)
            save_repo_metadata(repo_metadata)
    logger.info("Finished searching for new repos.")


def update_saved_repos(
    client: Github,
    include_blacklisted: bool,
    last_crawled_before: Optional[datetime] = None,
):
    """Update repo metadata for all repos that are currently in the database."""
    query = sa.select(repos_table.c.full_name)
    if not include_blacklisted:
        query = query.where(repos_table.c.blacklisted_reason.is_(None))
    if last_crawled_before is not None:
        query = query.where(repos_table.c.last_crawled_at < last_crawled_before)
    with engine.begin() as conn:
        repo_names = list(conn.execute(query).scalars())
    logger.info(
        "Updating metadata for %i previously saved GitHub repos.",
        len(repo_names),
    )
    if not repo_names:
        return

    def get_repo(repo_name: str):
        try:
            return client.get_repo(repo_name)
        except UnknownObjectException as exp:
            logger.warning("Could not find %s: %s", repo_name, exp)
            with engine.begin() as conn:
                conn.execute(
                    sa.update(repos_table)
                    .where(repos_table.c.full_name == repo_name)
                    .values(blacklisted_reason="not found")
                )
        except RateLimitExceededException as exp:
            logger.exception(
                "Rate limit exceeded requesting repo %s. Sleeping for 30 minutes. Exception: %s",
                repo_name,
                exp,
            )
            sleep(60 * 30)
            return get_repo(repo_name)

    for repo_name in tqdm(repo_names):
        if repo := get_repo(repo_name):
            repo_metadata = extract_repo_metadata(repo)
            save_repo_metadata(repo_metadata)
            logger.info("Updated metadata for %s.", repo_name)


def rate_limited_repo_search(
    client: Github, query: str, search_start: date, search_end: date
) -> Generator[Repository, None, None]:
    """Search for repositories while obeying rate limits."""
    # search in 20 day increments, since search is limited to 1000 results.
    dt = timedelta(days=20)
    while search_start < search_end:
        start = search_start.strftime("%Y-%m-%d")
        end = min(search_end, search_start + dt).strftime("%Y-%m-%d")
        q = f"{query} created:{start}..{end}"
        logger.info("Starting search: %s.", q)
        try:
            for repo in client.search_repositories(
                query=q, sort="updated", order="asc"
            ):
                yield repo
                req_rem, req_lim = client.rate_limiting
                if req_rem == 0:
                    sleep_t = client.rate_limiting_resettime - time() + 1
                    if sleep_t > 0:
                        logger.warning(
                            "Reached max requests (%i) for time period. Sleeping for %i s.",
                            req_lim,
                            sleep_t,
                        )
                        sleep(sleep_t)
            search_start += dt
        except RateLimitExceededException as exp:
            logger.exception(
                "Rate limit exceeded. Sleeping for 30 minutes. Exception: %s",
                exp,
            )
            sleep(60 * 30)
        except GithubException as exp:
            if (
                '"message": "You have exceeded a secondary rate limit. Please wait a few minutes before you try again."'
                in str(exp)
            ):
                logger.exception(
                    "Rate limit exceeded. Sleeping for 30 min. Exception: %s", exp
                )
                sleep(60 * 30)
            else:
                raise


def extract_repo_metadata(repo: Repository) -> Dict[str, Any]:
    """Extract repo metadata needed for the database."""
    assert repo.watchers == repo.watchers_count
    assert repo.open_issues == repo.open_issues_count
    assert repo.forks == repo.forks_count
    data = {
        "repo_name": repo.name,
        "username": repo.owner.login,
        "user_type": repo.owner.type,
        "user_avatar_url": repo.owner.avatar_url,
    }
    for field in ("created_at", "updated_at", "pushed_at"):
        val = getattr(repo, field)
        data[field] = val.replace(tzinfo=timezone.utc) if val else None
    raw_data = repo.raw_data
    if lic := raw_data["license"]:
        data["license"] = lic["name"]
    for c in repos_table.columns:
        name = c.name
        if name not in data and name in raw_data:
            data[name] = raw_data[name]
    return data
