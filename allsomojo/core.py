from datetime import date, timedelta
from multiprocessing import Process, Queue
from pathlib import Path

import sqlalchemy as sa
from task_flows import task

from .common import config, logger, task_alerts
from .db import check_tables_exist, engine, pg_url_str, repos_table
from .files import count_file_lines, find_code_files, local_repo_paths
from .ghub import save_github_repos_metadata
from .gitops import (
    clone_new_repos,
    get_repo_changes,
    git_pull_local_repos,
    total_commits,
)
from .sheet import save_comment_repos


@task(name="allsomojo-update-db", required=True, alerts=task_alerts)
def update_db(
    start_search_at_last_crawl: bool = True,
    include_blacklisted: bool = False,
):
    """Update database with new repos / repo changes."""
    check_tables_exist()
    save_comment_repos()
    save_github_repos_metadata(start_search_at_last_crawl, include_blacklisted)
    git_pull_local_repos(include_blacklisted)
    clone_new_repos(include_blacklisted)
    parse_local_repos(include_blacklisted)
    blacklist_repos()


def parse_local_repos(include_blacklisted: bool):
    """Extract statistics and metadata from local repos."""

    def worker(q):
        eng = sa.create_engine(pg_url_str())
        commit_stats_since = date.today() - timedelta(days=30)
        while n_remaining := q.qsize():
            if n_remaining % 100 == 0:
                print(f"{n_remaining} repos remaining")
            repo_dir = q.get()
            commit_stats = get_repo_changes(repo=repo_dir, since=commit_stats_since)
            code_files = find_code_files(repo_dir)
            n_code_lines = 0
            for f in code_files:
                if n_lines := count_file_lines(f):
                    n_code_lines += n_lines
            # get the number of code files and total number of lines in all code files.
            n_commits = total_commits(repo_dir)
            statement = (
                sa.update(repos_table)
                .where(repos_table.c.local_path == str(repo_dir))
                .values(
                    commits=n_commits,
                    n_code_files=len(code_files),
                    n_code_lines=n_code_lines,
                    code_file_suffixes={f.suffix for f in code_files},
                    lines_added_30d=commit_stats.lines_added,
                    lines_deleted_30d=commit_stats.lines_deleted,
                    files_changed_30d=len(commit_stats.files_changed),
                )
            )
            with eng.begin() as conn:
                conn.execute(statement)
        eng.dispose()

    q = Queue()
    for d in local_repo_paths(include_blacklisted):
        q.put(Path(d))
    logger.info(
        "Saving repo metadata for %i local repos. Using %i processes",
        q.qsize(),
        config.max_process,
    )
    procs = [Process(target=worker, args=(q,)) for _ in range(config.max_process)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()


def blacklist_repos():
    """Find repos that should be blacklisted or flagged for manual review."""
    # automatically blacklist repos that do not contain any targeted code file extensions.
    with engine.begin() as conn:
        res = conn.execute(
            sa.update(repos_table)
            .where(
                sa.or_(
                    repos_table.c.code_file_suffixes == [],
                    repos_table.c.code_file_suffixes.is_(None),
                ),
                repos_table.c.blacklisted_reason.is_(None),
                repos_table.c.local_path.isnot(None),
                repos_table.c.manually_checked == False,
            )
            .values(blacklisted_reason="no code files")
        )
        logger.info("Blacklisted %i repos containing no code files.", res.rowcount)

    has_mojo_file = sa.or_(
        sa.text("'.🔥' = ANY(allsomojo.repos.code_file_suffixes)"),
        sa.text("'.mojo' = ANY(allsomojo.repos.code_file_suffixes)"),
    )
    # whitelist any repos that now have mojo file extensions and where not manually blacklisted.
    with engine.begin() as conn:
        res = conn.execute(
            sa.update(repos_table)
            .values(blacklisted_reason=None)
            .where(
                has_mojo_file,
                repos_table.c.blacklisted_reason.isnot(None),
                repos_table.c.manually_checked == False,
            )
        )
        logger.info(
            "Whitelisted %i previously blacklisted repos that now have Mojo files.",
            res.rowcount,
        )

    # manually review repos that only contain Python files and/or notebooks.
    with engine.begin() as conn:
        res = conn.execute(
            sa.update(repos_table)
            .values(blacklisted_reason="needs review")
            .where(
                sa.not_(has_mojo_file),
                repos_table.c.manually_checked == False,
                repos_table.c.blacklisted_reason.is_(None),
            )
        )
        logger.info("Flagged %i repos for manual review.", res.rowcount)
