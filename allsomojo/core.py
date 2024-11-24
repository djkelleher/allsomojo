import os
from datetime import date, timedelta
from multiprocessing import Process, Queue
from pathlib import Path

import sqlalchemy as sa
from taskflows import task

from .common import config, logger, task_alerts
from .db import check_tables_exist, engine, repos_table
from .files import count_file_lines, find_code_files, local_repo_paths
from .ghub import save_github_repos_metadata
from .gitops import (
    clone_new_repos,
    get_repo_changes,
    git_pull_local_repos,
    total_commits,
)
from .sheet import get_sheet_data, save_comment_repos


@task(name="allsomojo-update-db", required=True, alerts=task_alerts)
def update_db(
    start_search_at_last_crawl: bool = True,
    include_blacklisted: bool = False,
):
    """Update database with new repos / repo changes."""
    check_tables_exist()
    save_comment_repos()
    save_github_repos_metadata(start_search_at_last_crawl, include_blacklisted)
    # git_pull_local_repos(include_blacklisted)
    # clone_new_repos(include_blacklisted)
    # parse_local_repos(include_blacklisted)
    # blacklist_repos()
    # update_symlinks()


def parse_local_repos(include_blacklisted: bool):
    """Extract statistics and metadata from local repos."""

    def worker(q):
        eng = sa.create_engine(config.db_url)
        commit_stats_since = date.today() - timedelta(days=30)
        while n_remaining := q.qsize():
            if n_remaining % 100 == 0:
                print(f"{n_remaining} repos remaining")
            repo_dir = q.get()
            commit_stats = get_repo_changes(repo=repo_dir, since=commit_stats_since)
            mojo_files = find_code_files(repo_dir, (".mojo", ".🔥"))
            python_files = find_code_files(
                repo_dir, (".py", ".pxd", ".pyx", ".pyi", ".pyd")
            )
            notebook_files = find_code_files(repo_dir, (".ipynb",))
            n_code_lines = 0
            for f in mojo_files + python_files + notebook_files:
                if n_lines := count_file_lines(f):
                    n_code_lines += n_lines
            # get the number of code files and total number of lines in all code files.
            n_commits = total_commits(repo_dir)
            statement = (
                sa.update(repos_table)
                .where(repos_table.c.local_path == str(repo_dir))
                .values(
                    commits=n_commits,
                    n_mojo_files=len(mojo_files),
                    n_python_files=len(python_files),
                    n_notebook_files=len(notebook_files),
                    n_code_lines=n_code_lines,
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
                repos_table.c.n_python_files == 0,
                repos_table.c.n_mojo_files == 0,
                repos_table.c.n_notebook_files == 0,
                repos_table.c.blacklisted_reason.is_(None),
                repos_table.c.local_path.isnot(None),
                repos_table.c.manually_checked == False,
            )
            .values(blacklisted_reason="no code files")
        )
        logger.info("Blacklisted %i repos containing no code files.", res.rowcount)

    # whitelist any repos that now have mojo file extensions and where not manually blacklisted.
    with engine.begin() as conn:
        res = conn.execute(
            sa.update(repos_table)
            .values(blacklisted_reason=None)
            .where(
                repos_table.c.n_mojo_files > 0,
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
                repos_table.c.n_mojo_files == 0,
                repos_table.c.manually_checked == False,
                repos_table.c.blacklisted_reason.is_(None),
            )
        )
        logger.info("Flagged %i repos for manual review.", res.rowcount)


def update_symlinks():
    """Symlink confirmed Mojo repos to another directory for convenient access."""
    if not config.selected_repos_dir:
        return
    df = get_sheet_data()
    config.selected_repos_dir.mkdir(parents=True, exist_ok=True)
    for _, row in df.iterrows():
        src_path = config.repos_base_dir.joinpath(row["username"], row["repo_name"])
        dst_path = config.selected_repos_dir.joinpath(
            f"{row['username']}_{row['repo_name']}"
        )
        if not dst_path.is_symlink():
            logger.info("Creating symlink: %s -> %s", src_path, dst_path)
            os.symlink(src_path, dst_path)
