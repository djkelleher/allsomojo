import re
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from time import sleep
from typing import Generator, Sequence, Set, Tuple

import sh
import sqlalchemy as sa
from sh import ErrorReturnCode, TimeoutException, git, wc

from .common import config, logger
from .db import engine, repos_table
from .files import local_repo_paths


def total_commits(repo: Path) -> int:
    """Get total number of commits to all branches of repo."""
    n_commits = wc(
        "-l",
        _in=git("--no-pager", "log", "--oneline", "--all", "--no-color", _cwd=repo),
    )
    return int(n_commits.strip())


def git_pull_local_repos(include_blacklisted: bool):
    """Run `git pull` on all local repos."""
    pull_cmds = [
        git.pull.bake(
            "--quiet",
            _cwd=d,
            _timeout=400,
            _tty_out=False,
            _tty_in=False,
        )
        for d in local_repo_paths(
            include_blacklisted,
            where_conditions=sa.or_(
                repos_table.c.last_pulled_at.is_(None),
                repos_table.c.last_pulled_at < repos_table.c.pushed_at,
            ),
        )
    ]
    if not pull_cmds:
        logger.info("No local repos to pull updates for.")
        return
    logger.info("Running `git pull` on %i local repos.", len(pull_cmds))
    for command, _ in run_git_commands(pull_cmds, is_local=False):
        cwd = command._partial_call_args["cwd"]
        with engine.begin() as conn:
            conn.execute(
                sa.update(repos_table)
                .where(repos_table.c.local_path == cwd)
                .values(last_pulled_at=datetime.utcnow().replace(tzinfo=timezone.utc))
            )


def clone_new_repos(include_blacklisted: bool, retries: int = 2) -> None:
    """Clone all repos that we don't currently have locally."""

    def _update_local_repo_paths(full_name: str, local_path: Path):
        assert local_path.is_dir(), local_path
        # make sure this is actually a git repo.
        assert local_path.joinpath(".git").is_dir(), local_path
        logger.info("Recording local path for %s: %s", full_name, local_path)
        statement = (
            sa.update(repos_table)
            .where(repos_table.c.full_name == full_name)
            .values(local_path=str(local_path.resolve()))
        )
        with engine.begin() as conn:
            conn.execute(statement)

    query = sa.select(
        repos_table.c.full_name,
        repos_table.c.username,
        repos_table.c.clone_url,
    ).where(repos_table.c.local_path.is_(None))
    if not include_blacklisted:
        query = query.where(repos_table.c.blacklisted_reason.is_(None))
    with engine.begin() as conn:
        repos_metadata = list(conn.execute(query).fetchall())
    if not repos_metadata:
        logger.info("No new repos to clone.")
        return
    clone_cmds = []
    for full_name, username, clone_url in repos_metadata:
        local_path = config.repos_base_dir.joinpath(full_name)
        if local_path.is_dir():
            logger.info(
                "%s is already cloned locally but local_path was not in database. Adding local_path now.",
                full_name,
            )
            _update_local_repo_paths(full_name, local_path)
            continue
        user_dir = config.repos_base_dir / username
        user_dir.mkdir(exist_ok=True, parents=True)
        clone_cmds.append(
            git.clone.bake(
                clone_url,
                user_dir / Path(clone_url).stem,
                _timeout=600,
                _out=sys.stdout,
                _err=sys.stderr,
            )
        )

    logger.info("Cloning %i new repos.", len(clone_cmds))

    def _try_clone_repos(clone_cmds, retries):
        failed_cmds = []
        for cmd, _ in run_git_commands(clone_cmds, is_local=False):
            clone_dir = Path(cmd._partial_baked_args[-1])
            full_name = f"{clone_dir.parent.name}/{clone_dir.name}"
            if not clone_dir.is_dir():
                failed_cmds.append(cmd)
                logger.error(
                    "Local path for %s does not exist: %s", full_name, clone_dir
                )
                continue
            _update_local_repo_paths(full_name, clone_dir)
        if failed_cmds:
            retries -= 1
            if retries >= 0:
                logger.warning(
                    "Retrying %i failed git commands. Retries remaining: %i",
                    len(failed_cmds),
                    retries,
                )
                _try_clone_repos(failed_cmds, retries)

    _try_clone_repos(clone_cmds, retries)


@dataclass
class RepoChanges:
    lines_added: int = 0
    lines_deleted: int = 0
    files_changed: Set[str] = field(default_factory=set)


def get_repo_changes(repo: Path, since: date) -> RepoChanges:
    """Get number of changes to repo since provided date."""
    repo_changes = RepoChanges()
    try:
        commit_log = git(
            "--no-pager",
            "log",
            "--stat",
            "--all",
            "--no-color",
            "--after=" + since.strftime("%Y-%m-%d"),
            _cwd=repo,
        )
    except ErrorReturnCode as exp:
        logger.error(
            "%s error executing git command: %s",
            type(exp),
            exp,
        )
        return repo_changes
    if not (matches := list(re.finditer(r"commit [a-z0-9]{40}", commit_log))):
        return repo_changes
    commits = []
    for idx_next, match in enumerate(matches[:-1], start=1):
        commits.append(commit_log[match.start() : matches[idx_next].start()])
    commits.append(commit_log[matches[-1].start() :])
    commits = [s.strip() for s in commits]
    for commit in commits:
        lines = [l.strip() for l in commit.splitlines()]
        if lines and (
            match := re.search(
                r"(\d+) files? changed(?:, (\d+) insertions?\(\+\))?(?:, (\d+) deletions?\(-\))?",
                lines.pop(),
            )
        ):
            repo_changes.lines_added = int(match.group(2) or 0)
            repo_changes.lines_deleted = int(match.group(3) or 0)
        for line in reversed(lines):
            if line == "":
                break
            if "|" in line:
                repo_changes.files_changed.add(line.split("|")[0].strip())
    return repo_changes


def run_git_commands(
    commands: sh.Command | Sequence[sh.Command], is_local: bool
) -> Generator[Tuple[sh.Command, str], None, None]:
    """Run all provided git commands."""
    if not commands:
        return
    if isinstance(commands, sh.Command):
        commands = [commands]

    logger.info("Running %i git commands.", len(commands))

    def _run_command(command: sh.Command):
        cmd_repr = f"{command} {command._partial_call_args.get('cwd', '')}"
        logger.info("Running git command %s", cmd_repr)
        try:
            return command()
        except (TimeoutException, ErrorReturnCode) as exp:
            logger.error(
                "%s error executing git command %s: %s",
                type(exp),
                cmd_repr,
                exp,
            )

    n_commands = len(commands)
    future_to_command = {}
    if is_local:
        n_workers = min(n_commands, config.max_process)
        logger.info(
            "Running %i local git commands with %i process workers.",
            n_commands,
            n_workers,
        )
        # cpu bound, use process
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            for cmd in commands:
                future_to_command[pool.submit(_run_command, cmd)] = cmd
    else:
        n_workers = min(n_commands, config.max_git_io)
        logger.info(
            "Running %i git commands with %i thread workers.", n_commands, n_workers
        )
        # io bound, use threads
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            for cmd in commands:
                future_to_command[pool.submit(_run_command, cmd)] = cmd
                # don't blow up the server
                sleep(0.25)
    for future in as_completed(future_to_command):
        command = future_to_command[future]
        yield command, future.result()
