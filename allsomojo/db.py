from datetime import datetime, timezone
from typing import Any, Dict, Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import insert

from .common import config, logger

engine = sa.create_engine(config.db_url)


sa_meta = sa.MetaData()


def check_tables_exist():
    """Check if tables exist."""
    with engine.begin() as conn:
        for table in (repo_queries_table, repos_table):
            logger.info("Checking table %s exists.", table.name)
            table.create(conn, checkfirst=True)


# table for repository metadata.
repos_table = sa.Table(
    "repos",
    sa_meta,
    sa.Column("full_name", sa.Text, primary_key=True, comment="{repo_name}/{username}"),
    sa.Column("repo_name", sa.Text),
    sa.Column("username", sa.Text),
    sa.Column("fork", sa.Boolean, comment="Is this repo a fork?"),
    sa.Column("user_type", sa.Text, comment="User or Organization"),
    sa.Column("user_avatar_url", sa.Text),
    sa.Column("description", sa.Text),
    sa.Column(
        "forks", sa.Integer, comment="Number of times this repo has been forked."
    ),
    sa.Column("created_at", sa.DateTime(timezone=True)),
    # last time a GitHub-specific attribute was updated. may be greater than or less than pushed_at.
    sa.Column("updated_at", sa.DateTime(timezone=True)),
    # pushed_at will be updated any time a commit is pushed to any of the repository’s branches.
    sa.Column("pushed_at", sa.DateTime(timezone=True)),
    sa.Column("clone_url", sa.Text),
    sa.Column("homepage", sa.Text),
    sa.Column("size", sa.Integer),
    sa.Column("stargazers_count", sa.Integer),
    sa.Column("watchers", sa.Integer),
    sa.Column("language", sa.Text),
    sa.Column(
        "commits",
        sa.Integer,
        comment="Number of commits for all time for all branches.",
    ),
    sa.Column("open_issues", sa.Integer),
    sa.Column("license", sa.Text),
    sa.Column("topics", sa.Text),
    sa.Column(
        "n_mojo_files",
        sa.Integer,
        comment="Number of Mojo files.",
    ),
    sa.Column(
        "n_python_files",
        sa.Integer,
        comment="Number of Python files.",
    ),
    sa.Column(
        "n_notebook_files",
        sa.Integer,
        comment="Number Jupyter notebook files.",
    ),
    sa.Column(
        "n_code_lines",
        sa.Integer,
        comment="Total number of lines of code from all code files (Mojo, Python, Jupyter).",
    ),
    sa.Column(
        "lines_added_30d",
        sa.Integer,
        comment="Number of lines added in last 30 days.",
    ),
    sa.Column(
        "lines_deleted_30d",
        sa.Integer,
        comment="Number of lines deleted in last 30 days.",
    ),
    sa.Column(
        "files_changed_30d",
        sa.Integer,
        comment="Number files changed in last 30 days.",
    ),
    sa.Column("local_path", sa.Text, comment="Path to local clone of repo."),
    sa.Column(
        "first_crawled_at",
        sa.DateTime(timezone=True),
        default=lambda: datetime.utcnow().replace(tzinfo=timezone.utc),
        nullable=False,
        comment="Time of first crawl.",
    ),
    sa.Column(
        "last_crawled_at",
        sa.DateTime(timezone=True),
        default=lambda: datetime.utcnow().replace(tzinfo=timezone.utc),
        nullable=False,
        comment="Time of last crawl.",
    ),
    sa.Column(
        "last_pulled_at",
        sa.DateTime(timezone=True),
        comment="Last time `git pull` was ran on local repo.",
    ),
    sa.Column(
        "n_crawls",
        sa.Integer,
        default=1,
        nullable=False,
        comment="Number of times crawled.",
    ),
    sa.Column(
        "blacklisted_reason",
        sa.Text,
        comment="Reason for blacklisting repo: 'needs review', 'not mojo code', 'no code files', 'not found'",
    ),
    sa.Column(
        "manually_checked",
        sa.Boolean,
        nullable=False,
        default=False,
    ),
)

# multiple queries can find the same repo, so this needs to be a separate table.
repo_queries_table = sa.Table(
    "repo_queries",
    sa_meta,
    sa.Column("full_name", sa.Text, primary_key=True, comment="{repo_name}/{username}"),
    sa.Column(
        "query",
        sa.Text,
        primary_key=True,
        comment="Searched text or query.",
        nullable=False,
    ),
)


def save_repo_metadata(data: Dict[str, Any]):
    """Insert or update a row for the give {username}/{repo} (aka full_name)."""
    statement = insert(repos_table).values(data)
    on_conf_set = {c.name: c for c in statement.excluded}
    on_conf_set = {n: c for n, c in on_conf_set.items() if n in data}
    # If no conflict, default values will be used for n_crawls, last_crawled_at
    on_conf_set["n_crawls"] = sa.text(
        f'{repos_table.metadata.schema}."{repos_table.name}".n_crawls + 1'
    )
    on_conf_set["last_crawled_at"] = datetime.utcnow().replace(tzinfo=timezone.utc)
    statement = statement.on_conflict_do_update(
        index_elements=repos_table.primary_key.columns, set_=on_conf_set
    )
    with engine.begin() as conn:
        conn.execute(statement)


def save_repo_query(full_name: str, query: str):
    """Save the `query` that was used to find `full_name` repo."""
    statement = (
        insert(repo_queries_table)
        .values(full_name=full_name, query=query)
        .on_conflict_do_nothing(index_elements=repo_queries_table.primary_key.columns)
    )
    with engine.begin() as conn:
        conn.execute(statement)
