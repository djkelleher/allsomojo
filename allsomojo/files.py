import json
from pathlib import Path
from typing import Any, List, Optional, Sequence, Set

import sqlalchemy as sa

from .common import logger
from .db import engine, repos_table


def count_file_lines(file: Path) -> int:
    """Count the number of non-blank lines in a file."""
    if not (content := decode_file(file)):
        return
    if file.suffix == ".ipynb":
        # parse Jupyter notebook JSON.
        try:
            content = json.loads(content)
        except json.JSONDecodeError:
            logger.error("Invalid JSON in %s", file)
            return
        return sum(
            [
                len([l for l in cell["source"] if l.strip()])
                for cell in content.get("cells", [])
            ]
        )
    return len([l for l in content.splitlines() if l.strip()])


def local_repo_paths(
    include_blacklisted: bool = False, where_conditions: Optional[Any] = None
) -> Set[str]:
    """Paths to local repo directories"""
    # use the (single source of truth) database instead of looping through directory.
    query = sa.select(repos_table.c.local_path).where(
        repos_table.c.local_path.isnot(None)
    )
    if not include_blacklisted:
        query = query.where(repos_table.c.blacklisted_reason.is_(None))
    if where_conditions is not None:
        query = query.where(where_conditions)
    with engine.begin() as conn:
        local_repos = set(conn.execute(query).scalars())
    return local_repos


def find_code_files(path: Path, suffixes: Sequence[str]) -> List[Path]:
    """Find all code files in a directory recursively."""
    code_files = []
    for suffix in suffixes:
        for p in path.rglob(f"*{suffix}"):
            if p.is_file():
                code_files.append(p)
            elif p.is_dir():
                code_files.extend(find_code_files(p, suffixes))
    return code_files


def decode_file(file: Path) -> str:
    """Decode a file to string."""
    if not file.is_file():
        logger.error("Not a file: %s", file)
        return
    for encoding in ("utf-8", "latin-1"):
        try:
            return file.read_text(encoding=encoding)
        except UnicodeDecodeError:
            pass
    logger.error("Failed to decode %s.", file)
