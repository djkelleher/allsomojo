from os import cpu_count
from pathlib import Path

from dotenv import load_dotenv
from ezloggers import get_logger
from pydantic import PositiveInt, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

base_dir = Path(__file__).parents[1]
repos_base_dir = base_dir / "repos"
logger = get_logger("allsomojo", stdout=True, file_dir=base_dir / "logs")

env_file = base_dir / ".env"
logger.info("Loaded .env (%s): %s", env_file, load_dotenv(env_file))

# File suffixes of targeted code types.
code_file_suffixes = (".mojo", ".🔥", ".py", ".ipynb", ".pxd", ".pyx")


class Config(BaseSettings):
    """Settings from environment variables."""

    # github token
    gh_token: str
    # postgres URL
    pg_url: PostgresDsn
    # max number of simultaneous bandwidth-using git commands (e.g. clone, pull). Defaults to 5.
    max_git_io: PositiveInt = 5
    max_process: PositiveInt = max(int(cpu_count() * 0.66), 1)
    # Google Sheets
    spreadsheet_name: str = "Mojo🔥 Repos"
    worksheet_name: str = "Sheet1"
    # comment for manual repo additions.
    comment_anchor: str = '{"type":"workbook-range","uid":0,"range":"421009453"}'
    # env vars prefix
    model_config = SettingsConfigDict(env_prefix="allsomojo_")


config = Config()
