from pathlib import Path

from dotenv import load_dotenv

base_dir = Path(__file__).parents[1]

env_file = base_dir / ".env"
env_file_loaded: bool = load_dotenv(env_file)

from datetime import datetime
from os import cpu_count
from typing import List, Optional
from zoneinfo import ZoneInfo

from pydantic import PositiveInt, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from quicklogs import get_logger
from taskflows import Alerts, SlackChannel

mojo_launch_date = datetime(2023, 5, 1, tzinfo=ZoneInfo("UTC"))


class Config(BaseSettings):
    """Settings from environment variables."""

    # github token
    gh_token: str
    # postgres URL
    db_url: str
    # max number of simultaneous bandwidth-using git commands (e.g. clone, pull).
    max_git_io: PositiveInt = 8
    max_process: PositiveInt = max(int(cpu_count() * 0.66), 1)
    # where repos should be cloned.
    repos_base_dir: Path = base_dir / "repos"
    # symlink confirmed Mojo repos to this directory.
    selected_repos_dir: Optional[Path] = base_dir / "selected_repos"
    # were log files should be stored.
    logs_dir: Path = base_dir / "logs"
    # Google Sheets
    spreadsheet_name: str = "Mojo🔥 Repos"
    time_sorted_worksheet: str = "Latest Created"
    star_sorted_worksheet: str = "Most Stars"
    most_active_worksheet: str = "Biggest Updates(30 days)"
    # comment for manual repo additions.
    comment_anchor: str = '{"type":"workbook-range","uid":0,"range":"421009453"}'
    # Slack alerts.
    slack_app_token: Optional[SecretStr] = None
    slack_bot_token: Optional[SecretStr] = None
    slack_channel: Optional[str] = None
    slack_alert_on: Optional[List[str]] = ["start", "error", "finish"]
    # env vars prefix
    model_config = SettingsConfigDict(env_prefix="allsomojo_")


config = Config()

if all(
    v is not None
    for v in (
        config.slack_app_token,
        config.slack_bot_token,
        config.slack_channel,
        config.slack_alert_on,
    )
):
    task_alerts = [
        Alerts(
            send_to=[
                SlackChannel(
                    app_token=config.slack_app_token,
                    bot_token=config.slack_bot_token,
                    channel=config.slack_channel,
                )
            ],
            send_on=config.slack_alert_on,
        )
    ]

else:
    task_alerts = None

logger = get_logger("allsomojo", file_dir=config.logs_dir)
logger.info("Loaded .env (%s): %s", env_file, env_file_loaded)
