from os import cpu_count
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from ezloggers import get_logger
from pydantic import PositiveInt, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from task_flows import Alerts, Slack

base_dir = Path(__file__).parents[1]

env_file = base_dir / ".env"
env_file_loaded: bool = load_dotenv(env_file)


class Config(BaseSettings):
    """Settings from environment variables."""

    # github token
    gh_token: str
    # postgres URL
    pg_url: PostgresDsn
    # max number of simultaneous bandwidth-using git commands (e.g. clone, pull).
    max_git_io: PositiveInt = 4
    max_process: PositiveInt = max(int(cpu_count() * 0.66), 1)
    # where repos should be cloned.
    repos_base_dir: Path = base_dir / "repos"
    # were log files should be stored.
    logs_dir: Path = base_dir / "logs"
    # Google Sheets
    spreadsheet_name: str = "Mojo🔥 Repos"
    worksheet_name: str = "Sheet1"
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
                Slack(
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

logger = get_logger("allsomojo", stdout=True, file_dir=config.logs_dir)
logger.info("Loaded .env (%s): %s", env_file, env_file_loaded)
