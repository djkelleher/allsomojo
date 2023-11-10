import re
from dataclasses import asdict, dataclass
from typing import List, Literal, Optional, Sequence, Tuple

import googleapiclient.discovery
import gspread
import numpy as np
import pandas as pd
import sqlalchemy as sa
from gspread import Spreadsheet, Worksheet
from gspread.utils import ValueInputOption
from sqlalchemy.dialects.postgresql import insert
from task_flows import task

from .common import config, logger, task_alerts
from .db import engine, repos_table

ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


columns_header = {
    "username": "User",
    "repo_name": "Repo",
    "created_at": "Created",
    "pushed_at": "Updated",
    "stargazers_count": "Stars",
    "n_code_lines": "Lines\nof\nCode",
    "lines_added_30d": "Lines\nAdded\n(30 days)",
    "lines_deleted_30d": "Lines\nDeleted\n(30 days)",
    "lines_changed_30d": "Lines\nChanged\n(30 days)",
    "files_changed_30d": "Files\nChanged\n(30 days)",
    "open_issues": "Open\nIssues",
}


@dataclass
class CellPadding:
    """Padding around cell content."""

    top: int
    bottom: int
    left: int
    right: int


@dataclass
class RGBColor:
    """RGB color. Values should be between in range [0,1]."""

    red: int
    green: int
    blue: int

    @classmethod
    def from_0_255(cls, red: int, green: int, blue: int):
        """RGB color using [0,255] range values."""
        return cls(red=red / 255, green=green / 255, blue=blue / 255)


white = RGBColor(red=1, green=1, blue=1)
orange = RGBColor.from_0_255(red=255, green=153, blue=0)
dark_orange = RGBColor.from_0_255(red=255, green=109, blue=1)


@task(name="allsomojo-update-sheet", required=True, alerts=task_alerts)
def update_sheet():
    """Update Google Sheet with current database data."""
    df = get_sheet_data()
    client = gspread.service_account()
    spreadsheet = client.open(config.spreadsheet_name)
    worksheet = spreadsheet.worksheet(config.worksheet_name)
    set_sheet_data(df, worksheet)
    format_sheet(
        data=df,
        worksheet=worksheet,
        spreadsheet=spreadsheet,
    )


def set_sheet_data(data: pd.DataFrame, worksheet: Worksheet):
    """Update Google Sheet with current database data."""
    worksheet.clear()
    columns = data.columns.values.tolist()
    # set header.
    header = [
        columns_header.get(col) or col.replace("_", " ").title() for col in columns
    ]
    header.append("Manual Additions")
    worksheet.update(
        f"A1:{ALPHABET[len(header) - 1]}1",
        [header],
    )

    def set_columns_data(col_names, raw):
        logger.info("Updating columns: %s", col_names)
        for start, end in column_ranges(col_names, columns):
            df_cols = data.iloc[:, start:end]
            worksheet.update(
                f"{ALPHABET[start]}2:{ALPHABET[end]}{len(df_cols)+1}",
                df_cols.values.tolist(),
                value_input_option=ValueInputOption.raw
                if raw
                else ValueInputOption.user_entered,
            )

    # strings need to be 'raw' format so they don't get converted to other types (e.g. number or links)
    str_cols = [
        c for c in data.select_dtypes(include="object").columns if c != "homepage"
    ]
    # format datetimes so they will be parsable by Google.
    data = data.copy()
    for col in data.select_dtypes(include="datetimetz").columns:
        data[col] = data[col].dt.strftime("%Y-%m-%d")
    set_columns_data(str_cols, True)
    set_columns_data([c for c in columns if c not in str_cols], False)


def format_sheet(
    data: pd.DataFrame,
    worksheet: Worksheet,
    spreadsheet: Spreadsheet,
    auto_resize_columns: bool = False,
):
    """Set Google Sheet formatting."""
    columns = data.columns.values.tolist()
    sheet_id = worksheet.id
    end_row_idx = len(data) + 1
    requests = []

    def set_values_format(
        col_name_or_row_idx: str | int,
        font_size: int = 10,
        h_align: Literal["LEFT", "CENTER", "RIGHT"] = "CENTER",
        v_align: Literal["TOP", "MIDDLE", "BOTTOM"] = "MIDDLE",
        bold: bool = False,
        bg_color: Optional[RGBColor] = None,
        text_color: Optional[RGBColor] = None,
        padding: Optional[CellPadding] = None,
    ):
        if isinstance(col_name_or_row_idx, str):
            col_idx = columns.index(col_name_or_row_idx)
            rng = {
                # skip header.
                "startRowIndex": 1,
                "endRowIndex": end_row_idx,
                "startColumnIndex": col_idx,
                "endColumnIndex": col_idx + 1,
            }
        else:
            assert isinstance(col_name_or_row_idx, int)
            rng = {
                "startRowIndex": col_name_or_row_idx,
                "endRowIndex": col_name_or_row_idx + 1,
            }
        text_fmt = {
            "fontSize": font_size,
            "bold": bold,
        }
        if text_color is not None:
            text_fmt["foregroundColor"] = asdict(text_color)

        user_entered_fmt = {
            "horizontalAlignment": h_align,
            "verticalAlignment": v_align,
            "textFormat": text_fmt,
        }
        if bg_color is not None:
            user_entered_fmt["backgroundColor"] = asdict(bg_color)
        if padding is not None:
            user_entered_fmt["padding"] = asdict(padding)
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        **rng,
                    },
                    "cell": {"userEnteredFormat": user_entered_fmt},
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,padding)",
                }
            }
        )

    ## FORMAT HEADER
    set_values_format(
        col_name_or_row_idx=0,
        font_size=10,
        bold=True,
    )
    # freeze header row.
    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }
    )
    ## FORMAT DATE COLUMNS
    timestamptz_cols = data.select_dtypes(include="datetimetz").columns.tolist()
    for start_col, end_col in column_ranges(timestamptz_cols, columns):
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": end_row_idx,
                        "startColumnIndex": start_col,
                        "endColumnIndex": end_col,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE",
                                "pattern": "yyyy-mm-dd",
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                },
            }
        )
    numeric_cols = data.select_dtypes(include="number").columns.tolist()
    ## SET VALUE COLORS
    for col in numeric_cols + timestamptz_cols:
        col_idx = columns.index(col)
        requests.append(
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [
                            {
                                "sheetId": sheet_id,
                                "startColumnIndex": col_idx,
                                "endColumnIndex": col_idx + 1,
                            }
                        ],
                        "gradientRule": {
                            "minpoint": {
                                "color": asdict(white),
                                "type": "MIN",
                            },
                            "maxpoint": {"color": asdict(orange), "type": "MAX"},
                        },
                    },
                    "index": 0,
                }
            }
        )

    ## FORMAT COLUMN VALUES
    obj_cols = list(data.select_dtypes(include="object").columns)
    for col in columns:
        if col not in ("username", "repo_name", "homepage"):
            set_values_format(col, h_align="LEFT" if col in obj_cols else "CENTER")

    ## FORMAT USER REPO COLUMNS
    for col_name, row_to_url in (
        ("username", lambda row: f"https://github.com/{row['username']}"),
        (
            "repo_name",
            lambda row: f"https://github.com/{row['username']}/{row['repo_name']}",
        ),
    ):
        rows = []
        for _, row in data.iterrows():
            # add user link.
            rows.append(
                {
                    "values": [
                        {
                            "userEnteredFormat": {
                                "textFormat": {
                                    "bold": True,
                                    "fontSize": 10,
                                    "link": {"uri": row_to_url(row)},
                                }
                            }
                        }
                    ]
                }
            )
        col_idx = columns.index(col_name)
        requests.append(
            {
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": end_row_idx,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1,
                    },
                    "rows": rows,
                    "fields": "userEnteredFormat.textFormat",
                }
            }
        )
    ## ADD NOTES
    for col_name in ("description", "topics"):
        rows = []
        for _, row in data.iterrows():
            # add user link.
            update = {}
            cell = row[col_name]
            if cell and len(cell) > 18:
                update["values"] = [{"note": cell}]
            rows.append(update)
        col_idx = columns.index(col_name)
        requests.append(
            {
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": end_row_idx,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1,
                    },
                    "rows": rows,
                    "fields": "note",
                }
            }
        )

    if auto_resize_columns:
        requests.append(
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(columns),
                    }
                }
            }
        )
    # execute requests.
    spreadsheet.batch_update({"requests": requests})


def save_comment_repos():
    """Save comment replies for manual repo additions."""
    # set env var GOOGLE_APPLICATION_CREDENTIALS to service account json path.
    drive = googleapiclient.discovery.build("drive", "v3")
    files = drive.files().list(pageSize=10, fields="files(id, name)").execute()["files"]
    file_id = None
    for f in files:
        if f["name"] == config.spreadsheet_name:
            file_id = f["id"]
            break
    if file_id is None:
        raise FileNotFoundError("Could not find Mojo🔥 Repos spreadsheet.")
    comments = (
        drive.comments()
        .list(fileId=file_id, fields="comments")
        .execute()
        .get("comments")
    )
    comment = [c for c in comments if c["anchor"] == config.comment_anchor]
    assert len(comment) == 1
    comment = comment[0]
    repos = []
    for reply in comment["replies"]:
        repos += re.findall(r"[^\s\/,]+\/[^\s\/,]+", reply["content"])
    repos = [
        {"full_name": full_name, "blacklisted_reason": None, "manually_checked": True}
        for full_name in repos
        if full_name
    ]
    logger.info("Found %i manual repo additions from comments:\n%s", len(repos), repos)
    if repos:
        statement = insert(repos_table).values(repos)
        on_conf_set = {c.name: c for c in statement.excluded}
        on_conf_set = {
            n: c
            for n, c in on_conf_set.items()
            if n in ("blacklisted_reason", "manually_checked")
        }
        statement = statement.on_conflict_do_update(
            index_elements=repos_table.primary_key.columns, set_=on_conf_set
        )
        with engine.begin() as conn:
            conn.execute(statement)


def get_sheet_data() -> pd.DataFrame:
    """Read data from repos table."""
    query = (
        sa.select(
            repos_table.c.username,
            repos_table.c.repo_name,
            repos_table.c.homepage,
            repos_table.c.description,
            repos_table.c.topics,
            # repos_table.c.fork,
            repos_table.c.forks,
            repos_table.c.created_at,
            repos_table.c.pushed_at,
            repos_table.c.stargazers_count,
            # repos_table.c.watchers,
            repos_table.c.open_issues,
            repos_table.c.n_code_lines,
            repos_table.c.commits,
            # repos_table.c.lines_added_30d,
            # repos_table.c.lines_deleted_30d,
            (repos_table.c.lines_added_30d + repos_table.c.lines_deleted_30d).label(
                "lines_changed_30d"
            ),
            repos_table.c.files_changed_30d,
            repos_table.c.license,
        )
        .where(repos_table.c.blacklisted_reason.is_(None), repos_table.c.fork == False)
        .order_by(repos_table.c.created_at.desc())
    )
    with engine.begin() as conn:
        df = pd.read_sql_query(query, conn, parse_dates=["created_at", "pushed_at"])
    df["topics"] = df["topics"].apply(lambda x: ", ".join(x))
    # df["user_avatar_url"] = df["user_avatar_url"].apply(lambda x: f'=IMAGE("{x}")')
    df.replace(np.nan, None, inplace=True)
    return df


def column_ranges(
    col_names: Sequence[str], columns: Sequence[str]
) -> List[Tuple[int, int]]:
    """Get index ranges of `col_names` in `columns`."""
    col_idxs = sorted([columns.index(c) for c in col_names])
    ranges = []
    start = col_idx = col_idxs[0]
    for prev, col_idx in enumerate(col_idxs[1:]):
        prev_col_idx = col_idxs[prev]
        if col_idx > (prev_col_idx + 1):
            ranges.append((start, prev_col_idx + 1))
            start = col_idx
    ranges.append((start, col_idx + 1))
    return ranges
