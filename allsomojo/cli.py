import click
from click import Group

cli = Group(chain=True)


@cli.command
@click.option("--search-from-start", "-s", is_flag=True)
@click.option("--include-blacklisted", "-b", is_flag=True)
def update_db(search_from_start, include_blacklisted):
    """Update database with new repos / repo changes."""
    click.echo(click.style("Updating database.", fg="cyan"))
    from .common import env_file_loaded
    from .core import update_db as _update_db
    from .core import update_symlinks

    _update_db(
        start_search_at_last_crawl=not search_from_start,
        include_blacklisted=include_blacklisted,
    )
    update_symlinks()
    click.echo(click.style("Done!", fg="bright_green"))


@cli.command
def update_sheet():
    """Update Google Sheet with current database data."""
    from .common import env_file_loaded
    from .sheet import update_sheet as _update_sheet

    click.echo(click.style("Updating Google Sheet.", fg="cyan"))

    _update_sheet()
    click.echo(click.style("Done!", fg="bright_green"))


@cli.command
def review():
    """Manually review repos that only contain Python files and/or notebooks."""
    import webbrowser

    import sqlalchemy as sa
    from tqdm import tqdm

    from .common import env_file_loaded
    from .db import engine, repos_table

    with engine.begin() as conn:
        local_paths = list(
            conn.execute(
                sa.select(repos_table.c.local_path).where(
                    repos_table.c.blacklisted_reason == "needs review"
                )
            ).scalars()
        )
    click.echo(
        click.style(
            "Manually review repos containing only Python files and/or notebooks.",
            fg="bright_magenta",
        )
    )
    for local_path in tqdm(local_paths):
        click.echo(click.style(local_path, fg="cyan"))
        webbrowser.open(f"file://{local_path}")
        while (answer := input("Is 🔥? (y/n): ").strip().lower()) not in (
            "y",
            "5",
            "n",
            "4",
        ):
            pass
        values = {
            "blacklisted_reason": "not mojo code" if answer in ("n", "4") else None,
            "manually_checked": True,
        }
        click.echo(click.style(f"Updating entry for {local_path}: {values}", fg="cyan"))
        with engine.begin() as conn:
            conn.execute(
                sa.update(repos_table)
                .where(repos_table.c.local_path == local_path)
                .values(values)
            )
    click.echo(click.style("Done!", fg="bright_green"))
