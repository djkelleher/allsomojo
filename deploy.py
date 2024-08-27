"""Deploy as systemd services. Run `taskflows create deploy.py`."""

from taskflows.service import Calendar, MambaEnv, Service

venv = MambaEnv("allsomojo")

default_update = Service(
    name="allsomojo-update-default",
    venv=venv,
    start_command="allsomojo update-db update-sheet",
    start_schedule=Calendar("Mon..Sat 1:00 America/New_York"),
)

update_recheck_blacklist = Service(
    name="allsomojo-update-recheck-blacklist",
    venv=venv,
    start_command="allsomojo update-db -b update-sheet",
    start_schedule=Calendar("Sun 1:00 America/New_York"),
)
