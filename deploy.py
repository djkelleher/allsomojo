"""Deploy as systemd services. Run `taskflows create deploy.py`."""
from taskflows.service import Calendar, Service, mamba_command

default_update = Service(
    name="allsomojo-update-default",
    command=mamba_command(
        env_name="allsomojo", command="allsomojo update-db update-sheet"
    ),
    schedule=Calendar("Mon..Sat 1:00 America/New_York"),
)

update_recheck_blacklist = Service(
    name="allsomojo-update-recheck-blacklist",
    command=mamba_command(
        env_name="allsomojo", command="allsomojo update-db -b update-sheet"
    ),
    schedule=Calendar("Sun 1:00 America/New_York"),
)
