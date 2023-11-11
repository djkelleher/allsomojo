"""Deploy as systemd services. Run `taskflows create deploy.py`."""
from task_flows import OnCalendar, ScheduledMambaTask

default_update = ScheduledMambaTask(
    task_name="allsomojo-update-default",
    command="allsomojo update-db update-sheet",
    env_name="allsomojo",
    timer=OnCalendar("Mon..Sat 1:00 America/New_York"),
)

update_recheck_blacklist = ScheduledMambaTask(
    task_name="allsomojo-update-recheck-blacklist",
    command="allsomojo update-db -b update-sheet",
    env_name="allsomojo",
    timer=OnCalendar("Sun 1:00 America/New_York"),
)
