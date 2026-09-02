from functools import partial
from contextlib import contextmanager
from collections.abc import Callable, Generator

from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    DownloadColumn,
)

progress_bar: Progress = Progress(
    TextColumn("[bold blue]{task.description}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•",
    DownloadColumn(),
)


@contextmanager
def progress_task(
    desc: str,
    total: int | None = None,
) -> Generator[Callable[..., None], None, None]:
    task_id = progress_bar.add_task(description=desc, total=total)
    progress_bar.start_task(task_id)
    if not progress_bar.live.is_started:
        progress_bar.start()

    try:
        yield partial(progress_bar.update, task_id)
    finally:
        progress_bar.remove_task(task_id)
        if not progress_bar.tasks:
            progress_bar.stop()
