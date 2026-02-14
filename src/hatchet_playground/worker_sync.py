from hatchet_playground.hatchet_client import hatchet
from hatchet_playground.workflows.cpu_bound_process_pool import (
    cpu_heavy_with_process_pool,
)
from hatchet_playground.workflows.cpu_bound_sync_sleep import sync_sleep_task


def main() -> None:
    worker = hatchet.worker(
        name="cpu-bound-sync-sleep-worker",
        slots=3,  # NOTE: Important to give low slots for this example, since the task is blocking.
        workflows=[sync_sleep_task, cpu_heavy_with_process_pool],
    )
    worker.start()


if __name__ == "__main__":
    main()
