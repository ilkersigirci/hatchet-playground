import time
from datetime import timedelta

from hatchet_sdk import Context, EmptyModel

from hatchet_playground.hatchet_client import hatchet


@hatchet.task(
    name="sync-sleep-task",
    input_validator=EmptyModel,
    execution_timeout=timedelta(seconds=20),
    retries=1,
)
def sync_sleep_task(input: EmptyModel, ctx: Context) -> dict[str, str | int | float]:
    print("Executing sync_sleep_task")

    start_time = time.time()
    iterations = 10
    for i in range(iterations):
        print(f"[sync task] sleeping {i + 1}/{iterations}")
        time.sleep(1)

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Completed sync sleep task in {execution_time:.2f} seconds")

    return {
        "task": "sync-sleep-task",
        "iterations": iterations,
        "execution_time": execution_time,
    }
