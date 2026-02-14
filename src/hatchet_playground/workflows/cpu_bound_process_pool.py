import asyncio
import atexit
import hashlib
import os
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta

from hatchet_sdk import Context, EmptyModel

from hatchet_playground.hatchet_client import hatchet

_MAX_WORKERS = max(1, min(4, os.cpu_count() or 1))
_PROCESS_POOL = ProcessPoolExecutor(max_workers=_MAX_WORKERS)
atexit.register(_PROCESS_POOL.shutdown)


def _hash_work(iterations: int) -> dict[str, int | float]:
    start_time = time.time()
    for i in range(iterations):
        hashlib.sha256(f"data{i}".encode()).hexdigest()
    end_time = time.time()
    return {
        "iterations": iterations,
        "execution_time": end_time - start_time,
    }


@hatchet.task(
    name="cpu-heavy-with-process-pool",
    input_validator=EmptyModel,
    execution_timeout=timedelta(seconds=120),
    retries=1,
)
async def cpu_heavy_with_process_pool(
    input: EmptyModel, ctx: Context
) -> dict[str, str | int | float]:
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(_PROCESS_POOL, _hash_work, 8_000_000)
    return {
        "task": "cpu-heavy-with-process-pool",
        **result,
    }
