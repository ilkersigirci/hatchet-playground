## Hatchet Python Quickstart

This is an example project demonstrating how to use Hatchet with Python. For detailed setup instructions, see the [Hatchet Setup Guide](https://docs.hatchet.run/home/setup).

Set the required environment variable `HATCHET_CLIENT_TOKEN` created in the [Getting Started Guide](https://docs.hatchet.run/home/hatchet-cloud-quickstart).

1. Start the worker in terminal 1:

```shell
make run-worker
```

2. Trigger from terminal 2:

```shell
make run-external-trigger
```

This triggers the externally triggered task and prints the workflow run id.

## Choose a pattern

- Sync task (`sync def` + `time.sleep`): simplest way to run blocking sync work safely.
- Process-pool task (`async def` + `ProcessPoolExecutor`): better for truly heavy CPU workloads.

### Run sync task example

- Start worker: `make run-worker-sync`
- Trigger (`run_external.py --task-name sync-sleep-task`): `make run-sync-trigger`

### Run process pool example

- Start worker: `make run-worker-sync`
- Trigger (`run_external.py --task-name cpu-heavy-with-process-pool`): `make run-sync-process-pool-trigger`

### Rule of thumb

- Don't run blocking sync code directly inside `async def` tasks.
- Use `sync def` task, or offload CPU work to a process pool.

## Track workflow status from workflow_run_id

The external trigger example in `src/hatchet_playground/run_external.py` uses:

- `aio_run_no_wait(...)` to get a run reference and `workflow_run_id`
- `hatchet.runs.aio_get_status(workflow_run_id)` for status checks
- `hatchet.runs.get_run_ref(workflow_run_id).stream()` for live run events
- `workflow_run_ref.aio_result()` to fetch typed output after completion

Current script entrypoint runs the stream-based flow (`main_no_wait_stream`) so you can watch progress in real time.

If you want polling instead of streaming, switch to `main_no_wait` in the `__main__` block.

## External runner (`run_external.py`)

- By task name (default polling): `uv run src/hatchet_playground/run_external.py --task-name <task_name>`
- Pass input as JSON object: `--input-json '{"name":"Hatchet"}'`
- Stream events: `uv run src/hatchet_playground/run_external.py --task-name <task_name> --stream`
- List configured task names: `uv run src/hatchet_playground/run_external.py --task-name externally-triggered-task --list-tasks`
- Task input/output schemas are mapped by task name in `src/hatchet_playground/task_schemas.py`.

## Available commands

```shell
make run-worker
make run-local
make run-external
make run-external-list-tasks
make run-external-trigger
make run-external-trigger-stream
make run-worker-sync
make run-sync-trigger
make run-sync-process-pool-trigger
```
