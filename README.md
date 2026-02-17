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
- Trigger (`external/runner.py --task-name sync-sleep-task`): `make run-sync-trigger`

### Run process pool example

- Start worker: `make run-worker-sync`
- Trigger (`external/runner.py --task-name cpu-heavy-with-process-pool`): `make run-sync-process-pool-trigger`

### Rule of thumb

- Don't run blocking sync code directly inside `async def` tasks.
- Use `sync def` task, or offload CPU work to a process pool.

## Track workflow status from workflow_run_id

The external trigger example in `src/hatchet_playground/external/runner.py` uses:

- `aio_run_no_wait(...)` to get a run reference and `workflow_run_id`
- `hatchet.runs.aio_get_status(workflow_run_id)` for status checks
- `hatchet.runs.get_run_ref(workflow_run_id).stream()` for live run events
- `workflow_run_ref.aio_result()` to fetch typed output after completion

By default, `external/runner.py` triggers and polls for terminal status. Use `--stream` to watch live events.

## External runner (`external/runner.py`)

- By task name (default polling): `uv run src/hatchet_playground/external/runner.py --task-name <task_name>`
- Pass input as JSON object: `--input-json '{"name":"Hatchet"}'`
- Stream events: `uv run src/hatchet_playground/external/runner.py --task-name <task_name> --stream`
- List configured task names: `uv run src/hatchet_playground/external/runner.py --task-name externally-triggered-task --list-tasks`
- Task input/output schemas are mapped by task name in `src/hatchet_playground/task_schemas.py`.

## Bulk run benchmark (`notebooks/task_status.ipynb`)

The benchmark notebook uses Hatchet bulk trigger APIs via `ExternalTaskRunner.trigger_many_no_wait(...)`:

- Bulk submit multiple runs per task in one call (`aio_run_many_no_wait`)
- Track statuses until terminal states
- Use deterministic run keys for dedupe/idempotency testing:
  `f"{BENCHMARK_KEY_PREFIX}:{task_name}:{run_index}"`

Open the notebook with:

```shell
make run-task-status-benchmark
```
