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

## Track workflow status from workflow_run_id

The external trigger example in `src/hatchet_playground/run_external.py` uses:

- `aio_run_no_wait(...)` to get a run reference and `workflow_run_id`
- `hatchet.runs.aio_get_status(workflow_run_id)` for status checks
- `hatchet.runs.get_run_ref(workflow_run_id).stream()` for live run events
- `workflow_run_ref.aio_result()` to fetch typed output after completion

Current script entrypoint runs the stream-based flow (`main_no_wait_stream`) so you can watch progress in real time.

If you want polling instead of streaming, switch to `main_no_wait` in the `__main__` block.

## Available commands

```shell
make run-worker
make run-local
make run-external-trigger
```
