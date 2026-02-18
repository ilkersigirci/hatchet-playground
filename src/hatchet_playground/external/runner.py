import argparse
import asyncio
import json
import logging
import sys
import time
from typing import Any

from hatchet_sdk import EmptyModel, Hatchet
from hatchet_sdk.clients.rest.models.v1_task_status import V1TaskStatus
from pydantic import BaseModel

from .task_schemas import TASK_SCHEMAS, TaskSchema, resolve_task_schema

TERMINAL_STATUSES = {
    V1TaskStatus.COMPLETED,
    V1TaskStatus.FAILED,
    V1TaskStatus.CANCELLED,
}


class ExternalTaskRunner:
    def __init__(
        self,
        task_name: str,
        input_payload: dict[str, Any],
        stream: bool = False,
        hatchet: Hatchet | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialize a task runner for externally triggering Hatchet tasks.

        Args:
            task_name: Registered Hatchet task name to trigger.
            input_payload: Raw input payload to validate/serialize.
            stream: Whether to stream run events in ``run()``.
            hatchet: Optional shared Hatchet client instance.
            logger: Optional logger instance. Defaults to module logger.
        """
        self.hatchet = hatchet or Hatchet()
        self.task_name = task_name
        self.input_payload = input_payload
        self.stream = stream
        self._logger = logger or logging.getLogger(__name__)
        self._schema = resolve_task_schema(task_name)
        self._stub = self._create_stub(self._schema)

    def _create_stub(self, schema: TaskSchema):
        """Create a typed task stub for the configured task name."""
        if schema.output_validator is None:
            return self.hatchet.stubs.task(
                name=self.task_name,
                input_validator=schema.input_validator,
            )

        return self.hatchet.stubs.task(
            name=self.task_name,
            input_validator=schema.input_validator,
            output_validator=schema.output_validator,
        )

    def _build_input(self, input_payload: dict[str, Any] | None = None) -> Any:
        """Build validated task input from a payload.

        Args:
            input_payload: Optional payload override for bulk triggers.

        Returns:
            The validated input object expected by Hatchet.

        Raises:
            TypeError: If the task schema validator type is unsupported.
        """
        validator = self._schema.input_validator
        payload = self.input_payload if input_payload is None else input_payload

        if validator is EmptyModel:
            return EmptyModel()

        if isinstance(validator, type) and issubclass(validator, BaseModel):
            return validator.model_validate(payload)

        raise TypeError(
            f"Unsupported input validator type for task={self.task_name}. "
            "Only Pydantic BaseModel and EmptyModel are supported."
        )

    async def trigger_no_wait(self):
        """Trigger one run and return immediately with a run reference."""
        return await self._stub.aio_run_no_wait(input=self._build_input())

    async def trigger_many_no_wait(
        self, input_payloads: list[dict[str, Any]], keys: list[str] | None = None
    ) -> list[Any]:
        """Trigger many runs in bulk and return run references.

        Args:
            input_payloads: Payloads to validate and submit in one bulk call.
            keys: Optional dedupe/idempotency keys, one per payload.

        Returns:
            A list of run references in submission order.

        Raises:
            ValueError: If ``keys`` length does not match payload count.
        """
        if keys is not None and len(keys) != len(input_payloads):
            raise ValueError("keys must have the same length as input_payloads")

        bulk_items = [
            self._stub.create_bulk_run_item(
                input=self._build_input(payload),
                key=None if keys is None else keys[index],
            )
            for index, payload in enumerate(input_payloads)
        ]
        return await self._stub.aio_run_many_no_wait(bulk_items)

    async def wait_for_terminal_status(
        self,
        workflow_run_id: str,
        poll_interval_seconds: float = 1.0,
        timeout_seconds: float | None = None,
    ) -> V1TaskStatus:
        """Poll a run until it reaches a terminal status.

        Raises:
            ValueError: If poll interval is non-positive.
            TimeoutError: If timeout elapses before a terminal status is reached.
        """
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be > 0")

        started = time.monotonic()
        while True:
            status = await self.hatchet.runs.aio_get_status(workflow_run_id)
            self._logger.info("workflow_run_id=%s status=%s", workflow_run_id, status.value)

            if status in TERMINAL_STATUSES:
                return status

            elapsed = time.monotonic() - started
            if timeout_seconds is not None and elapsed >= timeout_seconds:
                raise TimeoutError(
                    f"Timed out waiting for workflow_run_id={workflow_run_id} "
                    f"after {elapsed:.1f}s"
                )

            await asyncio.sleep(poll_interval_seconds)

    async def stream_run_events(self, workflow_run_id: str) -> V1TaskStatus:
        """Stream run events and return the final run status."""
        run_ref = self.hatchet.runs.get_run_ref(workflow_run_id)
        self._logger.info("Streaming events for workflow_run_id=%s", workflow_run_id)

        async for event in run_ref.stream():
            self._logger.info("[%s] %s", event.type, event.payload)

        final_status = await self.hatchet.runs.aio_get_status(workflow_run_id)
        self._logger.info(
            "workflow_run_id=%s final_status=%s", workflow_run_id, final_status.value
        )
        return final_status

    async def run(
        self,
        poll_interval_seconds: float = 1.0,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Trigger a run, observe completion, and return final outcome."""
        self._logger.info("Triggering task_name=%s", self.task_name)
        workflow_run_ref = await self.trigger_no_wait()
        workflow_run_id = workflow_run_ref.workflow_run_id
        self._logger.info("Triggered workflow run: %s", workflow_run_id)

        if self.stream:
            if timeout_seconds is None:
                final_status = await self.stream_run_events(workflow_run_id)
            else:
                final_status = await asyncio.wait_for(
                    self.stream_run_events(workflow_run_id),
                    timeout=timeout_seconds,
                )
        else:
            final_status = await self.wait_for_terminal_status(
                workflow_run_id=workflow_run_id,
                poll_interval_seconds=poll_interval_seconds,
                timeout_seconds=timeout_seconds,
            )

        outcome: dict[str, Any] = {
            "workflow_run_id": workflow_run_id,
            "status": final_status.value,
            "result": None,
        }

        if final_status == V1TaskStatus.COMPLETED:
            result = await workflow_run_ref.aio_result()
            outcome["result"] = result
            self._logger.info("Result: %s", result)
            return outcome

        details = await self.hatchet.runs.aio_get(workflow_run_id)
        self._logger.info("Run ended with status: %s", details.run.status)
        return outcome


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-name", required=True)
    parser.add_argument(
        "--input-json",
        default="{}",
        help='Task input as JSON object, e.g. \'{"name": "Hatchet"}\'',
    )
    parser.add_argument("--stream", action="store_true")
    parser.add_argument(
        "--list-tasks",
        action="store_true",
        help="List configured task names and exit",
    )
    return parser.parse_args()


def parse_input_json(input_json: str) -> dict[str, Any]:
    try:
        parsed = json.loads(input_json)
    except json.JSONDecodeError as exc:
        raise ValueError(f"--input-json must be valid JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError("--input-json must be a JSON object")
    return parsed


if __name__ == "__main__":
    args = parse_args()

    if args.list_tasks:
        for task_name in sorted(TASK_SCHEMAS.keys()):
            sys.stdout.write(f"{task_name}\n")
        raise SystemExit(0)

    runner = ExternalTaskRunner(
        task_name=args.task_name,
        input_payload=parse_input_json(args.input_json),
        stream=args.stream,
    )
    asyncio.run(runner.run())
