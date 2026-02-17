import argparse
import asyncio
import json
from dataclasses import is_dataclass
from typing import Any

from hatchet_sdk import EmptyModel, Hatchet
from hatchet_sdk.clients.rest.models.v1_task_status import V1TaskStatus
from pydantic import BaseModel

from .task_schemas import TASK_SCHEMAS, resolve_task_schema

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
    ) -> None:
        """Initialize a task runner for externally triggering Hatchet tasks.

        Args:
            task_name: Registered Hatchet task name to trigger.
            input_payload: Raw input payload to validate/serialize.
            stream: Whether to stream run events in ``run()``.
            hatchet: Optional shared Hatchet client instance.
        """
        self.hatchet = hatchet or Hatchet()
        self.task_name = task_name
        self.input_payload = input_payload
        self.stream = stream

    def _create_stub(self):
        """Create a typed task stub for the configured task name."""
        schema = resolve_task_schema(self.task_name)
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
        validator = resolve_task_schema(self.task_name).input_validator
        payload = self.input_payload if input_payload is None else input_payload

        if validator is EmptyModel:
            return EmptyModel()

        if isinstance(validator, type) and issubclass(validator, BaseModel):
            return validator.model_validate(payload)

        if is_dataclass(validator):
            return validator(**payload)

        raise TypeError(f"Unsupported input validator type for task={self.task_name}")

    async def trigger_no_wait(self):
        """Trigger one run and return immediately with a run reference."""
        stub = self._create_stub()
        return await stub.aio_run_no_wait(input=self._build_input())

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

        stub = self._create_stub()
        bulk_items = [
            stub.create_bulk_run_item(
                input=self._build_input(payload),
                key=None if keys is None else keys[index],
            )
            for index, payload in enumerate(input_payloads)
        ]
        return await stub.aio_run_many_no_wait(bulk_items)

    async def wait_for_terminal_status(
        self, workflow_run_id: str, poll_interval_seconds: float = 1.0
    ) -> V1TaskStatus:
        """Poll a run until it reaches a terminal status."""
        while True:
            status = await self.hatchet.runs.aio_get_status(workflow_run_id)
            print(f"workflow_run_id={workflow_run_id} status={status.value}")

            if status in TERMINAL_STATUSES:
                return status

            await asyncio.sleep(poll_interval_seconds)

    async def stream_run_events(self, workflow_run_id: str) -> V1TaskStatus:
        """Stream run events and return the final run status."""
        run_ref = self.hatchet.runs.get_run_ref(workflow_run_id)
        print(f"Streaming events for workflow_run_id={workflow_run_id}")

        async for event in run_ref.stream():
            print(f"[{event.type}] {event.payload}")

        final_status = await self.hatchet.runs.aio_get_status(workflow_run_id)
        print(f"workflow_run_id={workflow_run_id} final_status={final_status.value}")
        return final_status

    async def run(self) -> None:
        """Trigger a run, observe completion, and print final outcome."""
        print(f"Triggering task_name={self.task_name}")
        workflow_run_ref = await self.trigger_no_wait()
        workflow_run_id = workflow_run_ref.workflow_run_id
        print("Triggered workflow run:", workflow_run_id)

        if self.stream:
            final_status = await self.stream_run_events(workflow_run_id)
        else:
            final_status = await self.wait_for_terminal_status(workflow_run_id)

        if final_status == V1TaskStatus.COMPLETED:
            result = await workflow_run_ref.aio_result()
            print("Result:", result)
            return

        details = await self.hatchet.runs.aio_get(workflow_run_id)
        print("Run ended with status:", details.run.status)


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
    parsed = json.loads(input_json)
    if not isinstance(parsed, dict):
        raise ValueError("--input-json must be a JSON object")
    return parsed


if __name__ == "__main__":
    args = parse_args()

    if args.list_tasks:
        for task_name in sorted(TASK_SCHEMAS.keys()):
            print(task_name)
        raise SystemExit(0)

    runner = ExternalTaskRunner(
        task_name=args.task_name,
        input_payload=parse_input_json(args.input_json),
        stream=args.stream,
    )
    asyncio.run(runner.run())
