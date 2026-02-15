import argparse
import asyncio
import json
from dataclasses import is_dataclass
from typing import Any

from hatchet_sdk import EmptyModel, Hatchet
from hatchet_sdk.clients.rest.models.v1_task_status import V1TaskStatus
from pydantic import BaseModel

from hatchet_playground.task_schemas import TASK_SCHEMAS, resolve_task_schema

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
    ) -> None:
        self.hatchet = Hatchet()
        self.task_name = task_name
        self.input_payload = input_payload
        self.stream = stream

    def _create_stub(self):
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

    def _build_input(self) -> Any:
        validator = resolve_task_schema(self.task_name).input_validator

        if validator is EmptyModel:
            return EmptyModel()

        if isinstance(validator, type) and issubclass(validator, BaseModel):
            return validator.model_validate(self.input_payload)

        if is_dataclass(validator):
            return validator(**self.input_payload)

        raise TypeError(f"Unsupported input validator type for task={self.task_name}")

    async def wait_for_terminal_status(
        self, workflow_run_id: str, poll_interval_seconds: float = 1.0
    ) -> V1TaskStatus:
        while True:
            status = await self.hatchet.runs.aio_get_status(workflow_run_id)
            print(f"workflow_run_id={workflow_run_id} status={status.value}")

            if status in TERMINAL_STATUSES:
                return status

            await asyncio.sleep(poll_interval_seconds)

    async def stream_run_events(self, workflow_run_id: str) -> V1TaskStatus:
        run_ref = self.hatchet.runs.get_run_ref(workflow_run_id)
        print(f"Streaming events for workflow_run_id={workflow_run_id}")

        async for event in run_ref.stream():
            print(f"[{event.type}] {event.payload}")

        final_status = await self.hatchet.runs.aio_get_status(workflow_run_id)
        print(f"workflow_run_id={workflow_run_id} final_status={final_status.value}")
        return final_status

    async def run(self) -> None:
        stub = self._create_stub()
        print(f"Triggering task_name={self.task_name}")
        workflow_run_ref = await stub.aio_run_no_wait(input=self._build_input())
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
