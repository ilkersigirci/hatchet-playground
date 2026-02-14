import argparse
import asyncio

from hatchet_sdk import EmptyModel, Hatchet
from hatchet_sdk.clients.rest.models.v1_task_status import V1TaskStatus
from pydantic import BaseModel


# > Define models
class TaskInput(BaseModel):
    user_id: int


class TaskOutput(BaseModel):
    ok: bool


TERMINAL_STATUSES = {
    V1TaskStatus.COMPLETED,
    V1TaskStatus.FAILED,
    V1TaskStatus.CANCELLED,
}


class ExternalTaskRunner:
    def __init__(
        self, task_name: str, stream: bool = False, user_id: int = 1234
    ) -> None:
        self.hatchet = Hatchet()
        self.task_name = task_name
        self.stream = stream
        self.user_id = user_id

    def _create_stub(self):
        if self.task_name == "externally-triggered-task":
            return self.hatchet.stubs.task(
                name=self.task_name,
                input_validator=TaskInput,
                output_validator=TaskOutput,
            )

        return self.hatchet.stubs.task(
            name=self.task_name,
            input_validator=EmptyModel,
        )

    def _build_input(self) -> TaskInput | EmptyModel:
        if self.task_name == "externally-triggered-task":
            return TaskInput(user_id=self.user_id)
        return EmptyModel()

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
    parser.add_argument(
        "--task-name",
        default="externally-triggered-task",
    )
    parser.add_argument("--stream", action="store_true")
    parser.add_argument("--user-id", type=int, default=1234)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    runner = ExternalTaskRunner(
        task_name=args.task_name,
        stream=args.stream,
        user_id=args.user_id,
    )
    asyncio.run(runner.run())
