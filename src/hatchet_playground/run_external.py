import asyncio

from hatchet_sdk import Hatchet
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


async def wait_for_terminal_status(
    hatchet: Hatchet, workflow_run_id: str, poll_interval_seconds: float = 1.0
) -> V1TaskStatus:
    while True:
        status = await hatchet.runs.aio_get_status(workflow_run_id)
        print(f"workflow_run_id={workflow_run_id} status={status.value}")

        if status in TERMINAL_STATUSES:
            return status

        await asyncio.sleep(poll_interval_seconds)


async def stream_run_events(hatchet: Hatchet, workflow_run_id: str) -> V1TaskStatus:
    run_ref = hatchet.runs.get_run_ref(workflow_run_id)
    print(f"Streaming events for workflow_run_id={workflow_run_id}")

    async for event in run_ref.stream():
        print(f"[{event.type}] {event.payload}")

    final_status = await hatchet.runs.aio_get_status(workflow_run_id)
    print(f"workflow_run_id={workflow_run_id} final_status={final_status.value}")
    return final_status


async def main() -> None:
    hatchet = Hatchet()

    # > Create a stub task
    stub = hatchet.stubs.task(
        # make sure the name and schemas exactly match the implementation
        name="externally-triggered-task",
        input_validator=TaskInput,
        output_validator=TaskOutput,
    )

    # > Trigger the task
    # input type checks properly
    result = await stub.aio_run(input=TaskInput(user_id=1234))

    # `result.ok` type checks properly
    print("Is successful:", result.ok)


async def main_no_wait() -> None:
    hatchet = Hatchet()

    # > Create a stub task
    stub = hatchet.stubs.task(
        # make sure the name and schemas exactly match the implementation
        name="externally-triggered-task",
        input_validator=TaskInput,
        output_validator=TaskOutput,
    )

    # > Trigger the task
    # input type checks properly
    workflow_run_ref = await stub.aio_run_no_wait(input=TaskInput(user_id=1234))
    workflow_run_id = workflow_run_ref.workflow_run_id
    print("Triggered workflow run:", workflow_run_id)

    final_status = await wait_for_terminal_status(hatchet, workflow_run_id)

    if final_status == V1TaskStatus.COMPLETED:
        result = await workflow_run_ref.aio_result()
        print("Is successful:", result.ok)
    else:
        details = await hatchet.runs.aio_get(workflow_run_id)
        print("Run ended with status:", details.run.status)


async def main_no_wait_stream() -> None:
    hatchet = Hatchet()

    stub = hatchet.stubs.task(
        name="externally-triggered-task",
        input_validator=TaskInput,
        output_validator=TaskOutput,
    )

    workflow_run_ref = await stub.aio_run_no_wait(input=TaskInput(user_id=1234))
    workflow_run_id = workflow_run_ref.workflow_run_id
    print("Triggered workflow run:", workflow_run_id)

    final_status = await stream_run_events(hatchet, workflow_run_id)

    if final_status == V1TaskStatus.COMPLETED:
        result = await workflow_run_ref.aio_result()
        print("Is successful:", result.ok)
    else:
        details = await hatchet.runs.aio_get(workflow_run_id)
        print("Run ended with status:", details.run.status)


if __name__ == "__main__":
    # asyncio.run(main())
    # asyncio.run(main_no_wait())
    asyncio.run(main_no_wait_stream())
