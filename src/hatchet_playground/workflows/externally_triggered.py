from hatchet_sdk import Context, Hatchet

from hatchet_playground.workflows.schemas import (
    ExternallyTriggeredTaskInput as TaskInput,
    ExternallyTriggeredTaskOutput as TaskOutput,
)

hatchet = Hatchet()


@hatchet.task(name="externally-triggered-task", input_validator=TaskInput)
async def externally_triggered_task(input: TaskInput, ctx: Context) -> TaskOutput:
    return TaskOutput(ok=True)
