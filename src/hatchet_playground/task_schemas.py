from dataclasses import dataclass
from typing import Any

from hatchet_sdk import EmptyModel

from hatchet_playground.workflows.schemas import (
    ExternallyTriggeredTaskInput,
    ExternallyTriggeredTaskOutput,
    SayHelloInput,
    SayHelloOutput,
)


@dataclass(frozen=True)
class TaskSchema:
    input_validator: type[Any]
    output_validator: type[Any] | None = None


DEFAULT_TASK_SCHEMA = TaskSchema(input_validator=EmptyModel)

TASK_SCHEMAS: dict[str, TaskSchema] = {
    "externally-triggered-task": TaskSchema(
        input_validator=ExternallyTriggeredTaskInput,
        output_validator=ExternallyTriggeredTaskOutput,
    ),
    "first-workflow": TaskSchema(input_validator=EmptyModel),
    "say_hello": TaskSchema(
        input_validator=SayHelloInput,
        output_validator=SayHelloOutput,
    ),
    "sync-sleep-task": TaskSchema(input_validator=EmptyModel),
    "cpu-heavy-with-process-pool": TaskSchema(input_validator=EmptyModel),
}


def resolve_task_schema(task_name: str) -> TaskSchema:
    return TASK_SCHEMAS.get(task_name, DEFAULT_TASK_SCHEMA)
