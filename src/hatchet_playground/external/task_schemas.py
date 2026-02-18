from typing import Any

from hatchet_schemas import (
    ChatOtelInput,
    ChatOtelOutput,
    ExternallyTriggeredTaskInput,
    ExternallyTriggeredTaskOutput,
    SayHelloInput,
    SayHelloOutput,
)
from hatchet_sdk import EmptyModel
from pydantic import BaseModel, ConfigDict


class TaskSchema(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

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
    "chat-otel": TaskSchema(
        input_validator=ChatOtelInput,
        output_validator=ChatOtelOutput,
    ),
}


def resolve_task_schema(task_name: str) -> TaskSchema:
    return TASK_SCHEMAS.get(task_name, DEFAULT_TASK_SCHEMA)
