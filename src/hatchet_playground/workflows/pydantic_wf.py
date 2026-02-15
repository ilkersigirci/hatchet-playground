from hatchet_sdk import Context

from hatchet_playground.hatchet_client import hatchet
from hatchet_playground.workflows.schemas import (
    SayHelloInput as Input,
    SayHelloOutput as Output,
)


@hatchet.task(input_validator=Input)
def say_hello(input: Input, ctx: Context) -> Output:
    return Output(message=f"Hello, {input.name}!")
