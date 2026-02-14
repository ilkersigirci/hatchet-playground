from dataclasses import dataclass

from hatchet_sdk import Context

from hatchet_playground.hatchet_client import hatchet


@dataclass
class Input:
    name: str


@dataclass
class Output:
    message: str


@hatchet.task(input_validator=Input)
def say_hello(input: Input, ctx: Context) -> Output:
    return Output(message=f"Hello, {input.name}!")
