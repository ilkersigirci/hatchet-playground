from hatchet_schemas import SayHelloInput, SayHelloOutput
from hatchet_sdk import Context

from hatchet_playground.hatchet_client import hatchet


@hatchet.task(input_validator=SayHelloInput)
def say_hello(input: SayHelloInput, ctx: Context) -> SayHelloOutput:
    return SayHelloOutput(message=f"Hello, {input.name}!")
