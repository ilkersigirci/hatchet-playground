from dataclasses import dataclass

from pydantic import BaseModel


class ExternallyTriggeredTaskInput(BaseModel):
    user_id: int


class ExternallyTriggeredTaskOutput(BaseModel):
    ok: bool


@dataclass
class SayHelloInput:
    name: str


@dataclass
class SayHelloOutput:
    message: str
