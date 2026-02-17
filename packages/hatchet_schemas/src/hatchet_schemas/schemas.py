from dataclasses import dataclass
from typing import Literal

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


class ChatOtelInput(BaseModel):
    question: str
    model: Literal["gpt-4o-mini", "gpt-4o", "gpt-4.1-mini", "gpt-4.1"] = "gpt-4o-mini"
    system_prompt: str = "You are a helpful assistant."


class ChatOtelOutput(BaseModel):
    answer: str | None
