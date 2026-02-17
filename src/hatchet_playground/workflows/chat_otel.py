import os

from hatchet_schemas import ChatOtelInput, ChatOtelOutput
from hatchet_sdk import Context
from langfuse.openai import AsyncOpenAI

from hatchet_playground.hatchet_client import hatchet

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", None)

if OPENAI_API_KEY is None:
    raise ValueError("OPENAI_API_KEY environment variable is not set.")


@hatchet.task(name="chat-otel", input_validator=ChatOtelInput)
async def langfuse_task(input: ChatOtelInput, ctx: Context) -> ChatOtelOutput:
    openai = AsyncOpenAI(api_key=OPENAI_API_KEY)

    # Usage, cost, etc. of this call will be sent to Langfuse.
    generation = await openai.chat.completions.create(
        model=input.model,
        messages=[
            {"role": "system", "content": input.system_prompt},
            {"role": "user", "content": input.question},
        ],
    )

    return ChatOtelOutput(answer=generation.choices[0].message.content)
