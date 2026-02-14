import asyncio

from hatchet_playground.workflows.first_wf import my_task
from hatchet_playground.workflows.pydantic_wf import Input, say_hello


async def first_wf_main() -> None:
    result = await my_task.aio_run()

    print(
        "Finished running task, and got the meaning of life! The meaning of life is:",
        result["meaning_of_life"],
    )


async def pydantic_wf_main() -> None:
    result = await say_hello.aio_run(input=Input(name="Hatchet"))

    print(
        "Finished running task, and got the message! The message is:",
        result.message,
    )


if __name__ == "__main__":
    asyncio.run(pydantic_wf_main())
