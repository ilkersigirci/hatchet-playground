from hatchet_sdk import Context, EmptyModel

from hatchet_playground.hatchet_client import hatchet


@hatchet.task(name="first-workflow")
def my_task(input: EmptyModel, ctx: Context) -> dict[str, int]:
    print("executed task")

    return {"meaning_of_life": 42}
