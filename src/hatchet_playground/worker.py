from hatchet_playground.hatchet_client import hatchet
from hatchet_playground.workflows.chat_otel import langfuse_task
from hatchet_playground.workflows.externally_triggered import externally_triggered_task
from hatchet_playground.workflows.first_wf import my_task
from hatchet_playground.workflows.pydantic_wf import say_hello


def main() -> None:
    worker = hatchet.worker(
        name="test-worker",
        slots=100,
        workflows=[my_task, say_hello, externally_triggered_task, langfuse_task],
    )
    worker.start()


if __name__ == "__main__":
    main()
