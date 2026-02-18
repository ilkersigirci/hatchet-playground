import os
from dataclasses import asdict, is_dataclass
from typing import Any

from fastapi import FastAPI, HTTPException
from hatchet_sdk import Hatchet
from hatchet_sdk.opentelemetry.instrumentor import HatchetInstrumentor
from langfuse import Langfuse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.trace import get_tracer_provider
from pydantic import BaseModel, Field

from hatchet_playground.external.runner import ExternalTaskRunner
from hatchet_playground.external.task_schemas import TASK_SCHEMAS

# NOTE: Langfuse must be initialized before Hatchet and FastAPI object creation to ensure proper tracing integration.
lf = Langfuse()

HatchetInstrumentor(tracer_provider=get_tracer_provider()).instrument()

hatchet = Hatchet(debug=False)
app = FastAPI(title="Hatchet External Trigger API", version="0.1.0")
FastAPIInstrumentor.instrument_app(app, tracer_provider=get_tracer_provider())


class TriggerTaskRequest(BaseModel):
    input_payload: dict[str, Any] = Field(default_factory=dict)
    wait_for_completion: bool = False
    poll_interval_seconds: float = Field(default=1.0, ge=0.1, le=30.0)


class TriggerTaskResponse(BaseModel):
    workflow_run_id: str
    status: str | None = None
    result: Any | None = None


class RunStatusResponse(BaseModel):
    workflow_run_id: str
    status: str


def _serialize_result(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump()
    if is_dataclass(value):
        return asdict(value)
    return value


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/tasks")
async def list_tasks() -> dict[str, list[str]]:
    return {"tasks": sorted(TASK_SCHEMAS.keys())}


@app.post("/tasks/{task_name}/run", response_model=TriggerTaskResponse)
async def run_task(task_name: str, request: TriggerTaskRequest) -> TriggerTaskResponse:
    runner = ExternalTaskRunner(
        task_name=task_name,
        input_payload=request.input_payload,
        hatchet=hatchet,
    )

    try:
        workflow_run_ref = await runner.trigger_no_wait()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not request.wait_for_completion:
        return TriggerTaskResponse(workflow_run_id=workflow_run_ref.workflow_run_id)

    final_status = await runner.wait_for_terminal_status(
        workflow_run_id=workflow_run_ref.workflow_run_id,
        poll_interval_seconds=request.poll_interval_seconds,
    )

    if final_status.value != "COMPLETED":
        return TriggerTaskResponse(
            workflow_run_id=workflow_run_ref.workflow_run_id,
            status=final_status.value,
        )

    result = await workflow_run_ref.aio_result()
    return TriggerTaskResponse(
        workflow_run_id=workflow_run_ref.workflow_run_id,
        status=final_status.value,
        result=_serialize_result(result),
    )


@app.get("/runs/{workflow_run_id}/status", response_model=RunStatusResponse)
async def run_status(workflow_run_id: str) -> RunStatusResponse:
    status = await hatchet.runs.aio_get_status(workflow_run_id)
    return RunStatusResponse(workflow_run_id=workflow_run_id, status=status.value)


if __name__ == "__main__":
    uvicorn_host = os.getenv("FASTAPI_HOST", "0.0.0.0")
    uvicorn_port = int(os.getenv("FASTAPI_PORT", "8000"))

    import uvicorn

    uvicorn.run(app, host=uvicorn_host, port=uvicorn_port)
