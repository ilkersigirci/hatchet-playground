from hatchet_sdk import Hatchet
from hatchet_sdk.opentelemetry.instrumentor import HatchetInstrumentor
from langfuse import Langfuse
from opentelemetry.trace import get_tracer_provider

## Note: Langfuse sets the global tracer provider
lf = Langfuse()

HatchetInstrumentor(tracer_provider=get_tracer_provider()).instrument()

# Initialize Hatchet client
hatchet = Hatchet(debug=False)
