# Oneshell means one can run multiple lines in a recipe in the same shell, so one doesn't have to
# chain commands together with semicolon
.ONESHELL:
SHELL=/bin/bash

.PHONY: run

TASK_NAME ?= externally-triggered-task
INPUT_JSON ?= {}

help:
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) |\
		 awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m\
		 %s\n", $$1, $$2}'

# If .env file exists, include it and export its variables
ifeq ($(shell test -f .env && echo 1),1)
    include .env
    export
endif

run-local: ## Run the local
	uv run src/hatchet_playground/run_local.py

run-worker: ## Run the worker
	uv run src/hatchet_playground/worker.py

run-external: ## Trigger any task externally: make run-external TASK_NAME=say_hello INPUT_JSON='{"name":"Hatchet"}'
	uv run src/hatchet_playground/run_external.py --task-name "$(TASK_NAME)" --input-json '$(INPUT_JSON)'

run-external-list-tasks: ## List task names configured for the external runner
	uv run src/hatchet_playground/run_external.py --task-name externally-triggered-task --list-tasks

run-external-trigger: ## Trigger externally-triggered-task
	uv run src/hatchet_playground/run_external.py --task-name externally-triggered-task --input-json '{"user_id":1234}'

run-worker-sync: ## Run the sync worker
	uv run src/hatchet_playground/worker_sync.py

run-sync-trigger: ## Trigger the sync cpu-bound workflow
	uv run src/hatchet_playground/run_external.py --task-name sync-sleep-task

run-sync-process-pool-trigger: ## Trigger the cpu process-pool workflow
	uv run src/hatchet_playground/run_external.py --task-name cpu-heavy-with-process-pool

run-external-trigger-stream: ## Trigger externally-triggered-task and stream events
	uv run src/hatchet_playground/run_external.py --task-name externally-triggered-task --input-json '{"user_id":1234}' --stream
