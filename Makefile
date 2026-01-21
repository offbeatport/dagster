.PHONY: install up run-job 

# Default target
.DEFAULT_GOAL := up

RUN_JOB_ARG := $(word 2,$(MAKECMDGOALS))
JOB := $(or $(JOB),$(RUN_JOB_ARG))
ifneq ($(strip $(RUN_JOB_ARG)),)
$(eval $(RUN_JOB_ARG): ;)
.PHONY: $(RUN_JOB_ARG)
endif

define print-job-list
	@set -a && [ -f .env ] && . .env || true && set +a && \
	  export DAGSTER_HOME=$$(pwd) && \
	  .venv/bin/python - <<'PY'
from burningdemand import definitions
jobs = sorted(job.name for job in definitions.defs.jobs)
for name in jobs:
    print(f"- {name}")
PY
endef

up:
	$(MAKE) -j1 serve

## -----------------------------
## Dagster 
## -----------------------------

install: ## Install Dagster dependencies
	@echo "📦 Installing Dagster deps..."
	@python3 -m venv .venv
	@.venv/bin/pip install \
		--progress-bar=on \
		--disable-pip-version-check \
		--quiet \
		-r requirements.txt
	@mkdir -p dagster_home
	@echo "✅ Dagster venv ready: .venv"

serve: ## Run Dagster UI locally (no Docker, no code-server)
	@echo "🧠 Starting Dagster..."
	@set -a && [ -f .env ] && . .env || true && set +a && \
	  export DAGSTER_HOME=$$(pwd)/dagster_home && \
	  .venv/bin/dagster dev -h 0.0.0.0 -p 8091



clean: ## Remove build artifacts (App + PocketBase + Dagster)
	@echo "🧹 Cleaning build artifacts..."
	@rm -rf  dagster_home __pycache__
	@echo "✅ Clean complete"
