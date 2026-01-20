.PHONY: install up 

# Default target
.DEFAULT_GOAL := up

up:
	$(MAKE) -j1 dagster

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

dagster: ## Run Dagster UI locally (no Docker, no code-server)
	@echo "🧠 Starting Dagster..."
	@set -a && [ -f .env ] && . .env || true && set +a && \
	  export DAGSTER_HOME=$$(pwd) && \
	  .venv/bin/dagster dev -h 0.0.0.0 -p 8091 


clean: ## Remove build artifacts (App + PocketBase + Dagster)
	@echo "🧹 Cleaning build artifacts..."
	@rm -rf .venv dagster_home __pycache__
	@echo "✅ Clean complete"
