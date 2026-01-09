DC = docker compose
PRE_COMMIT = pre-commit

.PHONY: help up down restart build logs lint

up: ## start containers
	$(DC) up -d

down: ## stop containers
	$(DC) down

build:
	$(DC) build --no-cache

restart: down up ## full restart

rebuild: down build up ## full rebuild and restart

logs: ## watch logs of airflow in realtime
	$(DC) logs -f webserver

lint: ## start all linters
	$(PRE_COMMIT) run --all-files

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
