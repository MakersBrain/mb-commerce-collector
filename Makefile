# Everything CI runs, runnable by hand in the same way.
#
# The point of a Makefile here rather than a list of commands in a README is
# that the build and the person get the same behaviour out of one definition.
# `make check` is what a change has to pass.

DUMP     := catalogue-dump
SCRAPER  := commerce-scraper
CONTROL  := catalogue-control
SERVICE  := catalogue-service
EXPLORER := catalogue-explorer

# `--directory` also changes the working directory, so every path below is
# relative to the project it names. VIRTUAL_ENV is cleared because an activated
# environment elsewhere in the tree is not this project's, and uv would
# otherwise warn about it on every single invocation.
UV       := VIRTUAL_ENV= uv --directory $(DUMP)
RUN      := $(UV) run --
UVSCRAPER := VIRTUAL_ENV= uv --directory $(SCRAPER)
RUNSCRAPER := $(UVSCRAPER) run --extra dev --
UVC      := VIRTUAL_ENV= uv --directory $(CONTROL)
RUNC     := $(UVC) run --
UVS      := VIRTUAL_ENV= uv --directory $(SERVICE)
RUNS     := $(UVS) run --

.DEFAULT_GOAL := help

.PHONY: help
help:  ## List the targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: install
install:  ## Sync every project's virtualenv, including dev groups
	$(UVSCRAPER) sync --extra dev
	$(UV) sync --all-groups
	$(UVC) sync --all-groups
	$(UVS) sync --all-groups
	cd $(EXPLORER) && npm install

.PHONY: lint
lint:  ## ruff, across all three Python projects
	$(RUNSCRAPER) ruff check .
	$(RUN) ruff check .
	$(RUNC) ruff check .
	$(RUNS) ruff check .

.PHONY: format
format:  ## ruff, fixing what it can
	$(RUNSCRAPER) ruff check --fix .
	$(RUN) ruff check --fix .
	$(RUNC) ruff check --fix .
	$(RUNS) ruff check --fix .

.PHONY: typecheck
typecheck:  ## mypy, and svelte-check for the explorer
	$(RUNSCRAPER) mypy
	$(RUN) mypy
	$(RUNC) mypy
	$(RUNS) mypy
	cd $(EXPLORER) && npm run check

.PHONY: test
test:  ## The fast suites: no network, no database, no cache replay
	$(RUNSCRAPER) pytest
	$(RUN) pytest
	$(RUNC) pytest
	$(RUNS) pytest
	cd $(EXPLORER) && npm test

.PHONY: test-golden
test-golden:  ## Replay every cached source and compare against its frozen dump
	$(RUN) pytest -m golden

.PHONY: cache-pull
cache-pull:  ## Fetch the recorded response cache the golden tests replay
	$(RUN) catalogue-cache-archive pull --force

.PHONY: cache-push
cache-push:  ## Publish the local cache and update cache-archive.json
	$(RUN) catalogue-cache-archive push

.PHONY: golden-update
golden-update:  ## Rewrite the frozen dumps. Review the diff; it is the change.
	$(RUN) pytest -m golden --update-golden

# A throwaway server on a port of its own, so a run cannot touch the development
# database on 5434. The tests drop and recreate the `catalogue` schema, which is
# not something to point at anything that matters.
PGTEST_PORT ?= 55432
PGTEST_DSN  ?= postgresql://postgres:postgres@127.0.0.1:$(PGTEST_PORT)/postgres
NATSTEST_PORT ?= 54222
NATSTEST_TOKEN ?= catalogue-test
NATSTEST_URL ?= nats://127.0.0.1:$(NATSTEST_PORT)

.PHONY: pg-up
pg-up:  ## Start the throwaway PostgreSQL the database tests need
	@docker run -d --rm --name catalogue-pgtest \
	  -e POSTGRES_PASSWORD=postgres -p 127.0.0.1:$(PGTEST_PORT):5432 \
	  postgres:17-alpine >/dev/null
	@until docker exec catalogue-pgtest pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
	@echo "catalogue-pgtest listening on $(PGTEST_PORT)"

.PHONY: pg-down
pg-down:  ## Stop it
	@docker stop catalogue-pgtest >/dev/null 2>&1 || true

.PHONY: test-postgres
test-postgres:  ## Database-backed tests: the queue, edges, run closure, the API
	CATALOGUE_TEST_DSN=$(PGTEST_DSN) $(RUN) pytest -m postgres
	CATALOGUE_TEST_DSN=$(PGTEST_DSN) $(RUNC) pytest -m postgres

.PHONY: nats-up
nats-up:  ## Start the throwaway JetStream server the delivery tests need
	@docker run -d --rm --name catalogue-natstest \
	  -p 127.0.0.1:$(NATSTEST_PORT):4222 nats:2.11-alpine \
	  --jetstream --store_dir=/tmp/nats --auth $(NATSTEST_TOKEN) >/dev/null
	@until docker logs catalogue-natstest 2>&1 | grep -q "Server is ready"; do sleep 1; done
	@echo "catalogue-natstest listening on $(NATSTEST_PORT)"

.PHONY: nats-down
nats-down:  ## Stop the throwaway JetStream server
	@docker stop catalogue-natstest >/dev/null 2>&1 || true

.PHONY: test-nats
test-nats:  ## Broker-backed publish, pull, acknowledgement and outbox tests
	CATALOGUE_TEST_NATS_URL=$(NATSTEST_URL) CATALOGUE_TEST_NATS_TOKEN=$(NATSTEST_TOKEN) \
	  $(RUN) pytest -m nats

# -- generated contracts -----------------------------------------------------
#
# Never hand-edit the generated documents. Change the Pydantic registries, run
# `make openapi`, and commit the diff — which is what makes an API change
# visible in the review of the pull request that makes it.

.PHONY: openapi
openapi:  ## Regenerate both OpenAPI documents and the explorer's TypeScript
	$(RUNS) catalogue-openapi
	$(RUNC) catalogue-ops-openapi
	$(RUNC) catalogue-ops-types

.PHONY: openapi-check
openapi-check:  ## Fail if a generated contract has drifted from the code
	$(RUNS) catalogue-openapi --check
	$(RUNC) catalogue-ops-openapi --check
	$(RUNC) catalogue-ops-types --check

.PHONY: check
check: lint typecheck test openapi-check scraper-build scraper-contracts  ## What every change has to pass

.PHONY: scraper-lint scraper-typecheck scraper-test scraper-schemas scraper-build scraper-contracts scraper-example scraper-release-check scraper-check
scraper-lint:  ## Lint the reusable scraper distribution
	$(RUNSCRAPER) ruff check .
scraper-typecheck:  ## Type-check the reusable scraper distribution
	$(RUNSCRAPER) mypy
scraper-test:  ## Run scraper unit and conformance tests
	$(RUNSCRAPER) pytest
scraper-schemas:  ## Verify frozen public schemas and representative payloads
	$(RUNSCRAPER) python scripts/generate_schemas.py --check
scraper-build:  ## Build wheel and source distribution
	$(RUNSCRAPER) python -m build
scraper-contracts: scraper-build  ## Run dependency, clean-import, and installed-wheel contract tests
	$(RUNSCRAPER) pytest tests/test_boundaries.py
	$(RUNSCRAPER) python scripts/verify_wheel.py
scraper-example: scraper-build  ## Install and exercise the external connector example
	$(RUNSCRAPER) python scripts/verify_custom_connector.py
scraper-release-check: scraper-build  ## Verify changelog, source, and artifact versions
	$(RUNSCRAPER) python scripts/verify_release.py
scraper-check: scraper-lint scraper-typecheck scraper-test scraper-schemas scraper-build scraper-contracts scraper-example scraper-release-check  ## All scraper gates

.PHONY: check-all
check-all: check test-golden  ## check, the replay suite, and the database suite
	@$(MAKE) pg-up
	@$(MAKE) nats-up
	@CATALOGUE_TEST_NATS_URL=$(NATSTEST_URL) CATALOGUE_TEST_NATS_TOKEN=$(NATSTEST_TOKEN) \
	  $(MAKE) test-postgres test-nats; status=$$?; $(MAKE) pg-down; $(MAKE) nats-down; exit $$status
