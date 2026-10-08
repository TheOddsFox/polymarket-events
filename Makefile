# Operator entry points. Each target runs one stage under the run lock and exits non-zero on
# failure. Targets that touch Dagster use the project-local DAGSTER_HOME.

UV ?= uv
RUN := $(UV) run --locked
CATALOGUE := $(RUN) catalogue
export DAGSTER_HOME := $(CURDIR)/.state/dagster_home

.PHONY: sync lint test dagster-home bootstrap refresh reconcile replay validate publish \
	current status dbt-parse backup verify-backup rebuild

sync:
	$(UV) sync --locked

lint:
	$(RUN) ruff check .
	$(RUN) ruff format --check .

test:
	$(RUN) pytest -q

dagster-home:
	mkdir -p $(DAGSTER_HOME)
	cp ops/dagster.yaml $(DAGSTER_HOME)/dagster.yaml

# First run on an empty project: captures every bootstrap page, loads, builds, publishes.
bootstrap: dagster-home
	$(CATALOGUE) refresh --mode bootstrap

# Daily incremental refresh: open events plus recently changed events.
refresh: dagster-home
	$(CATALOGUE) refresh --mode daily

# Weekly full reconcile: archived events and by-ID checks for open events.
reconcile: dagster-home
	$(CATALOGUE) refresh --mode reconcile

# Load pending raw pages, build, and publish without calling Gamma.
replay:
	$(CATALOGUE) replay

validate:
	$(CATALOGUE) validate

publish:
	$(CATALOGUE) publish

current:
	$(CATALOGUE) current

status:
	$(CATALOGUE) status

dbt-parse:
	$(CATALOGUE) dbt -- parse

# Point-in-time backup of ledger, warehouse, raw pages, releases, and dlt state.
backup:
	$(CATALOGUE) backup create

# Usage: make verify-backup BACKUP=data/backups/20261008T060000Z
verify-backup:
	$(CATALOGUE) backup verify $(BACKUP)

# Rebuild from raw in a scratch area and compare table fingerprints with the live warehouse.
rebuild:
	$(CATALOGUE) rebuild --verify
