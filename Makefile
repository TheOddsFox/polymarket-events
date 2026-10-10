# Operator entry points. Each target runs one stage under the run lock and exits non-zero on
# failure. Targets that touch Dagster use the project-local DAGSTER_HOME.

UV ?= uv
RUN := $(UV) run --locked
CATALOGUE := $(RUN) catalogue
export CATALOGUE_ROOT ?= $(CURDIR)
export DAGSTER_HOME ?= $(CATALOGUE_ROOT)/.state/dagster_home

.PHONY: sync lint test test-dev dagster-home bootstrap refresh reconcile replay validate publish \
	current status verify dbt-parse backup verify-backup restore-backup rebuild

sync:
	$(UV) sync --locked

lint:
	$(RUN) ruff check .
	$(RUN) ruff format --check .

test:
	$(RUN) pytest -q

# Fast loop for repo-change-review: lint, then the full test suite.
test-dev: lint test

dagster-home:
	mkdir -p "$(DAGSTER_HOME)"
	cp ops/dagster.yaml "$(DAGSTER_HOME)/dagster.yaml"

# First run on an empty project: captures every bootstrap page, loads, builds, publishes.
bootstrap:
	$(CATALOGUE) refresh --mode bootstrap

# Daily refresh: direct open markets/events and missing previously open or unknown records.
refresh:
	$(CATALOGUE) refresh --mode daily

# Weekly full pass: every event and every closed market by id range, open markets by keyset, then by-ID fetches for referenced events those scans did not return.
reconcile:
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

verify:
	$(CATALOGUE) verify

dbt-parse:
	$(CATALOGUE) dbt -- parse

# Point-in-time backup of ledger, warehouse, raw pages, releases, and dlt state.
backup:
	$(CATALOGUE) backup create

# Usage: make verify-backup BACKUP=data/backups/20261008T060000Z
verify-backup:
	$(CATALOGUE) backup verify "$(BACKUP)"

# Usage: make restore-backup BACKUP=... DESTINATION=/absolute/path/to/fresh/root
restore-backup:
	$(CATALOGUE) backup restore "$(BACKUP)" --destination "$(DESTINATION)"

# Rebuild from raw in a scratch area and compare table fingerprints with the live warehouse.
rebuild:
	$(CATALOGUE) rebuild --verify
