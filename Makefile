UV ?= uv
RUN := $(UV) run --locked
CATALOGUE := $(RUN) catalogue
export CATALOGUE_ROOT ?= $(CURDIR)/data/catalogue-v3

.PHONY: sync lint test test-dev init refresh reconcile publish current status verify
sync:
	$(UV) sync --locked
lint:
	$(RUN) ruff check .
	$(RUN) ruff format --check .
test:
	$(RUN) pytest -q
test-dev: lint test
init:
	$(CATALOGUE) init
refresh:
	$(CATALOGUE) refresh
reconcile:
	$(CATALOGUE) reconcile
publish:
	$(CATALOGUE) publish
current:
	$(CATALOGUE) current
status:
	$(CATALOGUE) status
verify:
	$(CATALOGUE) verify
