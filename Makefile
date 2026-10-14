UV ?= uv
RUN := $(UV) run --locked
CATALOGUE := $(RUN) catalogue
export CATALOGUE_ROOT ?= $(CURDIR)/data/catalogue-v3

.PHONY: sync lint test test-dev init refresh reconcile publish current status verify backup verify-backup restore-backup import-release bootstrap
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
backup:
	$(CATALOGUE) backup create
verify-backup:
	$(CATALOGUE) backup verify "$(BACKUP)"
restore-backup:
	$(CATALOGUE) backup restore "$(BACKUP)" --destination "$(DESTINATION)"
import-release:
	$(CATALOGUE) import-release "$(RELEASE)" --manifest-sha256 "$(MANIFEST_SHA256)" --raw-root "$(RAW_ROOT)"
bootstrap:
	@echo 'Historical bootstrap retired. Use init and probe for discovery, or import-release for a verified v2 seed; preserve the legacy runtime/root.'
	@exit 2
