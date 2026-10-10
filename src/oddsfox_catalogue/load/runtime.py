"""The catalogue owns dlt's process runtime and keeps it offline under its data root."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import DictionaryProvider
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.runtime.run_context import RunContext
from dlt.common.runtime.telemetry import stop_telemetry

_RUNTIME_LOCK = threading.RLock()


class CatalogueRunContext(RunContext):
    """Picklable root-bound context; ambient dlt configuration is never consulted."""

    def __init__(self, pipelines_dir: Path):
        self.pipelines_dir = Path(pipelines_dir).absolute()
        self.runtime_dir = self.pipelines_dir / "_runtime"
        super().__init__(str(self.runtime_dir))

    @property
    def run_dir(self) -> str:
        return str(self.runtime_dir)

    @property
    def local_dir(self) -> str:
        return self.run_dir

    @property
    def global_dir(self) -> str:
        return str(self.runtime_dir / "global")

    @property
    def settings_dir(self) -> str:
        return str(self.runtime_dir / "settings")

    @property
    def data_dir(self) -> str:
        return str(self.pipelines_dir)

    @property
    def module(self):
        return None

    def initial_providers(self):
        provider = DictionaryProvider()
        for name in ("enable_airflow_secrets", "enable_google_secrets", "enable_aws_secrets"):
            provider.set_value(name, False, None, "providers")
        provider.set_value("dlthub_telemetry", False, None, "runtime")
        provider.set_value("enable_runtime_trace", False, None)
        for section in ("extract", "normalize", "load"):
            provider.set_value("workers", 1, None, section)
        provider.set_value("max_parallel_items", 1, None, "extract")
        return [provider]

    def initialize_runtime(self, runtime_config: RuntimeConfiguration | None = None) -> None:
        # Reapply on worker restoration too; explicit false cannot stop an existing tracker.
        stop_telemetry()
        super().initialize_runtime(
            RuntimeConfiguration(
                dlthub_telemetry=False,
                dlthub_telemetry_endpoint=None,
                sentry_dsn=None,
                dlthub_dsn=None,
                slack_incoming_hook=None,
                config_files_storage_path=str(self.runtime_dir / "config"),
            )
        )


@contextmanager
def confined_runtime(pipelines_dir: Path) -> Iterator[None]:
    """Serialize root changes across complete loads and retain a safe context afterward."""
    with _RUNTIME_LOCK:
        container = Container()
        # Container.get creates missing defaults, which would initialize ambient telemetry.
        active = container[PluggableRunContext] if PluggableRunContext in container else None  # noqa: SIM401
        root = Path(pipelines_dir).absolute()
        if not (
            active is not None
            and isinstance(active.context, CatalogueRunContext)
            and active.context.pipelines_dir == root
        ):
            active = PluggableRunContext(CatalogueRunContext(root))
            # Set explicitly without creating a default context or restoring ambient settings.
            container[PluggableRunContext] = active
        with container.injectable_context(active, lock_context_on_yield=True):
            yield
