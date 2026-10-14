"""Payload-free monotonic measurements; never part of semantic evidence."""

import logging
from contextlib import contextmanager
from time import perf_counter

logger = logging.getLogger(__name__)


@contextmanager
def phase_timing(name):
    started = perf_counter()
    try:
        yield
    finally:
        logger.info("pipeline_phase name=%s elapsed_s=%.6f", name, perf_counter() - started)


def record_counter(name, value):
    logger.info("pipeline_counter name=%s value=%s", name, value)
