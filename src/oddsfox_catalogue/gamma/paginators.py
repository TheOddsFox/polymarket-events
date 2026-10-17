"""Response-shape decoding and bounded ID-window request parameters for Gamma list endpoints."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from oddsfox_catalogue.gamma.http import MalformedResponse


def unpack(body: Any, record_key: str) -> tuple[list[Any], str | None]:
    """Return ``(records, next_cursor)`` for any documented Gamma response shape.

    Shapes:
      * keyed list:    ``{"<record_key>": [...], "next_cursor": "..."}``
      * offset list:   ``[...]``
      * single object: ``{"id": ..., ...}`` (for ``/events/{id}``)
    """
    if isinstance(body, list):
        records, next_cursor = body, None
    elif isinstance(body, dict) and record_key in body:
        records = body[record_key]
        next_cursor = body.get("next_cursor")
        if next_cursor is not None and not isinstance(next_cursor, str):
            raise MalformedResponse("next_cursor must be a string")
        if not isinstance(records, list):
            raise MalformedResponse(f"{record_key} must be a list")
    elif isinstance(body, dict) and "id" in body:
        records, next_cursor = [body], None
    else:
        raise MalformedResponse(f"unrecognised response shape (expected {record_key!r})")

    # Record-level validity (objects with an id) is checked at load time, where
    # invalid records are quarantined instead of failing the whole page.
    return records, next_cursor


def _window_params(
    ids: list[int],
    record_key: str,
    closed: bool | None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": len(ids), "id": ids}
    if closed is not None:
        params["closed"] = closed
    if record_key == "markets":
        params["include_tag"] = True
    if extra:
        params.update(extra)
    return params
