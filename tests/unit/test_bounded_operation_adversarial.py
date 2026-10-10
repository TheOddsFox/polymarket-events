"""Untrusted transport and retained evidence must remain finite and confined."""

from __future__ import annotations

import gzip
import json
import os
from dataclasses import replace

import httpx
import pytest

from fakes.harness import FakeClock
from oddsfox_catalogue.capture.writer import (
    manifest_path,
    read_body,
    read_manifest,
    read_regular_bytes,
    verify_page,
    write_page,
)
from oddsfox_catalogue.config import GammaSettings, load_settings
from oddsfox_catalogue.gamma.http import (
    GammaClient,
    GammaError,
    MalformedResponse,
    RequestBudgetExceeded,
    RetriesExhausted,
)
from oddsfox_catalogue.limits import enforce_storage_limits


def _page(root, body=b"{}"):
    directory = root / "scan"
    write_page(directory, 1, body, {"page_id": "page", "http_status": 200})
    manifest = read_manifest(manifest_path(directory, 1))
    assert manifest is not None
    return directory, manifest


def _client(handler, *, clock=None, settings=None, **bounds):
    clock = clock or FakeClock()
    settings = settings or GammaSettings(
        base_url="https://gamma.fake.test", requests_per_second=1000, max_retries=2
    )
    return GammaClient(
        settings,
        transport=httpx.MockTransport(handler),
        clock=clock,
        sleep=clock.sleep,
        **bounds,
    )


@pytest.mark.parametrize("path_kind", ["parent", "absolute", "nested"])
def test_valid_checksums_do_not_authorize_unsafe_payload_paths(tmp_path, path_kind):
    directory, manifest = _page(tmp_path)
    original = directory / manifest["file"]
    outside = tmp_path / "outside.json.gz"
    outside.write_bytes(original.read_bytes())
    nested = directory / "nested"
    nested.mkdir()
    (nested / "inside.json.gz").write_bytes(original.read_bytes())
    filename = {
        "parent": "../outside.json.gz",
        "absolute": str(outside),
        "nested": "nested/inside.json.gz",
    }[path_kind]
    unsafe = {**manifest, "file": filename}
    assert verify_page(directory, unsafe) is False
    with pytest.raises(ValueError):
        read_body(directory, unsafe)


def test_payload_symlink_with_matching_bytes_is_rejected(tmp_path):
    directory, manifest = _page(tmp_path)
    payload = directory / manifest["file"]
    outside = tmp_path / "outside.json.gz"
    payload.rename(outside)
    payload.symlink_to(outside)
    assert verify_page(directory, manifest) is False
    with pytest.raises(ValueError):
        read_body(directory, manifest)


def test_manifest_symlink_is_not_a_committed_manifest(tmp_path):
    directory, _ = _page(tmp_path)
    path = manifest_path(directory, 1)
    outside = tmp_path / "outside.json"
    path.rename(outside)
    path.symlink_to(outside)
    assert read_manifest(path) is None


def test_symlinked_scan_ancestor_is_rejected_even_inside_trusted_root(tmp_path):
    directory, manifest = _page(tmp_path)
    alias = tmp_path / "alias"
    alias.symlink_to(directory, target_is_directory=True)
    assert verify_page(alias, manifest, trusted_root=tmp_path) is False
    with pytest.raises(ValueError):
        read_body(alias, manifest, trusted_root=tmp_path)


@pytest.mark.parametrize(
    "changes",
    [
        {"gz_sha256": "0" * 64},
        {"body_sha256": "0" * 64},
        {"body_bytes": 1},
        {"body_bytes": True},
        {"body_bytes": -1},
        {"manifest_version": 999},
        {"manifest_version": True},
        {"file": None},
    ],
)
def test_one_raw_reader_checks_compressed_body_size_and_schema(tmp_path, changes):
    directory, manifest = _page(tmp_path)
    tampered = {**manifest, **changes}
    assert verify_page(directory, tampered) is False
    with pytest.raises(ValueError):
        read_body(directory, tampered)


def test_decompressed_size_limit_is_enforced_before_unbounded_allocation(tmp_path, monkeypatch):
    directory, manifest = _page(tmp_path, b"x" * 8192)
    manifest["body_bytes"] = 2

    def unbounded(*_args, **_kwargs):
        raise AssertionError("raw verification cannot use unbounded gzip.decompress")

    monkeypatch.setattr(gzip, "decompress", unbounded)
    assert verify_page(directory, manifest, max_body_bytes=1024) is False
    with pytest.raises(ValueError):
        read_body(directory, manifest, max_body_bytes=1024)


def test_exact_raw_body_size_boundary_is_valid(tmp_path):
    directory, manifest = _page(tmp_path, b"12345")
    assert verify_page(directory, manifest, max_body_bytes=5) is True
    assert read_body(directory, manifest, max_body_bytes=5) == b"12345"
    assert verify_page(directory, manifest, max_body_bytes=4) is False


def test_raw_fifo_is_rejected_before_opening_it(tmp_path):
    path = tmp_path / "fifo"
    os.mkfifo(path)
    with pytest.raises(ValueError):
        read_regular_bytes(path, max_bytes=100, trusted_root=tmp_path)


def test_bounded_regular_reader_uses_actual_file_size_and_confines_root(tmp_path):
    path = tmp_path / "fixture"
    path.write_bytes(b"12345")
    assert read_regular_bytes(path, max_bytes=5, trusted_root=tmp_path) == b"12345"
    with pytest.raises(ValueError):
        read_regular_bytes(path, max_bytes=4, trusted_root=tmp_path)
    other = tmp_path / "other"
    other.mkdir()
    with pytest.raises(ValueError):
        read_regular_bytes(path, max_bytes=5, trusted_root=other)


def test_transport_error_details_never_enter_retry_logs_or_terminal_diagnostics(caplog):
    secret = "synthetic-account-secret"

    def fail(request):
        raise httpx.ConnectError(
            f"https://user:{secret}@proxy.invalid/?token={secret}", request=request
        )

    with _client(fail) as client, pytest.raises(RetriesExhausted) as failure:
        client.get("/markets/1")
    assert secret not in str(failure.value)
    assert secret not in caplog.text


@pytest.mark.parametrize("status", [400, 422, 500])
def test_http_error_bodies_never_enter_diagnostics(status):
    secret = "synthetic-api-secret"
    with _client(lambda _: httpx.Response(status, text=secret)) as client:
        params = {"after_cursor": "opaque"} if status == 422 else {}
        with pytest.raises(GammaError) as failure:
            client.get("/markets/keyset", params)
    assert secret not in str(failure.value)


def test_global_attempt_cap_applies_to_every_retry_and_spawn():
    called = []

    def unavailable(_):
        called.append(1)
        return httpx.Response(503, content=b"abc")

    with _client(unavailable, max_requests=2, max_download_bytes=100) as client:
        with client.spawn() as child, pytest.raises(RequestBudgetExceeded):
            child.get("/markets/1")
        with pytest.raises(RequestBudgetExceeded):
            client.get("/events/1")
        assert client.stats.requests == 2
        assert client.stats.downloaded_bytes == 6
    assert len(called) == 2


def test_partial_stream_and_error_retry_bytes_share_one_allowance():
    class Partial(httpx.SyncByteStream):
        def __iter__(self):
            yield b"abc"
            raise httpx.ReadError("interrupted")

    calls = []

    def response(_):
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(200, stream=Partial())
        return httpx.Response(503, content=b"def")

    with _client(response, max_requests=4, max_download_bytes=5) as client:
        with pytest.raises(RequestBudgetExceeded):
            client.get("/markets/1")
        assert client.stats.requests == 2
        assert client.stats.downloaded_bytes == 6
    assert len(calls) == 2


def test_duration_limit_stops_retry_after_wait_before_another_http_attempt():
    clock = FakeClock()
    calls = []
    with (
        _client(
            lambda _: calls.append(1) or httpx.Response(429, headers={"Retry-After": "60"}),
            clock=clock,
            max_duration_s=2,
        ) as client,
        pytest.raises(RequestBudgetExceeded),
    ):
        client.get("/markets/1")
    assert calls == [1]
    assert clock.t <= 2


def test_spawned_client_cannot_start_a_new_duration_allowance():
    clock = FakeClock()
    calls = []
    with _client(
        lambda _: calls.append(1) or httpx.Response(200, content=b"[]"),
        clock=clock,
        max_duration_s=2,
    ) as client:
        clock.sleep(2)
        with client.spawn() as child, pytest.raises(RequestBudgetExceeded):
            child.get("/markets/1")
        assert client.stats.requests == 0
    assert calls == []


def test_duration_limit_covers_response_streaming():
    clock = FakeClock()

    class SlowBody(httpx.SyncByteStream):
        def __iter__(self):
            yield b"["
            clock.sleep(2)
            yield b"]"

    with _client(
        lambda _: httpx.Response(200, stream=SlowBody()), clock=clock, max_duration_s=1
    ) as client:
        with pytest.raises(RequestBudgetExceeded):
            client.get("/markets/1")
        assert client.stats.requests == 1
        assert client.stats.downloaded_bytes == 2


@pytest.mark.parametrize("body", [b"[]", b"[ ]"])
def test_response_byte_limit_uses_actual_streamed_size(body):
    with _client(lambda _: httpx.Response(200, content=body), max_body_bytes=2) as client:
        if len(body) == 2:
            assert client.get("/markets/1").body == body
        else:
            with pytest.raises(MalformedResponse):
                client.get("/markets/1")
        assert client.stats.downloaded_bytes == len(body)


def test_non_gamma_hosts_and_implicit_loopback_are_rejected_without_network():
    for url in (
        "https://evil.invalid",
        "http://gamma-api.polymarket.com",
        "https://user:secret@gamma-api.polymarket.com",
        "https://gamma-api.polymarket.com/?token=secret",
        "http://127.0.0.1:9",
    ):
        with pytest.raises(ValueError):
            GammaClient(GammaSettings(base_url=url))


def test_loopback_support_requires_explicit_setting():
    settings = replace(GammaSettings(), base_url="http://127.0.0.1:9", allow_loopback=True)
    with GammaClient(settings):
        pass


def test_transport_ignores_ambient_proxy_credentials(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "https://user:synthetic-secret@proxy.invalid")
    with _client(lambda request: httpx.Response(200, json=dict(request.headers))) as client:
        result = client.get("/markets/1")
    headers = json.loads(result.body)
    assert "authorization" not in headers and "proxy-authorization" not in headers


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://evil.invalid/markets/1",
        "//evil.invalid/markets/1",
        "/markets/1?token=secret",
        "/../events",
    ],
)
def test_endpoint_cannot_escape_the_fixed_origin_or_leak_embedded_query(endpoint):
    calls = []
    with _client(lambda _: calls.append(1) or httpx.Response(200, content=b"[]")) as client:
        with pytest.raises(MalformedResponse):
            client.get(endpoint)
        assert client.stats.requests == 0
    assert calls == []


def test_retained_limit_counts_old_releases_and_state_without_counting_nested_warehouse_twice(
    tmp_path,
):
    settings = load_settings(root=tmp_path, env={})
    old_release = settings.published_dir / "releases" / "old" / "retained"
    old_release.parent.mkdir(parents=True)
    old_release.write_bytes(b"1234")
    settings.warehouse_path.parent.mkdir(parents=True)
    settings.warehouse_path.write_bytes(b"5678")
    settings.state_dir.mkdir(parents=True)
    (settings.state_dir / "retained").write_bytes(b"90")
    settings = replace(settings, capture=replace(settings.capture, max_retained_bytes=10))
    enforce_storage_limits(settings)
    with pytest.raises(RequestBudgetExceeded):
        enforce_storage_limits(settings, additional_bytes=1)
    assert old_release.read_bytes() == b"1234"
    assert settings.warehouse_path.read_bytes() == b"5678"


def test_temporary_storage_limit_counts_partial_work(tmp_path):
    settings = load_settings(root=tmp_path, env={})
    settings.temporary_dir.mkdir(parents=True)
    partial = settings.temporary_dir / "partial.tmp"
    partial.write_bytes(b"12345")
    settings = replace(settings, capture=replace(settings.capture, max_temp_bytes=5))
    enforce_storage_limits(settings)
    partial.write_bytes(b"123456")
    with pytest.raises(RequestBudgetExceeded):
        enforce_storage_limits(settings)


def test_retained_storage_symlink_is_rejected_even_when_target_is_under_root(tmp_path):
    settings = load_settings(root=tmp_path, env={})
    settings.data_dir.mkdir(parents=True)
    target = settings.data_dir / "target"
    target.write_bytes(b"small")
    (settings.data_dir / "alias").symlink_to(target)
    with pytest.raises(RequestBudgetExceeded):
        enforce_storage_limits(settings)


def test_operation_defaults_are_finite_and_shared_across_installed_roots(tmp_path):
    settings = load_settings(root=tmp_path, env={})
    assert settings.capture.workers == 1
    assert settings.gamma.requests_per_second == 2
    assert settings.capture.max_requests == 25_000
    assert settings.capture.max_download_bytes == 4 * 1024**3
    assert settings.capture.max_duration_s == 14_400
    assert settings.capture.max_response_bytes == 16 * 1024**2
    assert settings.capture.max_retained_bytes == 64 * 1024**3
    assert settings.capture.max_temp_bytes == 8 * 1024**3
    assert settings.load.duckdb_memory_limit == "2GB"
