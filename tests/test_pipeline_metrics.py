import asyncio
import importlib.util
import io
import json
import sys
import types
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import pytest


@pytest.fixture
def pipeline(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    database = types.ModuleType("legendsbot.db")
    for name in (
        "delete_player_and_logs", "get_all_player_tags", "make_pool",
        "safe_delete_clan", "snapshot_reset_clan_tags", "upsert_clan",
        "upsert_player", "mark_clan_active", "mark_clan_inactive",
        "reconcile_mirror_duplicates",
    ):
        setattr(database, name, AsyncMock())
    paths = types.ModuleType("legendsbot.paths")
    paths.KEYS_FILE = "metrics-test-keys"
    state = types.ModuleType("legendsbot.scan_state")
    state.record_update_run = lambda: None
    monkeypatch.setitem(sys.modules, "legendsbot.db", database)
    monkeypatch.setitem(sys.modules, "legendsbot.paths", paths)
    monkeypatch.setitem(sys.modules, "legendsbot.scan_state", state)
    original_open = open

    def fake_open(path, *args, **kwargs):
        if path == paths.KEYS_FILE:
            return io.StringIO('{"test": "secret-token"}')
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", fake_open)
    spec = importlib.util.spec_from_file_location(
        "metrics_test_pipeline", root / "src/legendsbot/pipeline.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.asyncio, "sleep", AsyncMock())
    return module


class Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = 0

    @asynccontextmanager
    async def get(self, *args, **kwargs):
        self.calls += 1
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        status, data = response
        yield types.SimpleNamespace(
            status=status, headers={}, json=AsyncMock(return_value=data)
        )


def capture_pass(pipeline, operation):
    return asyncio.run(pipeline._measure_pass(operation)())


def summary(capsys):
    line = next(
        line for line in capsys.readouterr().out.splitlines()
        if line.startswith("[PASS_METRICS] ")
    )
    return json.loads(line.removeprefix("[PASS_METRICS] "))


def test_retries_rows_and_privacy(pipeline, capsys):
    rows = [
        {"battleType": "legend", "battleTimestamp": "20260625T010000.000Z"},
        {"battleType": "war", "battleTimestamp": "20260625T010000.000Z"},
    ]
    session = Session([(429, {}), (200, {"items": rows})])
    limiter = types.SimpleNamespace(throttle=AsyncMock())

    async def operation():
        data = await pipeline.fetch_battlelog(session, limiter, "#PRIVATE", "secret-token")
        assert data == {"items": rows}
        assert len(pipeline.parse_legend_battles(rows, None)[0]) == 1

    capture_pass(pipeline, operation)
    metrics = summary(capsys)
    assert metrics["requests"] == {"battlelog": 2}
    assert metrics["responses"] == {"battlelog:429": 1, "battlelog:200": 1}
    assert metrics["battle_rows"] == {"returned": 2, "new_cursor_candidates": 1}
    assert metrics["inserted_battle_rows"] == "unknown"
    assert metrics["successful_battlelog_freshness"] == "unknown"
    assert metrics["limiter_seconds"]["battlelog"] >= 0
    assert metrics["http_headers_seconds"]["battlelog"] >= 0
    assert session.calls == limiter.throttle.await_count == 2
    assert "#PRIVATE" not in json.dumps(metrics)
    assert "secret-token" not in json.dumps(metrics)
    assert pipeline._PASS_METRICS.get() is None


@pytest.mark.parametrize("fetch,endpoint,status", [
    ("fetch_clan", "clan", 404),
    ("fetch_player", "player", 404),
    ("fetch_leaguehistory", "leaguehistory", 503),
])
def test_fixed_endpoints(pipeline, capsys, fetch, endpoint, status):
    session = Session([(status, {})])
    limiter = types.SimpleNamespace(throttle=AsyncMock())

    async def operation():
        await getattr(pipeline, fetch)(session, limiter, "#PRIVATE", "secret-token")

    capture_pass(pipeline, operation)
    metrics = summary(capsys)
    assert metrics["requests"] == {endpoint: 1}
    assert metrics["responses"] == {f"{endpoint}:{status}": 1}


@pytest.mark.parametrize("responses,expected", [
    ([(200, {})], {"battlelog:200": 1}),
    ([TimeoutError("private-url")] * 3, {"battlelog:no_response": 3}),
])
def test_unknown_yield(pipeline, capsys, responses, expected):
    session = Session(responses)
    limiter = types.SimpleNamespace(throttle=AsyncMock())

    async def operation():
        await pipeline.fetch_battlelog(session, limiter, "#PRIVATE", "secret-token")

    capture_pass(pipeline, operation)
    metrics = summary(capsys)
    assert metrics["responses"] == expected
    assert metrics["battle_rows"] == {"unknown_fetches": 1}


def test_failure_and_reporting_are_isolated(pipeline, monkeypatch):
    async def operation():
        pipeline._PASS_METRICS.set({"requests": None})
        pipeline._metric("requests", "clan")
        raise ValueError("original failure")

    def broken_print(*args, **kwargs):
        raise OSError("output unavailable")

    monkeypatch.setattr("builtins.print", broken_print)
    with pytest.raises(ValueError, match="original failure"):
        capture_pass(pipeline, operation)
    assert pipeline._PASS_METRICS.get() is None


def test_update_phases_with_mocked_resources(pipeline, monkeypatch, capsys):
    @asynccontextmanager
    async def acquire():
        yield object()

    pool = types.SimpleNamespace(acquire=acquire)
    monkeypatch.setattr(pipeline, "get_shared_session", AsyncMock(return_value=object()))
    monkeypatch.setattr(pipeline, "get_shared_limiters", AsyncMock(return_value={}))
    monkeypatch.setattr(pipeline, "get_clans_to_scan", AsyncMock(return_value=[]))
    monkeypatch.setattr(pipeline, "load_active_player_state", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(pipeline, "get_all_player_tags", AsyncMock(return_value=[]))
    monkeypatch.setattr(pipeline, "reconcile_mirror_duplicates", AsyncMock(return_value=0))
    asyncio.run(pipeline.update_all(pool))
    metrics = summary(capsys)
    assert set(metrics["phase_seconds"]) == {"clans", "unseen", "reconcile"}
    assert metrics["outcome"] == "complete"
    assert metrics["requests"] == {}


def test_overlapping_passes_are_separate(pipeline, capsys):
    async def operation(endpoint):
        pipeline._metric("requests", endpoint)
        await asyncio.sleep(0)
        pipeline._metric("requests", endpoint)

    async def run():
        await asyncio.gather(
            pipeline._measure_pass(operation)("clan"),
            pipeline._measure_pass(operation)("player"),
        )

    asyncio.run(run())
    metrics = [
        json.loads(line.removeprefix("[PASS_METRICS] "))
        for line in capsys.readouterr().out.splitlines()
    ]
    assert {tuple(item["requests"].items()) for item in metrics} == {
        (("clan", 2),), (("player", 2),),
    }
