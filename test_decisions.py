"""Check that audited decisions follow actual fills and preserve legacy history."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3
from subprocess import CompletedProcess
from tempfile import TemporaryDirectory
from unittest.mock import patch

import app


def check_decisions(directory):
    db = directory / "quant.sqlite3"
    config = app.load_config(directory / "config.json")
    paused = dict(config, rule=dict(config["rule"], enabled=False))
    base = 1795620600
    app.initialize(db)

    def collect(step, probability, *, cfg=config, price=100, source_lag=0, quote_lag=0):
        bucket = base + step * app.PERIOD
        now = bucket + 20
        assets = [{
            "id": "BTC", "base": "BTC", "label": "BTC", "observed_at": now - 5,
            "source_at": bucket - source_lag, "probabilities": {"5m": probability}, "data": {},
        }]
        if step == 0:
            assets.append(dict(assets[0], id="ETH", base="ETH", label="ETH"))
        source = {"assets": assets, "expected": len(assets), "errors": []}
        market = {
            "exchange": {"symbols": [
                {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT",
                 "contractType": "PERPETUAL", "status": "TRADING"},
            ]},
            "last": [{"symbol": "BTCUSDT", "price": price, "time": (now - quote_lag) * 1000}],
        }
        with patch("app.time.time", return_value=now):
            app.store_round(db, bucket, source, market, [], cfg)

    collect(0, 0.8)
    with closing(app.open_db(db)) as conn, conn:
        conn.execute("DROP TABLE paper_decisions")
    app.initialize(db)
    collect(1, 0.8)
    collect(2, 0.2)
    collect(3, 0.5, source_lag=600)
    collect(4, 0.5, quote_lag=15)
    collect(5, 0.5, price=96)
    collect(6, 0.5, price=96)
    collect(7, 0.8)
    collect(8, 0.8, cfg=paused, price=80)
    collect(8, 0.2)
    with closing(app.open_db(db)) as conn, conn:
        full = app.decisions(conn, "BTC")
        rows = full["decisions"]
        assert full["audit_started_at"] == base + app.PERIOD
        assert [r["outcome"] for r in rows] == [
            "liquidate", "open", "flat", "close", "skipped", "skipped", "reverse", "hold", "open",
        ]
        assert [r["reason"] for r in rows[4:6]] == ["quote_before_signal", "stale_signal"]
        assert rows[0]["rule"]["enabled"] is False and rows[0]["skip_reason"] == "strategy_disabled"
        reverse = rows[6]
        assert reverse["previous_side"] == 1 and reverse["target_side"] == reverse["result_side"] == -1
        assert [trade["action"] for trade in reverse["trades"]] == ["close", "open"]
        assert reverse["rule"]["short_at"] == 0.35 and reverse["price"] == 100
        assert rows[-1]["legacy"] and rows[-1]["rule"] is None and rows[-1]["target_side"] is None
        assert app.decisions(conn, "ETH")["decisions"][1]["reason"] == "missing_snapshot"
        traded = app.decisions(conn, "BTC", limit=2, outcome_filter="trades")
        older = app.decisions(conn, "BTC", before=traded["next_before"], outcome_filter="trades")
        assert [r["outcome"] for r in older["decisions"]] == ["close", "reverse", "open"]
        assert [r["outcome"] for r in app.decisions(conn, "BTC", outcome_filter="held")["decisions"]] == [
            "flat", "hold",
        ]
        assert len(app.decisions(conn, "BTC", outcome_filter="skipped")["decisions"]) == 2
        conn.execute("""
            CREATE TRIGGER fail_decision BEFORE INSERT ON paper_decisions
            BEGIN SELECT RAISE(ABORT, 'test audit failure'); END
        """)

    try:
        collect(9, 0.2)
    except sqlite3.IntegrityError as error:
        assert str(error) == "test audit failure"
    else:
        raise AssertionError("round unexpectedly succeeded without writing its decisions")
    with closing(app.open_db(db)) as conn:
        for table in ("rounds", "snapshots", "quotes", "paper_equity", "paper_decisions", "paper_trades"):
            assert not conn.execute(f"SELECT 1 FROM {table} WHERE bucket=?", (base + 9 * app.PERIOD,)).fetchone()


def check_strategies(directory):
    db = directory / "quant.sqlite3"
    config = directory / "config.json"
    config.write_text('{"rule":{"short_at":0.2,"long_at":0.8}}', encoding="utf-8")
    base = 1795622400
    due = base + 3600

    def once(bucket, probability, *, price=100, funding=None):
        now = bucket + 20
        source = {"expected": 1, "errors": [], "assets": [{
            "id": "BTC", "base": "BTC", "label": "BTC", "observed_at": now - 5,
            "source_at": bucket, "probabilities": {"5m": probability}, "data": {"source": "fixture"},
        }]}
        market = {
            "exchange": {"symbols": [
                {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT",
                 "contractType": "PERPETUAL", "status": "TRADING"},
            ]},
            "last": [{"symbol": "BTCUSDT", "price": price, "time": now * 1000}],
        }
        cmd = ["app.py", "--once", "--data-dir", str(directory)]
        with (
            patch("sys.argv", cmd),
            patch("app.time.time", return_value=now),
            patch("app.subprocess.run", return_value=CompletedProcess([], 0, json.dumps(source), "")) as capture,
            patch("app.market_snapshot", return_value=(market, [])) as quotes,
            patch("app.market_get", return_value=funding, side_effect=funding if isinstance(funding, Exception) else None)
                as settlements,
        ):
            assert app.main() == 0
        return capture.call_count, quotes.call_count, settlements.call_count

    assert once(base, 0.58) == (1, 1, 0)
    assert once(base, 0.85) == (0, 0, 0)
    initial_outcomes = []
    for strategy in app.STRATEGIES:
        with closing(app.open_db(app.strategy_db(db, strategy))) as conn:
            initial_outcomes.append(app.decisions(conn, "BTC")["decisions"][0]["outcome"])
            assert len(app.history(conn, "BTC", 1, "5m", before=base + 25)["points"]) == 1
    assert initial_outcomes == ["flat", "flat", "open"]
    assert once(base + 300, 0.62) == (1, 1, 0)
    assert once(base + 600, 0.68) == (1, 1, 0)
    assert once(due + 300, 0.7, funding=OSError("temporary outage")) == (1, 1, 1)
    event = [{"symbol": "BTCUSDT", "fundingTime": due * 1000, "fundingRate": "0.001", "markPrice": "100"}]
    assert once(due + 1200, 0.5, price=102, funding=event) == (1, 1, 1)
    with closing(app.open_db(db)) as primary:
        assert app.get_snapshot(primary, "BTC", base)["data"] == {"source": "fixture"}
        summaries = app.strategy_summaries(db, app.PRIMARY_STRATEGY, "BTC", app.account(primary, "BTC", 1000), 1000)
    assert len(summaries) == 3 and all(row["win_rate"] == 1 and row["closed_trades"] == 1 for row in summaries)
    for strategy, (short_at, long_at) in app.STRATEGIES.items():
        with closing(app.open_db(app.strategy_db(db, strategy))) as conn:
            decisions = app.decisions(conn, "BTC")["decisions"]
            assert decisions[0]["outcome"] == "close" and decisions[1]["funding_unverified"]
            assert decisions[0]["rule"]["short_at"] == short_at and decisions[0]["rule"]["long_at"] == long_at
            assert decisions[0]["trades"][0]["funding_pnl"] < 0
            assert conn.execute("SELECT COUNT(*) FROM paper_funding").fetchone()[0] == 1
            if strategy != app.PRIMARY_STRATEGY:
                assert conn.execute("SELECT COUNT(*) FROM payloads").fetchone()[0] == 1


if __name__ == "__main__":
    with TemporaryDirectory(dir=Path(__file__).resolve().parent) as directory:
        check_decisions(Path(directory))
    with TemporaryDirectory(dir=Path(__file__).resolve().parent) as directory:
        check_strategies(Path(directory))
    print("Paper decision history, migration, pagination, and atomic rollback passed.")
    print("Live presets, independent balances, shared market snapshot, and funding retry passed.")
