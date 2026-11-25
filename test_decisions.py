"""Check that audited decisions follow actual fills and preserve legacy history."""

from contextlib import closing
from pathlib import Path
import sqlite3
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


if __name__ == "__main__":
    with TemporaryDirectory(dir=Path(__file__).resolve().parent) as directory:
        check_decisions(Path(directory))
    print("Paper decision history, migration, pagination, and atomic rollback passed.")
