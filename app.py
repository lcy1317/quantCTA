"""Archive CTA factors and serve a read-only paper-trading dashboard."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import math
import os
from pathlib import Path
from queue import Empty, Full, Queue
import sqlite3
import subprocess
import threading
import time
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import ProxyHandler, Request, build_opener, urlopen

import ledger


ROOT = Path(__file__).resolve().parent
DATA = Path.home() / "Library" / "Application Support" / "QuantCTA"
PERIOD = 300
CAPTURE_TIMEOUT = 245
REFERENCE_MARKS = {"TON": "MARK:TONUSDT"}
TIMEFRAMES = ("5m", "15m", "30m", "1h", "4h", "1d", "1w")
DEFAULT_RULE = {"enabled": True, "mode": "probability", "period": "5m",
                "long_at": 0.65, "short_at": 0.35}
LOG = logging.getLogger("quant")
SCHEMA = """
CREATE TABLE IF NOT EXISTS rounds(
    bucket INTEGER PRIMARY KEY,
    observed_at INTEGER NOT NULL,
    expected INTEGER NOT NULL,
    captured INTEGER NOT NULL,
    errors TEXT NOT NULL,
    quotes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS assets(
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    base TEXT NOT NULL,
    symbol TEXT
);
CREATE TABLE IF NOT EXISTS payloads(
    hash TEXT PRIMARY KEY,
    data BLOB NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS snapshots(
    asset TEXT NOT NULL REFERENCES assets(id),
    bucket INTEGER NOT NULL REFERENCES rounds(bucket),
    observed_at INTEGER NOT NULL,
    source_at INTEGER,
    probabilities TEXT NOT NULL,
    payload TEXT NOT NULL REFERENCES payloads(hash),
    PRIMARY KEY(asset,bucket)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS quotes(
    asset TEXT NOT NULL REFERENCES assets(id),
    symbol TEXT NOT NULL,
    bucket INTEGER NOT NULL REFERENCES rounds(bucket),
    price REAL NOT NULL,
    mark_price REAL NOT NULL,
    bid REAL,
    ask REAL,
    observed_at INTEGER NOT NULL,
    PRIMARY KEY(asset,bucket)
) WITHOUT ROWID;
"""


def open_db(path):
    conn = sqlite3.connect(path, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def initialize(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(open_db(path)) as conn, conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
        ledger.install(conn)


def numeric(value, positive=False):
    try:
        result = float(value)
        if math.isfinite(result) and (not positive or result > 0):
            return result
    except (TypeError, ValueError, OverflowError):
        pass
    return None


def collector_command(*options):
    return [os.environ.get("NODE_BINARY", "node"), str(ROOT / "collector.mjs"), *options]


def decode_capture(output):
    data = json.loads(output)
    if (not isinstance(data, dict) or not isinstance(data.get("assets"), list)
            or not isinstance(data.get("errors"), list)):
        raise ValueError("Unexpected CTA collector output")
    return data


def capture(probe=False):
    command = collector_command("--probe") if probe else collector_command()
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True,
                            encoding="utf-8", timeout=CAPTURE_TIMEOUT, check=False)
    if result.returncode:
        raise ValueError((result.stderr or result.stdout)[-1800:].strip())
    return decode_capture(result.stdout)


class WatchingCollector:
    """Keep one browser alive across five-minute collection rounds."""

    def __init__(self):
        self.process = None
        self.responses = None

    @staticmethod
    def read_responses(output, responses):
        try:
            with output:
                for line in output:
                    responses.put_nowait(line)
        except (OSError, ValueError, Full):
            pass
        finally:
            try:
                responses.put_nowait(None)
            except Full:
                pass

    def __call__(self):
        if self.process is None or self.process.poll() is not None:
            self.close()
            self.responses = Queue(maxsize=2)
            self.process = subprocess.Popen(
                collector_command("--watch"), cwd=ROOT,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="utf-8")
            threading.Thread(target=self.read_responses,
                             args=(self.process.stdout, self.responses), daemon=True).start()
        try:
            self.process.stdin.write("capture\n")
            self.process.stdin.flush()
            line = self.responses.get(timeout=CAPTURE_TIMEOUT)
            if line is None:
                raise ValueError("CTA collector exited before replying")
            return decode_capture(line)
        except Empty as exc:
            self.close()
            raise ValueError("CTA collector timed out") from exc
        except (OSError, ValueError):
            self.close()
            raise

    def close(self):
        process, self.process = self.process, None
        if process is None:
            return
        try:
            process.stdin.close()
        except OSError:
            pass
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def market_get(endpoint, *, proxy=None, **parameters):
    url = "https://fapi.binance.com" + endpoint
    if parameters:
        url += "?" + urlencode(parameters)
    req = Request(url, headers={"User-Agent": "QuantCTA/1.0"})
    open_url = build_opener(ProxyHandler({"https": proxy})).open if proxy else urlopen
    with open_url(req, timeout=15) as response:
        return json.load(response)


def market_snapshot(proxy=None):
    endpoints = {
        "exchange": "/fapi/v1/exchangeInfo",
        "last": "/fapi/v2/ticker/price",
        "books": "/fapi/v1/ticker/bookTicker",
        "marks": "/fapi/v1/premiumIndex",
    }
    data, errors = {}, []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {key: pool.submit(market_get, url, proxy=proxy) for key, url in endpoints.items()}
        for key, future in futures.items():
            try:
                data[key] = future.result()
            except (OSError, ValueError) as exc:
                errors.append(f"Binance {key}: {exc}")
    return data, errors


def contracts(market):
    return {item["baseAsset"]: item["symbol"]
            for item in market.get("exchange", {}).get("symbols", [])
            if (item["quoteAsset"], item["contractType"], item["status"]) ==
            ("USDT", "PERPETUAL", "TRADING")}


def resolve_contract(mapping, base):
    base = base.upper()
    if base in mapping:
        return mapping[base]
    scaled = [mapping[prefix + base] for prefix in ("1000", "1000000")
              if prefix + base in mapping]
    return scaled[0] if len(scaled) == 1 else None


def quote_snapshot(market, symbols, now):
    index = {key: {row["symbol"]: row for row in market.get(key, [])}
             for key in ("last", "books", "marks")}
    quotes = {}
    for symbol in symbols:
        if symbol.startswith("MARK:"):
            # TON is settling. Use the published reference mark as a synthetic fill.
            mark = index["marks"].get(symbol.removeprefix("MARK:"), {})
            mark_at = (numeric(mark.get("time")) or 0) / 1000
            price = numeric(mark.get("markPrice"), True)
            if price and abs(now - mark_at) <= 120:
                quotes[symbol] = {"price": price, "mark_price": price, "bid": None, "ask": None,
                                  "book_at": 0, "last_at": mark_at}
            continue
        last = index["last"].get(symbol, {})
        book = index["books"].get(symbol, {})
        mark = index["marks"].get(symbol, {})
        last_at = (numeric(last.get("time")) or 0) / 1000
        book_at = (numeric(book.get("time")) or 0) / 1000
        mark_at = (numeric(mark.get("time")) or 0) / 1000
        bid = numeric(book.get("bidPrice"), True) if abs(now - book_at) <= 120 else None
        ask = numeric(book.get("askPrice"), True) if abs(now - book_at) <= 120 else None
        if not (bid and ask and bid <= ask):
            bid = ask = None
        last_price = numeric(last.get("price"), True) if abs(now - last_at) <= 120 else None
        mark_price = numeric(mark.get("markPrice"), True) if abs(now - mark_at) <= 120 else None
        price = last_price or ((bid + ask) / 2 if bid else mark_price)
        if price is not None:
            quotes[symbol] = {
                "price": price, "mark_price": mark_price or price, "bid": bid, "ask": ask,
                "last_at": last_at if last_price else 0, "book_at": book_at if bid else 0,
            }
    return quotes


def load_config(path):
    config = json.loads(path.read_text(encoding="utf-8-sig")) if path.is_file() else {}
    proxy = config.get("market_proxy")
    if proxy is not None:
        if not isinstance(proxy, str):
            raise ValueError("market_proxy must be an HTTP proxy URL")
        url = urlsplit(proxy)
        if (url.scheme not in ("http", "https") or not url.hostname or url.path not in ("", "/")
                or url.query or url.fragment):
            raise ValueError("market_proxy must be an HTTP proxy URL")
        if url.port is not None and url.port < 1:
            raise ValueError("market_proxy port must be positive")
    rule = {**DEFAULT_RULE, **config.get("rule", {})}
    if rule.get("enabled"):
        period = rule.get("period")
        if period not in TIMEFRAMES:
            raise ValueError("Invalid rule timeframe")
        if rule["mode"] == "probability":
            short_at, long_at = numeric(rule.get("short_at")), numeric(rule.get("long_at"))
            if short_at is None or long_at is None or not 0 <= short_at < long_at <= 1:
                raise ValueError("Enabled probability rule requires 0 <= short_at < long_at <= 1")
            rule = dict(rule, short_at=short_at, long_at=long_at)
        elif rule["mode"] != "direction":
            raise ValueError("Rule mode must be probability or direction")
    return {
        "rule": rule,
        "overrides": config.get("contract_overrides", {}),
        "funding": config.get("funding", True),
        "market_proxy": proxy,
        "settings": ledger.Settings(initial_equity=1000, leverage=10,
                                    fee_bps=config.get("fee_bps", 5),
                                    slippage_bps=config.get("slippage_bps", 2)),
    }


def normalized_probabilities(item):
    result = {}
    for period, value in item.get("probabilities", {}).items():
        probability = numeric(value)
        if period in TIMEFRAMES and probability is not None and 0 <= probability <= 1:
            result[period] = probability
    return result


def decide(item, probabilities, rule):
    if not rule.get("enabled"):
        return None
    if rule.get("mode") == "direction":
        direction = item.get("directions", {}).get(rule["period"])
        return {"long": 1, "short": -1, "neutral": 0}.get(direction)
    probability = probabilities.get(rule["period"])
    if probability is None:
        return None
    if probability >= rule["long_at"]:
        return 1
    if probability <= rule["short_at"]:
        return -1
    return 0


def confirmed_funding(conn, assets, now, enabled, proxy=None):
    if not enabled:
        return {}, []
    positions = conn.execute("""
        SELECT p.symbol AS asset, p.opened_at, p.marked_at, MAX(f.time) AS settled
        FROM paper_positions p LEFT JOIN paper_funding f ON p.symbol=f.symbol GROUP BY p.symbol
    """).fetchall()
    pending = {}
    for position in positions:
        asset = position["asset"]
        symbol = assets.get(asset)
        start = max(position["settled"] or 0, position["opened_at"])
        if symbol and not symbol.startswith("MARK:") and start // 3600 < now // 3600 and (
                now % 3600 < 900 or position["marked_at"] // 3600 < now // 3600):
            pending[asset] = (symbol, start)
    if not pending:
        return {}, []

    # Fetch the settlement window once for all open positions.
    start = min(value[1] for value in pending.values()) * 1000 + 1
    end = now * 1000
    settlements = []
    try:
        for _ in range(10):
            batch = market_get("/fapi/v1/fundingRate", proxy=proxy, startTime=start,
                               endTime=end, limit=1000)
            settlements.extend(batch)
            if len(batch) < 1000:
                break
            newest = max(row["fundingTime"] for row in batch)
            if newest <= start:
                raise ValueError("Settlement pagination made no progress")
            # Overlap the last timestamp in case it straddles two pages.
            start = newest
        else:
            return {}, ["Binance funding: settlement window too large"]
    except (OSError, ValueError, KeyError) as exc:
        return {}, [f"Binance funding: {exc}"]
    by_symbol = {}
    for row in settlements:
        if row.get("markPrice"):
            by_symbol.setdefault(row["symbol"], []).append({
                "time": row["fundingTime"] // 1000, "rate": row["fundingRate"],
                "mark_price": row["markPrice"],
            })
    return {asset: by_symbol.get(value[0], []) for asset, value in pending.items()}, []


def store_round(db, bucket, source, market, errors, config):
    now = int(time.time())
    mapping = contracts(market)
    allowed = set(mapping.values())
    items = {item["id"]: item for item in source["assets"]}
    rule = config["rule"]
    with closing(open_db(db)) as conn:
        if conn.execute("SELECT 1 FROM rounds WHERE bucket=?", (bucket,)).fetchone():
            return
        assets = {row["id"]: row["symbol"] for row in conn.execute("SELECT id,symbol FROM assets")}
        open_assets = {row[0] for row in conn.execute("SELECT symbol FROM paper_positions")}
        for asset, item in items.items():
            if asset in open_assets:
                # The same contract must price and settle a position until it closes.
                continue
            override = config["overrides"].get(asset)
            if "exchange" in market:
                if override and override not in allowed:
                    errors = list(errors) + [f"Binance contract override inactive: {asset} → {override}"]
                assets[asset] = (override if override in allowed else None) if override else (
                    resolve_contract(mapping, item["base"])
                )
                if not override and assets[asset] is None:
                    assets[asset] = REFERENCE_MARKS.get(item["base"])
            else:
                assets.setdefault(asset, None)
        quotes = quote_snapshot(market, {symbol for symbol in assets.values() if symbol}, now)
        funding, funding_errors = confirmed_funding(
            conn, assets, now, config["funding"], config["market_proxy"])
        errors = list(source["errors"]) + errors + funding_errors
        previous_total = conn.execute(
            "SELECT expected FROM rounds WHERE expected>0 ORDER BY bucket DESC LIMIT 1"
        ).fetchone()
        expected = source.get("expected") or (previous_total[0] if previous_total else len(items))
        targets, paper_quotes = {}, {}
        with conn:
            write = conn.execute("INSERT OR IGNORE INTO rounds VALUES(?,?,?,?,?,?)",
                                 (bucket, now, expected, len(items),
                                  json.dumps(errors, ensure_ascii=False), len(quotes)))
            if not write.rowcount:
                return
            for asset, item in items.items():
                conn.execute(
                    "INSERT INTO assets VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                    "label=excluded.label,base=excluded.base,symbol=excluded.symbol",
                    (asset, item["label"], item["base"], assets[asset]),
                )
                probabilities = normalized_probabilities(item)
                raw = json.dumps(item["data"], sort_keys=True, ensure_ascii=False,
                                 separators=(",", ":"), allow_nan=False).encode("utf-8")
                digest = hashlib.sha256(raw).hexdigest()
                conn.execute("INSERT OR IGNORE INTO payloads VALUES(?,?)",
                             (digest, gzip.compress(raw, compresslevel=3, mtime=0)))
                observed_at, source_at = item.get("observed_at") or now, item.get("source_at")
                conn.execute("INSERT INTO snapshots VALUES(?,?,?,?,?,?)",
                             (asset, bucket, observed_at, source_at,
                              json.dumps(probabilities), digest))
                target = decide(item, probabilities, rule)
                quote = quotes.get(assets[asset])
                if (target is not None and quote and source_at
                        and bucket - PERIOD <= source_at <= now + 10 and now < bucket + PERIOD):
                    if quote["book_at"] >= observed_at:
                        targets[asset] = target
                    elif quote["last_at"] >= observed_at:
                        targets[asset] = target
                        paper_quotes[asset] = dict(quote, bid=None, ask=None)
            for asset, symbol in assets.items():
                if symbol in quotes:
                    quote = quotes[symbol]
                    conn.execute("INSERT INTO quotes VALUES(?,?,?,?,?,?,?,?)",
                                 (asset, symbol, bucket, quote["price"], quote["mark_price"],
                                  quote["bid"], quote["ask"], now))
                    paper_quotes.setdefault(asset, quote)
            ledger.step(conn, bucket, targets, paper_quotes, config["settings"],
                        symbols=assets, funding=funding, observed_at=now)
    LOG.info("Archived %s/%s assets, %s Binance quotes, %s notices",
             len(items), expected, len(quotes), len(errors))


def collect(db, config_path, bucket, read_source=capture):
    with closing(open_db(db)) as conn:
        if conn.execute("SELECT 1 FROM rounds WHERE bucket=?", (bucket,)).fetchone():
            return
    config = load_config(config_path)
    try:
        source = read_source()
    except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
        source = {"assets": [], "errors": [f"CTA: {exc}"], "expected": 0}
    market, errors = market_snapshot(config["market_proxy"])
    store_round(db, bucket, source, market, errors, config)


def schedule(db, config_path, stopping):
    previous = None
    with closing(WatchingCollector()) as read_source:
        while not stopping.is_set():
            now = time.time()
            bucket = int(now) // PERIOD * PERIOD
            if bucket != previous:
                if stopping.wait(max(0, bucket + 8 - now)):
                    break
                try:
                    collect(db, config_path, bucket, read_source)
                except Exception:
                    LOG.exception("Collection failed for %s", bucket)
                previous = bucket
            stopping.wait(max(1, bucket + PERIOD + 8 - time.time()))


def get_asset(conn, asset):
    if asset:
        return conn.execute("SELECT * FROM assets WHERE id=?", (asset,)).fetchone()
    return conn.execute("SELECT * FROM assets ORDER BY base<>'BTC',id LIMIT 1").fetchone()


def account(conn, asset, initial):
    data = conn.execute("SELECT * FROM paper_equity WHERE symbol=? ORDER BY bucket DESC LIMIT 1",
                        (asset,)).fetchone()
    closed = conn.execute("SELECT COUNT(*), AVG(net_pnl>0) FROM paper_trades "
                          "WHERE symbol=? AND net_pnl IS NOT NULL", (asset,)).fetchone()
    equity = data["equity"] if data else initial
    return {"equity": equity, "cash": data["cash"] if data else initial,
            "return_pct": 100 * (equity / initial - 1),
            "realized": data["cash"] - initial if data else 0,
            "unrealized": data["unrealized_pnl"] if data else 0,
            "open_positions": data["positions"] if data else 0,
            "fees": data["fees"] if data else 0, "funding": data["funding_pnl"] if data else 0,
            "stale_positions": data["stale_positions"] if data else 0,
            "closed_trades": closed[0], "win_rate": closed[1]}


def overview(conn, requested, config):
    selected = get_asset(conn, requested)
    asset = selected["id"] if selected else None
    rule = config["rule"]
    initial = config["settings"].initial_equity
    now = conn.execute("SELECT * FROM rounds ORDER BY bucket DESC LIMIT 1").fetchone()
    source = conn.execute("SELECT source_at FROM snapshots WHERE asset=? ORDER BY bucket DESC LIMIT 1",
                          (asset,)).fetchone()
    if not rule.get("enabled"):
        description = "仅归档，模拟开平仓已暂停"
    elif rule.get("mode") == "direction":
        description = f"{rule['period']} 综合方向，多做多、空做空、中性平仓"
    else:
        description = "{} 多头概率 ≥{:.0%} 做多，≤{:.0%} 做空，中间平仓".format(
            rule["period"], rule["long_at"], rule["short_at"])
    if selected and (selected["symbol"] or "").startswith("MARK:"):
        description += " · Binance 参考标记价模拟，无可成交盘口，不计资金费"
    health = {"status": "delayed", "count": 0, "expected": 0, "message": "等待首次采集"}
    if now:
        messages = json.loads(now["errors"])
        if time.time() - now["observed_at"] > PERIOD * 2:
            messages.append("采集已超过 10 分钟未更新")
        complete = now["captured"] and now["captured"] == now["expected"] and now["quotes"] and not messages
        health = {"status": "normal" if complete else "delayed" if now["captured"] else "failure",
                  "count": now["captured"], "expected": now["expected"],
                  "message": " · ".join(messages) or
                  ("采集正常" if complete else "部分币种或 Binance 行情尚未获取")}
    assets = [dict(row, return_pct=100 * (row["equity"] / initial - 1))
              for row in conn.execute("""
        WITH outcomes AS (
            SELECT symbol, COUNT(*) AS closed_trades, AVG(net_pnl>0) AS win_rate
            FROM paper_trades WHERE net_pnl IS NOT NULL GROUP BY symbol
        )
        SELECT a.id, a.label, a.symbol, COALESCE(e.equity, ?) AS equity,
               COALESCE(o.closed_trades, 0) AS closed_trades, o.win_rate
        FROM assets a
        LEFT JOIN paper_equity e ON e.symbol=a.id AND e.bucket=(SELECT MAX(bucket) FROM paper_equity)
        LEFT JOIN outcomes o ON o.symbol=a.id
        ORDER BY a.base<>'BTC', a.id
    """, (initial,))]
    positions = [{
        "asset": asset, "direction": "long" if row["side"] == 1 else "short",
        "qty": row["quantity"], "entry_price": row["entry_price"], "mark_price": row["mark_price"],
        "unrealized": row["side"] * row["quantity"] * (row["mark_price"] - row["entry_price"]),
        "opened_at": row["opened_at"],
    } for row in conn.execute("SELECT * FROM paper_positions WHERE symbol=?", (asset,))]
    trades = [{
        "at": row["observed_at"], "asset": asset, "action": row["action"],
        "direction": "long" if row["side"] == 1 else "short", "price": row["price"],
        "qty": row["quantity"], "fee": row["fee"], "pnl": row["net_pnl"], "reason": row["reason"],
    } for row in conn.execute("SELECT * FROM paper_trades WHERE symbol=? ORDER BY id DESC LIMIT 100",
                              (asset,))]
    return {"assets": assets, "selected": asset, "latest_at": now["observed_at"] if now else None,
            "source_at": source["source_at"] if source else None, "health": health,
            "strategy": {"description": description, "balance": initial, "leverage": 10,
                         "enabled": bool(rule.get("enabled")), "period": rule.get("period", "5m")},
            "account": account(conn, asset, initial), "positions": positions, "trades": trades}


def history(conn, requested, hours, timeframe, before=None):
    selected = get_asset(conn, requested)
    if not selected:
        return {"points": [], "snapshots": []}
    asset = selected["id"]
    end = int(time.time()) if before is None else before
    start = end - max(1, min(hours, 31 * 24)) * 3600
    rows = conn.execute("""
        SELECT r.bucket AS at,s.observed_at,s.source_at,s.probabilities,q.price,
               e.equity,(e.realized_pnl-e.fees+e.funding_pnl) AS realized,e.unrealized_pnl AS unrealized
        FROM rounds r
        LEFT JOIN snapshots s ON r.bucket=s.bucket AND s.asset=:asset AND s.observed_at<=:end
        LEFT JOIN quotes q ON r.bucket=q.bucket AND q.asset=:asset AND q.observed_at<=:end
        LEFT JOIN paper_equity e ON r.bucket=e.bucket AND e.symbol=:asset AND r.observed_at<=:end
        WHERE r.bucket BETWEEN :start AND :end
          AND (s.asset IS NOT NULL OR q.asset IS NOT NULL OR e.symbol IS NOT NULL)
        ORDER BY r.bucket
    """, {"asset": asset, "start": start, "end": end})
    points, snapshots = [], []
    for row in rows:
        point = dict(row)
        probabilities = json.loads(point.pop("probabilities") or "{}")
        point["probability"] = probabilities.get(timeframe)
        points.append(point)
        if point["observed_at"]:
            snapshots.append({"at": point["at"], "source_at": point["source_at"]})
    return {"points": points, "snapshots": snapshots}


def get_snapshot(conn, requested, at):
    selected = get_asset(conn, requested)
    if not selected:
        return None
    sql = ("SELECT s.*,p.data FROM snapshots s JOIN payloads p ON s.payload=p.hash "
           "WHERE s.asset=?")
    parameters = [selected["id"]]
    if at is not None:
        sql += " AND s.bucket=?"
        parameters.append(at)
    item = conn.execute(sql + " ORDER BY s.bucket DESC LIMIT 1", parameters).fetchone()
    if not item:
        return None
    return {"at": item["bucket"], "source_at": item["source_at"], "observed_at": item["observed_at"],
            "asset": selected["id"], "data": json.loads(gzip.decompress(item["data"]))}


def serve(db, config_path, addr, port):
    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, body, mime="application/json; charset=utf-8"):
            self.send_response(status)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def json(self, status, data):
            self.reply(status, json.dumps(data, ensure_ascii=False, allow_nan=False).encode())

        def do_GET(self):
            url = urlsplit(self.path)
            values = parse_qs(url.query)
            asset = values.get("asset", [None])[0]
            try:
                if url.path == "/":
                    self.reply(200, (ROOT / "index.html").read_bytes(), "text/html; charset=utf-8")
                    return
                with closing(open_db(db)) as conn:
                    if url.path in ("/api/overview", "/healthz"):
                        result = overview(conn, asset, load_config(config_path))
                        self.json(200, result["health"] if url.path == "/healthz" else result)
                    elif url.path == "/api/history":
                        hours = int(values.get("hours", ["24"])[0])
                        before = int(values["before"][0]) if "before" in values else None
                        default = load_config(config_path)["rule"].get("period", "5m")
                        timeframe = values.get("timeframe", [default])[0]
                        self.json(200, history(conn, asset, hours, timeframe, before))
                    elif url.path == "/api/snapshot":
                        at = int(values["at"][0]) if "at" in values else None
                        result = get_snapshot(conn, asset, at)
                        if result:
                            self.json(200, result)
                        else:
                            self.json(404, {"error": "该时刻暂无快照"})
                    else:
                        self.json(404, {"error": "路径不存在"})
            except ValueError:
                self.json(400, {"error": "请求参数无效"})
            except (OSError, sqlite3.Error):
                LOG.exception("Dashboard request failed")
                self.json(500, {"error": "服务读取失败"})

    with ThreadingHTTPServer((addr, port), Handler) as server:
        stopping = threading.Event()
        worker = threading.Thread(target=schedule, args=(db, config_path, stopping), daemon=True)
        worker.start()
        LOG.info("Listening on http://%s:%s", addr, port)
        try:
            server.serve_forever()
        finally:
            stopping.set()
            worker.join(timeout=4)


def check(config):
    with ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(market_snapshot, config["market_proxy"])
        try:
            source = capture(probe=True)
        except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
            source = {"assets": [], "errors": [str(exc)]}
        market, errors = future.result()
    ids = [item["id"] for item in source["assets"]]
    references = set(REFERENCE_MARKS.values())
    quotes = quote_snapshot(market, {"BTCUSDT"} | references, int(time.time()))
    print(json.dumps({"cta_assets": ids, "cta_expected": source.get("expected"),
                      "cta_periods": {item["id"]: list(normalized_probabilities(item))
                                      for item in source["assets"]},
                      "cta_errors": source["errors"], "binance_contracts": len(contracts(market)),
                      "binance_btc_quote": quotes.get("BTCUSDT"),
                      "binance_reference_marks": {symbol: quotes[symbol] for symbol in sorted(references & quotes.keys())},
                      "binance_errors": errors},
                     ensure_ascii=False, indent=2))
    return 0 if ids and "BTCUSDT" in quotes else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--serve", action="store_true")
    modes.add_argument("--once", action="store_true")
    modes.add_argument("--check", action="store_true")
    parser.add_argument("--data-dir", type=Path, default=DATA)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config_path = args.config or args.data_dir / "config.json"
    config = load_config(config_path)
    if args.check:
        return check(config)
    db = args.data_dir / "quant.sqlite3"
    initialize(db)
    if args.once:
        collect(db, config_path, int(time.time()) // PERIOD * PERIOD)
    else:
        serve(db, config_path, args.host, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
