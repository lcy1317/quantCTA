"""SQLite ledger for USDT-margined paper positions.

Call step inside the same write transaction as the archived market snapshot.
Quote and funding timestamps are UTC Unix seconds. Each asset has a separate
wallet and one isolated-margin position, checked for liquidation at each mark.
"""

from dataclasses import dataclass
from math import isfinite
from typing import Mapping, Optional


@dataclass(frozen=True)
class Settings:
    initial_equity: float = 1000.0
    leverage: float = 10.0
    fee_bps: float = 5.0
    slippage_bps: float = 2.0
    maintenance_margin_rate: float = 0.005

    def __post_init__(self):
        values = (
            self.initial_equity, self.leverage, self.fee_bps,
            self.slippage_bps, self.maintenance_margin_rate,
        )
        if not all(isfinite(value) for value in values):
            raise ValueError("paper settings must be finite")
        if self.initial_equity <= 0 or not 1 <= self.leverage <= 10:
            raise ValueError("paper equity must be positive and leverage between 1 and 10")
        if not 0 <= self.fee_bps < 10000 or not 0 <= self.slippage_bps < 10000:
            raise ValueError("fees and slippage must be between 0 and 10000 basis points")
        if not 0 <= self.maintenance_margin_rate < 1 / self.leverage:
            raise ValueError("maintenance margin must be lower than initial margin")


@dataclass(frozen=True)
class Quote:
    mark: float
    bid: float
    ask: float

    def fill(self, side, slippage):
        reference = self.ask if side == 1 else self.bid
        return reference * (1 + side * slippage)


@dataclass
class Position:
    symbol: str
    side: int
    quantity: float
    entry_price: float
    margin: float
    opened_at: int
    mark_price: float
    marked_at: int
    entry_fee: float
    funding_pnl: float = 0.0

    @property
    def unrealized_pnl(self):
        return self.side * self.quantity * (self.mark_price - self.entry_price)


def install(connection):
    connection.execute("""
        CREATE TABLE IF NOT EXISTS paper_positions (
            symbol TEXT PRIMARY KEY, side INTEGER NOT NULL, quantity REAL NOT NULL,
            entry_price REAL NOT NULL, margin REAL NOT NULL, opened_at INTEGER NOT NULL,
            mark_price REAL NOT NULL, marked_at INTEGER NOT NULL,
            entry_fee REAL NOT NULL, funding_pnl REAL NOT NULL
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, symbol TEXT NOT NULL,
            observed_at INTEGER NOT NULL,
            action TEXT NOT NULL, side INTEGER NOT NULL, quantity REAL NOT NULL,
            price REAL NOT NULL, fee REAL NOT NULL, realized_pnl REAL NOT NULL,
            net_pnl REAL, funding_pnl REAL NOT NULL, reason TEXT NOT NULL
        )
    """)
    connection.execute("""
        CREATE INDEX IF NOT EXISTS paper_trades_symbol_bucket
        ON paper_trades(symbol, bucket)
    """)
    connection.execute("""
        CREATE INDEX IF NOT EXISTS paper_trades_bucket ON paper_trades(bucket)
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS paper_equity (
            bucket INTEGER NOT NULL, symbol TEXT NOT NULL,
            equity REAL NOT NULL, cash REAL NOT NULL,
            margin REAL NOT NULL, realized_pnl REAL NOT NULL, unrealized_pnl REAL NOT NULL,
            fees REAL NOT NULL, funding_pnl REAL NOT NULL, positions INTEGER NOT NULL,
            stale_positions INTEGER NOT NULL, PRIMARY KEY (bucket, symbol)
        )
    """)
    connection.execute("""
        CREATE INDEX IF NOT EXISTS paper_equity_symbol_bucket
        ON paper_equity(symbol, bucket)
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS paper_funding (
            symbol TEXT NOT NULL, time INTEGER NOT NULL, bucket INTEGER NOT NULL,
            rate REAL NOT NULL, mark_price REAL NOT NULL, pnl REAL NOT NULL,
            PRIMARY KEY (symbol, time)
        )
    """)


def _positive_number(value):
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return number if isfinite(number) and number > 0 else None


def _quote(value) -> Optional[Quote]:
    if isinstance(value, Mapping):
        price = _positive_number(value.get("price"))
        mark = _positive_number(value.get("mark_price"))
        book_bid = _positive_number(value.get("bid"))
        book_ask = _positive_number(value.get("ask"))
    else:
        price = _positive_number(value)
        mark = book_bid = book_ask = None
    if price is None:
        return None
    bid = book_bid or price
    ask = book_ask or price
    if bid > ask:
        bid = ask = price
    return Quote(mark or price, bid, ask)


def _positions(connection):
    rows = connection.execute("""
        SELECT symbol, side, quantity, entry_price, margin, opened_at, mark_price, marked_at,
               entry_fee, funding_pnl
        FROM paper_positions ORDER BY symbol
    """)
    return {row[0]: Position(*row) for row in rows}


def position_rows(connection):
    return [
        dict(vars(position), unrealized_pnl=position.unrealized_pnl,
             notional=position.quantity * position.mark_price)
        for position in _positions(connection).values()
    ]


def trade_stats(connection):
    rows = connection.execute("""
        SELECT symbol, COUNT(*), SUM(net_pnl > 0), AVG(net_pnl > 0)
        FROM paper_trades WHERE net_pnl IS NOT NULL GROUP BY symbol ORDER BY symbol
    """)
    return [
        dict(symbol=symbol, closed_trades=closed, wins=wins, win_rate=win_rate)
        for symbol, closed, wins, win_rate in rows
    ]


def _save_position(connection, position):
    connection.execute("""
        INSERT INTO paper_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET mark_price=excluded.mark_price,
                                          marked_at=excluded.marked_at,
                                          funding_pnl=excluded.funding_pnl
    """, (
        position.symbol, position.side, position.quantity, position.entry_price,
        position.margin, position.opened_at, position.mark_price, position.marked_at,
        position.entry_fee, position.funding_pnl,
    ))


def _record_trade(connection, bucket, observed_at, position, action, price, fee, pnl, reason):
    net_pnl = pnl - fee - position.entry_fee + position.funding_pnl if action != "open" else None
    connection.execute("""
        INSERT INTO paper_trades (bucket, observed_at, symbol, action, side, quantity, price,
                                  fee, realized_pnl, net_pnl, funding_pnl, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (bucket, observed_at, position.symbol, action, position.side, position.quantity,
          price, fee, pnl, net_pnl, position.funding_pnl, reason))


def _apply_funding(connection, bucket, observed_at, position, settlements):
    total = 0.0
    for event in settlements:
        try:
            timestamp = int(event["time"])
            rate = float(event["rate"])
        except (KeyError, ValueError, TypeError, OverflowError):
            continue
        price = _positive_number(event.get("mark_price"))
        if not position.opened_at < timestamp <= observed_at or price is None or not isfinite(rate):
            continue
        pnl = -position.side * position.quantity * price * rate
        saved = connection.execute("""
            INSERT OR IGNORE INTO paper_funding (symbol, time, bucket, rate, mark_price, pnl)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (position.symbol, timestamp, bucket, rate, price, pnl))
        if saved.rowcount == 1:
            total += pnl
    return total


def _close_trade(connection, bucket, observed_at, position, quote, fee_rate, slippage,
                 collateral, liquidated, reason):
    price = quote.fill(-position.side, slippage)
    if liquidated:
        price = min(price, quote.mark) if position.side == 1 else max(price, quote.mark)
    pnl = position.side * position.quantity * (price - position.entry_price)
    fee = position.quantity * price * fee_rate
    if pnl - fee < -collateral:
        # Approximate isolated margin loss at the fee-adjusted bankruptcy price.
        price = position.entry_price - position.side * collateral / position.quantity
        price /= 1 - position.side * fee_rate
        pnl = position.side * position.quantity * (price - position.entry_price)
        fee = position.quantity * price * fee_rate
        if not liquidated:
            liquidated, reason = True, "bankruptcy_price"
    action = "liquidate" if liquidated else "close"
    _record_trade(connection, bucket, observed_at, position, action, price, fee, pnl, reason)
    connection.execute("DELETE FROM paper_positions WHERE symbol=?", (position.symbol,))
    return pnl, fee, liquidated


def step(connection, bucket, targets, quotes, settings, *, symbols=(), funding=None,
         observed_at=None, outcomes=None):
    """Record one tick and return True, or return False for a duplicate or old tick.

    Each symbol gets its own starting equity. Target sides are -1, 0 and 1.
    A missing target leaves an existing position
    open. A missing quote retains the last mark and suspends fills for the symbol.
    Funding consumes exchange-confirmed settlements only. The caller supplies
    their original settlement timestamps, rates and settlement mark prices.
    If supplied, outcomes receives the before/after state and entry rejection
    for each asset. Executed legs are stored in paper_trades.
    """
    observed_at = bucket if observed_at is None else observed_at
    if observed_at < bucket:
        raise ValueError("observation time must be at or after the start of the bucket")
    last_bucket, = connection.execute("SELECT MAX(bucket) FROM paper_equity").fetchone()
    if last_bucket is not None and bucket <= last_bucket:
        return False
    previous = {
        row[0]: tuple(row)[1:] for row in connection.execute("""
            SELECT symbol, cash, realized_pnl, fees, funding_pnl
            FROM paper_equity WHERE bucket=?
        """, (last_bucket,))
    }
    fee_rate = settings.fee_bps / 10000
    slippage = settings.slippage_bps / 10000
    funding = funding or {}
    positions = _positions(connection)
    universe = set(symbols).union(previous, positions, targets, quotes)
    initial = (settings.initial_equity, 0.0, 0.0, 0.0)
    for symbol in sorted(universe):
        cash, realized, fees, funding_pnl = previous.get(symbol, initial)
        position = positions.get(symbol)
        previous_side = position.side if position is not None else 0
        quote = _quote(quotes.get(symbol))
        target = targets.get(symbol)
        liquidated = False

        if position is not None:
            flow = _apply_funding(connection, bucket, observed_at, position, funding.get(symbol, ()))
            position.funding_pnl += flow
            cash += flow
            funding_pnl += flow
            if quote is not None:
                position.mark_price = quote.mark
                position.marked_at = observed_at
                collateral = max(0.0, min(position.margin, cash))
                maintenance = position.quantity * quote.mark * settings.maintenance_margin_rate
                liquidated = collateral + position.unrealized_pnl <= maintenance
                close_requested = target in (-1, 0, 1) and target != position.side
                if liquidated or close_requested:
                    reason = "maintenance_margin"
                    if not liquidated:
                        reason = "signal_flat" if target == 0 else "signal_reverse"
                    pnl, fee, liquidated = _close_trade(
                        connection, bucket, observed_at, position, quote, fee_rate,
                        slippage, collateral, liquidated, reason,
                    )
                    realized += pnl
                    fees += fee
                    cash += pnl - fee
                    position = None
        if position is not None:
            _save_position(connection, position)

        wants_entry = position is None and not liquidated and target in (-1, 1) and quote is not None
        if wants_entry and cash > 0.01:
            entry = quote.fill(target, slippage)
            adverse_mark_rate = max(0.0, target * (entry - quote.mark) / entry)
            # Reserve initial margin, entry fees and the immediate mark-to-market loss.
            margin = cash / (1 + settings.leverage * (fee_rate + adverse_mark_rate))
            quantity = margin * settings.leverage / entry
            fee = quantity * entry * fee_rate
            proposed = Position(symbol, target, quantity, entry, margin, observed_at,
                                quote.mark, observed_at, fee)
            maintenance = quantity * quote.mark * settings.maintenance_margin_rate
            if proposed.margin + proposed.unrealized_pnl > maintenance:
                position = proposed
                fees += fee
                cash -= fee
                _save_position(connection, position)
                _record_trade(connection, bucket, observed_at, position, "open", entry, fee, 0.0, "signal_entry")

        margin = position.margin if position is not None else 0.0
        unrealized = position.unrealized_pnl if position is not None else 0.0
        stale = position is not None and quote is None
        connection.execute("""
            INSERT INTO paper_equity VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (bucket, symbol, cash + unrealized, cash, margin, realized, unrealized,
              fees, funding_pnl, int(position is not None), int(stale)))
        if outcomes is not None:
            rejection = None
            if wants_entry and position is None:
                rejection = "insufficient_cash" if cash <= 0.01 else "entry_maintenance"
            outcomes[symbol] = (previous_side, position.side if position else 0, rejection)
    return bool(universe)
