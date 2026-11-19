"""Read a local RC6 PAPER market-data database and optionally sample public aggTrades."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import closing
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
import time


def inspection_symbols(log_path: Path) -> tuple[str, ...]:
    """Return the most recent exact universe from the current log or its rotation."""
    marker = "AUTONOMOUS_UNIVERSE_UPDATED "
    for candidate in (log_path, log_path.with_name(log_path.name + ".1")):
        if not candidate.is_file():
            continue
        symbols = ()
        with candidate.open(encoding="utf-8", errors="replace") as log:
            for line in log:
                if marker in line and " symbols=" in line:
                    symbols = tuple(line.rsplit(" symbols=", 1)[-1].strip().split(","))
        if symbols:
            return symbols
    return ()


def inspect_database(db_path: Path, symbols: tuple[str, ...]) -> dict:
    path = db_path.resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN")
        if not symbols:
            symbols = tuple(row["symbol"] for row in db.execute(
                """SELECT symbol FROM aggregate_trade_stream_state WHERE exchange='BINANCE_UM'
                   UNION SELECT symbol FROM order_flow_state WHERE exchange='BINANCE_UM'
                   ORDER BY symbol"""
            ))

        rows = {}
        for symbol in symbols:
            aggregate = db.execute(
                """SELECT status, last_contiguous_aggregate_trade_id, last_contiguous_trade_time_ms,
                          latest_observed_aggregate_trade_id, latest_observed_trade_time_ms,
                          unresolved_gap_from_id, unresolved_gap_to_id
                   FROM aggregate_trade_stream_state WHERE exchange='BINANCE_UM' AND symbol=?""", (symbol,),
            ).fetchone()
            derived = db.execute(
                """SELECT status, last_processed_aggregate_trade_id, last_trade_time_ms, integrity_reason
                   FROM order_flow_state WHERE exchange='BINANCE_UM' AND symbol=?""", (symbol,),
            ).fetchone()
            raw = db.execute(
                """SELECT aggregate_trade_id, trade_time_ms, first_source_kind, last_source_kind
                   FROM aggregate_trades WHERE exchange='BINANCE_UM' AND symbol=?
                   ORDER BY aggregate_trade_id DESC LIMIT 1""", (symbol,),
            ).fetchone()
            candle = db.execute(
                """SELECT open_time_ms, close_time_ms FROM market_klines
                   WHERE exchange='BINANCE_UM' AND symbol=? AND interval='1m'
                   ORDER BY open_time_ms DESC LIMIT 1""", (symbol,),
            ).fetchone()
            state = {
                "aggregate": dict(aggregate) if aggregate is not None else None,
                "raw": dict(raw) if raw is not None else None,
                "derived": dict(derived) if derived is not None else None,
                "candle": dict(candle) if candle is not None else None,
                "processed_id_lag": None,
                "raw_age_at_candle_ms": None,
                "derived_age_at_candle_ms": None,
            }
            if aggregate is not None and derived is not None:
                state["processed_id_lag"] = (
                    aggregate["last_contiguous_aggregate_trade_id"] - derived["last_processed_aggregate_trade_id"]
                )
            if candle is not None and raw is not None:
                state["raw_age_at_candle_ms"] = candle["close_time_ms"] - raw["trade_time_ms"]
            if candle is not None and derived is not None and derived["last_trade_time_ms"] is not None:
                state["derived_age_at_candle_ms"] = candle["close_time_ms"] - derived["last_trade_time_ms"]
            rows[symbol] = state
    return {"observed_at_utc": datetime.now(UTC).isoformat(timespec="seconds"), "symbols": rows}


async def sample_public_aggregate_trades(seconds: int) -> dict:
    # Import only for the optional public probe. The SQLite report needs only
    # Python's standard library and can run offline.
    from divatrading.market_data.aggregate_trades import BinanceAggregateTradeWebSocketFeed
    from divatrading.market_data.websocket import BINANCE_UM_MARKET_WS_ENDPOINT

    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT")
    feed = BinanceAggregateTradeWebSocketFeed(
        symbols=symbols, endpoint=BINANCE_UM_MARKET_WS_ENDPOINT,
        queue_capacity=4096, max_reconnect_attempts=0,
    )
    stop = asyncio.Event()
    run = asyncio.create_task(feed.run(stop))
    sample = {symbol: {"events": 0, "last_aggregate_trade_id": None, "last_trade_time_ms": None} for symbol in symbols}
    error_type = None
    shutdown_timed_out = False
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    event_task = None
    try:
        while (remaining := deadline - loop.time()) > 0:
            event_task = asyncio.create_task(feed.get_event())
            done, _ = await asyncio.wait((event_task, run), timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
            if event_task not in done:
                break
            event = event_task.result()
            row = sample[event.symbol]
            row["events"] += 1
            row["last_aggregate_trade_id"] = event.aggregate_trade_id
            row["last_trade_time_ms"] = event.trade_time_ms
            event_task = None
    finally:
        if event_task is not None:
            event_task.cancel()
            await asyncio.gather(event_task, return_exceptions=True)
        stop.set()
        try:
            await asyncio.wait_for(run, timeout=8)
        except TimeoutError:
            shutdown_timed_out = True
        except Exception as error:
            error_type = type(error).__name__
    return {
        "sample_seconds": seconds,
        "endpoint": feed.endpoint,
        "connections": feed.stats.connection_count,
        "acknowledgements": feed.stats.acknowledgements_received,
        "malformed": feed.stats.malformed_messages,
        "stale": feed.stats.stale_messages,
        "aggregate_id_gaps": feed.stats.aggregate_id_gaps,
        "error_type": error_type,
        "shutdown_timed_out": shutdown_timed_out,
        "symbols": sample,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("data/paper-auto.db"))
    parser.add_argument("--log", type=Path, default=Path("logs/paper-auto/divatrading.log"))
    parser.add_argument("--symbols", help="Comma-separated exact symbols")
    parser.add_argument("--probe-seconds", type=int, choices=range(5, 31), metavar="5-30")
    parser.add_argument("--output", type=Path, default=Path("rc6-flow-check.json"))
    args = parser.parse_args()
    try:
        if args.output.suffix.lower() != ".json" or args.output.resolve() == args.db.resolve():
            parser.error("Choose a JSON report path separate from the database")
        symbols = tuple(dict.fromkeys(x.strip().upper() for x in args.symbols.split(",") if x.strip())) \
            if args.symbols else inspection_symbols(args.log)
        report = inspect_database(args.db, symbols)
    except (OSError, sqlite3.Error) as error:
        print(f"FLOW_CHECK_FAILED ({type(error).__name__}): {error}")
        return 2
    if args.probe_seconds:
        try:
            report["public_websocket"] = asyncio.run(sample_public_aggregate_trades(args.probe_seconds))
        except ImportError as error:
            report["public_websocket"] = {"error_type": type(error).__name__}
    report["database_opened_read_only"] = True
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"FLOW_CHECK_SAVED {args.output} ({len(report['symbols'])} symbols)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
