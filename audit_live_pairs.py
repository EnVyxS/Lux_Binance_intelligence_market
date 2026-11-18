"""Read-only Binance USD-M perpetual pair audit for the installed RC6."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal
import json
import os
from pathlib import Path

import httpx

from divatrading.core.config import _as_bool, _parse_env_file
from divatrading.exchange.binance.models import parse_exchange_info
from divatrading.execution.binance_private import LIVE_BASE_URL, BinancePrivateApiError, BinancePrivateRestClient
from divatrading.strategy.scalping_market_data import InstrumentSpec
from divatrading.trading.entry_quote_guard import EntryQuoteGuardConfig
from divatrading.trading.exchange_feasibility import (
    SymbolCommission, SymbolConfiguration, SymbolMetadataError, dec, parse_leverage_brackets,
)
from divatrading.trading.risk import PortfolioRiskConfig
from divatrading.trading.sweep_reclaim import SweepReclaimConfig
from divatrading.trading.universe import SymbolUniverseConfig, SymbolUniverseSelector


STRATEGY_SPREAD_LIMIT_BPS = SweepReclaimConfig().max_spread_bps
QUOTE_GUARD = EntryQuoteGuardConfig()


def indexed_rows(payload):
    if not isinstance(payload, list):
        raise ValueError("Expected an array of symbol rows")
    rows = defaultdict(list)
    for row in payload:
        if not isinstance(row, dict) or not isinstance(row.get("symbol"), str) or not row["symbol"].strip():
            raise ValueError("Missing symbol in array row")
        rows[row["symbol"].strip().upper()].append(row)
    return rows


def ticker_metrics(rows):
    row, = rows
    quote_volume = dec(row["quoteVolume"], "24h quote volume")
    count = dec(row["count"], "24h trade count")
    if count != count.to_integral_value():
        raise ValueError("Trade count must be an integer")
    return quote_volume, int(count)


def instrument_findings(spec):
    if spec.tick_size is None or spec.min_notional is None:
        yield "PRICE_OR_NOTIONAL_FILTER_UNAVAILABLE"
    if not {"LIMIT", "STOP_MARKET", "TAKE_PROFIT_MARKET"}.issubset(spec.order_types) or "IOC" not in spec.time_in_force:
        yield "IOC_ENTRY_OR_PROTECTION_NOT_ENABLED"
    if spec.lot_step_size is None or spec.min_qty is None or spec.max_qty is None:
        yield "LIMIT_QUANTITY_FILTER_INCOMPLETE"
    elif (spec.market_lot_step_size is None or spec.market_min_qty is None or spec.market_max_qty is None
          or spec.lot_step_size % spec.market_lot_step_size or spec.market_min_qty > spec.lot_step_size
          or min(spec.max_qty, spec.market_max_qty) < max(spec.min_qty, spec.lot_step_size)):
        yield "IOC_PARTIAL_FILL_EXIT_GRID_INCOMPATIBLE"


def spread_bps(bid, ask):
    if bid <= 0 or ask <= bid:
        raise ValueError("Quote must be positive and uncrossed")
    return (ask - bid) * Decimal(20000) / (bid + ask)


def depth_levels(side, *, descending):
    if not isinstance(side, list) or not side:
        raise ValueError("No depth levels")
    levels = [(dec(price, "depth price"), dec(quantity, "depth quantity")) for price, quantity in side]
    if any(price <= 0 or qty <= 0 for price, qty in levels):
        raise ValueError("Invalid depth level")
    prices = [price for price, _ in levels]
    if prices != sorted(set(prices), reverse=descending):
        raise ValueError("Depth levels are out of order or duplicated")
    return levels


def inspect_depth(client, symbol):
    payload = client.get_order_book_top(symbol, limit=5)
    age_ms = client.exchange_now_ms() - int(payload.get("T") or payload["E"])
    bids = depth_levels(payload["bids"], descending=True)
    asks = depth_levels(payload["asks"], descending=False)
    spread = spread_bps(bids[0][0], asks[0][0])
    return {
        "bid_levels": len(bids),
        "ask_levels": len(asks),
        "bid_depth_usdt": str(sum(price * qty for price, qty in bids)),
        "ask_depth_usdt": str(sum(price * qty for price, qty in asks)),
        "snapshot_age_ms": age_ms,
        "spread_bps": str(spread.quantize(Decimal("0.0001"))),
    }, spread


def audit(client, *, risk_usdt=Decimal("0.20"), max_spread_bps=STRATEGY_SPREAD_LIMIT_BPS, deep_limit=8):
    if client.environment != "LIVE" or client.base_url != LIVE_BASE_URL:
        raise ValueError("Audit requires the official Binance USD-M mainnet REST host")
    universe = SymbolUniverseConfig(max_active_symbols=deep_limit)
    risk_config = PortfolioRiskConfig()
    report = {
        "observed_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "order_submission_performed": False,
        "account_mutation_performed": False,
        "rate_limited": False,
        "risk_usdt": str(risk_usdt),
        "max_spread_bps": str(max_spread_bps),
        "min_quote_volume_24h_usdt": str(universe.min_quote_volume_usdt),
        "min_trade_count_24h": universe.min_trade_count_24h,
        "deep_limit": deep_limit,
        "reads": {},
        "pairs": {},
    }
    stop_reason = None
    missing_data = False

    def read(name, fetch):
        nonlocal stop_reason
        if stop_reason:
            report["reads"][name] = {"status": "SKIPPED", "reason": stop_reason}
            return None
        try:
            result = fetch()
        except (httpx.HTTPError, RuntimeError, ValueError, KeyError, TypeError, ArithmeticError) as error:
            detail = {"status": "FAIL", "error": type(error).__name__}
            if isinstance(error, (BinancePrivateApiError, httpx.HTTPStatusError)):
                status = error.status_code if isinstance(error, BinancePrivateApiError) else error.response.status_code
                detail["http_status"] = status
                if isinstance(error, BinancePrivateApiError) and type(error.code) is int:
                    detail["binance_code"] = error.code
                if status in {418, 429} or detail.get("binance_code") == -1003:
                    stop_reason = "BINANCE_RATE_LIMIT"
                    report["rate_limited"] = True
                elif status == 451:
                    stop_reason = "HTTP_451"
            report["reads"][name] = detail
            return None
        report["reads"][name] = {"status": "PASS"}
        return result

    server_time = read("SERVER_TIME", client.sync_server_time)
    snapshot = read("EXCHANGE_INFO", lambda: parse_exchange_info(
        client.get_exchange_info(), source_endpoint=f"{LIVE_BASE_URL}/fapi/v1/exchangeInfo"))
    tickers = read("TICKERS_24H", lambda: indexed_rows(client.request("GET", "/fapi/v1/ticker/24hr", signed=False)))
    quotes = read("BOOK_TICKERS", lambda: indexed_rows(client.request("GET", "/fapi/v1/ticker/bookTicker", signed=False)))
    brackets = configs = None
    if server_time is not None:
        brackets = read("LEVERAGE_BRACKETS", lambda: indexed_rows(client.get_leverage_brackets()))
        configs = read("SYMBOL_CONFIGS", lambda: indexed_rows(client.get_symbol_config()))

    active = snapshot.tradable_usdt_perpetuals if snapshot else ()
    metrics = {}
    ticker_rows = []
    for info in active:
        rows = (tickers or {}).get(info.symbol, ())
        try:
            metrics[info.symbol] = ticker_metrics(rows)
        except (ValueError, KeyError):
            continue
        ticker_rows.append(rows[0])
    ranks = {}
    if snapshot is not None and tickers is not None:
        try:
            selection = SymbolUniverseSelector(universe).select(snapshot, ticker_rows)
        except ValueError:
            selection = None
        if selection:
            ranks = {item.info.symbol: item for item in selection.selected}

    specs = {}
    for info in sorted(active, key=lambda item: item.symbol):
        symbol = info.symbol
        findings = []
        pair = {"findings": findings, "deep": {"status": "NOT_SELECTED"}}
        report["pairs"][symbol] = pair
        try:
            spec = InstrumentSpec.from_symbol_info(info)
            specs[symbol] = spec
            findings.extend(instrument_findings(spec))
            if spec.min_notional is not None:
                pair["min_notional_usdt"] = str(spec.min_notional)
        except ValueError:
            findings.append("INVALID_INSTRUMENT_FILTERS")
            missing_data = True
        if symbol not in metrics:
            findings.append("TICKER_24H_MISSING_DUPLICATE_OR_INVALID")
            missing_data = True
        else:
            volume, count = metrics[symbol]
            pair["quote_volume_24h_usdt"] = str(volume)
            pair["trade_count_24h"] = count
            if symbol in ranks:
                pair["auto_rank"] = ranks[symbol].rank
            elif volume < universe.min_quote_volume_usdt:
                findings.append("QUOTE_VOLUME_BELOW_MINIMUM")
            elif count < universe.min_trade_count_24h:
                findings.append("TRADE_COUNT_BELOW_MINIMUM")
            else:
                pair["auto_selection"] = "ACTIVE_SYMBOL_CAP"
        try:
            quote, = (quotes or {}).get(symbol, ())
            if dec(quote["bidQty"], "bid quantity") <= 0 or dec(quote["askQty"], "ask quantity") <= 0:
                raise ValueError("Empty best quote")
            spread = spread_bps(dec(quote["bidPrice"], "bid"), dec(quote["askPrice"], "ask"))
            pair["spread_bps"] = str(spread.quantize(Decimal("0.0001")))
            if spread > max_spread_bps:
                findings.append("SPREAD_ABOVE_LIMIT")
        except (ValueError, KeyError, ArithmeticError):
            findings.append("BOOK_TICKER_MISSING_OR_INVALID")
            missing_data = True
        try:
            tiers = parse_leverage_brackets((brackets or {}).get(symbol, ()), symbol)
            pair["bracket_count"] = len(tiers)
            pair["max_reported_leverage"] = max(tier.initial_leverage for tier in tiers)
        except SymbolMetadataError as error:
            findings.append(f"BRACKET_{error}")
            missing_data = True
        try:
            config = SymbolConfiguration.from_binance((configs or {}).get(symbol, ()), symbol)
            pair["current_margin_type"] = config.margin_type
            pair["current_leverage"] = config.leverage
        except SymbolMetadataError as error:
            findings.append(f"CONFIG_{error}")
            missing_data = True

    def commission_for(symbol):
        payload = client.get_commission_rate(symbol)
        if payload.get("symbol") != symbol:
            raise ValueError("Commission symbol mismatch")
        return SymbolCommission.from_binance(payload)

    for symbol in ranks:
        pair = report["pairs"][symbol]
        deep = pair["deep"]
        deep["status"] = "UNAVAILABLE"
        if server_time is None:
            continue
        fee = read(f"COMMISSION_{symbol}", lambda: commission_for(symbol))
        depth = read(f"DEPTH_{symbol}", lambda: inspect_depth(client, symbol))
        if depth is not None:
            deep["depth"], spread = depth
            if spread > max_spread_bps:
                pair["findings"].append("DEPTH_SPREAD_ABOVE_LIMIT")
            age_ms = deep["depth"]["snapshot_age_ms"]
            if not -QUOTE_GUARD.max_future_skew_ms <= age_ms <= QUOTE_GUARD.max_snapshot_age_ms:
                pair["findings"].append("DEPTH_SNAPSHOT_STALE_OR_FUTURE")
        if fee is not None:
            deep["maker_rate"] = str(fee.maker_rate)
            deep["taker_rate"] = str(fee.taker_rate)
            minimum = specs[symbol].min_notional if symbol in specs else None
            if minimum is not None:
                reserve_bps = risk_config.slippage_reserve_bps * 2 + risk_config.funding_reserve_bps
                cost_floor = (fee.round_trip_reserve(minimum, multiplier=risk_config.fee_reserve_multiplier)
                              + minimum * reserve_bps / Decimal(10000))
                deep["cost_reserve_floor_usdt_before_stop"] = str(cost_floor)
                if cost_floor >= risk_usdt:
                    pair["findings"].append("RISK_BUDGET_BELOW_COST_FLOOR")
        if fee is not None and depth is not None:
            deep["status"] = "SAMPLED"

    report["coverage"] = {
        "active_pairs": len(active),
        "pairs_with_findings": sum(bool(pair["findings"]) for pair in report["pairs"].values()),
        "deep_selected": len(ranks),
        "deep_sampled": sum(pair["deep"]["status"] == "SAMPLED" for pair in report["pairs"].values()),
    }
    report["finding_counts"] = dict(Counter(code for pair in report["pairs"].values() for code in pair["findings"]))
    reads_passed = all(result["status"] == "PASS" for result in report["reads"].values())
    report["status"] = "COMPLETE" if active and reads_passed and not missing_data else "INCOMPLETE"
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deep-limit", type=int, default=8, choices=range(1, 25), metavar="1-24")
    parser.add_argument("--risk-usdt", type=Decimal, default=Decimal("0.20"))
    parser.add_argument("--output", type=Path, default=Path("rc6-pair-audit.json"))
    args = parser.parse_args()
    try:
        values = _parse_env_file(Path(".env")) | os.environ
        read_only = _as_bool("DIVA_ENABLE_LIVE_READ_ONLY_PREFLIGHT",
                             values.get("DIVA_ENABLE_LIVE_READ_ONLY_PREFLIGHT", "false"))
        trading_enabled = _as_bool("DIVA_LIVE_TRADING_ENABLED", values.get("DIVA_LIVE_TRADING_ENABLED", "false"))
        execution_spread_limit = dec(values.get("DIVA_MAX_ENTRY_SPREAD_BPS", "20"), "spread limit")
    except (OSError, ValueError):
        parser.error("Check local .env and spread limit")
    if values.get("DIVA_EXECUTION_MODE", "").strip().upper() != "LIVE" or trading_enabled or not read_only:
        parser.error("Set DIVA_EXECUTION_MODE=LIVE, DIVA_LIVE_TRADING_ENABLED=false and DIVA_ENABLE_LIVE_READ_ONLY_PREFLIGHT=true")
    if not args.risk_usdt.is_finite() or args.risk_usdt <= 0 or execution_spread_limit <= 0:
        parser.error("Risk and spread limit must be positive and finite")
    key, secret = values.get("BINANCE_API_KEY"), values.get("BINANCE_API_SECRET")
    if not key or not secret:
        parser.error("Set BINANCE_API_KEY and BINANCE_API_SECRET locally")
    if args.output.exists() and not args.output.is_file():
        parser.error("Output path must be a file")
    if args.output.suffix.lower() != ".json":
        parser.error("Output filename must end in .json")
    spread_limit = min(STRATEGY_SPREAD_LIMIT_BPS, execution_spread_limit)
    with BinancePrivateRestClient(key, secret, environment="LIVE") as client:
        report = audit(client, risk_usdt=args.risk_usdt, max_spread_bps=spread_limit, deep_limit=args.deep_limit)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], **report["coverage"], "report": str(args.output)}, indent=2))
    return 0 if report["status"] == "COMPLETE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
