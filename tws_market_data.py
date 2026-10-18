import csv
import math
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper


# -------------------- Configuration --------------------

TWS_HOST = "127.0.0.1"
TWS_PORT = 7497                  # TWS paper trading
CLIENT_ID = 21

RUN_SECONDS = 120
SAMPLE_INTERVAL = 1.0
INITIAL_DATA_WAIT_SECONDS = 20

# 3 = delayed streaming
# 4 = delayed-frozen: useful when testing outside trading hours
# IBKR may return live data instead if your account has permission.
MARKET_DATA_TYPE = 4

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"

COLUMNS = [
    "snapshot_time_utc",
    "symbol",
    "sec_type",
    "exchange",
    "currency",
    "bid",
    "ask",
    "last",
    "bid_size",
    "ask_size",
    "last_size",
    "daily_volume",
    "last_trade_time_utc",
    "bid_received_time_utc",
    "ask_received_time_utc",
    "last_received_time_utc",
    "requested_market_data_type",
    "market_data_type",
    "status",
    "last_api_message",
]

# Live and delayed tick IDs mapped to the same output fields.
PRICE_FIELDS = {
    1: "bid",
    2: "ask",
    4: "last",
    66: "bid",
    67: "ask",
    68: "last",
}

SIZE_FIELDS = {
    0: "bid_size",
    3: "ask_size",
    5: "last_size",
    8: "daily_volume",
    69: "bid_size",
    70: "ask_size",
    71: "last_size",
    74: "daily_volume",
}

LAST_TIMESTAMP_TICKS = {45, 88}
CONNECTION_NOTICES = {2104, 2106, 2107, 2108, 2158}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def numeric_value(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError, OverflowError):
        return None


def stock(symbol, primary_exchange=None):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"

    if primary_exchange:
        contract.primaryExchange = primary_exchange

    return contract


def crypto(symbol, exchange="PAXOS"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "CRYPTO"
    contract.exchange = exchange
    contract.currency = "USD"
    return contract


CONTRACTS = [
    stock("AAPL"),
    stock("SNDK"),
    stock("MSFT"),
    stock("NVDA"),
    stock("TSLA"),
    stock("NBIS"),
    stock("META"),
    crypto("BTC"),
]


class MarketDataApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.ready = threading.Event()
        self.lock = threading.Lock()
        self.states = {}
        self.active_requests = []

    def nextValidId(self, orderId):
        # Signals that the API handshake has completed.
        self.ready.set()

    def subscribe(self, req_id, contract):
        with self.lock:
            self.states[req_id] = {
                "symbol": contract.symbol,
                "sec_type": contract.secType,
                "exchange": contract.exchange,
                "currency": contract.currency,
                "bid": None,
                "ask": None,
                "last": None,
                "bid_size": None,
                "ask_size": None,
                "last_size": None,
                "daily_volume": None,
                "last_trade_time_utc": None,
                "bid_received_time_utc": None,
                "ask_received_time_utc": None,
                "last_received_time_utc": None,
                "requested_market_data_type": MARKET_DATA_TYPE,
                "market_data_type": None,
                "last_api_message": "",
            }

        self.reqMktData(req_id, contract, "", False, False, [])
        self.active_requests.append(req_id)

        print(
            f"Subscribed: request={req_id} "
            f"{contract.symbol} {contract.secType} "
            f"{contract.exchange}",
            flush=True,
        )

    def marketDataType(self, reqId, marketDataType):
        with self.lock:
            state = self.states.get(reqId)
            if state is not None:
                state["market_data_type"] = marketDataType

    def tickPrice(self, reqId, tickType, price, attrib):
        field = PRICE_FIELDS.get(tickType)
        if field is None:
            return

        value = numeric_value(price)

        # Nonpositive prices are not usable quotes for these instruments.
        if value is not None and value <= 0:
            value = None

        with self.lock:
            state = self.states.get(reqId)
            if state is None:
                return

            state[field] = value
            state[f"{field}_received_time_utc"] = utc_now()

    def tickSize(self, reqId, tickType, size):
        field = SIZE_FIELDS.get(tickType)
        if field is None:
            return

        value = numeric_value(size)
        if value is not None and value < 0:
            value = None

        with self.lock:
            state = self.states.get(reqId)
            if state is not None:
                state[field] = value

    def tickString(self, reqId, tickType, value):
        if tickType not in LAST_TIMESTAMP_TICKS:
            return

        try:
            timestamp = datetime.fromtimestamp(
                float(value), tz=timezone.utc
            ).isoformat()
        except (ValueError, TypeError, OverflowError, OSError):
            return

        with self.lock:
            state = self.states.get(reqId)
            if state is not None:
                state["last_trade_time_utc"] = timestamp

    def error(self, reqId, *args):
        # Support both older and newer ibapi error callback signatures:
        # old: errorCode, errorString, [advancedOrderRejectJson]
        # new: errorTime, errorCode, errorString, [...]
        if len(args) >= 3 and isinstance(args[1], int):
            code, message = args[1], str(args[2])
        elif len(args) >= 2:
            code, message = args[0], str(args[1])
        else:
            print(f"API message: request={reqId}, details={args}", flush=True)
            return

        label = "INFO" if code in CONNECTION_NOTICES else "API"
        print(
            f"{label} request={reqId} code={code}: {message}",
            flush=True,
        )

        with self.lock:
            state = self.states.get(reqId)
            if state is not None:
                state["last_api_message"] = f"{code}: {message}"

    def connectionClosed(self):
        print("TWS connection closed.", flush=True)

    def all_contracts_have_prices(self):
        with self.lock:
            return bool(self.states) and all(
                any(state[field] is not None for field in ("bid", "ask", "last"))
                for state in self.states.values()
            )

    def snapshot_rows(self):
        timestamp = utc_now()
        rows = []

        with self.lock:
            for state in self.states.values():
                row = dict(state)
                row["snapshot_time_utc"] = timestamp

                if row["bid"] is not None and row["ask"] is not None:
                    row["status"] = "BID_ASK_AVAILABLE"
                elif any(
                    row[field] is not None
                    for field in ("bid", "ask", "last")
                ):
                    row["status"] = "PARTIAL_PRICE_DATA"
                elif row["last_api_message"]:
                    row["status"] = "NO_PRICES_CHECK_API_MESSAGE"
                else:
                    row["status"] = "WAITING_FOR_PRICES"

                rows.append(row)

        return rows


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    csv_path = OUTPUT_DIR / f"tws_quotes_{run_id}.csv"

    app = MarketDataApp()
    api_thread = None

    try:
        print(
            f"Connecting to TWS at {TWS_HOST}:{TWS_PORT}...",
            flush=True,
        )

        app.connect(TWS_HOST, TWS_PORT, clientId=CLIENT_ID)

        api_thread = threading.Thread(target=app.run, daemon=True)
        api_thread.start()

        if not app.ready.wait(timeout=20):
            raise RuntimeError(
                "TWS API handshake timed out. Check TWS login, "
                "socket port, and API connection permissions."
            )

        print("TWS API handshake complete.", flush=True)

        app.reqMarketDataType(MARKET_DATA_TYPE)

        for req_id, contract in enumerate(CONTRACTS, start=1000):
            app.subscribe(req_id, contract)
            time.sleep(0.1)

        print(
            f"Waiting up to {INITIAL_DATA_WAIT_SECONDS}s "
            "for initial prices...",
            flush=True,
        )

        wait_deadline = time.monotonic() + INITIAL_DATA_WAIT_SECONDS

        while time.monotonic() < wait_deadline:
            if not app.isConnected():
                raise RuntimeError("TWS disconnected during initialization.")
            if app.all_contracts_have_prices():
                break
            time.sleep(0.25)

        print(f"Writing CSV: {csv_path}", flush=True)
        print(
            f"Sampling {len(CONTRACTS)} contracts "
            f"for {RUN_SECONDS} seconds...",
            flush=True,
        )

        total_rows = 0
        start = time.monotonic()
        next_report = 0

        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=COLUMNS)
            writer.writeheader()
            handle.flush()

            while time.monotonic() - start < RUN_SECONDS:
                if not app.isConnected():
                    raise RuntimeError("TWS disconnected during collection.")

                rows = app.snapshot_rows()
                writer.writerows(rows)
                handle.flush()
                total_rows += len(rows)

                elapsed = time.monotonic() - start
                if elapsed >= next_report:
                    summary = ", ".join(
                        f"{row['symbol']}={row['status']}"
                        for row in rows
                    )
                    print(
                        f"{elapsed:5.1f}s | rows={total_rows} | {summary}",
                        flush=True,
                    )
                    next_report += 10

                time.sleep(SAMPLE_INTERVAL)

        print(f"\nFinished. Wrote {total_rows} rows.", flush=True)
        print(f"CSV: {csv_path}", flush=True)

        for row in app.snapshot_rows():
            print(
                f"{row['symbol']:5} "
                f"mode={row['market_data_type']} "
                f"bid={row['bid']} "
                f"ask={row['ask']} "
                f"last={row['last']} "
                f"status={row['status']}",
                flush=True,
            )

    except KeyboardInterrupt:
        print("\nStopped. Already-written CSV rows are preserved.", flush=True)

    finally:
        if app.isConnected():
            for req_id in app.active_requests:
                app.cancelMktData(req_id)

            time.sleep(0.2)
            app.disconnect()

        if api_thread is not None:
            api_thread.join(timeout=2)


if __name__ == "__main__":
    main()
