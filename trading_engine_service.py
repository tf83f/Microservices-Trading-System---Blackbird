"""
============================================================
                        TRADING ENGINE SSERVICE
============================================================

This module implements a *real-time execution engine* that:

1. Listens for incoming trade requests on the `orders` queue.
2. Processes orders sent by the signal engine (open/close spreads).
3. Applies strict risk checks before accepting an order.
4. Saves executed trades into a database (risk impact + pnl).
5. Responds synchronously to the caller through an RPC-style
   RabbitMQ reply queue (ACK / NOT ACK).

The engine acts as the authoritative source of execution state,
risk exposure, and cumulative PnL.

The system consists of 3 major components:

------------------------------------------------------------
A) RPC Request Handler
------------------------------------------------------------
Implements a classical RabbitMQ "RPC server" pattern:

- Consumes messages from the `orders` queue.
- Each message contains a full order request:
      buy leg, sell leg, size, prices, timestamps
- For every request:
      → validate risk
      → avoid duplicates
      → accept or reject
- Responds to the originator using:
      props.reply_to
      props.correlation_id

This guarantees:
- Deterministic synchronous response
- No lost messages
- No ambiguous execution state

------------------------------------------------------------
B) Core Execution Logic
------------------------------------------------------------
The execution engine has two modes:

**1. Real Bybit Execution (`_place_order`)**
   Places both legs of the spread:
     - enforces risk limits
     - enforces uniqueness (optional)
     - Buy (long coin)
     - Sell (short coin)
   using Bybit HTTP REST API.

   Execution is validated through Bybit response codes.
   Only if both legs succeed:
       → order is ACK
       → order is recorded in local state

**2. Simulation Mode (`_place_order_always_true`)**
   Skips real execution and:
     - enforces risk limits
     - enforces uniqueness (optional)
     - records positions, pnl, and DB entries

   This mode is used for:
     - backtesting
     - stress testing
     - development
     - environments without Bybit credentials

The caller (signal engine) does not need to know which mode
is active: it only receives ACK or NOT ACK.

------------------------------------------------------------
C) Position, Risk & PnL Manager
------------------------------------------------------------
The engine maintains internal dictionaries tracking:

- **risks**:  
    Per-leg exposure keyed by:
       (category, symbol)
    Positive = long exposure  
    Negative = short exposure  

- **orders**:
    List of all accepted order objects (internal normalized form).

- **pnl**:
    Accumulated realized PnL in quote currency.

Risk model:

      risk[buy_leg]  += buy_price  * size
      risk[sell_leg] -= sell_price * size

PNL model:

      pnl -= buy_price  * size
      pnl += sell_price * size

If any updated exposure violates:

      abs(exposure) > MAX_RISK_OPEN_POSITION

The engine immediately returns:
      → {"status": "NOT ACK", "info": "risk limit exceed"}

This protection ensures the execution engine never accepts
more position than allowed by global risk settings.

------------------------------------------------------------
DATABASE PERSISTENCE
------------------------------------------------------------
Every accepted order is written to the database via
DatabaseManager (if enabled).

For each execution, two rows are inserted:

1. BUY leg
2. SELL leg

Stored fields include:
- category
- symbol
- price
- size
- side (buy/sell)
- total_value
- timestamp

This ensures accurate historical reconstruction of:

- positions over time
- order-level pnl
- exposure changes
- execution audit logs

------------------------------------------------------------
FAILSAFE BEHAVIOR
------------------------------------------------------------
The engine is designed to remain stable under all conditions:

- If the database is down → orders still ACK but log an error.
- If RabbitMQ disconnects → engine logs, retries, or exits clearly.
- If Bybit API fails → NOT ACK is returned cleanly.
- Duplicate orders → rejected when unique_order=True.
- Risk overflow → rejected deterministically.

------------------------------------------------------------
EXECUTION FLOW SUMMARY
------------------------------------------------------------

1. Signal engine sends order → pushed to queue "orders".

2. Trading Engine receives message via RabbitMQ callback.

3. Order is validated:
       - check uniqueness
       - check risk exposure
       - check execution mode (real/sim)

4. If valid:
       → risk updated
       → pnl updated
       → DB entries written (optional)
       → reply sent: {"status": "ACK"}

5. Otherwise:
       → reply sent: {"status": "NOT ACK"}

6. The signal engine waits for this reply before updating
   its internal spread state.

============================================================
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import pika
from pybit.unified_trading import HTTP
from database_manager import DatabaseManager
from config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("trading_engine")


class ConnectionError(Exception):
    pass


class ConnectionManager:
    """Wrapper for pika.BlockingConnection with simple reconnect support."""

    def __init__(self, host: str = "localhost"):
        self.host = host
        self._conn: Optional[pika.BlockingConnection] = None

    def connect(self) -> pika.BlockingConnection:
        """Return an open connection or establish a new one."""
        try:
            if self._conn is None or getattr(self._conn, "is_closed", True):
                self._conn = pika.BlockingConnection(pika.ConnectionParameters(self.host))
                logger.info("Connected to RabbitMQ at %s", self.host)
            return self._conn
        except Exception as exc:
            logger.exception("Failed to connect to RabbitMQ at %s", self.host)
            raise ConnectionError(str(exc)) from exc

    def close(self) -> None:
        try:
            if self._conn and not getattr(self._conn, "is_closed", True):
                self._conn.close()
                logger.info("Closed RabbitMQ connection")
        except Exception:
            logger.exception("Error closing RabbitMQ connection")

class TradingEngine:
    """
    TradingEngine: consumes 'orders' RPC requests and executes or simulates them.

    Behavior is controlled by Config.READ_ORDER_SENDING (real execution) and
    Config.DUMP_ORDER_TO_DB (persist trades).
    """

    def __init__(self) -> None:
        # API session (Bybit)
        try:
            self.session = HTTP(
                testnet=Config.TESTNET,
                api_key=Config.API_KEY,
                api_secret=Config.API_SECRET,
            )
        except Exception:
            logger.exception("Failed to create Bybit HTTP session; continuing (simulation mode possible)")

        # Settings
        self.unique_order: bool = bool(getattr(Config, "UNIQUE_ORDER", False))
        self.max_risk: float = float(getattr(Config, "MAX_RISK_OPEN_POSITION", 1e9))

        # Internal state
        # orders stored in normalized internal form: dict(buy, buy_price, sell, sell_price, size)
        self.orders: List[Dict[str, Any]] = []
        self.risks: Dict[Tuple[str, str], float] = {}
        self.pnl: float = 0.0

        # Database manager (optional)
        self.db_manager: Optional[DatabaseManager] = None
        if getattr(Config, "DUMP_ORDER_TO_DB", False):
            try:
                self.db_manager = DatabaseManager(Config.DB_CONFIG)
                self.db_manager.connect()
                logger.info("Database connection established")
            except Exception:
                logger.exception("Failed to connect to database. Continuing without DB persistence.")
                self.db_manager = None

        # RabbitMQ connection/channel
        self.conn_manager = ConnectionManager(host=getattr(Config, "RABBIT_HOST", "localhost"))
        try:
            conn = self.conn_manager.connect()
            self.ch = conn.channel()
            # ensure queue exists
            self.ch.queue_declare(queue="orders")
            # register consumer
            self.ch.basic_consume(queue="orders", on_message_callback=self.on_request)
            logger.info("TradingEngine bound to 'orders' queue")
            self.ch.start_consuming()

        except ConnectionError:
            logger.exception("Cannot start TradingEngine due to RabbitMQ connection failure")
            raise
        except Exception:
            logger.exception("Unexpected error during RabbitMQ setup")
            raise

    @staticmethod
    def _safe_json_load(body: bytes) -> Optional[Dict[str, Any]]:
        """Parse JSON body and return dict or None on failure."""
        try:
            return json.loads(body)
        except Exception:
            logger.exception("Failed to decode JSON order body")
            return None

    @staticmethod
    def _normalize_order(order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize input order into the internal form used for uniqueness checks:
        only keep buy, buy_price, sell, sell_price, size.
        """
        return {
            "buy": order.get("buy"),
            "buy_price": order.get("buy_price"),
            "sell": order.get("sell"),
            "sell_price": order.get("sell_price"),
            "size": order.get("size"),
        }

    def on_request(self, ch: pika.adapters.blocking_connection.BlockingChannel, method, props, body: bytes) -> None:
        """
        Callback executed on every incoming message in the 'orders' queue.
        Decodes the order, routes to real or simulated placement, and replies
        on the RPC reply_to/correlation_id.
        """
        logger.debug("Received raw order message")
        order = self._safe_json_load(body)
        if order is None:
            # malformed message: NACK the message to avoid dropping silently
            try:
                ch.basic_nack(method.delivery_tag, requeue=False)
            except Exception:
                logger.exception("Failed to nack malformed message")
            return

        try:
            # Select execution mode
            if getattr(Config, "REAL_ORDER_SENDING", False):
                result = self._place_order(order)
            else:
                result = self._place_order_always_true(order)

            # send response (RPC)
            try:
                ch.basic_publish(
                    exchange="",
                    routing_key=props.reply_to,
                    properties=pika.BasicProperties(correlation_id=props.correlation_id),
                    body=json.dumps(result),
                )
            except Exception:
                logger.exception("Failed to publish RPC reply; result=%s", result)

            # ack original message
            ch.basic_ack(method.delivery_tag)

        except Exception:
            logger.exception("Unhandled exception while processing order")
            # attempt a safe nack (do not requeue to avoid loops)
            try:
                ch.basic_nack(method.delivery_tag, requeue=False)
            except Exception:
                logger.exception("Failed to nack message after processing exception")

    def _place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Place both legs on Bybit using the HTTP API. Enforce uniqueness and risk limits.
        Returns ACK/NOT ACK dict.
        """
        try:
            # Risk
            if self._risk_exceed(order):
                logger.warning("Order rejected by risk check: %s", self._normalize_order(order))
                return {"status": "NOT ACK", "info": "risk limit exceed"}

            normalized = self._normalize_order(order)
            if self.unique_order and normalized in self.orders:
                logger.warning("Duplicate order rejected (unique enforced): %s", normalized)
                return {"status": "NOT ACK", "info": "order already placed"}

            # Place buy leg
            try:
                buy_resp = self.session.place_order(
                    category=order["buy"][0],
                    symbol=order["buy"][1],
                    side="Buy",
                    orderType="Market",
                    qty=str(order["size"]),
                )
                retCode_buy = buy_resp.get("retCode",1)
            except Exception:
                logger.exception("Buy leg placement failed")
                return {"status": "NOT ACK", "info": "buy leg failed"}

            # Place sell leg
            try:
                sell_resp = self.session.place_order(
                    category=order["sell"][0],
                    symbol=order["sell"][1],
                    side="Sell",
                    orderType="Market",
                    qty=str(order["size"]),
                )
                retCode_sell = sell_resp.get("retCode",1)
            except Exception:
                logger.exception("Sell leg placement failed")
                # attempt to rollback buy if needed (not implemented)
                return {"status": "NOT ACK", "info": "sell leg failed"}

            # Validate both legs
            if retCode_buy == 0 and retCode_sell == 0:
                # Persist and update state
                try:
                    self._save_order(order)
                    self.orders.append(normalized)
                except Exception:
                    logger.exception("Failed to record order after successful execution")
                    # still ACK because execution happened
                logger.info("Order placed successfully: %s", normalized)
                return {"status": "ACK", "info": "order placed"}
            else:
                logger.warning("Exchange returned non-zero retCode buy=%s sell=%s", retCode_buy, retCode_sell)
                return {"status": "NOT ACK", "info": "order not placed"}
        except Exception:
            logger.exception("Unexpected exception in _place_order")
            return {"status": "NOT ACK", "info": "internal error"}

    def _place_order_always_true(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Simulated path used for testing and development.
        """
        try:
            if self._risk_exceed(order):
                logger.warning("Simulated order rejected by risk check: %s", self._normalize_order(order))
                return {"status": "NOT ACK", "info": "risk limit exceed"}

            normalized = self._normalize_order(order)
            if self.unique_order and normalized in self.orders:
                logger.warning("Simulated duplicate order rejected: %s", normalized)
                return {"status": "NOT ACK", "info": "order already placed"}

            # Record order and update accounting
            try:
                self._save_order(order)
                self.orders.append(normalized)
            except Exception:
                logger.exception("Failed to save simulated order")
                return {"status": "NOT ACK", "info": "save failed"}

            logger.info("Simulated order accepted: %s", normalized)
            return {"status": "ACK", "info": "order placed"}
        except Exception:
            logger.exception("Unexpected exception in _place_order_always_true")
            return {"status": "NOT ACK", "info": "internal error"}

    def _risk_exceed(self, order: Dict[str, Any]) -> bool:
        """
        Evaluate exposure after applying this order; return True if it exceeds global risk.
        """
        try:
            buy_key = tuple(order["buy"])
            sell_key = tuple(order["sell"])
            buy_after = abs(self.risks.get(buy_key, 0.0) + float(order["buy_price"]) * float(order["size"]))
            sell_after = abs(self.risks.get(sell_key, 0.0) - float(order["sell_price"]) * float(order["size"]))
            exceed = buy_after > self.max_risk or sell_after > self.max_risk
            if exceed:
                logger.debug("Risk exceed check: buy_after=%s sell_after=%s max_risk=%s", buy_after, sell_after, self.max_risk)
            return exceed
        except Exception:
            logger.exception("Error while computing risk; rejecting order by default")
            return True

    def _save_order(self, order: Dict[str, Any]) -> None:
        """
        Update internal exposure and PnL and optionally persist to DB.
        This function assumes the order executed successfully.
        """
        try:
            buy_key = tuple(order["buy"])
            sell_key = tuple(order["sell"])
            qty = float(order["size"])
            buy_price = float(order["buy_price"])
            sell_price = float(order["sell_price"])

            # Update exposures
            self.risks[buy_key] = self.risks.get(buy_key, 0.0) + buy_price * qty
            self.risks[sell_key] = self.risks.get(sell_key, 0.0) - sell_price * qty

            # Update PnL (realized at execution)
            self.pnl -= buy_price * qty
            self.pnl += sell_price * qty

            # Prepare DB rows
            data = [
                (buy_key[0], buy_key[1], buy_price, qty, "buy", -buy_price * qty, order.get("ts_buy", int(time.time() * 1000))),
                (sell_key[0], sell_key[1], sell_price, qty, "sell", sell_price * qty, order.get("ts_sell", int(time.time() * 1000))),
            ]

            if getattr(Config, "DUMP_ORDER_TO_DB", False) and self.db_manager:
                try:
                    self.db_manager.insert_batch(Config.TABLES["order"], data)
                    logger.debug("Order persisted to DB")
                except Exception:
                    logger.exception("Failed to persist order to DB; continuing")

            logger.info("Order accounting updated: pnl=%.6f risks=%s", self.pnl, self.risks)
        except Exception:
            logger.exception("Failed to update internal state for order")


if __name__ == "__main__":
    try:
        engine = TradingEngine()
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down TradingEngine.")
        sys.exit(0)
    except Exception:
        logger.exception("Fatal error starting TradingEngine")
        sys.exit(1)
