"""
============================================================
                SIGNAL ENGINE
============================================================

This module implements a real-time *spread-trading engine* that:

1. Subscribes to live orderbook updates from RabbitMQ.
2. Groups related assets into "matching groups" using a matching_key.
3. Continuously evaluates cross-asset spreads inside each group.
4. Opens a spread when:
       bid(short_asset) > ask(long_asset)
   meaning there is a strictly positive arbitrage opportunity.

5. Closes previously opened spreads when:
       exit_cost < initial_entry_spread
   meaning the convergence allows us to lock in realized PnL.

6. Sends trades to an external trading engine through a
   request/response RabbitMQ pattern and waits for ACK/NACK.

The system consists of 3 major components:

------------------------------------------------------------
A) OrderManager
------------------------------------------------------------
Responsible for sending orders to the trading engine and
waiting for synchronous confirmation. It implements a
RabbitMQ “RPC-style” pattern using correlation IDs.

It guarantees:
- No local spread state is updated until the trading engine
  explicitly acknowledges the execution.
- Messages are routed through an "orders" queue.
- Replies are received on an auto-generated callback queue.

------------------------------------------------------------
B) Signal (Core Engine)
------------------------------------------------------------
Maintains the complete in-memory view of:
- bids and asks for each asset (SortedDict for best-price access)
- mapping between matching groups → assets
- open spreads and remaining quantities
- orderbook timestamps for data freshness validation

Its responsibilities:
- Parse incoming orderbooks
- Update internal market state
- Find profitable spreads (open)
- Find profitable exits (close)
- Maintain consistency with subscription updates

A matching group may contain 2 or more instruments.
For each group, the engine evaluates all directional
permutations, e.g.:

   (BTCUSDT, BTCPERP)  and  (BTCPERP, BTCUSDT)

Thus it considers spreads in both directions.

------------------------------------------------------------
C) Spread Object
------------------------------------------------------------
Represents an open spread:
- original spread amount (bid(short) - ask(long))
- quantity opened
- which instrument was long, which was short

The spread may be closed partially or fully depending on
available liquidity.

------------------------------------------------------------
SPREAD OPEN LOGIC
------------------------------------------------------------
We consider a profitable entry when:

    bid(short) > ask(long)

This guarantees strictly positive instantaneous PnL.
Quantity chosen = min(bid_size, ask_size)
This ensures delta-neutral execution and prevents oversizing.

After execution ACK, we recursively continue scanning deeper
orderbook levels. This allows scaling into larger volumes if
multiple profitable levels exist.

------------------------------------------------------------
SPREAD CLOSE LOGIC
------------------------------------------------------------
To close:
- the long leg must be sold onto the best bid
- the short leg must be bought back at the best ask

Exit is profitable when:

    abs(close_bid - close_ask) < initial_spread

This ensures that realized PnL is positive.

Closing can be:
- partial: reduce spread.quantity
- full: remove spread from the open list

Recursion continues until no further profitable close exists.

------------------------------------------------------------
SUBSCRIPTION MANAGEMENT
------------------------------------------------------------
The engine also listens for subscription updates.
If an asset is no longer subscribed:
- remove its bids/asks
- remove it from its matching group
- remove stale orderbook references

This prevents the engine from making spread decisions based
on outdated or invalid market data.

============================================================
"""

import sys
import json
import uuid
import logging
import pika

from typing import Dict, Tuple, List, Any
from dataclasses import dataclass
from collections import defaultdict
from enum import Enum
from itertools import permutations
from sortedcontainers import SortedDict
from typing import Optional, Sequence, Set


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class Spread:
    """
    Represents an open spread position created when a profitable spread was detected.

    spread : Initial unrealized PnL per unit at the moment of opening. Defined as: bid(short) - ask(long)
    quantity : Total quantity opened for this spread. May be reduced when closing partially.
    long : Unique key representing the "long" asset: (channel, symbol).
    short : Unique key representing the "short" asset: (channel, symbol).
    """

    spread: float
    quantity: float
    long: Tuple[str, str]
    short: Tuple[str, str]


class OrderAction(Enum):
    """
    Defines the type of action being sent to the trading engine.

    OPEN  = initiating a spread (buy long, sell short)
    CLOSE = closing a spread (sell long, buy short)
    """

    OPEN = "Open Spread"
    CLOSE = "Close Spread"


class ConnectionError(RuntimeError):
    """Raised when a RabbitMQ connection cannot be established."""

    pass


class ConnectionManager:
    """
    Wrapper around pika.BlockingConnection to centralize connect/close.
    """

    def __init__(self, host: str = "localhost"):
        self.host = host
        self._conn: Optional[pika.BlockingConnection] = None

    def connect(self) -> pika.BlockingConnection:
        """Establish or return an existing open BlockingConnection.

        Raises ConnectionError if the connection cannot be established.
        """
        try:
            if self._conn is None or getattr(self._conn, "is_closed", True):
                self._conn = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host)
                )
            return self._conn
        except Exception as exc:
            logger.exception("Failed to connect to RabbitMQ at %s", self.host)
            raise ConnectionError("Unable to connect to RabbitMQ") from exc

    def close(self) -> None:
        """Close the underlying connection if open."""
        try:
            if self._conn and not getattr(self._conn, "is_closed", True):
                self._conn.close()
        except Exception:
            logger.exception("Error while closing RabbitMQ connection")


class OrderManager:
    """
    Sends orders to a trading engine using a simple RPC pattern over RabbitMQ.
      - Publish order to `rpc_queue` (default: "orders")
      - Provide `reply_to` and `correlation_id`
      - Block and process events until the reply with same correlation id is received
      - Return parsed JSON response, or {"status": "ERROR", "msg": "..."} on internal failure
    """

    def __init__(
        self,
        conn_manager: Optional[ConnectionManager] = None,
        rpc_queue: str = "orders",
    ):
        self.conn_manager = conn_manager or ConnectionManager()
        conn = self.conn_manager.connect()
        self.ch = conn.channel()

        # Auto-generate a callback queue for replies
        result = self.ch.queue_declare(queue="", exclusive=True)
        self.callback_queue: str = result.method.queue

        # Register a basic consumer for the callback queue
        self.ch.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self._on_response,
            auto_ack=True,
        )

        self.rpc_queue = rpc_queue
        self._response: Optional[Dict[str, Any]] = None
        self._corr_id: Optional[str] = None

    def _on_response(self, ch, method, props, body):
        """Internal callback used to set the response when correlation IDs match."""
        try:
            if (
                self._corr_id
                and getattr(props, "correlation_id", None) == self._corr_id
            ):
                self._response = json.loads(body)
        except Exception:
            logger.exception("Failed parsing RPC response payload")

    def send_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Publish an order and block until the engine replies or an internal error occurs.

        Returns parsed dict response from the trading engine (expects {"status": "ACK"} on success).
        On internal error returns {"status": "ERROR", ...}.
        """
        try:
            self._response = None
            self._corr_id = str(uuid.uuid4())

            self.ch.basic_publish(
                exchange="",
                routing_key=self.rpc_queue,
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=self._corr_id,
                ),
                body=json.dumps(order),
            )

            # Block and process Rabbit events until a response arrives
            while self._response is None:
                self.ch.connection.process_data_events()

            return self._response
        except Exception:
            logger.exception("OrderManager.send_order encountered an exception")
            return {"status": "ERROR", "msg": "OrderManager internal failure"}


class Signal:
    """
    The main signal engine that:
      - consumes orderbook snapshots and subscription updates from RabbitMQ
      - keeps an in-memory view of bids/asks per (channel, symbol)
      - identifies, opens, and closes spread opportunities
      - coordinates with OrderManager for actual trade execution
    """

    def __init__(self, order_manager: Optional[OrderManager] = None):
        # mapping matching_key -> list of UniqueKey (channel, symbol)
        self.matching_symbols: Dict[str, List[Tuple[str, str]]] = defaultdict(list)

        # per-unique-key orderbook views (SortedDicts for price -> size)
        # bids: SortedDict sorted descending (best bid first)
        # asks: SortedDict sorted ascending (best ask first)
        self.bids: Dict[Tuple[str, str], SortedDict] = {}
        self.asks: Dict[Tuple[str, str], SortedDict] = {}

        # timestamp of last orderbook snapshot for each unique key
        self.ts_orderbook: Dict[Tuple[str, str], float] = {}

        # list of open Spread objects
        self.open_spread: List[Spread] = []

        # Order execution client
        self.order_manager = order_manager or OrderManager()

        # RabbitMQ connection for data feeder
        self._conn_manager = ConnectionManager()
        self._channel = None  # set when rabbit starts

        # Start consuming (blocking call)
        self._start_rabbitmq()

    def _start_rabbitmq(self) -> None:
        """
        Configure queues and start consuming. This call blocks at start_consuming().

        Queues:
          - exchange 'data_feeder' (direct)
          - 'signal' (orderbook updates)
          - 'subscription' (subscribe/unsubscribe notifications)
        """
        try:
            conn = self._conn_manager.connect()
            self._channel = conn.channel()
            self._channel.exchange_declare(
                exchange="data_feeder", exchange_type="direct"
            )

            # signal queue (orderbook updates)
            result_signal = self._channel.queue_declare(queue="signal", exclusive=True)
            self.signal_queue = result_signal.method.queue
            self._channel.queue_bind(
                exchange="data_feeder", queue=self.signal_queue, routing_key="signal"
            )
            self._channel.basic_consume(
                queue=self.signal_queue, on_message_callback=self.signal_callback
            )

            # subscription queue
            result_sub = self._channel.queue_declare(
                queue="subscription", exclusive=True
            )
            self.subscription_queue = result_sub.method.queue
            self._channel.queue_bind(
                exchange="data_feeder",
                queue=self.subscription_queue,
                routing_key="subscription",
            )
            self._channel.basic_consume(
                queue=self.subscription_queue,
                on_message_callback=self.subscription_callback,
            )

            logger.info(
                "Signal engine ready: waiting for messages on 'signal' and 'subscription'"
            )
            self._channel.start_consuming()
        except ConnectionError:
            logger.critical(
                "Cannot start Signal engine due to RabbitMQ connection failure"
            )
            raise
        except Exception:
            logger.exception("Unexpected error while starting RabbitMQ consumers")
            raise

    def _parse_orderbook(
        self, message: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[Tuple[str, str]]]:
        """
        Parse an orderbook snapshot message and update internal state.

        Expects message fields:
          - 'channel': str
          - 'matching_key': str
          - 'data': dict with keys 's' (symbol), 'b' (bids), 'a' (asks)
          - 'cts': timestamp int

        Returns:
          (matching_key, unique_key) if successful, otherwise (None, None)
        """
        try:
            channel = message.get("channel")
            matching_key = message.get("matching_key")
            data = message.get("data")

            if not (channel and matching_key and data):
                logger.error(
                    "Malformed orderbook message, missing required fields: %s", message
                )
                return None, None

            symbol = data.get("s")
            if not symbol:
                logger.error("Orderbook data missing symbol: %s", data)
                return None, None

            unique_key = (channel, symbol)
            ts_orderbook = int(message.get("cts", 0))

            # register unique_key under matching_key
            if unique_key not in self.matching_symbols[matching_key]:
                self.matching_symbols[matching_key].append(unique_key)
                logger.debug("Added %s to matching group %s", unique_key, matching_key)

            # parse bids/asks into SortedDicts
            try:
                bids_raw = data.get("b", [])
                asks_raw = data.get("a", [])
                self.bids[unique_key] = SortedDict(
                    lambda p: -float(p), {float(p): float(s) for p, s in bids_raw}
                )
                self.asks[unique_key] = SortedDict(
                    lambda p: float(p), {float(p): float(s) for p, s in asks_raw}
                )
                self.ts_orderbook[unique_key] = ts_orderbook
            except Exception:
                logger.exception("Failed parsing bids/asks for %s", unique_key)
                return None, None

            return matching_key, unique_key
        except Exception:
            logger.exception("Exception while parsing orderbook message")
            return None, None

    def _open_spread_matching(
        self, long_key: Tuple[str, str], short_key: Tuple[str, str]
    ) -> None:
        """
        Identify and execute profitable openings where:
            bid(short) > ask(long)

        For each matching best-level combination, attempt to open a delta-neutral spread
        with qty = min(size_at_bid, size_at_ask). After ACK, update internal OB state
        to reflect that liquidity was consumed.
        """
        if long_key not in self.asks or short_key not in self.bids:
            # missing data to attempt the open
            return

        try:
            # Use list(...) to freeze iteration since we'll modify the SortedDicts in-place
            for bid_price, bid_size in list(self.bids[short_key].items()):
                for ask_price, ask_size in list(self.asks[long_key].items()):
                    if bid_price <= ask_price:
                        # no profitable entry at these top levels
                        return

                    unrealized_spread = bid_price - ask_price
                    quantity_to_open = min(bid_size, ask_size)

                    if quantity_to_open <= 0:
                        continue

                    # Prepare the order dict including timestamps for traceability
                    order = {
                        "action": OrderAction.OPEN.value,
                        "buy": long_key,
                        "buy_price": ask_price,
                        "sell": short_key,
                        "sell_price": bid_price,
                        "size": quantity_to_open,
                        "ts_buy": self.ts_orderbook.get(long_key),
                        "ts_sell": self.ts_orderbook.get(short_key),
                    }

                    try:
                        response = self.order_manager.send_order(order)

                        if response.get("status") != "ACK":
                            logger.warning(
                                "Order OPEN not acknowledged by trading engine: %s",
                                response,
                            )
                            continue
                        else:
                            logger.info(
                                "Order open spread ACK: buy=%s@%s sell=%s@%s size=%.6f",
                                order["buy"],
                                order["buy_price"],
                                order["sell"],
                                order["sell_price"],
                                order["size"],
                            )
                    except Exception:
                        logger.exception("Exception while sending order %s", order)
                        continue

                    # Record opened spread
                    self.open_spread.append(
                        Spread(
                            spread=unrealized_spread,
                            quantity=quantity_to_open,
                            long=long_key,
                            short=short_key,
                        )
                    )

                    logger.info(
                        "Opened spread: LONG=%s @%s | SHORT=%s @%s | QTY=%.6f | unrealized spread (unit) =%.6f",
                        long_key,
                        ask_price,
                        short_key,
                        bid_price,
                        quantity_to_open,
                        unrealized_spread,
                    )

                    # Update local orderbook snapshot to reflect filled/partially filled levels
                    if quantity_to_open == bid_size:
                        # consumed full bid level on short side
                        del self.bids[short_key][bid_price]
                        # subtract from ask level of long side
                        self.asks[long_key][ask_price] -= quantity_to_open
                    elif quantity_to_open == ask_size:
                        # consumed full ask level on long side
                        del self.asks[long_key][ask_price]
                        self.bids[short_key][bid_price] -= quantity_to_open
                    else:
                        # consumed both price levels fully
                        del self.asks[long_key][ask_price]
                        del self.bids[short_key][bid_price]

                    # attempt deeper levels recursively (keeps the original behavior)
                    return self._open_spread_matching(long_key, short_key)
        except Exception:
            logger.exception(
                "Error while opening spread between %s and %s", long_key, short_key
            )

    def _close_spread_matching(self, spread: Spread) -> None:
        """
        Attempt to close an opened spread if exit prices allow capturing
        positive realized PnL.

        Logic:
          - For the long leg, we sell into the best bids (where others buy).
          - For the short leg, we buy back at the best asks (where others sell).
          - Close when exit cost (abs(bid_long - ask_short)) < spread.spread
        """
        long_key = spread.long
        short_key = spread.short

        if long_key not in self.bids or short_key not in self.asks:
            return

        try:
            for price_bid, size_bid in list(self.bids[long_key].items()):
                for price_ask, size_ask in list(self.asks[short_key].items()):
                    # realized cost of closing these legs
                    realized_cost = abs(price_bid - price_ask)

                    # exit only if it preserves some of the unrealized profit
                    if realized_cost >= spread.spread:
                        return

                    quantity_to_close = min(min(size_bid, size_ask), spread.quantity)
                    if quantity_to_close <= 0:
                        continue

                    # prepare and send close order
                    order = {
                        "action": OrderAction.CLOSE.value,
                        "buy": short_key,  # buy back the short
                        "buy_price": price_ask,
                        "sell": long_key,  # sell the long
                        "sell_price": price_bid,
                        "size": quantity_to_close,
                        "ts_buy": self.ts_orderbook.get(short_key),
                        "ts_sell": self.ts_orderbook.get(long_key),
                    }

                    try:
                        response = self.order_manager.send_order(order)

                        if response.get("status") != "ACK":
                            logger.warning(
                                "Order CLOSE not acknowledged by trading engine: %s",
                                response,
                            )
                            continue
                        else:
                            logger.info(
                                "Order close spread ACK: buy=%s@%s sell=%s@%s size=%.6f",
                                order["buy"],
                                order["buy_price"],
                                order["sell"],
                                order["sell_price"],
                                order["size"],
                            )
                    except Exception:
                        logger.exception("Exception while sending order %s", order)
                        continue

                    # modify internal state
                    spread.quantity -= quantity_to_close
                    logger.info(
                        "Closed qty=%.6f of spread LONG=%s @%s SHORT=%s @%s remaining_qty=%.6f",
                        quantity_to_close,
                        long_key,
                        price_bid,
                        short_key,
                        price_ask,
                        spread.quantity,
                    )

                    if quantity_to_close == size_ask:
                        # consumed full ask on short side
                        del self.asks[short_key][price_ask]
                        self.bids[long_key][price_bid] -= quantity_to_close
                    elif quantity_to_close == size_bid:
                        # consumed full bid on long side
                        del self.bids[long_key][price_bid]
                        self.asks[short_key][price_ask] -= quantity_to_close
                    else:
                        # partially consumed both
                        self.bids[long_key][price_bid] -= quantity_to_close
                        self.asks[short_key][price_ask] -= quantity_to_close

                    # if the spread fully closed, remove it
                    if spread.quantity <= 0:
                        try:
                            self.open_spread.remove(spread)
                        except ValueError:
                            logger.debug(
                                "Spread already removed concurrently: %s", spread
                            )
                        return

                    # attempt deeper closes if possible
                    return self._close_spread_matching(spread)
        except Exception:
            logger.exception("Error while closing spread %s", spread)

    def signal_callback(self, ch, method, properties, body) -> None:
        """
        Callback when an orderbook update is received on the 'signal' queue.
        - Parse orderbook
        - Find candidate pairs including the updated instrument
        - First try to open spreads
        - Then try to close spreads that involve those pairs
        """
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            logger.error("Invalid JSON in signal callback: %s", body)
            return

        matching_key, unique_key = self._parse_orderbook(payload)
        if not matching_key or not unique_key:
            return

        # candidate directional pairs that include the updated instrument
        symbol_pairs = [
            pair
            for pair in permutations(self.matching_symbols[matching_key], 2)
            if unique_key in pair
        ]

        # open opportunities first
        for long_k, short_k in symbol_pairs:
            self._open_spread_matching(long_k, short_k)

        # attempt to close any open spread that relates to updated pairs
        for spread in list(self.open_spread):
            if (spread.long, spread.short) in symbol_pairs:
                self._close_spread_matching(spread)

    def parse_subscription(
        self, message: Dict[str, Sequence[str]]
    ) -> List[Tuple[str, str]]:
        """
        Convert subscription payload (channel -> [symbols]) into a flat list
        of unique keys.
        """
        subscriptions: List[Tuple[str, str]] = []
        for channel, symbols in message.items():
            for sym in symbols or []:
                subscriptions.append((channel, sym))
        return subscriptions

    def confirm_subscription(
        self, active_subscriptions: Sequence[Tuple[str, str]]
    ) -> None:
        """
        Remove stale orderbook data for symbols that are not present in
        active_subscriptions.
        """
        active_set: Set[Tuple[str, str]] = set(active_subscriptions)
        for mkey, known_list in list(self.matching_symbols.items()):
            for known in list(known_list):
                if known not in active_set:
                    logger.info(
                        "Removing stale symbol %s from matching group %s", known, mkey
                    )
                    self.bids.pop(known, None)
                    self.asks.pop(known, None)
                    try:
                        known_list.remove(known)
                    except ValueError:
                        pass

    def subscription_callback(self, ch, method, properties, body) -> None:
        """
        Callback when a subscription update arrives on the 'subscription' queue.
        Keeps in-memory state aligned with the active subscription set.
        """
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            logger.error("Invalid JSON in subscription callback: %s", body)
            return

        subs = self.parse_subscription(payload)
        self.confirm_subscription(subs)


if __name__ == "__main__":
    try:
        Signal()
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting.")
        sys.exit(0)
    except Exception:
        logger.exception("Fatal error in signal engine")
        sys.exit(1)
