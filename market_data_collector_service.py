"""
============================================================
                MARKET DATA COLLECTOR SERVICE
============================================================

This module implements a *real-time market data feeder* that:

1. Subscribes dynamically to Bybit WebSocket channels.
2. Receives live orderbook updates for any symbol/channel pair.
3. Tags each update with a “matching_key” to route downstream.
4. Publishes all processed data to RabbitMQ for:
       - storage services
       - signal engines
       - subscription
5. Replicates complete subscription state on every update.
6. Automatically restarts WebSocket streams any time the
   subscription list changes.

------------------------------------------------------------
ARCHITECTURE OVERVIEW
------------------------------------------------------------

The system consists of 3 major components:

------------------------------------------------------------
A) WebSocket Stream Manager
------------------------------------------------------------
Each (channel, symbol) subscription results in a dedicated
Bybit WebSocket connection.

Responsibilities:
- Connect to Bybit with correct channel type
- Stream orderbook data at configured depth
- Push updates into the main asyncio event loop safely
- Remain alive until explicitly cancelled
- Close gracefully when a subscription changes

A partial() callback binds:
  - channel name
  - matching_key
  - the incoming message

This ensures that every update carries the metadata needed
by downstream systems.

------------------------------------------------------------
B) RabbitMQ Publisher
------------------------------------------------------------
A dedicated asynchronous publisher receives messages from an
internal asyncio.Queue. This isolates network I/O from the
WebSocket loops and guarantees ordering.

Two message types exist:

**1. ORDERBOOK**
   - Raw Bybit orderbook update
   - Enriched with:
         channel
         matching_key
   - Routed to:
         data_feeder → storage
         data_feeder → signal

**2. SUBSCRIPTION**
   - Full subscription map:
         { channel: [symbols...] }
   - Routed to:
         data_feeder → subscription

This allows other services to automatically adapt to
subscription changes without polling.

------------------------------------------------------------
C) Subscription Controller
------------------------------------------------------------
The FeedManager maintains:

- self.subscriptions[channel] → list of symbols
- self.matching_key[(channel, symbol)] → string tag

Every time a subscription is added or removed:

1. All running WebSocket tasks are cancelled.
2. A new set of tasks is created reflecting the updated state.
3. A SUBSCRIPTION message is broadcast to RabbitMQ.
4. The publisher continues seamlessly without any resets.

This "task restart" pattern guarantees:
- No orphan WebSockets
- No stale streams
- No illegal or outdated subscriptions

------------------------------------------------------------
MESSAGE ENRICHMENT LOGIC
------------------------------------------------------------
When an orderbook update arrives from Bybit:

   msg["channel"] = stream_channel
   msg["matching_key"] = matching_key

These fields allow downstream modules (signal engines, storage)
to group assets according to logical
trading strategies rather than raw symbol names.

------------------------------------------------------------
ASYNCHRONOUS FLOW SUMMARY
------------------------------------------------------------

1. FastAPI endpoint receives a /subscribe or /unsubscribe request.

2. FeedManager updates its internal subscription registry.

3. All active WebSocket tasks are cancelled and replaced with:
       - one task per (channel, symbol)
       - exactly one publisher task

4. Each WebSocket pushes messages into the asyncio.Queue using
   a thread-safe call from the callback thread.

5. The publisher task consumes messages and forwards them to
   RabbitMQ exchange "data_feeder".

6. External systems subscribe to:
       data_feeder.storage
       data_feeder.signal
       data_feeder.subscription depending on their role.

------------------------------------------------------------
FAILSAFE BEHAVIOR
------------------------------------------------------------

- If RabbitMQ fails: the publisher logs an error but continues.
- If WebSocket disconnects: the task restarts on next resubscribe.
- If clients unsubscribe: unused symbols are removed cleanly.
- On FastAPI shutdown: all tasks are cancelled gracefully.

============================================================
"""

import asyncio
import logging
import json
from typing import Dict, List, Tuple, Any
from functools import partial
from enum import Enum
import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from pybit.unified_trading import WebSocket, AVAILABLE_CHANNEL_TYPES
from config import Config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Request(BaseModel):
    """
    Defines the body of /subscribe and /unsubscribe REST endpoints.
    """

    symbol: str
    channel: str
    matching_key: str


class Update(Enum):
    """
    Defines the two types of messages sent to RabbitMQ:
    - ORDERBOOK: market data from Bybit WS
    - SUBSCRIPTION: subscription list updates
    """

    ORDERBOOK = "orderbook"
    SUBSCRIPTION = "subscription"


class FeedManager:
    """
    Main component responsible for:
    - Managing Bybit WebSocket subscriptions
    - Restarting async tasks when subscriptions change
    - Publishing orderbook updates to RabbitMQ
    - Broadcasting subscription lists
    """

    def __init__(self) -> None:
        # All active subscriptions grouped by channel
        self.subscriptions: Dict[str, List[str]] = {
            ch: [] for ch in AVAILABLE_CHANNEL_TYPES
        }

        # Maps (channel, symbol) → matching_key (string)
        self.matching_key: Dict[Tuple[str, str], str] = {}

        # All running asyncio tasks (WS streams + publisher)
        self.tasks: List[asyncio.Task] = []

        # Async queue for outgoing RabbitMQ messages
        self.publish_queue: asyncio.Queue = asyncio.Queue()

        # Start RabbitMQ connection
        self._start_rabbitmq()

    def _start_rabbitmq(self) -> None:
        """
        Opens a RabbitMQ blocking connection and declares the exchange.
        """
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        self.rabbit_channel = self.connection.channel()

        # Direct exchange = routing key determines queue
        self.rabbit_channel.exchange_declare(
            exchange="data_feeder", exchange_type="direct"
        )

        logger.info("RabbitMQ started")

    def _on_message(
        self, stream_channel: str, matching_key: str, msg: Dict[str, Any]
    ) -> None:
        """
        Callback executed each time Bybit WS sends a message.

        Because WebSocket callbacks run in another thread,
        we must forward the result into the asyncio loop safely.
        """

        # Add metadata
        msg["channel"] = stream_channel
        msg["matching_key"] = matching_key

        # Push message into asyncio queue thread-safely
        self.loop.call_soon_threadsafe(
            self.publish_queue.put_nowait, (Update.ORDERBOOK, msg)
        )

    async def _data_publisher(self) -> None:
        """
        Dedicated task consuming messages from the asyncio queue
        and publishing them to RabbitMQ.

        Ensures ORDERBOOK and SUBSCRIPTION messages
        are routed to correct exchange targets.
        """

        while True:
            head, body = await self.publish_queue.get()

            body_bytes = json.dumps(body).encode("utf-8")

            try:
                if head == Update.ORDERBOOK:
                    # Publish orderbook updates to 2 consumers
                    self.rabbit_channel.basic_publish(
                        exchange="data_feeder", routing_key="storage", body=body_bytes
                    )
                    self.rabbit_channel.basic_publish(
                        exchange="data_feeder", routing_key="signal", body=body_bytes
                    )

                elif head == Update.SUBSCRIPTION:
                    # Inform subscription consumers
                    self.rabbit_channel.basic_publish(
                        exchange="data_feeder",
                        routing_key="subscription",
                        body=body_bytes,
                    )

            except Exception as e:
                logger.error(f"RabbitMQ publish failed: {e}")

    async def _run_stream(self, channel: str, symbol: str, matching_key: str) -> None:
        """
        Dedicated task running a Bybit WebSocket stream.

        Logic:
        - Start a WS connection
        - Register callback
        - Keep task alive until cancelled
        """

        ws = WebSocket(testnet=Config.TESTNET, channel_type=channel)

        try:
            ws.orderbook_stream(
                depth=Config.ORDERBOOK_DEPTH,
                symbol=symbol,
                callback=partial(self._on_message, channel, matching_key),
            )

            # Keep the WS alive
            while True:
                await asyncio.sleep(1)

        except asyncio.CancelledError:
            # Closing WebSocket gracefully
            ws.exit()
            raise

    async def _restart_tasks(self) -> None:
        """
        When a subscription changes, we must:
        - Cancel all running tasks
        - Restart new tasks for each active subscription
        - Restart the publisher
        """

        # Cancel tasks properly
        for t in self.tasks:
            t.cancel()

        await asyncio.gather(*self.tasks, return_exceptions=True)

        # Rebuild full task list
        new_tasks: List[asyncio.Task] = []

        # Orderbook publishers
        for channel, symbols in self.subscriptions.items():
            for symbol in symbols:
                mk = self.matching_key[(channel, symbol)]
                new_tasks.append(
                    asyncio.create_task(self._run_stream(channel, symbol, mk))
                )

        # Add publisher as first task
        new_tasks.insert(0, asyncio.create_task(self._data_publisher()))

        self.tasks = new_tasks

        # Inform RabbitMQ subscribers about subscription changes
        self.loop = asyncio.get_event_loop()
        self.loop.call_soon_threadsafe(
            self.publish_queue.put_nowait, (Update.SUBSCRIPTION, self.subscriptions)
        )

    async def subscribe(self, symbol: str, channel: str, matching_key: str) -> None:
        """
        Adds a new subscription to the system and restarts tasks.
        """

        if channel not in AVAILABLE_CHANNEL_TYPES:
            raise ValueError(f"Invalid channel: {channel}")

        if symbol not in self.subscriptions[channel]:
            self.subscriptions[channel].append(symbol)
            self.matching_key[(channel, symbol)] = matching_key
            await self._restart_tasks()

    async def unsubscribe(self, symbol: str, channel: str) -> None:
        """
        Removes a subscription and restarts tasks.
        """

        if channel not in AVAILABLE_CHANNEL_TYPES:
            raise ValueError(f"Invalid channel: {channel}")

        if symbol in self.subscriptions[channel]:
            self.subscriptions[channel].remove(symbol)
            del self.matching_key[(channel, symbol)]
            await self._restart_tasks()

    async def cleanup(self) -> None:
        """
        Cancels all tasks when FastAPI shuts down.
        """
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


app = FastAPI()
manager = FeedManager()


@app.post("/subscribe")
async def subscribe(req: Request):
    try:
        await manager.subscribe(req.symbol, req.channel, req.matching_key)
        return {"status": "ok"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/unsubscribe")
async def unsubscribe(req: Request):
    try:
        await manager.unsubscribe(req.symbol, req.channel)
        return {"status": "ok"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.on_event("shutdown")
async def shutdown():
    await manager.cleanup()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
