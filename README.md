# Microservices Trading System - Blackbird

This repository implements a modular, event-driven trading system. The system streams market data, generates trading signals, executes
simulated trades, and stores all data for later analysis.

The assignment requires building a backend system capable of trading
across two products (e.g., **BTC spot** and **BTC perpetual futures**)
to capture potential spread opportunities.

Bybit's **Demo Trading** environment is recommended since it simulates
real market conditions with fake balances and supports both spot and
derivatives data.

This repository provides a structured microservices solution to meet
those requirements.
The architecture uses:

-   **TimescaleDB** : time-series storage
-   **RabbitMQ** : microservice communication
-   **Python microservices**
-   **docker-compose** : infrastructure orchestration
-   **uv** : Python environment management

<img src="./Trading System Architecture.png">

## 📦 Requirements

Ensure the following tools are installed:

-   **Docker** & **Docker Compose**
-   **bash**
-   **uv** (Python environment manager)\
    Install: https://docs.astral.sh/uv/

## ⚙️ Environment Configuration

Environment variables are stored in:

    variables.env

Example configuration:

``` env
# TimescaleDB
PGDATA_PATH=./pgdata
POSTGRES_PASSWORD=guest
POSTGRES_USER=postgres
POSTGRES_DB=postgres
CONTAINER_NAME_DB=timescaledb

# RabbitMQ
RABBITMQ_DEFAULT_USER=guest
RABBITMQ_DEFAULT_PASS=guest
CONTAINER_NAME_RABBITMQ=rabbitMQ
RABBITMQ_HOSTNAME=my-rabbit
RABBITMQ_DATA_PATH=./rabbitmq_data
```

Modify these values before running the system if necessary.

## 📘 Configuration Module (`config.py`)

This module centralizes all configuration parameters used across the microservices of the trading system.  
It defines database table layouts, global runtime flags, and risk parameters.

Tables are represented using the `TableConfig` dataclass:

```python
@dataclass
class TableConfig:
    """Database table configuration"""
    name: str
    columns: List[str]
```

The application defines two main tables:

**Orderbook Table**
- **Name:** `bybit.orderbook`  
- **Purpose:** Stores incoming orderbook snapshots and updates  
- **Columns:** Topic, symbol name, side, price, size, timestamps, matching key (full list in code)

**Order Table**
- **Name:** `bybit.order`  
- **Purpose:** Stores generated buy/sell orders from the trading engine  
- **Columns:** Channel, symbol, price, size, type, flux, timestamps

The `Config` class provides application-wide constants:

**Database Connection**
```python
DB_CONFIG = {
    "database": "postgres",
    "host": "localhost",
    "user": "postgres",
    "password": "guest",
    "port": "5433",
}
```

Defines how the storage service connects to TimescaleDB/PostgreSQL.


**Risk Settings**
```python
MAX_RISK_OPEN_POSITION = 1_000_000
```
Maximum allowed open exposure before the trading engine restricts new orders.

**Feature Toggles**

These flags allow enabling/disabling subsystems without code changes:

```python
DUMP_ORDERBOOK_TO_DB = True
DUMP_ORDER_TO_DB     = True
REAL_ORDER_SENDING    = False
```

**DUMP_ORDERBOOK_TO_DB** — Store market data in the DB  
**DUMP_ORDER_TO_DB** — Store executed/simulated orders  
**REAL_ORDER_SENDING** — When `False`, trades are simulated; no real exchange orders are placed

## 🧩 Setup Instructions

### Step 1 : Install Python dependencies

``` bash
uv sync
```

### Step 2 : Open your docker app

### Step 3 : Start infrastructure (DB + RabbitMQ)

``` bash
./setup.sh
```
You may have to change permission 

``` bash
cd ..
chmod -R 700 Microservices-Trading-System---Blackbird-main
cd Microservices-Trading-System---Blackbird-main
```

Check containers:

``` bash
docker ps
```

Expected:

-   `timescaledb`
-   `rabbitMQ`

### Step 4 : Start the Microservices

``` bash
./start_microservice.sh
```
`market_data_collector_service.py`   Streams Bybit orderbook/BBO data into RabbitMQ

`data_storage_service.py`            Stores market data in TimescaleDB

`signal_service.py`                  Computes spreads and emits trading signals

`trading_engine_service.py`          Converts signals into trades and persists them

### 📡 Step 5 : Subscribe to Market Data Feeds

Docs: https://bybit-exchange.github.io/docs/v5/market/orderbook

``` bash
curl -X POST "http://127.0.0.1:8000/subscribe"   -H "Content-Type: application/json"   -d '{"symbol": "BTCUSDT", "channel": "spot", "matching_key": "a"}'
```

``` bash
curl -X POST "http://127.0.0.1:8000/subscribe"   -H "Content-Type: application/json"   -d '{"symbol": "BTCPERP", "channel": "linear", "matching_key": "a"}'
```

🔑 What is `matching_key`?

Groups instruments for spread comparison (e.g., `"a"` for BTC pair,
`"b"` for ETH pair).

## 🔍 Monitoring & Debugging

### Logs

Each service prints logs to its terminal.

### Database

``` bash
docker exec -it timescaledb psql -U postgres
```

Tables:

-   `bybit.orderbook`
-   `bybit.order`

### RabbitMQ UI

    http://localhost:15672

Login: `guest / guest`

## 🧠 Design Highlights

``` bash
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

```

``` bash
============================================================
                       DATA STORAGE SERVICE
============================================================

Consumes orderbook updates from RabbitMQ and inserts them
into PostgreSQL using DatabaseManager.

Responsibilities:
1. Connect to RabbitMQ exchange "data_feeder"
2. Consume orderbook updates from routing key "storage"
3. Parse/flatten entries into DB-ready rows
4. Insert batches safely into PostgreSQL
5. Stay resilient:
       - Bad messages are isolated
       - DB failures never stop the consumer

============================================================
```

``` bash
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
```

``` bash
============================================================
                        TRADING ENGINE SERVICE
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
```

``` bash
============================================================
                     DATABASE MANAGER
============================================================

Responsible for:
1. Managing PostgreSQL connections
2. Executing safe batched inserts
3. Handling conflict-safe upserts
4. Ensuring stable database interaction throughout the
   trading & signal pipeline.

This module is intentionally resilient:
- Reconnects automatically if connection drops.
- Logs errors without interrupting.
- Never throws uncaught exceptions to the upper layers,
  so the execution engine remains stable even if the DB fails.

============================================================
```

