# Code Review Report: Automated Trading Strategy

This report contains a comprehensive review of the python-based automated trading strategy targeting the Zerodha broker for options selling (capturing theta decay). The review covers both the core strategy logic (under the `strategy` folder) and the backend services/API implementation (under `web/backend`).

*Note: The `sensibull` directory was excluded from this review as requested.*

---

## 1. High-Level Architecture & Design

### Strengths
- **Modular Design:** The separation between `web/backend` (API/WebSocket layer) and `strategy/` (core algorithmic logic) provides good separation of concerns.
- **Service Abstraction:** Usage of `BrokerService` as a facade for broker API endpoints limits the impact if the broker SDK changes.
- **Multiprocessing for Strategies:** In `StrategyManager`, running strategies in a separate isolated process (`multiprocessing.Process`) is a solid architectural decision, ensuring that a blocking loop in the strategy doesn't block the FastAPI asynchronous event loop.
- **Registry Pattern:** The `StrategyRegistry` effectively manages metadata and configurations for different algorithms, allowing future extensibility.
- **Risk Management layers:** Decoupling `GapRiskManager` and `PositionManager` keeps the core strategy logic slightly cleaner.

### Areas for Improvement
- **Global State vs. Object Instances:** The usage of singletons (`_instance` overriding `__new__`) is prevalent across services (`BrokerService`, `LiveDataManager`, `StrategyManager`, `StrategyRegistry`). While acceptable for basic state sharing, relying heavily on singletons can make unit testing and dependency injection harder.
- **Inconsistent Logging:** The codebase mixes standard `logging`, custom `logger` files, and plain `print()` statements (especially in `LiveDataManager` and `LocalMonitor`).
- **Data Sharing mechanisms:** A `multiprocessing.Queue` is used to send state updates from the strategy process back to the manager process. However, there's a risk of the queue filling up if the manager process does not read fast enough.
- **Hardcoded values:** Scattered hardcoded strings like "NSE:NIFTY 50", specific tags, and specific time limits could be lifted into global config files or `.env` to prevent duplication.

---

## 2. Core Strategy Logic (`strategy/` directory)

### `base_strategy.py`
- **Good:** Consolidates common tasks (Broker interaction, Position Info tracking, Config abstraction).
- **Risk:** `self.all_instruments['symbol'].str.contains(...)` inside `_initialize_instruments` uses pandas string matching, which is fine for startup but can be slow if dealing with large instrument universes.
- **Risk:** `place_order` methods heavily assume the existence of an `OrderResponse` object with `status` or `order_id`. If the broker connection times out or fails silently, exception handling might fail to track order IDs properly.

### `survivor.py` (SurvivorStrategy)
- **Concept:** A short-strangle/straddle dynamic adjustment strategy driven by Nifty index price moving past specific Gap triggers. Uses ATR dynamic scaling and RSI/EMA/ADX filters.
- **Strengths:**
  - Robust tracking of both broker positions and local mapping via `PositionManager`.
  - The logic for placing SL orders immediately after the fresh leg executes (`_place_sl_order`) is sound.
  - Good integration of trailing stop-loss (`_check_enhanced_trailing_sl`) logic.
- **Bugs/Risks:**
  - **Looping/Blocking:** The `while True:` loop in `_handle_pe_trade_enhanced` and `_handle_ce_trade_enhanced` looks for instruments until it finds a valid premium. It has no break condition if the `temp_gap` drops to zero or below. This could result in an infinite loop blocking the strategy tick processor.
    - *Fix:* Introduce a hard threshold: `if temp_gap <= 0: break`.
  - **Duplicate Prevention:** The duplicate order check looks at `if symbol in self.positions`. However, if order placement is slow, tick processing might trigger again before the position is recorded. A `pending_orders` lock or set per symbol should be maintained.
  - **API Rate limits:** `_check_stop_losses` and `_check_profit_targets` iterate over all positions and call `self.broker.get_quote()`. For 10+ positions, this translates to 10 HTTP requests per tick. This violates rate limits easily.
    - *Fix:* Use WebSocket stream data or bulk fetch quotes.

### `wave.py` (WaveStrategy)
- **Concept:** Uses Mibian for BS pricing options, restricts trades based on Portfolio Delta.
- **Strengths:** Implements extensive Greek calculations to track the overall portfolio Delta and suspends Buy/Sell orders if the Delta drifts too far.
- **Bugs/Risks:**
  - Heavy usage of synchronous `time.sleep(60)` and `time.sleep(3)` inside execution functions. While it runs in a separate process, sleeps are a code smell for event-driven architectures.
  - The method `_complete_order` deletes elements from `self.orders` while iterating or processing, which can lead to runtime dictionaries changing size during iteration.

### `position_manager.py` (SL Reconciliation)
- **Strengths:** Cleans up "orphan" orders by cross-referencing open positions against open broker orders on algorithm startup.
- **Risk:** `_is_short_nfo_margin` accurately targets option selling, but if manual trades are mixed in the same broker account, the bot might mistakenly place SL orders for a human's trade. A specific Strategy Tag filter should be implemented on open positions if possible (though broker API limitations often apply here).

### `gap_risk_manager.py`
- **Strengths:** Elegant `enum` mapping and logical thresholds for weekend risks (Monday opening gap handling).
- **Risk:** Assumes the machine local time is perfectly synchronized with IST (Indian Standard Time). Should ideally use timezone-aware `datetime` objects.

---

## 3. Web Backend Logic (`web/backend/`)

### `main.py`
- **Strengths:** Good integration of FastAPI lifespan events for starting/stopping background loops (`heartbeat_loop`, `state_broadcast_loop`).
- **Risk:** In `state_broadcast_loop`, it calls `broker_service.get_nifty_data()` every second. If the broker driver does not cache this or uses HTTP rather than WS internally, it will hammer the broker API with 1 req/sec.

### `services/broker_service.py`
- **Strengths:** Clear abstraction of broker functionality. Uses a clean Pydantic schema mapping (`PositionsResponse`, `OrdersResponse`).
- **Bugs/Risks:**
  - `get_positions` uses silent `try...except Exception: pass` when attempting to fetch funds to verify auth state. Silent exception swallowing makes debugging connection issues impossible.
  - The exception parsing `if "api_key" in error_msg.lower()...` is brittle if the broker changes their error strings.

### `services/live_data_manager.py`
- **Strengths:** Uses `asyncio.Lock()` to prevent race conditions when updating the in-memory positions dictionaries, before broadcasting via WebSocket.
- **Bugs/Risks:**
  - The `_fetch_and_update` uses synchronous calls `broker.get_positions()` and `broker.get_orderbook()` inside an `async def`. This means the FastAPI event loop is blocked while waiting for the HTTP responses from the broker.
  - *Fix:* Either use `run_in_executor` to make synchronous calls asynchronous, or use an async broker driver.

### `services/strategy_manager.py`
- **Strengths:** Good abstraction of the multiprocessing layer. Cleanly maps `StrategyStatus`.
- **Bugs/Risks:**
  - `stop()` method uses `self._process.join(timeout=10)` followed by `self._process.kill()`. This is safe for cleanup, but abrupt killing might leave orphaned orders if the strategy was mid-execution.
  - *Fix:* Ensure the strategy handles SIGTERM correctly to cancel pending orders before shutting down.

### `services/telegram_notifier.py`
- **Strengths:** Clean, async-friendly usage of `aiohttp.ClientSession` for sending messages without blocking.
- **Minor Improvement:** Could include rate-limiting logic. If the strategy goes rogue and spams 100 orders, Telegram will rate limit the bot.

---

## 4. Security & Error Handling
- **Security:** Ensure `TELEGRAM_BOT_TOKEN`, broker API Keys, and passwords are never logged. Currently, `BrokerService._sanitize_error` is mentioned in memory but isn't explicitly enforcing redacting in all logger instances.
- **Error Handling:** Overall robust, but reliance on catching raw `Exception` is too common. Using specific exceptions (`ConnectionError`, `BrokerAPIError`, `ValueError`) would make the system more resilient to unexpected states.

---

## 5. Actionable Recommendations

1. **Fix Blocking Loops in `SurvivorStrategy`:** Implement a breakout condition in the `while True:` loop inside `_handle_pe_trade_enhanced` and `_handle_ce_trade_enhanced` to prevent infinite loops if premiums don't meet conditions.
2. **Asynchronous I/O:** Wrap synchronous broker API calls inside `LiveDataManager._fetch_and_update` using `asyncio.get_event_loop().run_in_executor(...)` to prevent blocking the FastAPI server.
3. **API Rate Limiting:** Optimize the `_check_stop_losses` loop in `survivor.py` to stop fetching individual quotes for every position per tick. Rely on the WebSocket streaming data (ticks update) instead.
4. **Remove Print Statements:** Standardize the backend completely onto Python's built in `logging` module and remove plain `print()` statements in `BrokerService` and `LiveDataManager`.
5. **Timezone Awareness:** Enforce `pytz` or `zoneinfo` across `gap_risk_manager.py` to ensure local server time differences don't accidentally execute or halt Monday morning trades at the wrong time.