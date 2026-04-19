# Code Review Report: Survivor Automated Trading Strategy

This document contains a comprehensive code review of the Python-based automated trading strategy and its backend integration under the `web` and `strategy` folders.

## Executive Summary

The project provides a well-structured, multi-process architecture consisting of a FastAPI backend and isolated trading strategy processes. The core strategy ("Survivor") is an option-selling strategy designed to capture theta decay on index options (NIFTY). The overall design exhibits modern Python features, clear modularity, real-time WebSocket communication, and robust risk management implementation.

However, there are several areas that could be improved, particularly regarding error handling robustness, hardcoded constants, tight coupling to specific broker driver implementations, and type safety.

---

## 1. Strategy Logic Review (`strategy/`)

### 1.1 `survivor.py`
The Survivor strategy is an advanced option selling strategy that uses dynamic risk adjustment.

**Strengths:**
- **Dynamic Risk Management:** Implements dynamic gap adjustments using ATR and day-of-week multipliers to mitigate risk based on market context.
- **Entry Filters:** Excellent implementation of technical indicators (RSI, EMA, ADX) to avoid entering trades in unfavorable trends or extreme momentum conditions.
- **Stop Loss Mechanism:** Comprehensive broker-level stop-loss logic (`STOP` and `STOP_LIMIT`) combined with an active memory-tracked reconciliation system at startup.
- **Trailing Stop Loss:** A robust custom trailing stop-loss implementation that protects gains once they surpass a certain percentage.
- **Clean Extensibility:** The transition to a class-based architecture (`SurvivorStrategy`) is well done.

**Issues & Improvements:**
- **Synchronous vs Asynchronous:** The strategy executes synchronously. When dealing with high-frequency ticks, multiple API calls (`broker.get_quote()`, `broker.place_order()`) block the event loop, possibly resulting in missed ticks. It is highly recommended to refactor critical paths using `asyncio`.
- **Error Swallowing:** Inside `_place_sl_order`, errors during `broker.place_order()` only result in a logged warning. If the SL fails to place, the position becomes naked. The system should ideally retry or immediately square off the base position if the protection order strictly fails.
- **Tick Time vs Local Time:** `on_ticks_update` uses `datetime.now()` for tracking execution, rather than the `exchange_timestamp` extracted directly from the tick. Under heavy load, using local time could create a desynchronization between historical candle calculation and real-time execution.
- **Code Duplication:** `_handle_pe_trade_enhanced` and `_handle_ce_trade_enhanced` contain almost identical logic. They should be refactored into a single generic function that accepts an `OptionType` enum and handles the respective signs (e.g., `-` for CE, `+` for PE).

### 1.2 `wave.py`
The Wave strategy scraper runs in continuous cycles.

**Strengths:**
- **Dynamic Restrictions:** Good use of Black-Scholes (via `mibian`) to calculate the portfolio's net delta and dynamically apply buy/sell restrictions when delta exceeds `min_nifty_delta` or `max_nifty_delta`.
- **Dynamic Gap Scaling:** Uses an array-based multiplier logic (`_generate_multiplier_scale`) based on position imbalance to adjust entry points.

**Issues & Improvements:**
- **Time.sleep() during Execution:** `_prepare_final_prices` uses a hardcoded `time.sleep(self.cool_off_time)`. This blocks the thread completely, preventing any order updates or tick processing from occurring during the cool-off period. This must be refactored to an async wait or state machine.
- **State Mutation during Iteration:** In `check_and_enforce_restrictions_on_active_orders`, iterating over `list(self.orders.items())` helps prevent `RuntimeError`, but the code still mixes tracking loops with synchronous broker API calls (`self.broker.cancel_order()`).

### 1.3 `position_manager.py`
Responsible for reconciling actual broker positions with the strategy memory to ensure SL protection.

**Strengths:**
- **Safety First:** Excellent decision to fetch positions directly from the broker rather than saving state to disk, which entirely mitigates the issue of stale data leading to false executions.
- **Clear Reconciliation Flow:** `reconcile_at_startup` correctly matches open orders against positions to calculate the unprotected quantity before issuing new SL orders.

**Issues & Improvements:**
- **Broker Coupling:** Contains hardcoded references to `NFO` and `NRML` product types which makes it less adaptable to other markets (e.g., MCX or CDS) without modification.

### 1.4 `gap_risk_manager.py` & `pre_market_data.py`
**Strengths:**
- **Proactive Risk Management:** The day-of-week multipliers (e.g., `Monday: 0.5x`, `Friday: 0.4x`) and gap analysis properly shield the strategy from overnight gap risks.
- **Graceful Fallbacks:** Good use of try-except blocks when fetching external data using `yfinance` to prevent total strategy crashes.

**Issues & Improvements:**
- **Hardcoded Tickers:** `yfinance` symbols (`^SGXNIFTY`, `^NSEI`, `^GSPC`) are hardcoded. These should ideally be part of a configuration file.
- **File Cache Reliability:** File locking is not implemented when writing to cache (`premarket_{date}.json`). Concurrent read/writes from multiple strategy processes could corrupt the JSON file.

---

## 2. Web Backend Integration (`web/backend/app/`)

### 2.1 `main.py` & `routes/strategy.py`
**Strengths:**
- **Lifespan Management:** Good usage of FastAPI's `asynccontextmanager` to start and teardown background tasks (heartbeat, state broadcast) cleanly.
- **API Design:** Clean REST endpoints that utilize Pydantic schemas for request/response validation.
- **Pre-flight Checks:** Route `start_strategy` properly validates broker authentication before attempting to spawn a strategy process.

### 2.2 `services/strategy_manager.py`
Manages spawning the trading algorithms in separate OS processes.

**Strengths:**
- **Multiprocessing Architecture:** Isolating the strategy execution from the API web server using `multiprocessing.Process` ensures that heavy trading computations do not block the web server's event loop.
- **Inter-process Communication (IPC):** Uses `multiprocessing.Queue` to securely pass state updates from the child trading process back to the parent web server for WebSocket broadcasting.

**Issues & Improvements:**
- **Zombie Processes:** In `stop()`, if `self._process.join(timeout=10)` times out, it uses `.kill()`. However, any child threads or websocket connections inside the strategy process may not be cleaned up gracefully. Consider sending a cancellation flag via the Queue to trigger a clean exit within the strategy loop.
- **Hardcoded Broker Instantiation:** Broker creation inside `_run_strategy_process` is hardcoded to use `os.getenv("BROKER_NAME", "zerodha")`. This makes the registry inherently dependent on environment variables rather than the passed strategy configuration.

### 2.3 `services/live_data_manager.py` & `websocket/manager.py`
**Strengths:**
- **Real-time Engine:** Custom WebSockets connection manager tracks subscribed clients effectively and cleans up disconnected sockets seamlessly.
- **Background Refreshing:** `_stream_loop` polls data cleanly and implements an error back-off counter (`_error_count`) to avoid spamming the broker API during downtimes.

**Issues & Improvements:**
- **Polling vs Webhooks:** `_fetch_and_update()` continuously polls `broker.get_positions()` and `broker.get_orderbook()` every second. For Zerodha, this strict polling risks rate-limit violations (Kite API enforces strict limits on orderbook calls). It is better to rely heavily on the broker's postback/webhook updates or the Order WebSocket stream to maintain order state, rather than 1-second polling.
- **Lock Contention:** `async with self._lock:` wraps the whole assignment block. While python dict assignments are atomic, it is a safe implementation, but wrapping the I/O broadcasting might slow down the loop.

---

## 3. General Codebase Observations

1. **Typing and Linting:** The codebase has good typing in the backend (`Dict`, `Optional`, `List`) but lacks strict typing within the strategy classes. Adding complete type hints in `survivor.py` would drastically improve maintainability.
2. **Logging:** A custom unified logger (`get_strategy_logger`) is utilized. Ensure that `logging.handlers.RotatingFileHandler` is used to prevent the log files from blowing up disk space on AWS instances.
3. **Dependency Management:** The project uses `pyproject.toml` via `uv` successfully, ensuring deterministic builds.

## Conclusion
The Survivor architecture is highly advanced, resilient, and well-designed for option selling. The primary recommendations for immediate improvement are:
1. Replace synchronous blocking calls (`time.sleep`) in strategy logic with asynchronous processing to avoid dropping ticks.
2. Refactor the 1-second polling mechanism in `LiveDataManager` to avoid hitting Broker API rate limits.
3. Implement fail-safes for Stop-Loss order placement failures.
4. Refactor duplicate logic in `_handle_pe_trade` and `_handle_ce_trade` into a unified handler.
