# Code Review Report: Automated Trading Strategy (Options Selling via Zerodha)

This document provides a comprehensive code review of the Python-based automated trading strategy implementation. The project consists of a FastAPI backend for management (`web/`) and core strategy execution algorithms (`strategy/`). The primary focus is selling options to capture theta decay on the NIFTY index via the Zerodha broker.

---

## 1. System Architecture & Concurrency Model

**Overview:**
The system runs a FastAPI web backend. The actual strategy logic is executed in a separate process using Python's `multiprocessing` library via the `StrategyManager`. Data flows from the broker WebSocket callbacks into a thread-safe `Queue`, read by the strategy process.

### Strengths:
- **Process Isolation:** Decoupling the strategy execution into its own process ensures that heavy indicator calculations (`pandas`, `numpy`) or blocking broker calls (like placing orders) do not block the FastAPI async event loop.
- **Queue-based Dispatching:** Using `DataDispatcher` with a `Queue` for tick data properly synchronizes asynchronous WebSocket callbacks into a synchronous processing loop inside the strategy process.

### Weaknesses & Bugs:
- **Missing WebSocket Reconnection Logic:** In `strategy_manager.py`, the strategy establishes a WebSocket connection to the broker (`broker.connect_websocket`). However, if the WebSocket disconnects during trading hours, there is no visible logic in the main loop to attempt a reconnection. The strategy will silently stop receiving ticks and miss entries/exits.
- **Process Zombie Risk:** When stopping the strategy (`StrategyManager.stop()`), it uses `.terminate()` and then `.kill()`. Terminating processes abruptly can leave broker connections dangling or fail to execute cleanup hooks (e.g., squaring off positions or canceling open orders).

---

## 2. Core Strategy Logic (`strategy/survivor.py`)

**Overview:**
The Survivor strategy sells NIFTY options (PE on rise, CE on fall) based on defined gap distances, scaling position sizes, and applying technical filters (RSI, EMA, ADX).

### Strengths:
- **Thorough Risk Management Components:** Contains explicit logic for gap risk (`gap_risk_manager.py`), day-based position sizing, maximum positions per side, consecutive loss limits, and daily max loss.
- **Volatility Adjustments:** Uses ATR to dynamically adjust strike distances and gaps (`_get_dynamic_gap`, `_get_atr_strike_distance`), adapting the strategy to different market regimes.
- **Position & SL Reconciliation:** Excellent feature in `reconcile_sl_at_startup()`. It detects existing positions from the broker upon startup, checks for open SL orders, and places missing ones. This ensures the bot recovers safely from a crash.

### Weaknesses & Bugs:
- **Blocking API Calls in the Hot Loop:** `on_ticks_update` eventually calls `self.broker.get_quote()` inside the `_handle_pe_trade` / `_handle_ce_trade` loops. `get_quote()` is typically a blocking HTTP call. If the strategy receives thousands of ticks per second, making HTTP requests sequentially for option premium checks will cause massive latency and lag behind the real-time market.
  - *Recommendation:* Cache option quotes via WebSocket subscriptions instead of HTTP polling, or rate-limit the entry checks.
- **Infinite Loop Risk in Strike Selection:** In `_handle_pe_trade`, there is a `while True:` loop checking for `quote.last_price < self.strat_var_min_price_to_sell`. If it fails, it reduces `temp_gap` by `self.lot_size` and loops again. If the premium is universally low, this could loop indefinitely or make excessive API calls, exhausting rate limits and hanging the strategy.
  - *Recommendation:* Add a maximum retry counter (e.g., max 5 strikes inward) before aborting the entry.
- **Indicator Recalculation Overhead:** Indicators (EMA, RSI) are recalculated *on every tick* over a growing list of up to 1000 candles (`_get_technical_indicators`). `pd.Series.ewm` and `np.mean` on large arrays per tick is computationally expensive.
  - *Recommendation:* Calculate indicators on 1-minute or 5-minute bar closes, not on every single microsecond tick.
- **`profit_target_price` Tick Rounding Issue:** In Python, float division and multiplication can cause precision issues. `round(profit_target_price / tick_size) * tick_size` is used, but it's safer to use `Decimal` or precise math for Indian market tick sizes (0.05).

---

## 3. Position and Risk Management (`strategy/position_manager.py`, `strategy/gap_risk_manager.py`)

### Strengths:
- **No File Persistence:** Storing state purely in memory and hydrating from the broker (`initialize_positions_from_broker`) is a robust design choice. State files often desync from reality if manual trades occur or if the system crashes during a write.
- **Gap Risk Assessment:** Prevents the algo from trading blindly into massive overnight gaps.

### Weaknesses & Bugs:
- **Stop-Loss Execution Mismatch:** In `survivor.py` `_check_stop_losses()`, it relies on the broker to hit the SL limit order. However, if the broker SL is cancelled manually by the user, the algo fallback checks `current_price >= stop_price` based on HTTP quotes. This polling approach is dangerous for fast market crashes.
- **Trailing SL Logic:** `_check_enhanced_trailing_sl` calculates trailing SL purely in memory based on HTTP polling. Trailing SLs should ideally be pushed to the broker (if supported) to avoid execution latency, or at least evaluated against high-frequency WebSocket data, not sequential HTTP calls.
- **Order Modification Missing:** If volatility expands, the SL price is never modified once placed. It might be beneficial to update the physical broker SL order when the trailing SL moves, rather than tracking trailing SL only in software.

---

## 4. Backend & API (`web/backend/app/`)

**Overview:**
FastAPI application that provides REST endpoints for control and WebSockets for UI state streaming.

### Strengths:
- **Clean Structure:** Good separation of concerns (Routes, Services, WebSocket, Models).
- **Graceful Async Operations:** `asyncio.create_task` is used correctly for background broadcasts and Telegram notifications.
- **Pre-flight Checks:** Route `/start` correctly verifies broker authentication and funds before spinning up the heavy strategy process.

### Weaknesses & Bugs:
- **Shared State Concurrency:** `StrategyManager` reads `state_queue.get_nowait()` in a background asyncio task. This is generally safe, but there's a risk of the queue filling up if the strategy writes state updates faster than the async loop consumes them (e.g., writing an update per tick).
  - *Recommendation:* The strategy should throttle state updates sent over the `Queue` to maybe once per second or only on state changes.
- **Global Instances:** `strategy_manager = StrategyManager()` is instantiated globally. While it acts as a Singleton, `StrategyManager` contains a `multiprocessing.Process` reference. If the ASGI server (Uvicorn) runs multiple workers (`workers > 1`), each worker will have its own memory space and its own `StrategyManager`, leading to race conditions where multiple processes try to start the strategy simultaneously.
  - *Recommendation:* Enforce a single Uvicorn worker, or move the strategy management to a centralized job queue (like Celery or Redis).
- **Error Swallowing:** In `BrokerService`, `get_positions` and `get_orders` swallow exceptions and return empty lists. If the broker API goes down, the frontend receives `[]` and assumes 0 open positions. This could cause the user to believe their positions were closed when they actually weren't.
  - *Recommendation:* Differentiate between "Zero positions" and "Broker API Error" in the API response schema.

---

## 5. Wave Strategy (`strategy/wave.py`)

**Overview:**
A grid-like/wave strategy utilizing dynamic gaps and Mibian for Black-Scholes Greeks calculations to restrict trading based on portfolio delta.

### Strengths:
- **Delta Hedging Logic:** Calculates portfolio delta dynamically and restricts new Buy/Sell orders if the delta exceeds the user-defined limits (`max_nifty_delta`).
- **Dynamic Gaps:** Adjusts the gap distance based on the net position imbalance, effectively slowing down accumulation when heavily weighted on one side.

### Weaknesses & Bugs:
- **Mibian Greeks Calculation Overhead:** Calculating options Greeks using `mibian.BS` inside a loop for *every open position* in `_get_portfolio_greeks` can be slow. Furthermore, this is called frequently during the main order cycle.
- **Sleep in Trading Loop:** `_prepare_final_prices` has a hardcoded `time.sleep(self.cool_off_time)`. Blocking the thread with `sleep()` means the strategy will completely miss any ticks or market events during that cool-off period.
- **Dict Mutating During Iteration:** In `check_and_enforce_restrictions_on_active_orders`, the code uses `list(self.orders.items())` to avoid `RuntimeError`, which is correct. However, it blindly assumes associated orders are successfully cancelled without validating the broker response.

---

## Summary of Critical Action Items

1. **Remove HTTP Polling from the Tick Loop:** Do not call `broker.get_quote()` inside `on_ticks_update`. Subscribe to the required option symbols via WebSocket and use the real-time cache.
2. **Optimize Indicator Calculations:** Throttle indicator math to run only on candle closures (e.g., top of the minute) rather than every tick.
3. **Fix Infinite Loops:** Add a safety counter to `_handle_pe_trade` / `_handle_ce_trade` to prevent the while loop from locking up the process when searching for strikes.
4. **Broker Error Handling:** Do not return empty arrays `[]` when the broker API times out; raise a clear error to the UI so the user knows the broker connection is failing.
5. **Add WebSocket Reconnection Logic:** Ensure the strategy process can detect and recover from dropped broker WebSocket connections.