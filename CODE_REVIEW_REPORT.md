# Automated Trading Strategy Code Review Report

## Executive Summary

The codebase represents a sophisticated, event-driven trading system primarily focused on option selling using Zerodha. It employs a multi-process architecture where a FastAPI backend orchestrates strategy execution, and a Next.js frontend visualizes real-time metrics via WebSockets.

The system features two main strategies (`Survivor` and `Wave`) and includes advanced risk management components such as the `GapRiskManager`, `PositionManager` (for SL reconciliation), and Greek calculations using the Black-Scholes model.

While the architecture is well-structured and extensible, there are several critical areas requiring attention, particularly regarding order execution safeguards, state synchronization, error handling, and hardcoded assumptions in the `Wave` strategy.

---

## 1. Strategy Logic (`strategy/` folder)

### 1.1 `survivor.py` (Enhanced Survivor Strategy)
The Survivor strategy is an advanced trend-following option selling algorithm.
*   **Strengths**:
    *   Implements a robust filtering system (RSI, EMA, ADX) before entering trades.
    *   Dynamic gap adjustments based on ATR volatility regimes.
    *   Comprehensive Stop-Loss logic, including placing immediate broker-level SL orders and handling partial/full SL hits.
*   **Issues & Recommendations**:
    *   **Blocking Order Placement loop**: The `_handle_pe_trade_enhanced` and `_handle_ce_trade_enhanced` methods use a `while True` loop to find a suitable instrument based on ATR gap. While there is a check for `if not instrument: return`, the loop modifies `temp_gap -= self.lot_size` if the premium is too low. This could potentially loop infinitely if the premium never meets `strat_var_min_price_to_sell` for all closer strikes until `temp_gap` becomes negative.
        *   **Recommendation**: Add a maximum iteration counter or a strict lower bound for `temp_gap` to prevent infinite loops.
    *   **Profit Booking Logic**: `check_profit_targets_at_startup` loops over `list(self.positions.items())` and triggers `_exit_position`. `_exit_position` immediately attempts to place a Market BUY order. If multiple positions hit the target simultaneously, it places multiple market orders in quick succession without pacing.
        *   **Recommendation**: Introduce a slight delay (e.g., `time.sleep(0.5)`) between order placements to prevent rate-limit errors from the broker API.
    *   **Data Structure Access**: In `on_ticks_update`, `current_price = ticks["last_price"] if "last_price" in ticks else ticks["ltp"]`. Ensure that the dispatcher guarantees dicts, as sometimes lists are sent (handled in the `__main__` block but better safeguarded at the method level).

### 1.2 `wave.py` (Wave Strategy)
The Wave strategy is a trend-following scraper.
*   **Strengths**:
    *   Uses a dynamic multiplier scale based on position imbalances to adjust entry/exit gaps.
    *   Integrates `mibian` for portfolio delta calculation to restrict trading directions.
*   **Critical Issues & Recommendations**:
    *   **Hardcoded Sleep**: `time.sleep(self.cool_off_time)` is used directly within `_prepare_final_prices`. In an event-driven async environment (or a fast ticker loop), blocking the thread with `time.sleep` will cause the strategy to miss ticks and build up queue latency.
        *   **Recommendation**: Refactor to use state-based timers (recording the last action time and returning early from `on_ticks` until the cool-off expires) instead of blocking `time.sleep`.
    *   **State Management Bug**: In `check_and_enforce_restrictions_on_active_orders`, if a restriction is violated, it calls `self._remove_order(order_id)`. Inside `_remove_order`, it uses `del self.orders[order_id]`. Since `check_and_enforce_restrictions` iterates over `list(self.orders.items())`, deleting from the underlying dict during iteration (even if iterating over a list copy) is generally safe, but the associated order cancellation logic alters `self.orders[assoc_order_id]['associated_order'] = -1` which might throw a KeyError if `assoc_order_id` was already removed.
        *   **Recommendation**: Ensure dictionary keys exist before updating associated order metadata.
    *   **Order ID `-1` Handling**: There are multiple checks for `if order_id == -1: continue`. A failed order should not be stored in `self.orders` in the first place.
    *   **Variable Name Typo**: In `check_and_enforce_restrictions_on_active_orders`, it references `self.final_quantity` (line 342) which does not exist; it should be `final_quantity`.

### 1.3 `position_manager.py` (SL Reconciliation)
*   **Strengths**: Excellent design choice to avoid file-based persistence for positions, relying entirely on fresh broker state to avoid stale data.
*   **Issues & Recommendations**:
    *   The `_calculate_protected_quantities` method checks if the transaction type is `BUY` and matches the symbol. If an independent limit BUY order is placed manually by the user, the algorithm might mistake it for an SL order and leave the short position unprotected.
        *   **Recommendation**: Filter orders by `order_type` (ensure they are `STOP` or `STOP_LIMIT`) or check for the specific `tag` assigned to strategy SL orders (`SE_SL_...`) to accurately identify algorithm-placed SL orders.

### 1.4 `gap_risk_manager.py`
*   **Strengths**: Clean implementation of day-of-week risk profiles and overnight gap limits.
*   **Issues & Recommendations**:
    *   The `can_trade_now` function checks `day_name == "Monday"` and compares `current_time < entry_time` (10:00 AM). This strictly blocks trading. However, if the algo starts at 9:15 AM, it continuously logs "Wait X min until 10:00 AM" every tick, spamming the logs.
        *   **Recommendation**: Add a throttle mechanism to log this warning only once per minute.

---

## 2. Backend Architecture (`web/backend/`)

### 2.1 `strategy_manager.py`
*   **Strengths**: Effectively utilizes Python's `multiprocessing` to isolate strategy execution from the FastAPI event loop, ensuring API responsiveness.
*   **Issues & Recommendations**:
    *   **Process Termination**: The `stop()` method uses `self._process.terminate()`, waits 10 seconds, and then uses `self._process.kill()`. Force killing a process can leave shared resources (like the `multiprocessing.Queue`) in a corrupted state or leave orphan broker WebSocket connections.
        *   **Recommendation**: Implement a graceful shutdown mechanism. Send a specific "SHUTDOWN" message through a control queue to the strategy process, allowing the strategy to call its `cleanup()` method, close WebSockets, and exit cleanly.
    *   **Queue Unbounded Growth**: `self._state_queue` is unbounded. If the FastAPI consumer (`_monitor_state`) crashes or lags, the queue will grow infinitely, consuming memory.
        *   **Recommendation**: Initialize with a max size (e.g., `Queue(maxsize=100)`) and use `.put_nowait()` with error handling to drop stale state updates if the queue is full.

### 2.2 `live_data_manager.py`
*   **Strengths**: Provides an efficient in-memory cache of live broker data, reducing API calls.
*   **Issues & Recommendations**:
    *   The `_fetch_and_update` loop polls the broker via `get_positions()` and `get_orderbook()` every 1 second (line 120). This translates to 2 API calls per second, which is 120 calls per minute. Most brokers (including Zerodha) heavily rate-limit REST API calls (typically 3 requests per second aggregate). If other parts of the system also poll, this will rapidly hit rate limits.
        *   **Recommendation**: Increase the polling interval (e.g., 3 to 5 seconds) or rely entirely on the WebSocket `on_order_update` callback to update local state rather than aggressively polling the REST API.

### 2.3 `greeks_calculator.py`
*   **Strengths**: Implements a full Black-Scholes model for delta, gamma, theta, vega, and rho. Excellent robust parsing of complex NSE symbol formats (monthly, weekly, numeric).
*   **Issues & Recommendations**:
    *   The calculator defaults `volatility` to `0.15` (15%) if not provided. In Indian markets, VIX frequently fluctuates between 11% and 25%. A static 15% volatility will yield inaccurate option prices and Greeks, particularly Vega and Delta for OTM options.
        *   **Recommendation**: Dynamically calculate Implied Volatility (IV) using the provided `calculate_implied_volatility` method (Newton-Raphson) by passing the current market premium (`entry_price` or a live quote) before calculating the other Greeks.

---

## 3. Frontend Integration (`web/frontend/`)

### 3.1 `useWebSocket.ts`
*   **Strengths**: Implements robust auto-reconnection logic with exponential backoff and connection state tracking.
*   **Issues & Recommendations**:
    *   The `WebSocket` object does not handle ping/pong natively in the browser. While the backend sends a heartbeat, the frontend hook does not have a timeout mechanism to detect silent connection drops (half-open TCP connections).
        *   **Recommendation**: Implement a watchdog timer in the frontend. If no message (including heartbeat) is received for X seconds, forcefully close and reconnect the socket.

### 3.2 `StrategySelector.tsx`
*   **Strengths**: Good UI flow utilizing the preview endpoint to confirm configuration overrides before starting the strategy.
*   **Issues & Recommendations**:
    *   The component polls `/api/strategy/available` every 5 seconds (line 38) to check the current strategy state. This negates the benefits of using WebSockets for state updates.
        *   **Recommendation**: Integrate the `useWebSocket` hook in this component to listen for `STRATEGY_STATE` messages, falling back to REST polling only if the WebSocket is disconnected.

---

## 4. Security & Error Handling

*   **Exception Sanitization**: The system relies on `broker_service.py` to catch auth errors. Ensure that `broker.get_positions()` and other driver methods utilize `_sanitize_error` (as noted in memory) to prevent logging API tokens in the stack traces.
*   **Environment Variables**: `os.getenv("BROKER_NAME")` is used frequently without fallbacks in some places, which could cause immediate crashes if the `.env` file is missing or malformed.

## 5. Conclusion

The architecture is highly advanced and well-suited for automated options trading. The primary risks lie in **rate-limiting from aggressive REST polling** (`live_data_manager.py`), **blocking calls in async/event-driven loops** (`time.sleep` in `wave.py`), and **static volatility assumptions** in Greek calculations. Addressing these specific issues will significantly improve the system's stability, accuracy, and compliance with broker API constraints.