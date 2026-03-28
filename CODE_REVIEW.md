# Detailed Code Review Report: Survivor Automated Trading Strategy

This report provides a comprehensive review of the `strategy/` and `web/backend/` directories for the Survivor and Wave automated trading strategies. The review focuses on architecture, logic, risk management, and overall code quality.

## 1. Executive Summary

The codebase implements a sophisticated, multi-strategy automated trading system using Python, FastAPI, and Next.js (frontend). The primary strategies, **Survivor** (an advanced options selling strategy) and **Wave** (a trend-following strategy), are well-structured and utilize a robust event-driven architecture.

**Strengths:**
*   **Decoupled Architecture:** The separation of the FastAPI backend from the strategy execution via `multiprocessing` ensures that intensive strategy calculations do not block the API server.
*   **Comprehensive Risk Management:** Features like dynamic gap adjustment, ATR-based strike selection, broker-level stop-loss orders, daily loss limits, and pre-market gap risk assessment are implemented effectively.
*   **Live Data Streaming:** The `LiveDataManager` provides efficient, in-memory tracking and WebSocket broadcasting of positions and orders, minimizing redundant API calls to the broker.
*   **Strategy Registry:** The system easily supports adding new strategies dynamically.

**Areas for Improvement:**
*   **Exception Handling:** Some broad `except Exception:` blocks could mask critical failures.
*   **Concurrency Issues:** The interaction between the main API process and the strategy process relies heavily on `multiprocessing.Queue`, which is good, but state synchronization can sometimes lag behind actual broker execution.
*   **Hardcoded Values:** Several places have hardcoded fallback values (e.g., `24000` for NIFTY in analysis routes) that should ideally be dynamic or fail gracefully.
*   **Order Update Handling:** The `handle_order_update` in the Wave strategy has some logic that seems fragile, specifically around associating buy and sell orders.

---

## 2. Strategy Logic (`strategy/` folder)

### 2.1 `survivor.py` (Survivor Strategy v2.0)
This is an advanced options selling strategy that sells Put (PE) and Call (CE) options based on NIFTY index movements.

**Strengths:**
*   **Technical Filters:** The implementation of RSI, EMA, and ADX filters inside `_check_entry_filters()` is clean and prevents trading in unfavorable market conditions (e.g., not selling PE when RSI is overbought).
*   **Dynamic Gaps & Sizing:** `_get_dynamic_gap` and `_get_position_size` use Volatility Regime classification based on ATR. This is excellent for adapting to market conditions.
*   **Robust SL Management:** The automatic placement of broker-level SL orders immediately after the fresh order is placed (`_place_order_enhanced`) is a crucial safety feature. The fallback manual check `_check_stop_losses()` is also a good redundancy.
*   **Reconciliation:** The `reconcile_sl_at_startup()` method elegantly handles the scenario where the bot restarts with open positions, ensuring they get protected.

**Issues / Recommendations:**
*   **Duplicate SL Prevention Risk:** In `_place_sl_order`, the code checks `self.positions` to prevent duplicate SLs. However, if a position is closed manually in the broker, the bot might still think it's open until the next full sync.
*   **Profit Target Logic:** The `_check_profit_targets` runs on a fixed interval (`profit_check_interval`, default 60s). In fast-moving markets, a 60-second delay might cause the bot to miss the target. *Recommendation:* Move the profit target check into `on_ticks_update` or use broker-level GTT/Limit orders for profit booking.
*   **Loop Safety:** In `_handle_pe_trade_enhanced`, the `while True:` loop used to find strikes has no explicit maximum iteration count. If `temp_gap` keeps decreasing and prices are consistently below `strat_var_min_price_to_sell`, it could infinite loop until `temp_gap` becomes invalid.

### 2.2 `wave.py` (Wave Strategy)
A trend-following strategy using Mibian for Greeks calculation.

**Strengths:**
*   **Dynamic Greeks:** The `_get_portfolio_greeks` calculates real-time delta limits using the Black-Scholes model to restrict buy/sell orders if the portfolio gets too directional.
*   **Position Imbalance Scaling:** `_generate_multiplier_scale` adjusting the gap based on current net positions is a smart way to manage inventory risk.

**Issues / Recommendations:**
*   **Order Tracking Fragility:** The tracking of `associated_order_id` in `_execute_orders` and `handle_order_update` is complex and prone to race conditions if updates arrive out of order.
*   **Blocking Sleep:** `time.sleep(self.cool_off_time)` in `_prepare_final_prices` blocks the strategy execution thread. This prevents processing new ticks during the cool-off period. *Recommendation:* Implement an asynchronous state machine or track the timestamp to enforce cool-offs without sleeping the thread.

### 2.3 Auxiliary Managers (`position_manager.py`, `gap_risk_manager.py`)
*   **`position_manager.py`:** Cleanly handles matching broker orders to positions to calculate protected vs. unprotected quantities. Avoiding disk persistence here (as noted in the docs) solves the "stale state" issue elegantly.
*   **`gap_risk_manager.py`:** The logic to skip or reduce trades based on overnight/GIFT Nifty gaps (`assess_gap_risk`) adds a strong layer of capital protection, especially for options sellers.

---

## 3. Web Backend Logic (`web/backend/` folder)

### 3.1 `app/services/strategy_manager.py`
The core component that bridges the FastAPI app and the isolated strategy processes.

**Strengths:**
*   **Process Isolation:** Running strategies in a `multiprocessing.Process` protects the FastAPI event loop from being blocked by heavy computations (like Black-Scholes).
*   **State Queue:** Using `multiprocessing.Queue` to stream status updates back to the main process is implemented correctly.
*   **Pre-flight Checks:** Running gap risk assessment and SL reconciliation *before* entering the main tick loop ensures safety from tick zero.

**Issues / Recommendations:**
*   **Process Termination:** The `stop()` method uses `self._process.terminate()` and then `self._process.kill()`. This is abrupt and prevents the strategy's `cleanup()` method (defined in `base_strategy.py`) from executing properly to cancel pending orders. *Recommendation:* Send a stop signal (e.g., via a Queue or Event) to let the strategy shut down gracefully, using `terminate()` only as a fallback.
*   **Queue Accumulation:** If the FastAPI backend (`_monitor_state`) is delayed, the `state_queue` might fill up. Ensure there are limits or proper flushing.

### 3.2 `app/services/live_data_manager.py`
**Strengths:**
*   **WebSocket Integration:** Seamlessly fetches positions and orders and broadcasts them to all connected clients.
*   **Error Tolerance:** The `_max_errors = 5` circuit breaker prevents infinite loops of failing broker calls.

**Issues / Recommendations:**
*   **Polling Frequency:** Polling `broker.get_positions()` and `get_orderbook()` every 1 second (`await asyncio.sleep(1)`) might hit broker API rate limits depending on the broker (e.g., Zerodha has strict limits). *Recommendation:* Subscribe to the broker's order update WebSocket callback to update internal state instead of aggressively HTTP polling, or increase the polling interval.

### 3.3 `app/services/broker_service.py`
**Strengths:**
*   Wraps the underlying broker drivers securely.
*   `_ensure_broker` handles dynamic loading of the specified broker (Zerodha, Fyers, etc.).
*   Gracefully catches authentication errors and sanitizes error responses.

### 3.4 API Routes (`app/routes/`)
*   **`analysis.py` & `greeks.py`:** Excellent use of external libraries for complex option payoff and Greek scenario analysis.
    *   *Issue in `analysis.py`:* `current_nifty = 24000  # Default fallback` - Hardcoding price fallbacks can severely skew payoff graphs if the broker API fails. It is better to return an error or require the client to supply the underlying price.
*   **`strategy_selector.py`:** Clean RESTful design to allow UI preview of configurations and differences from default parameters before starting.

---

## 4. Overall Code Quality & Architecture

*   **Typing:** Good use of Pydantic (`schemas.py`) and Python type hints throughout the codebase. This ensures reliable API contracts.
*   **Logging:** The unified logging system (`utils.logging`) is used consistently, which will aid immensely in debugging production issues.
*   **Configuration:** `config.py` uses `pydantic_settings` excellently, allowing environment variables to override defaults cleanly.

## 5. Summary of Actionable Recommendations

1.  **Remove `time.sleep` in Strategies:** Refactor `wave.py` to use non-blocking time checks instead of `time.sleep()` to ensure ticks aren't missed.
2.  **Graceful Shutdown:** Implement a graceful shutdown mechanism in `StrategyManager` so strategies can cancel pending limit orders before the process is killed.
3.  **Optimize Broker Polling:** Reduce the 1-second polling frequency in `LiveDataManager` or rely purely on WebSocket order updates to avoid rate-limiting bans from Zerodha.
4.  **Profit Booking Latency:** Move the profit booking check in `survivor.py` from a time-based interval to a tick-based check (or use GTTs) to ensure targets are hit instantly.
5.  **Remove Hardcoded Fallbacks:** Eliminate hardcoded fallback values like NIFTY at `24000` in `analysis.py`.
6.  **Loop Safety:** Add maximum iteration bounds to the strike-searching `while True:` loops in `survivor.py`.
