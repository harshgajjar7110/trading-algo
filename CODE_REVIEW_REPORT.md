# Automated Trading Strategy Code Review Report

This report provides a detailed code review of the automated trading strategy system, specifically focusing on the `strategy` and `web` directories. The system employs a Python-based FastAPI backend, a Next.js frontend, and utilizes the Zerodha broker for executing option selling strategies to capture theta decay.

---

## 1. Strategy Module Review (`/strategy`)

The `strategy` directory contains the core trading logic, risk management components, and specific strategy implementations.

### 1.1 `survivor.py` (Survivor Strategy v2.0)
The Survivor strategy is an advanced NIFTY options selling strategy designed to capture theta decay by selling Puts (PE) when the market rises and Calls (CE) when the market falls.

**Strengths:**
*   **Comprehensive Risk Management:** Implements multiple layers of risk management, including automatic Stop-Loss (SL) placement, trailing SL, max daily loss limits, consecutive loss tracking, and position limits per side.
*   **Technical Filters:** Uses RSI, EMA, and ADX filters to avoid entering trades in unfavorable market conditions (e.g., selling PE when RSI is overbought).
*   **Dynamic Gaps & Sizing:** Adjusts entry gaps based on ATR and modifies position sizes based on volatility regimes and day-of-the-week risk profiles.
*   **SL Reconciliation:** Robust logic to reconcile SL orders at startup, ensuring open positions are protected even after a system restart.

**Areas for Improvement / Weaknesses:**
*   **Broker Dependency in Strategy Logic:** The strategy directly interacts with `self.broker.place_order`. While abstracted by `BrokerGateway`, tightly coupling the core logic to broker order placement methods can make testing difficult. Mocking the broker is required for all tests.
*   **Synchronous API Calls:** The `on_ticks_update` method appears to perform synchronous operations (like fetching quotes or placing orders). In a high-frequency tick environment, blocking the main thread could lead to delayed processing. Moving order placement to an asynchronous queue could improve responsiveness.
*   **Complex `_place_order_enhanced`:** This method is quite long and handles SL calculation, order placement, SL order placement, profit target calculation, and state updates. It could be refactored into smaller, testable units.
*   **Duplicate SL Prevention Warning:** In `_place_sl_order`, the code checks for existing SL orders but logs a warning and returns if found. It might be safer to ensure the existing SL order perfectly matches the new quantity and price requirements, or replace it if it doesn't.

### 1.2 `wave.py` (Wave Strategy)
The Wave strategy places simultaneous buy and sell orders offset by a configured gap to scalp small movements.

**Strengths:**
*   **Dynamic Delta Hedging:** Actively monitors the portfolio Delta for NIFTY and BANKNIFTY. If the Delta breaches configured limits (`min_nifty_delta` / `max_nifty_delta`), it restricts the appropriate side (buy/sell) of the strategy to naturally rebalance the portfolio.
*   **Dynamic Gap Scaling:** Adjusts the buy/sell gaps (`multiplier_scale`) based on the current position imbalance to prevent holding too much directional risk.

**Areas for Improvement / Weaknesses:**
*   **Blocking `time.sleep`:** The `_prepare_final_prices` uses `time.sleep(self.cool_off_time)`. This blocks the strategy process. In a fast-moving market, blocking execution means missing ticks. This should be replaced with an asynchronous wait or a state-machine approach.
*   **Hardcoded Values:** `_generate_multiplier_scale` has hardcoded scales (`buy_scale = [1.3, 1.7, 2.5, 3, 10...]`). These should ideally be configurable parameters.
*   **Error Handling in Loop:** The main loop has a `time.sleep(60)` between periodic checks. While it catches exceptions, relying on `time.sleep` in the main control loop is generally an anti-pattern for event-driven trading systems.

### 1.3 `position_manager.py`
Manages SL order reconciliation at startup.

**Strengths:**
*   **Safe Re-entry:** The `reconcile_at_startup` method is excellent. It fetches open positions and open orders, calculates the unprotected quantity, and places new SL orders only for the unprotected amount.
*   **No File Persistence:** The explicit decision to avoid saving state to files (preventing stale data) and instead relying on live broker data is a highly secure design choice for position management.

**Areas for Improvement / Weaknesses:**
*   **Zerodha Specifics:** The comments explicitly state it's designed for Zerodha. If the system is to support Fyers or others fully, this logic might need abstraction if different brokers handle OCO/SL orders differently.

### 1.4 `gap_risk_manager.py` & `pre_market_data.py`
Assesses overnight gap risks to prevent massive losses at the market open.

**Strengths:**
*   **Rule-Based Constraints:** Enforces a Monday 10:00 AM entry delay and applies day-of-the-week position multipliers (e.g., 0.5x on Mondays) to mitigate weekend risk.
*   **Multi-source Assessment:** Uses GIFT Nifty and US market data to anticipate morning gaps.

**Areas for Improvement / Weaknesses:**
*   **External Dependency:** Relies on `yfinance`, which can be rate-limited or delayed. For live trading, a more robust data feed for SGX/GIFT Nifty might be necessary.

---

## 2. Web / Backend Module Review (`/web/backend`)

The backend is built with FastAPI and manages strategy execution via `multiprocessing`.

### 2.1 Architecture & `strategy_manager.py`
The `StrategyManager` controls the lifecycle of trading strategies.

**Strengths:**
*   **Process Isolation:** Runs strategies in separate processes using Python's `multiprocessing`. This ensures that heavy calculations or blocking calls in a strategy do not block the main FastAPI web server.
*   **Registry Pattern:** `StrategyRegistry` effectively manages different strategies (`survivor`, `wave`), allowing for dynamic loading and configuration.
*   **Inter-Process Communication:** Uses `multiprocessing.Queue` to send state updates from the isolated strategy process back to the main API process for WebSocket broadcasting.

**Areas for Improvement / Weaknesses:**
*   **Process Termination:** The `stop` and `_stop_on_error` methods use `self._process.terminate()` followed by `.kill()`. Terminating a process abruptly might leave broker orders in an unknown state. A graceful shutdown mechanism (e.g., sending a shutdown signal via a queue to let the strategy close positions or cancel open orders) would be safer.
*   **Queue Bottlenecks:** `DataDispatcher` routes ticks to the `_main_queue`. If the strategy process is slow (e.g., due to `time.sleep` in Wave), the queue can back up, leading to high memory usage and stale data processing. Queue size limits and staleness checks should be implemented.
*   **State Synchronization:** The `StrategyManager` caches state (`_nifty_pe_last_value`, etc.) via queue messages. There's a slight risk of the UI showing outdated state if the queue is backed up.

### 2.2 `main.py` & WebSocket (`websocket/manager.py`)
Handles API routing and real-time frontend updates.

**Strengths:**
*   **Real-time Broadcasting:** Efficiently broadcasts strategy state and price updates to connected Next.js frontend clients via WebSockets.
*   **Lifespan Management:** Uses FastAPI's `lifespan` context manager to gracefully start background tasks and ensure strategies are stopped on shutdown.

**Areas for Improvement / Weaknesses:**
*   **Error Handling in WebSocket:** The `state_broadcast_loop` ignores quote errors silently (`except Exception: pass`). While this prevents the loop from crashing, it makes debugging missing price updates difficult.

### 2.3 API Routes (`routes/strategy.py`, etc.)
**Strengths:**
*   **Pre-flight Checks:** The `/start` endpoint checks broker authentication (`funds.raw`) before starting the strategy process, preventing the strategy from starting in a broken state.
*   **Configuration Validation:** The `/preview-config` and validation endpoints ensure parameters are safe before strategy execution.

---

## 3. General Codebase Observations

*   **Theta Decay Focus:** The Survivor strategy is well-tailored for capturing theta decay by staying out-of-the-money (OTM) and resetting reference prices when the market moves against the position. The integration of trailing SL and profit targets (60% premium collected) aligns well with standard option selling principles.
*   **Logging:** The system uses a unified logger, which is great for debugging. However, excessive logging inside the `on_ticks_update` loop (even if commented out) should be carefully managed to avoid I/O bottlenecks in the strategy process.
*   **Error Sanitization:** The memory mentions `_sanitize_error` in `BrokerService` to redact API keys. This is an excellent security practice for logs.

## 4. Summary & Recommendations

The trading system is sophisticated, modular, and incorporates robust risk management specifically tailored for option selling (Survivor). The use of isolated processes for strategy execution is a strong architectural choice for Python.

**Top 3 Actionable Recommendations:**
1.  **Refactor Blocking Calls:** Remove `time.sleep()` from strategy execution loops (specifically in `wave.py`) and replace them with asynchronous state checks to ensure tick data is processed instantly.
2.  **Graceful Shutdown:** Implement a graceful shutdown signal for the `multiprocessing.Process` so strategies can clean up (cancel pending orders) before the process is killed by the manager.
3.  **Queue Management:** Implement size limits (`maxsize`) on the multiprocessing queues passing tick data to the strategy to drop stale ticks if the strategy falls behind.

This review confirms the codebase is well-structured for automated trading with Zerodha, with minor optimizations needed for high-frequency robustness.