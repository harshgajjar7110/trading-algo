# Code Review Report: Automated Trading Strategy

This report provides a comprehensive review of the automated trading strategy, encompassing the `strategy` execution logic, `web/backend` infrastructure, and `web/frontend` visualization components.

---

## 1. Strategy Folder Review (`strategy/`)

### **1.1 Survivor Strategy (`survivor.py`)**
**Overview:** An options selling strategy generating theta decay profit by trading NIFTY Index options based on GAP adjustments and technical filters.

*   **Technical Indicator Calculation:**
    *   **Logic:** Calculates RSI, EMA, and ADX correctly.
    *   **Critique:** `_calculate_rsi` has potential zero-division mitigation, but the reliance on `self.price_history` holding 5m candles mapped from real-time tick updates can introduce minor calculation drifts. It might be better to decouple candle formation from real-time pricing ticks or ensure periodic syncs with actual broker OHLC candles.
*   **Gap & Dynamic Volatility Sizing:**
    *   **Logic:** Dynamically computes `pe_gap` and `ce_gap` based on ATR (`indicators.volatility_regime`).
    *   **Critique:** High volatility appropriately scales down position sizes (`self.high_vol_size_reduction = 0.5`). Solid implementation.
*   **Risk Management & SL:**
    *   **Logic:** Stop-loss is placed immediately after an order is filled (`_place_sl_order`). It uses `position_manager` to track active positions.
    *   **Critique:** `_check_stop_losses` runs on every tick, which acts as a fallback for the broker SL. *Race Condition Risk:* If the broker triggers the SL, the fallback might simultaneously trigger an exit, possibly attempting to exit an already closed position. Although the `_cancel_sl_order` mitigates this, careful state syncing via WebSocket `on_order_update` is essential to prevent ghost orders.
*   **Profit Target Logic:**
    *   **Logic:** Handled by `_check_profit_targets`, executed every 5 minutes (300 seconds).
    *   **Critique:** Polling for profit targets instead of placing a persistent Limit Order introduces slippage risk. If the target is hit between the 5-minute intervals, the strategy misses the exit. Converting this to a bracket/OCO order at the broker level is highly recommended.

### **1.2 Wave Strategy (`wave.py`)**
**Overview:** Trades based on buy/sell gaps with position limits and dynamic scaling multipliers based on position imbalance.

*   **Dynamic Gap Scaling:**
    *   **Logic:** Uses a multiplier array (`[1.3, 1.7, 2.5, 3, 10, 10...]`) based on the net position count to scale gaps.
    *   **Critique:** Elegant way to progressively tighten/widen bands as position inventory increases.
*   **Greeks & Portfolio Delta Management:**
    *   **Logic:** Calculates Black-Scholes greeks using `mibian` library via `_get_portfolio_greeks()`.
    *   **Critique:** `_get_dynamic_restrictions` restricts buy/sell sides if NIFTY or BANKNIFTY deltas exceed pre-configured max/min limits. Very robust portfolio-level risk control.
*   **Execution Flow:**
    *   **Critique:** Placing simultaneous Limit Buy and Limit Sell orders. The code does not use async execution for these orders, causing a sequential blocking network call.

### **1.3 Position & Gap Risk Managers (`position_manager.py`, `gap_risk_manager.py`)**
*   **Position Manager:** Correctly strictly avoids file-based persistence for SL mappings to prevent stale states, fetching fresh states directly from the broker.
*   **Gap Risk Manager:** Enforces day-of-week scaling multipliers (Monday=0.5x, Friday=0.4x) and Monday AM delays. Excellent pre-market (GIFT Nifty) risk aversion.

---

## 2. Web Backend Review (`web/backend/`)

### **2.1 Strategy Manager (`strategy_manager.py`)**
*   **Logic:** Wraps the strategy instance in a separate `multiprocessing.Process` to prevent blocking the FastAPI event loop.
*   **Critique:**
    *   *Communication:* Uses a `Queue` for status updates (`state_queue.put`). Well-structured.
    *   *Bug/Issue Identified:* The test suite (`pytest`) revealed intermittent failures in `LiveDataManager` due to asynchronous timing issues (`AssertionError: 1 != 2`). The mock timings for `asyncio.sleep` are too tight to guarantee the underlying fetch thread has completed. This suggests the inter-process/async state synchronization needs slightly more robust locking or await conditions rather than arbitrary `sleep` timers.

### **2.2 API Routes (`routes/strategy.py`, `routes/strategy_selector.py`)**
*   **Logic:** Provides RESTful hooks for starting, stopping, and configuring strategies.
*   **Critique:** The integration of pre-flight authentication checks (`broker.get_funds()`) before starting a strategy is an excellent safeguard against silent start failures.

### **2.3 WebSocket Manager (`websocket/manager.py`)**
*   **Logic:** Broadcasts `PRICE_UPDATE`, `STRATEGY_STATE`, and `ORDER_UPDATE` payload types to connected React clients.
*   **Critique:** Employs a single global `connection_manager`. It appropriately cleans up disconnected clients (`disconnect()`) upon exception handling, preventing memory leaks.

---

## 3. Web Frontend Review (`web/frontend/`)

### **3.1 Strategy Selector & Component State (`StrategySelector.tsx`)**
*   **Logic:** Fetches the available configurations and shows preview diffs (`differences_from_default`) before forcing a "Confirm & Start" verification.
*   **Critique:** Excellent UX implementation. Validates config through `preview.is_valid` blocking the Start button.

### **3.2 API & WebSocket Abstraction (`lib/api.ts`, `useWebSocket.ts`)**
*   **Logic:** Wraps fetch calls cleanly with Typescript typings. `useWebSocket` manages reconnects.
*   **Critique:**
    *   `useWebSocket` employs `useRef` to maintain callback stability, avoiding React render loop reconnections.
    *   The `maxReconnectAttempts` correctly limits indefinite spinning.

---

## 4. Summary & Recommendations

### **Strengths:**
1.  **Risk Aversion First:** Deeply embedded risk logic (Delta boundaries, Day-of-week scaling multipliers, Gap risk avoidance, SL Reconciliation).
2.  **Stateless Tracking:** Deciding to ditch file-based state for SL tracking (`position_manager.py`) prevents devastating stale-state ghost orders.
3.  **Architecture:** The UI, REST API, Async WebSocket, and Multiprocessing strategy execution are well decoupled.

### **Areas for Improvement / Bug Fixes:**
1.  **Profit Booking Latency:** Transition `_check_profit_targets` from a 5-minute polling mechanism to a Limit/Bracket Order at the broker level to avoid slippage.
2.  **Async Tests:** Fix the `tests/unit/test_live_data_manager.py` timing issues. The assertion failures are a symptom of using `asyncio.sleep` to wait for queued events. Use `asyncio.Event` or `Condition` variables.
3.  **Race Condition with SL:** Ensure the manual fallback SL (`_check_stop_losses`) double-checks order status immediately before executing a manual market exit, as the broker's auto-SL and the algo's manual fallback SL might trigger concurrently during high volatility.
