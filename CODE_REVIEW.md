# Automated Trading Strategy Code Review Report

This report provides a detailed code review of the Python-based automated trading strategy using the Zerodha broker. The review covers the core strategies (`survivor`, `wave`, `base_strategy`), risk management components (`position_manager`, `gap_risk_manager`), the FastAPI backend (`web/backend`), and Next.js frontend (`web/frontend`), strictly avoiding the `sensibull` directory.

---

## 1. Strategies (`strategy/` directory)

### 1.1 `survivor.py` (Survivor Strategy - Advanced NIFTY Options Selling)

**Overview:**
A trend-following option selling strategy aiming to capture theta decay by selling PE/CE options based on NIFTY price movements and technical indicators (RSI, EMA, ADX). Features dynamic gap adjustments, risk management (SL, trailing SL, daily loss limits), and profit targets.

**Strengths:**
*   **Comprehensive Risk Management:** Implements static SL, trailing SL, time-based exits, and daily loss limits. Reconciles SL at startup via `PositionManager`.
*   **Technical Filters:** Uses RSI, EMA, and ADX effectively to filter entry signals, reducing the probability of entering bad trades.
*   **Dynamic Adjustments:** Adjusts gaps and position sizing dynamically based on Volatility (ATR) and Gap Risk (overnight/weekend).
*   **Robust State Management:** Handles resets of PE/CE reference values gracefully, preventing runaway entries in strong trends.

**Weaknesses & Potential Issues:**
*   **Tight Coupling with Broker:** Contains hardcoded `Exchange.NFO` and Zerodha-specific logic, slightly hindering true broker agnosticism.
*   **Complexity:** The class is massive (> 1500 lines) and handles too many responsibilities (trading logic, indicator calculation, risk management checks, SL placement).
*   **Blocking Calls in Loop:** The `on_ticks_update` method performs blocking API calls (e.g., `self.broker.get_quote`) which can introduce latency in high-frequency tick processing.
*   **SL Reconciliation Edge Cases:** Automatic SL cancellation and placement might race during rapid tick updates if network latency occurs.

**Recommendations:**
*   **Refactoring:** Break down `SurvivorStrategy` into smaller, cohesive classes (e.g., `SignalGenerator`, `RiskController`, `ExecutionEngine`).
*   **Async Operations:** Migrate blocking broker calls in the hot path (`on_ticks_update`) to asynchronous calls to prevent blocking the tick stream.

### 1.2 `wave.py` (Wave Trading Strategy)

**Overview:**
A trend-following strategy designed to place simultaneous buy and sell limit orders offset from the current market price (buy gap, sell gap). It dynamically adjusts restrictions based on portfolio delta calculated via Black-Scholes.

**Strengths:**
*   **Delta Hedging Logic:** Incorporates real-time portfolio delta calculation (`mibian` library) to restrict buy/sell orders if the delta exceeds defined thresholds.
*   **Margin Awareness:** Calculates and tracks margin requirements for spreads and naked options.
*   **Dynamic Scaling:** Implements a multiplier scale (`_generate_multiplier_scale`) to adjust gaps based on position imbalances.

**Weaknesses & Potential Issues:**
*   **State Drift:** Relies heavily on internal tracking dictionaries (`self.orders`, `self.handle_order_update_call_tracker`). If a WebSocket message is dropped, the internal state will fall out of sync with the broker.
*   **Complex Order Tracking:** The logic intertwining `associated_order` tracking and simultaneous cancellation of legs is brittle and prone to race conditions.
*   **Synchronous Waits:** Uses `time.sleep()` in the core logic (`_prepare_final_prices` and `place_wave_order`), which blocks the entire process and delays tick processing.

**Recommendations:**
*   **State Reconciliation:** Implement periodic reconciliation with the broker's actual order book instead of relying solely on WebSocket updates.
*   **Remove `time.sleep()`:** Replace synchronous sleeps with asynchronous delays or timer-based event triggers.

### 1.3 `base_strategy.py`

**Overview:**
Provides a common `BaseStrategy` abstract class outlining standard configuration (`StrategyConfig`), initialization, basic order placement, position checking, and cleanup.

**Strengths:**
*   **Standardization:** Enforces a consistent interface (`initialize`, `on_tick`) for new strategies.
*   **Configuration Parsing:** Utilizes a dataclass for robust configuration parsing and default management.

**Weaknesses:**
*   **Incomplete Migration:** `survivor.py` and `wave.py` do not currently inherit from `BaseStrategy`, leading to duplicated code (e.g., `place_order`, `get_positions`).

**Recommendations:**
*   Refactor `SurvivorStrategy` and `WaveStrategy` to inherit from `BaseStrategy` to reduce boilerplate and enforce consistency.

### 1.4 `position_manager.py` (SL Order Reconciliation)

**Overview:**
Designed specifically for the Survivor strategy to detect open positions at startup, match them with active SL orders, and place missing SL orders automatically. Tracks state in memory.

**Strengths:**
*   **Stateless Persistence:** Intentionally avoids saving state to disk, relying on the broker as the single source of truth, preventing stale data issues.
*   **Duplicate Prevention:** Actively checks the order book for existing SL orders before placing new ones.

**Weaknesses:**
*   **Symbol Parsing Assumptions:** Relies on specific symbol naming conventions (e.g., extracting "CE"/"PE") which may vary across instruments or brokers.

**Recommendations:**
*   Generalize the SL order detection logic to be less reliant on specific symbol string manipulations, perhaps utilizing broker instrument metadata.

### 1.5 `gap_risk_manager.py`

**Overview:**
Manages overnight gap risks by adjusting position sizing (e.g., halving sizes on Mondays) or blocking trades if overnight gaps exceed thresholds. Integrates pre-market data.

**Strengths:**
*   **Proactive Risk Mitigation:** Excellent protection against overnight black-swan events or weekend decay risks.
*   **Configurable Profiles:** Clear day-based risk profiles.

---

## 2. Backend (`web/backend/app/` directory)

### 2.1 Architecture & Services

**Overview:**
A FastAPI application that acts as the control plane for the trading strategies. Features WebSocket streaming, strategy lifecycle management (`multiprocessing`), live data management, and Greeks calculation.

**Strengths:**
*   **Multiprocessing Strategy Execution:** `StrategyManager` runs strategies in isolated processes (`multiprocessing.Process`), ensuring that heavy computations (like Black-Scholes) or blocking broker calls do not freeze the FastAPI event loop.
*   **Live Data Streaming:** `LiveDataManager` actively polls the broker and streams position/order updates via WebSockets, ensuring the frontend is highly responsive.
*   **Strategy Registry:** A flexible `StrategyRegistry` allows for dynamic discovery, configuration preview, and validation of different strategies.

**Weaknesses & Potential Issues:**
*   **LiveDataManager Polling:** The `LiveDataManager` polls the broker API every 1 second (`await asyncio.sleep(1)`). This aggressive polling can easily breach rate limits (e.g., Kite Connect allows 3 requests/second).
*   **State Queue Bottleneck:** State updates from the isolated strategy process are sent via a `multiprocessing.Queue`. If the FastAPI monitor task (`_monitor_state`) falls behind, the queue could grow unbounded.
*   **Error Handling in Live Stream:** If the broker API returns a transient error, the `LiveDataManager` increments an error counter and stops streaming after 5 failures, requiring manual intervention.

**Recommendations:**
*   **WebSocket Fallback:** Prefer subscribing to the broker's WebSocket for order/position updates instead of aggressive REST API polling.
*   **Exponential Backoff:** Implement exponential backoff for `LiveDataManager` retries instead of a hard abort after 5 errors.

### 2.2 API Routes (`routes/`)

**Overview:**
Provides REST endpoints for configuration (`config.py`), monitoring (`monitoring.py`), Greeks (`greeks.py`), and strategy control (`strategy.py`, `strategy_selector.py`).

**Strengths:**
*   **Comprehensive Greeks API:** The `greeks.py` endpoint calculates portfolio-wide and per-position Greeks efficiently, providing excellent risk visibility to the user.
*   **Configuration Validation:** The `/config/validate` endpoint provides robust sanity checks and warnings before applying strategy parameters.

**Weaknesses:**
*   **Authentication Coupling:** Broker authentication errors are handled inconsistently across routes, sometimes returning standard responses with error messages and sometimes throwing HTTP exceptions.

---

## 3. Frontend (`web/frontend/` directory)

**Overview:**
A Next.js 14 application providing a real-time dashboard for strategy monitoring, configuration, and visual analysis.

**Strengths:**
*   **Real-time Reactivity:** Utilizes WebSockets effectively to update the UI without manual refreshes.
*   **Strategy Selector Component:** `StrategySelector.tsx` provides a clear, user-friendly interface for previewing strategy configurations, viewing risk profiles, and confirming parameter differences before starting a strategy.
*   **Visual Engine:** Dedicated components (`VisualEngine/`) to visualize complex data like Entry Radar, Filter Panel, and Signal Timeline.

**Weaknesses:**
*   **Polling Fallbacks:** While WebSockets are used, some components (like `StrategySelector`) fall back to polling the REST API (`setInterval(fetchStrategies, 5000)`), which increases server load unnecessarily.
*   **State Management:** Heavy reliance on local component state for WebSocket data might lead to desyncs or redundant re-renders across different dashboard widgets.

**Recommendations:**
*   **Centralized WebSocket Context:** Implement a global React Context or state management store (like Zustand or Redux) to manage WebSocket incoming data and distribute it to components, removing the need for local polling fallbacks.

---

## 4. Overall Codebase Summary

**Tech Stack Integration:** Python (FastAPI, Pandas, Multiprocessing, Scipy) + Next.js + WebSockets.
**Architecture:** Solid control-plane/data-plane separation using Python's `multiprocessing`.

**Key Strengths:** The system is heavily focused on **Risk Management** (Gap Risk, Delta Hedging, Auto SL, Trailing SL) and **Resiliency** (Stateless SL reconciliation, Error catching).
**Key Areas for Improvement:**
1.  **Rate Limiting Vulnerability:** Aggressive REST API polling in `LiveDataManager` must be optimized or moved to WebSockets to prevent broker bans.
2.  **Inheritance Consistency:** Standardize all strategies (`Survivor`, `Wave`) to inherit from `BaseStrategy`.
3.  **Refactoring:** Break down massive files like `survivor.py` into modular components to improve testability and maintainability.
4.  **Asynchronous Hot Paths:** Ensure the tick processing loop does not contain synchronous, blocking operations like `time.sleep()` or blocking API calls.