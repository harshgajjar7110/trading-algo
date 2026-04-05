# Automated Trading Strategy Code Review Report

This document provides a comprehensive code review of the Python-based automated trading strategy targeting the Zerodha broker, primarily focusing on options selling to capitalize on theta decay. The review covers the core strategy modules located in the `strategy/` directory and the backend integration located in the `web/backend/` directory, while intentionally excluding the `sensibull/` directory.

## 1. Executive Summary

The trading system is a well-structured, multi-strategy platform designed to execute algorithmic trades, primarily options selling (Survivor) and simultaneous buy/sell (Wave) strategies. The system leverages `FastAPI` for the backend, `multiprocessing` for isolated strategy execution, and WebSocket callbacks for real-time tick processing.

**Strengths:**
- **Robust Risk Management:** The `SurvivorStrategy` incorporates comprehensive risk management, including broker-level GTT/Stop-Loss (SL) orders, trailing SLs, gap risk analysis (`GapRiskManager`), and position limits.
- **Modularity:** Strategies are managed dynamically through `StrategyRegistry` and executed in isolated processes (`StrategyManager`).
- **Resilience:** Features like `PositionManager` ensure that even upon restarts, unprotected positions are identified and SL orders are reconciled.

**Areas for Improvement:**
- **Code Duplication:** There is overlap between `SurvivorStrategy` and `BaseStrategy` that could be refactored.
- **Error Handling:** While errors are logged, some exceptions in critical execution paths (like WebSocket callbacks) could be handled more gracefully to avoid process crashes.
- **Hardcoded Values:** Some fallback values and configurations (like specific tags or string comparisons) could be moved to configuration constants.

---

## 2. Strategy Module Review (`strategy/` directory)

### 2.1 `survivor.py` (Survivor Strategy)
The `SurvivorStrategy` is the core options selling algorithm that sells PE on the rise and CE on the fall to capture theta decay.

**Review & Observations:**
- **Technical Indicators:** Calculates RSI, EMA, ATR, and ADX on a rolling 5-minute window. Used effectively to filter entries (e.g., avoiding selling PE in overbought zones).
- **Dynamic Gap & Sizing:** Integrates `GapRiskManager` to adjust position sizes based on volatility and the day of the week (e.g., lower exposure on Friday/Monday).
- **Stop Loss & Reconciliations:** SL orders are placed immediately after entry using the `_place_sl_order` method. The `reconcile_sl_at_startup` method is excellent for recovering state after a crash or restart.
- **Profit Booking:** Iterates through positions to check if the target profit (e.g., 60% of premium) is hit, exiting the position and cancelling the SL order.
- **Critique:** The class is massive (1500+ lines). It handles execution, technical indicator calculation, SL reconciliation, and profit checking. Refactoring technical indicator logic into a separate `TechnicalAnalyzer` class would improve maintainability.

### 2.2 `wave.py` (Wave Strategy)
A strategy designed to place simultaneous buy and sell orders offset by a configured gap.

**Review & Observations:**
- **Greeks Calculation:** Uses the `mibian` library to calculate Delta, allowing the strategy to enforce portfolio delta limits.
- **Order Tracking:** Maintains internal `self.orders` state and tracks associated orders (e.g., cancelling the buy order if the sell order is rejected).
- **Critique:** State management here is complex and slightly fragile. The logic heavily relies on nested dictionaries to map `order_id` to its status. A state machine or dedicated order tracking class could reduce the risk of orphaned orders.

### 2.3 `gap_risk_manager.py`
A crucial risk management module that assesses overnight gaps and pre-market indicators.

**Review & Observations:**
- **Logic:** Implements rules like skipping trades if the gap exceeds 1.5%, or delaying Monday entries until 10:00 AM.
- **Integration:** Exposes `GapRiskAssessment` dataclass which is neatly consumed by `SurvivorStrategy`.
- **Critique:** Solid, declarative implementation. The logic is clean and easy to test.

### 2.4 `position_manager.py`
Handles in-memory tracking and startup reconciliation of Stop-Loss orders for the Survivor strategy.

**Review & Observations:**
- **Design Choice:** Explicitly avoids file persistence to prevent stale data. It fetches the source of truth directly from the broker using `broker.get_positions()` and `broker.get_orderbook()`.
- **Reconciliation:** Iterates over open broker positions, compares them against open SL orders, and places new SL orders for any unprotected quantities.
- **Critique:** Excellent design choice for a live trading environment. Relying on broker state prevents out-of-sync issues.

### 2.5 `pre_market_data.py`
Fetches overnight market data (e.g., GIFT Nifty, US Markets) to feed the gap risk manager.

**Review & Observations:**
- **Caching:** Uses local file caching to avoid spamming external APIs (like Yahoo Finance) and handles fallbacks.
- **Critique:** Relies on optional dependency `yfinance`. Ensure that fallback mechanisms are robust if `yfinance` fails or is not installed in production environments.

### 2.6 `base_strategy.py`
An abstract base class meant to consolidate common strategy functionality.

**Review & Observations:**
- **Critique:** Currently, `SurvivorStrategy` does not appear to fully inherit or utilize this base class. Integrating `SurvivorStrategy` into `BaseStrategy` would reduce boilerplate code (like broker initialization, instrument downloading, and order placement).

---

## 3. Web Backend Module Review (`web/backend/app/` directory)

### 3.1 `services/strategy_manager.py`
Manages the lifecycle (start, stop, restart) of trading strategies.

**Review & Observations:**
- **Concurrency:** Uses `multiprocessing.Process` to run the strategy in an isolated process. This is a critical architectural decision that prevents strategy blocking from affecting the API responsiveness.
- **Communication:** Uses `multiprocessing.Queue` (`_state_queue`) to receive state updates, errors, and gap risk notifications from the isolated strategy process.
- **Notification:** Integrates with `telegram_notifier` to send alerts on start, stop, error, and gap risk events.
- **Critique:** The manager handles legacy single-strategy mode and new multi-strategy modes well. However, joining a process with `self._process.kill()` if it doesn't terminate cleanly is harsh; ensuring strategies handle `SIGTERM` gracefully would be cleaner.

### 3.2 `routes/strategy.py` & `routes/visual.py`
API endpoints for controlling the strategy and fetching visual state for the frontend.

**Review & Observations:**
- **`strategy.py`:** Provides REST endpoints for `/start`, `/stop`, `/restart`, and `/status`. Includes a pre-flight check to verify broker authentication before starting a strategy.
- **`visual.py`:** Provides endpoints to fetch predictions, filter statuses, and market context to power the Next.js frontend dashboard.
- **Critique:** Clean FastAPI implementation. The pre-flight auth check in `strategy.py` is a great safety feature to prevent the strategy from failing immediately upon start due to expired tokens.

---

## 4. Overall Architecture and Quality Assessment

1. **Safety and Reliability:** The architecture is designed with safety in mind. The `PositionManager` reconciling SL orders at startup and the `GapRiskManager` adjusting position sizes based on volatility are strong indicators of a mature algorithmic trading system.
2. **Process Isolation:** Running the heavy computational loop (WebSocket ticks, technical indicators) in a separate `multiprocessing` process ensures the FastAPI backend remains performant.
3. **Broker Agnosticism:** The use of `BrokerGateway` facades allows the system to abstract away the specifics of Zerodha/Fyers.

## 5. Recommendations

1. **Refactoring `SurvivorStrategy`:** Break down `SurvivorStrategy` into smaller classes (e.g., `IndicatorCalculator`, `OrderExecutor`).
2. **BaseStrategy Adoption:** Standardize all strategies (`survivor.py`, `wave.py`) to inherit from `BaseStrategy` to enforce a unified interface.
3. **Test Coverage:** Ensure robust unit tests exist specifically for `position_manager.py` and `gap_risk_manager.py`, mocking the broker responses to simulate various crash-recovery scenarios.
