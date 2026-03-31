# Automated Trading Strategy Code Review Report

This document contains a comprehensive review of the `web/` and `strategy/` directories for the Python-based automated trading strategy application utilizing the Zerodha broker. The trading strategy involves selling options to capture theta decay as profit.

## Overview

The application is structured into two main components:
1. **`strategy/`**: Contains the core algorithmic trading logics, including `survivor.py`, `wave.py`, and `position_manager.py`.
2. **`web/`**: Contains a FastAPI backend (`web/backend/`) and a Next.js frontend (`web/frontend/`) for interacting with, monitoring, and configuring the trading strategies.

---

## 1. Strategy Module (`strategy/`)

### 1.1 `survivor.py`
The Survivor strategy is an advanced NIFTY options selling strategy designed to capture theta decay.

**Review Findings:**
- **Robust Risk Management:** The strategy implements multiple layers of risk management, including Stop-Loss (SL) limit buffers, max daily loss checks, time-based square-offs, trailing stops, and profit booking configurations.
- **Dynamic Gaps and Volatility Adjustments:** Features like dynamic gap scaling (based on ATR) and position sizing reduction in high volatility regimes display an excellent adaptation to varying market conditions.
- **Broker-Level SL Integration:** The strategy successfully mitigates execution risk by immediately placing a broker-level SL order after a fresh entry. The logic effectively differentiates between STOP (SL-M) and STOP_LIMIT (SL) orders, rounding prices correctly to the 0.05 tick size.
- **Start-up Reconciliation:** SL reconciliation via `reconcile_sl_at_startup()` effectively fetches fresh broker positions, checks open orders for existing SLs, and places SL orders for any unprotected quantities.
- **Profit Booking:** The periodic profit target checks properly verify active targets and correctly cancel associated SL orders before exiting positions.
- **Potential Edge Cases / Improvements:**
  - In `_check_stop_losses()`, the manual SL check fallback relies on the `stop_loss_multiplier` (doubling of premium). It could be beneficial to add a fallback condition checking the manual price trigger if the broker SL fails to execute despite being ACTIVE.
  - Exception handling in `on_ticks_update` suppresses general exceptions per tick. It might be wise to incorporate a circuit breaker if successive ticks continuously raise exceptions.

### 1.2 `wave.py`
The Wave Strategy is a trend-following option selling system trading on pre-defined gaps with dynamic delta-based portfolio restrictions.

**Review Findings:**
- **Delta-Based Restrictions:** The strategy calculates portfolio Greeks natively using the `mibian` library, scaling gaps or restricting buy/sell orders if the portfolio delta exceeds strict bounds (`min_nifty_delta` / `max_nifty_delta`).
- **Dynamic Gap Generation:** Dynamic gaps based on position imbalance (`_get_scaled_gaps()`) help auto-correct excessive directional exposure.
- **Order Handling constraints:** The script relies on polling (`time.sleep(self.cool_off_time)`) during order execution cycles. In high-velocity environments, a non-blocking asynchronous approach would provide faster entries.
- **Potential Improvements:**
  - `_complete_order()` calls `self.place_wave_order()` which can cause recursive chains if orders execute instantly. Wrapping it with a delayed execution or task queue might prevent deep recursion.
  - The script relies heavily on predefined default numbers for restrictions if config is missing. A strict validation scheme should ensure missing configs halt execution.

### 1.3 `position_manager.py`
Handles stateful SL mapping, strictly avoiding disk persistence to prevent false trade executions derived from stale data.

**Review Findings:**
- **Excellent Data Freshness Guarantees:** The explicitly stated rule of never persisting position state to disk is an excellent safety feature, preventing "ghost" SLs from triggering on restart.
- **Reconciliation Logic:** The `reconcile_at_startup()` calculates "protected quantities" by scanning open broker BUY orders matching the short position. This prevents duplicate SL placement perfectly.
- **Potential Edge Cases / Improvements:**
  - The SL order tag uniquely identifies orders using `datetime.now().strftime("%H%M%S")` (`SE_SL_{timestamp}`). In very fast parallel execution, timestamps at the second level might collide. Appending a microsecond or random suffix would guarantee uniqueness.

---

## 2. Web Backend (`web/backend/`)

The backend is built using FastAPI, structuring logic into clean routers, models, and services.

**Review Findings:**
- **Architecture & Maintainability:** The backend properly separates broker-facing operations (`BrokerService`), strategy processes (`StrategyManager`), and Greeks calculation (`PortfolioGreeksCalculator`).
- **WebSocket Integration:** Real-time updates via WebSockets are robustly handled. Background tasks broadcast prices, strategy state, and heartbeats properly. The `state_broadcast_loop` selectively queries the NIFTY quote only when a strategy is running, preventing unnecessary API throttling.
- **Multi-Process Strategy Management:** The `StrategyManager` utilizes Python's `multiprocessing` to run strategies independently. This prevents heavy tick-processing inside the strategy from locking up the API server. State sync is properly established using `multiprocessing.Queue`.
- **Greeks Calculator:** The `BlackScholesGreeks` implementation natively extracts strikes and expiries using sophisticated Regex (`parse_option_symbol`). It robustly handles both Monthly and Weekly expiry patterns. Furthermore, the `PortfolioGreeksCalculator` appropriately aggregates Delta, Gamma, Theta, Vega, and Rho into a portfolio perspective, along with actionable interpretations.
- **Potential Edge Cases / Improvements:**
  - Multi-process termination in `StrategyManager` (`self._process.kill()`) is forceful. Introducing a Graceful Shutdown signal to the strategy before terminating would ensure open files and pending API requests close cleanly.
  - In `greeks_calculator.py`, the implied volatility is hardcoded or defaults to `0.15` (15%). Fetching live IV or using the provided Newton-Raphson fallback `calculate_implied_volatility` across the board would drastically improve Greek accuracy.

---

## 3. Web Frontend (`web/frontend/`)

Built on Next.js 14 and React 18 with TypeScript.

**Review Findings:**
- **Robust Typing:** The TypeScript definitions in `types/index.ts` strongly map to the FastAPI response schemas, preventing type-related runtime errors.
- **Dashboard Usability:** `page.tsx` is feature-rich, combining Payoff Charts, Greeks Displays, Strategy Selection, and detailed active positions tabular views. Sorting states are correctly implemented for open positions.
- **Strategy Selector:** `StrategySelector.tsx` introduces a well-designed preview and confirmation flow, providing users a visual breakdown of customized parameters vs default parameters, significantly reducing human error.
- **Real-Time Responsiveness:** The UI natively consumes WebSocket events via the `useWebSocket` hook, enabling smooth updates to P&L, Strategy State, and NIFTY quotes without constant REST polling.
- **Potential Edge Cases / Improvements:**
  - The Greeks display auto-refreshes using `setInterval` (polling every 30s). Connecting the Greeks calculations to trigger off WebSocket price changes (or periodic backend WS emissions) would sync Greeks perfectly with the live market data.

---

## Conclusion

The trading architecture demonstrates a high level of sophistication, successfully separating the broker integration from algorithmic execution and user interface. The implementation of real-time monitoring, multi-process execution, dynamic ATR-based gap scaling, and strict SL reconciliation provides a highly reliable foundation for automated options selling.

The logic is resilient, scalable, and correctly avoids critical failure points (like state persistence conflicts). Implementing the minor edge-case enhancements (like microsecond precision on order tags and graceful multi-process shutdowns) would further solidify this already excellent trading system.