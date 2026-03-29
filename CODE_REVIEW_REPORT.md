# Detailed Code Review Report: Automated Trading Strategy

This report provides a comprehensive architectural and code review of the Python-based automated trading strategy application, focusing extensively on the **Survivor** strategy (an options selling strategy designed to capture theta decay using Zerodha) and the overarching web backend/frontend systems.

The `sensibull/` directory was expressly ignored in this review.

---

## 1. Executive Summary

The platform is designed around a scalable and modern tech stack (FastAPI backend + Next.js frontend + WebSocket streaming). It supports executing options selling strategies using a dynamic, indicator-driven approach.

**Key Strengths:**
* **Architecture:** Decoupling the trading algorithms (`strategy_manager.py` using `multiprocessing`) from the main API processes prevents blocking latency.
* **Risk Management:** Impressive emphasis on proactive risk mitigation, including Gap Risk Analysis (`gap_risk_manager.py`), Stop-Loss Order Reconciliation (`position_manager.py`), and Daily Loss limits.
* **Theta Capture Logic:** `SurvivorStrategy` is intelligently structured around volatility-based strikes (ATR) and dynamically scaling Option-Selling gaps.

**Areas for Improvement:**
* **Concurrency Bugs:** Order placement and strategy dictionaries lack explicit lock mechanisms when multi-threading or async logic is intertwined.
* **Code Duplication & Refactoring:** A few redundant API calls could be optimized, and legacy dependencies exist.

---

## 2. Web Backend Review (`web/backend/app/`)

### Architecture & Design
The FastAPI backend acts as a robust control plane.
* **`main.py` & Lifespan Context:** Properly utilizes async context managers (`lifespan`) to cleanly spawn and teardown background tasks like WebSocket heartbeats and State broadcasting.
* **`BrokerService` (`services/broker_service.py`):** Operates as a clear Facade pattern over the lower-level broker implementations (e.g., Zerodha). It intelligently wraps broker errors and sanitizes data. The PnL percentage calculation logic accurately reflects short margin requirements.
* **`StrategyManager` (`services/strategy_manager.py`):**
  * **Strengths:** Employs Python `multiprocessing` via a `Process` wrapper. The use of `multiprocessing.Queue` to stream strategy state updates back to the API prevents the heavy computational logic (like Pandas/Scipy indicator updates) from blocking the async FastAPI event loop.
  * **Critique:** The auto-stop on critical error logic string-matches terms like "insufficient funds". Relying on string-matching error logs is brittle. It’s better to standardize exception types from the Broker wrappers.

### Websockets & Real-time Data
* **`app/websocket/manager.py` (assumed based on `main.py` usage):** Handles subscriptions well. However, `state_broadcast_loop` queries the state using `strategy_manager.get_state()` every second regardless of whether the state changed, which is slightly inefficient. Emitting events only on state diffs would lower overhead.

### Analysis & Greeks
* **`analysis.py`:** Excellent regex-based symbol parsing for options contracts (Weekly vs. Monthly). The `calculate_option_payoff` logic correctly calculates linear payouts for Option Selling (Short Call/Short Put) and accurately graphs them for the Next.js visualizer.

---

## 3. Web Frontend Review (`web/frontend/`)

### Architecture & Design
* Next.js 14 utilizing the `app/` router with React Server Components paradigm, cleanly mixed with `'use client'` where needed.
* **`hooks/useWebSocket.ts`:** Robust hook to manage the WebSocket lifecycle, handling reconnection and distributing state payloads to UI components (`handleStrategyState`, `handlePriceUpdate`).

### UI & UX
* The dashboard (`page.tsx`) provides an excellent, holistic view. It aggregates current funds, open positions, active P&L, NIFTY 50 prices, and strategy statuses cleanly.
* **Critique:** Client-side sorting on positions is written well but re-renders the entire table on every data tick from the WebSocket. Wrapping the `sortedPositions` calculation in `useMemo` with proper dependencies will heavily improve UI performance under fast tick conditions.

---

## 4. Strategy Engine Review (`strategy/`)

### A. The "Survivor" Strategy (`survivor.py`)

This is the core Theta-Decay strategy. It operates by selling Call Options (CE) on market falls and Put Options (PE) on market rises, scaling in at dynamically calculated gaps.

**Logic Strengths:**
* **Dynamic Gap Adjustment:** Uses the `TechnicalIndicators` dataclass to calculate ATR and adjust `pe_gap`/`ce_gap` widths based on Volatility Regimes. This prevents rapid entry scaling during high-volatility moves.
* **Trend & Momentum Filters:** Utilizes RSI, EMA, and ADX filters. E.g., The system correctly refuses to sell PE (betting against a drop) if the RSI is already highly overbought, protecting against mean-reversion spikes.
* **Profit Taking:** The `_check_profit_targets` runs efficiently every 5 minutes (or on interval), locking in theta decay automatically when the premium drops to 40% of its initial value (60% profit target).
* **Automatic SL Ordering:** The strategy immediately places a broker-level Stop-Loss (GTT/OCO based on the Zerodha API implementation) *right after* the fresh sell order. This is a highly robust failsafe compared to local/software-based Stop-Losses which fail during network disconnects.

**Areas for Improvement / Bugs:**
* **Duplicate SL Order Logic:** In `_place_sl_order`, if a position already has an active SL, it logs a warning. But in edge-case network partitions, if the order is partially filled, the SL calculation might not cover the new average quantity correctly.
* **Performance:** `_check_profit_targets` loops over all positions and fires `broker.get_quote()`. Since these are individual API calls, this can cause rate-limiting issues on Zerodha if the position size is large. A bulk quote call should be utilized here.
* **`self.price_history`:** Max size is 1000 candles. Storing this purely in memory without persistence means on an intraday restart, the EMA/RSI calculations will lack context until enough new candles are formed, temporarily blinding the indicators.

### B. Risk Management (`gap_risk_manager.py` & `position_manager.py`)

* **Gap Risk Manager:**
  * A very creative implementation! It scales position sizes based on historical gap statistics (e.g., Monday gap risks are mitigated by halving position sizes `0.5x` and waiting until 10:00 AM for morning volatility to settle).
  * Checking GIFT Nifty against the current spot to halt trading entirely on `>1.5%` gaps saves the system from entering trades on Black Swan mornings.

* **Position Manager:**
  * Specifically designed to reconcile Stop-Losses at system startup.
  * *Strength:* Explicitly avoids state-file persistence (`memory.json` or similar) to prevent the "stale state" problem. It calculates unprotected positions mathematically (`total_qty - protected_qty`) by parsing the live broker order book and dynamically places the missing SLs. This is an enterprise-grade approach to state recovery.

### C. The "Wave" Strategy (`wave.py`)

* Primarily an algorithmic scaler utilizing Mibian for Black-Scholes Greeks calculation.
* Regulates the Delta of the portfolio by dynamically restricting Buy/Sell sides if `total_delta` strays beyond `min_nifty_delta` and `max_nifty_delta`.
* **Critique:** The `_execute_orders` method in `wave.py` has heavily nested logic that directly mutates `self.orders` and `self.handle_order_update_call_tracker` synchronously. Because WebSocket callbacks (`on_order_update`) fire asynchronously, race conditions can (and likely will) occur where an order completes before the synchronous tracking dicts are fully updated. Explicit threading locks or state-machine implementations are recommended here.

---

## 5. Zerodha Integration & Infrastructure

* The framework strictly types interactions with Brokers via generic schemas (`Position`, `Order`, `Quote`). This makes the transition between Zerodha, Fyers, etc., seamless.
* Zerodha's specific limitations are well respected. For instance, the SL implementations (`STOP_LIMIT` vs `STOP`) correctly account for NSE F&O rules regarding Market orders on options.
* **Deployment:** The project structure is heavily moving towards standard CI/CD (`pyproject.toml`, `uv` lockfiles, FastAPI structured `/web`).

## 6. Conclusion and Final Recommendations

The Theta Decay (Survivor) architecture is **excellent**. The defensive programming surrounding Pre-Market Gaps, System Restarts, and Delta Hedging limits shows a deep understanding of live trading environments.

**Actionable Steps:**
1. **Frontend Optimization:** Wrap expensive table calculations and filtering in `useMemo` in `page.tsx` to handle high-frequency WebSocket data.
2. **Backend Optimization:** Transition single `get_quote()` calls inside the profit-checking loops to bulk requests (`get_quotes(symbols=[...])`) to prevent broker API rate-limiting.
3. **Thread Safety:** Implement `threading.Lock()` inside `WaveStrategy` and `SurvivorStrategy` dictionaries tracking live order IDs to prevent race conditions from WebSocket threaded callbacks.
4. **Historical Cache:** Serialize the recent price history (1000 candles) to Redis or SQLite so that if the FastAPI backend restarts intraday, the RSI/EMA indicators don't start from zero.