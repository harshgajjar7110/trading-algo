# Detailed Review Report: Python-Based Automated Trading Strategy

## 1. Overview
This report provides a comprehensive review of the Python-based automated trading strategy focused on option selling for theta decay using the Zerodha broker. The review covers the core trading strategies under the `strategy/` directory and the application architecture within the `web/` directory. The `sensibull/` directory was explicitly excluded from this review.

## 2. Strategy Architecture (`strategy/`)
The `strategy/` directory contains the core logic for the trading algorithms. The system is designed to sell options (Put and Call) to capture theta decay based on dynamic market movements.

### 2.1. `survivor.py` (Survivor Strategy v2.0)
The Survivor Strategy is a trend-following options selling strategy on the NIFTY index. It sells Put Options (PE) on market rise and Call Options (CE) on market fall.

**Strengths:**
- **Dynamic Risk Management:** Implements automated stop-loss (SL) via broker SL-M/SL limit orders (`_place_sl_order`) and profit booking targets.
- **Advanced Technical Filters:** Employs configurable technical indicators (RSI, EMA, ADX, ATR) to filter entries. The RSI filter prevents selling into extremes, while EMA and ADX verify trend strength.
- **Volatility-adjusted Sizing:** Adjusts gaps and position sizing dynamically based on ATR and Volatility Regime (High, Normal, Low). It applies a multiplier scaling down positions during high volatility.
- **SL Reconciliation:** Detects open positions without SL at startup and automatically places SL orders to prevent unmanaged risks after restarts.
- **Enhanced Trailing SL:** Incorporates a trailing stop loss that activates after a predefined profit percentage and tracks behind the maximum profit attained.
- **Profit target check:** Continuously scans for positions that hit profit goals (`_check_profit_targets`), ensuring the system scales out effectively.

**Potential Improvements:**
- **Single Point of Failure (Exception Handling):** In the main event loop (`on_ticks_update`), unhandled exceptions in one trade cycle could potentially disrupt state. While wrapped in try/excepts, deep errors might cause silent state mismatch.
- **Hardcoded Ticks / Rounding:** The `0.05` tick size is hardcoded in SL logic (`round(price / 0.05) * 0.05`). This should ideally be fetched from the instrument specification dynamically.
- **Complex State Management:** Position state is distributed across multiple dicts (`self.positions`, `OrderTracker`, `PositionManager`). Consolidating this could prevent potential synchronization bugs.

### 2.2. `wave.py` (Wave Trading Strategy)
The Wave Strategy relies on continuous scraping and placing dual limits (buy and sell gaps) around a central price.

**Strengths:**
- **Greeks Integration:** Uses `mibian` for real-time Greeks calculation (Delta, Gamma, Vega, Theta). It actively tracks the portfolio delta (`_get_portfolio_greeks`) to enforce risk constraints (`min_nifty_delta`, `max_nifty_delta`).
- **Dynamic Multipliers:** Calculates dynamic `buy_gap` and `sell_gap` adjustments based on the net open position ("position imbalance").
- **Constraint Enforcement:** Continuously checks delta limits and selectively restricts long or short orders to balance the portfolio delta automatically.

**Potential Improvements:**
- **Synchronous Delays:** Features a blocking `time.sleep(self.cool_off_time)` inside `_prepare_final_prices`. In an asynchronous or event-driven system, blocking the main thread can cause missed ticks and lag.
- **Complex Loop Tracking:** Order tracking handles associations (linked buy/sell orders) manually. If an order is partially executed, the logic might not handle the residual quantity cleanly.

### 2.3. `position_manager.py`
A crucial module handling the reconciliation of positions with broker states.
- **Strengths:** Prevents duplicate SL orders. Matches short NFO MARGIN positions perfectly and isolates open BUY orders.
- **Improvements:** Should implement persistent caching (e.g., SQLite or Redis) rather than relying solely on in-memory state. In the event of a hard crash where the broker API rate limits are hit during restart, state recovery might be delayed.

### 2.4. `gap_risk_manager.py` & `pre_market_data.py`
- **Strengths:** Evaluates overnight market gaps using GIFT Nifty and global markets (via `yfinance`). Excellent implementation of day-of-the-week risk multipliers (e.g., reducing Friday/Monday position sizing due to weekend risk).
- **Improvements:** The dependency on `yfinance` can be unstable. Fallback APIs or data sources should be considered if `yfinance` gets blocked or times out.

## 3. Web Architecture (`web/`)
The web application provides a clean separation of concerns, utilizing FastAPI for the backend and Next.js 14 for the frontend.

### 3.1. Backend (`web/backend/`)
Built with FastAPI, ensuring high performance via asynchronous request handling.
- **Strengths:**
  - **WebSocket Integration:** Uses a sophisticated WebSocket manager (`connection_manager`) with a dedicated background task (`state_broadcast_loop`) for real-time strategy state distribution.
  - **Multi-Processing for Strategies:** The `StrategyManager` utilizes Python's `multiprocessing` to isolate trading processes from the API layer. This ensures that a heavy API load does not interfere with the low-latency requirements of the trading algorithm.
  - **Broker Abstraction:** The `BrokerService` (`broker_service.py`) provides a clean interface over the underlying broker (Zerodha), allowing future expansion to other brokers (Fyers, etc.) without altering the core API routes.
  - **LiveDataManager:** In-memory queue-based dispatcher for routing broker WebSocket callbacks.
- **Potential Improvements:**
  - **State Persistence:** The `LiveDataManager` is completely in-memory. Implementing an Event Sourcing pattern or a fast database (Redis) would enhance fault tolerance.
  - **WebSocket Error Handling:** Broad exceptions are caught in WebSocket loops. Finer-grained exception handling and client reconnection logic (backoff) should be formalized on the server side.

### 3.2. Frontend (`web/frontend/`)
Built using Next.js 14 (App Router), React, Tailwind CSS, and Recharts.
- **Strengths:**
  - Real-time reactivity via custom WebSocket hooks (`useWebSocket`).
  - Strict typing implementation (TypeScript).
  - Modern UI practices with Tailwind CSS and modular components (e.g., `PayoffChart`, `GreeksDisplay`, `VisualEngine`).
- **Potential Improvements:**
  - Component rendering optimization. Ensuring that deep state updates from the WebSocket don't cause widespread unnecessary re-renders (using `useMemo` and `useCallback` effectively).

## 4. Overall Recommendations
1. **Remove Synchronous Blocking:** Replace `time.sleep()` in strategy execution loops (e.g., `wave.py`) with asynchronous equivalents (`asyncio.sleep`) or a purely event-driven architecture to prevent tick latency.
2. **Tick Size Abstraction:** Fetch tick size per instrument from the broker API instead of hardcoding `0.05`, making the system adaptable to index or stock changes.
3. **Database Integration:** Introduce Redis for in-memory position state persistence. This bridges the gap between purely in-memory execution and the latency of hitting the broker API for reconciliation repeatedly.
4. **Resiliency to Market Data Failure:** Implement robust fallback mechanisms in `PreMarketDataService` in case external sources like Yahoo Finance fail.

## 5. Conclusion
The codebase demonstrates a highly sophisticated approach to automated theta-decay options selling. The separation of concerns between the execution strategies (using `multiprocessing`) and the web/API interface is well executed. Risk management rules, dynamic gap tracking, and real-time greeks calculations represent professional-grade trading systems. Addressing the minor points regarding synchronous sleeps, persistent state, and hardcoded values will further enhance robustness and fault tolerance.
