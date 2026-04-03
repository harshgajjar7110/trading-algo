# Automated Trading Strategy Code Review Report

This report provides a detailed code review of the automated trading strategy application, specifically focusing on the `web` and `strategy` directories. The application is designed to trade options using the Zerodha broker (among others) to generate theta decay as profit.

## 1. Architecture Overview
The system follows a robust, decoupled architecture separating the FastAPI web server from the strategy execution processes.
- **Backend Framework:** FastAPI (Python 3.11+).
- **Concurrency & Isolation:** The `StrategyManager` utilizes Python's `multiprocessing` library to run trading strategies in isolated processes. This ensures that the main API application and WebSocket server remain responsive regardless of the strategy's computational load or blocking calls.
- **Real-time Communication:** Extensive use of WebSockets (`app/websocket/manager.py`) allows real-time streaming of `PRICE_UPDATE`, `STRATEGY_STATE`, `ORDER_UPDATE`, and `POSITION_UPDATE` to clients.
- **Broker Abstraction:** The `BrokerGateway` acts as a facade, standardizing interactions (e.g., getting quotes, placing GTT/OCO orders) across different brokers (Zerodha, Fyers, Fyrodha).

## 2. Strategy Logic (`strategy/` directory)

### A. Survivor Strategy (`survivor.py`)
This is the core NIFTY options selling strategy designed to capture theta decay while utilizing advanced risk management techniques.

**Strengths:**
- **Technical Filters:** Implements RSI, EMA, and ADX filters natively, preventing the strategy from selling options against strong momentum (e.g., rejecting PE sells when RSI is overbought).
- **Dynamic Gap Adjustment:** Adjusts `pe_gap` and `ce_gap` dynamically based on Average True Range (ATR) to accommodate shifting market volatility.
- **Automatic SL Order Management:** Excellent implementation of automated Stop-Loss. It calculates SL trigger and limit prices immediately upon fresh order execution and places broker-level `STOP_LIMIT` or `STOP` orders.
- **Startup Reconciliation:** The `reconcile_sl_at_startup` method and the associated `PositionManager` ensure that any open positions carried over are detected and protected with SL orders immediately after system restart.
- **Profit Booking:** Includes functionality to monitor and auto-exit positions when a defined profit percentage (e.g., 60% of premium collected) is achieved, effectively managing theta decay gains.
- **Gap Risk Management:** Uses `GapRiskManager` to assess overnight pre-market gaps, altering position sizing multipliers based on day-of-week and gap magnitudes.

**Areas for Improvement:**
- **Indicator Calculation Latency:** Technical indicators are calculated locally from a rolling window of history (`self.price_history`). While effective, relying purely on tick updates to form candles could occasionally diverge from actual exchange minute-bars.
- **Method Complexity:** Methods like `_handle_pe_trade_enhanced` and `_handle_ce_trade_enhanced` are quite lengthy. Breaking them down into smaller, testable pure functions (e.g., separating signal generation from order formatting) would improve maintainability.

### B. Wave Strategy (`wave.py`)
This is a trend-following option selling/buying strategy that places offset simultaneous orders.

**Strengths:**
- **Dynamic Multiplier Scaling:** Modifies gap requirements dynamically based on current position imbalances (`current_diff_scale`), naturally slowing down entries in heavily skewed portfolios.
- **Live Greeks Tracking:** Actively calculates portfolio Delta using the `mibian` library for Black-Scholes calculations.
- **Delta Restrictions:** Enforces portfolio-level Delta limits (`min_nifty_delta`, `max_nifty_delta`). If delta exceeds these boundaries, opposing orders are restricted or actively cancelled (`check_and_enforce_restrictions_on_active_orders`).

**Areas for Improvement:**
- **State Management:** The strategy maintains extensive manual tracking of active orders (`self.orders`) and previous wave prices. If the process crashes, resuming this exact state might be difficult without persistent storage.
- **Hardcoded Logic:** There is some hardcoded logic relating specifically to NIFTY (e.g., `final_prices['buy'] < 25`) which could be extracted into the YAML configuration for better flexibility.

## 3. Web Backend Components (`web/backend/app/` directory)

### A. API Routes (`routes/`)
- **Clean Segmentation:** Routes are well-organized into domains: `strategy`, `market`, `positions`, `monitoring`, `greeks`, and `strategy_selector`.
- **Strategy Selector:** `strategy_selector.py` is a well-designed addition allowing for dynamic querying, validation, and loading of multiple strategies via `StrategyRegistry`. This makes the system highly extensible.
- **Greeks Route:** `greeks.py` exposes endpoints for portfolio-wide Delta, Gamma, Theta, Vega, and Rho, including a scenario analysis endpoint (`/scenario`) which is highly valuable for risk visualization.

### B. Services (`services/`)
- **Strategy Manager (`strategy_manager.py`):** Correctly wraps strategy execution in a `multiprocessing.Process`. Uses `Queue` to stream tick data safely from the main process dispatcher to the isolated strategy. Exception handling and process cleanup are well implemented.
- **Broker Service (`broker_service.py`):** Wraps broker calls in safe `try/except` blocks to prevent the backend from crashing due to transient broker API failures. It also implements sanitization to prevent leaking API keys or tokens in error logs.
- **Live Data Manager (`live_data_manager.py`):** Acts as an in-memory cache for live positions and orders, minimizing API rate-limit exhaustion against the broker by relying on WebSocket updates when the strategy is actively running.

### C. WebSockets (`websocket/manager.py`)
- **Connection Management:** `ConnectionManager` is implemented as a singleton. It handles basic pub/sub functionality well, allowing clients to subscribe to specific symbols.
- **Broadcasting:** The main app `state_broadcast_loop` efficiently broadcasts NIFTY prices and strategy state once every second, ensuring the UI remains highly responsive.

## 4. Overall Security and Best Practices
- **Broker Abstraction:** The abstraction layers (`BrokerGateway` and `BrokerService`) prevent strong coupling to Zerodha, making it easier to integrate Fyers or others in the future.
- **Environment Management:** Uses `uv` and `pyproject.toml` for modern dependency resolution.
- **Error Handling:** The strategies explicitly catch exceptions during API calls, preventing unhandled exceptions from terminating the long-running strategy loops. Sanitize error messages properly.

## 5. Recommendations
1. **State Persistence:** Currently, features like trailing stop-loss and `wave.py` active orders are held in memory. Consider introducing Redis or SQLite to persist critical state variables across unexpected application restarts.
2. **GTT vs SL-M:** In `SurvivorStrategy`, it defaults to placing standard broker SL/STOP_LIMIT orders. Consider migrating to GTT (Good Till Triggered) OCO orders entirely, as they are maintained on the broker side for longer durations without requiring daily margin un-blocking.
3. **Type Hinting:** While present, increasing strict type hinting in `survivor.py` and `wave.py` would help prevent runtime bugs, particularly around nested dictionary data returned from broker APIs.

## Conclusion
The automated trading system is well-architected. The separation of the FastAPI web server from the multiprocessing strategy runner is an excellent design choice for a live trading application. The Survivor strategy in particular exhibits mature risk management logic (ATR sizing, SL reconciliation, technical filtering). The codebase is structured cleanly and is ready for further scaling.
