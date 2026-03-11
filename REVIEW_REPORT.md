# Code Review Report - Automated Trading Strategy

**Project Overview:**
This project is an algorithmic trading platform built with a Python backend (FastAPI) and a Next.js frontend (React). It implements automated trading strategies, primarily focusing on options selling (Short Strangle variation) to profit from theta decay ("Survivor" strategy) and gap/wave trading ("Wave" strategy). The system uses Zerodha's Kite Connect API as the primary broker integration.

## 1. Strategy Review (`strategy/`)

### 1.1 Survivor Strategy (`strategy/survivor.py`)
**Type:** Trend-Following Options Selling (Short Strangle Variation)
**Logic:**
- **Entry:** Sells PE (Put Option) when the underlying price rises above a reference point by a configured gap (`pe_gap`). Sells CE (Call Option) when price falls below a reference point by `ce_gap`. This is a bullish/neutral strategy for PE selling and bearish/neutral for CE selling.
- **Filters:** Uses RSI (Relative Strength Index), EMA (Exponential Moving Average), ADX (Average Directional Index), and ATR (Average True Range) to filter entries.
    - *Example:* Avoid selling PE if RSI is already overbought (>70) or if trend is bearish.
- **Risk Management:**
    - **Broker-Level SL:** Places Stop-Loss orders (GTT/Limit) immediately after entry (configurable `sl_percentage`, default 60%). This is a critical safety feature.
    - **Daily Loss Limit:** Stops trading if daily loss exceeds a percentage of starting capital.
    - **Time-Based Exit:** Squares off all positions at a specific time (e.g., 15:15).
    - **Profit Booking:** Automatically buys back options when a profit target is reached (e.g., 60% of premium collected).
    - **Dynamic Sizing:** Adjusts position size and gaps based on market volatility (ATR).
- **Code Quality:**
    - Well-structured class `SurvivorStrategy`.
    - Extensive logging with `strategy_logger`.
    - Handles edge cases like finding suitable strikes and instrument tokens.
    - Uses `OrderTracker` for persistence.
- **Observations:**
    - The strategy relies heavily on `on_ticks_update` for real-time decision making.
    - The "Reset Logic" (`_reset_reference_values`) allows the grid to move with the market, preventing it from getting stuck at old levels.
    - **Critical Feature:** The `reconcile_sl_at_startup` method ensures that if the system crashes and restarts, it detects open positions and re-places SL orders if missing. This is handled by `PositionManager`.

### 1.2 Wave Strategy (`strategy/wave.py`)
**Type:** Gap/Wave Trading
**Logic:**
- Places buy/sell limit orders at configurable gaps (`buy_gap`, `sell_gap`) from the current market price.
- Uses a "cool-off" period to let price settle before placing new orders.
- **Greeks Integration:** Calculates Portfolio Delta (using `mibian` library) for NIFTY/BANKNIFTY and restricts new orders if delta exceeds configured limits (`min_nifty_delta`, `max_nifty_delta`). This adds a layer of risk management based on overall portfolio exposure.
- **Execution:** Can run as a standalone script via CLI arguments or through the `StrategyManager`.
- **Observations:**
    - More aggressive than Survivor, relying on mean reversion or capturing small moves.
    - The delta-neutral (or delta-constrained) approach is sophisticated for retail algo trading.

### 1.3 Position Manager (`strategy/position_manager.py`)
**Role:** Safety & State Consistency
- **Function:** Handles the critical task of reconciling broker state with internal state.
- **Features:**
    - Fetches *live* positions and open orders from the broker at startup.
    - Identifies unprotected positions (no SL order).
    - Automatically places SL orders for them.
- **Best Practice:** Explicitly avoids persisting state to disk for this purpose, relying on the broker as the "source of truth". This prevents stale state issues.

---

## 2. Backend Review (`web/backend/`)

### 2.1 Architecture
- **Framework:** FastAPI (Python).
- **Service Layer:** Logic is encapsulated in services (`StrategyManager`, `BrokerService`, `StrategyRegistry`).
- **Concurrency:** Uses `multiprocessing` to run strategies in separate processes. This isolates the strategy execution from the API server, ensuring that a strategy crash doesn't take down the API, and vice-versa.
- **Communication:** Uses `multiprocessing.Queue` for inter-process communication (Strategy -> API) and WebSockets for real-time updates to the frontend.

### 2.2 Key Components
- **`app/services/strategy_manager.py`:** The core orchestrator.
    - Supports both single-strategy (legacy) and multi-strategy modes.
    - Manages the lifecycle (Start, Stop, Restart) of strategy processes.
    - Monitors strategy health and handles errors (e.g., stopping on "Insufficient Funds").
- **`app/routes/`:**
    - `strategy.py` & `strategy_selector.py`: REST endpoints for controlling strategies.
    - `market.py`: Proxy for market data (NIFTY quote).
- **WebSockets:**
    - `app/main.py` handles the WS endpoint `/ws`.
    - Broadcasts `STRATEGY_STATE`, `PRICE_UPDATE`, and `ORDER_UPDATE` events.

### 2.3 Security
- **Authentication:** Broker authentication is checked before starting a strategy (`router.post("/start")`).
- **Configuration:** Strategy configs are validated using Pydantic models in `StrategyRegistry`.

---

## 3. Frontend Review (`web/frontend/`)

### 3.1 Technology Stack
- **Framework:** Next.js (React).
- **Styling:** Tailwind CSS.
- **State Management:** React Hooks (`useWebSocket`) and local state.

### 3.2 Features (`app/page.tsx`)
- **Dashboard:**
    - Real-time status indicator (Running/Stopped).
    - Live NIFTY price ticker.
    - Total P&L and Margin utilization display.
    - Active Positions table with sorting.
- **Control:** Buttons to Start, Stop, and Restart the strategy.
- **Visualization:**
    - `PayoffChart`: Visualizes the P&L profile of current positions.
    - `GreeksDisplay`: Shows portfolio Greeks (Delta, Theta, etc.).
- **User Experience:**
    - Good error handling (alerts for errors, auth warnings).
    - "Sortable" tables for positions.
    - Connection status indicator for WebSocket.

---

## 4. Conclusion & Recommendations

### Strengths
1.  **Robust Architecture:** The separation of Strategy (Process) and API (Server) is excellent for stability.
2.  **Safety First:** The inclusion of `PositionManager` for SL reconciliation and startup checks (Broker Init) demonstrates a focus on reliability in live trading environments.
3.  **Extensible:** The `StrategyRegistry` and `StrategyManager` are designed to support multiple strategies easily.
4.  **Real-time:** The WebSocket integration provides a responsive user experience.

### Recommendations / Areas for Improvement
1.  **Testing:**
    - While `tests/` exist, ensure high coverage for `strategy/survivor.py` logic, especially the "filter" conditions and "gap" calculations. Mocking the broker responses is essential here.
2.  **Configuration Management:**
    - Currently, config seems to be mixed between YAML files and API overrides. Ensure there's a clear precedence rule (CLI/API > YAML > Defaults) and that the "active" config is always visible in the UI.
3.  **Error Recovery:**
    - The strategy loop has a generic `try...except Exception` block. While good for keeping the process alive, specific handling for "Network Disconnect" vs "Order Rejection" vs "Logic Error" could be improved to avoid infinite retry loops in fatal scenarios (though `StrategyManager` does catch some critical errors).
4.  **Broker Abstraction:**
    - The system is heavily tied to Zerodha (Kite). If multi-broker support is needed, the `BrokerGateway` and `PositionManager` (which currently has Zerodha-specific logic) would need further abstraction.

Overall, this is a well-engineered automated trading system with a strong focus on risk management and operational stability.
