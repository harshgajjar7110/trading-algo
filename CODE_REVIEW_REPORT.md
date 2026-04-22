# Code Review Report: Automated Trading Strategy

## 1. Strategy Folder
### `strategy/base_strategy.py`
- **Purpose**: Provides the `BaseStrategy` abstract class which consolidates common functionality like broker initialization, order management, position tracking, logging, and risk management (SL, profit targets) across different strategies.
- **Design & Logic**:
  - `StrategyConfig`: A robust dataclass encapsulating all necessary parameters for execution, dynamic gap adjustment, time settings, risk limits, technical indicators, and position sizing multipliers.
  - `BaseStrategy`: Initializes the unified logger, initializes instruments from the broker, sets up the reference PE/CE points based on current market ticks or configurations. Abstract methods like `initialize` and `on_tick` are forced for concrete implementations.
  - **Risk Logic**: Implements daily loss limit tracking, square off time checking, consecutive losses tracking, and limits on maximum positions.
- **Observations**: The code correctly segregates base behavior. The abstract design helps in ensuring that any new strategy enforces standard behaviors and logging natively without repetition.

### `strategy/gap_risk_manager.py`
- **Purpose**: Manages overnight gap risk. Evaluates day-of-the-week multipliers, pre-market signals (GIFT Nifty vs spot differences), and implements entry delay logic.
- **Logic Details**:
  - Sets up specific multipliers for each trading day (e.g., 0.5x on Mondays due to high risk, 0.4x on Friday).
  - Enforces a 10:00 AM entry delay on Mondays.
  - Uses `GAP_SKIP_THRESHOLD` (1.5%) and `GAP_REDUCE_THRESHOLD` (1.0%) to decide whether to block trades, reduce size, or proceed normally based on the overnight gap percentage.
- **Observations**: Excellent logic segregation for risk control. It prevents large overnight surprises from destroying the account and systematically decreases sizing based on temporal factors (weekend/expiry).

### `strategy/position_manager.py`
- **Purpose**: Tracks live positions and reconciles stop-loss (SL) orders specifically for the Survivor strategy on Zerodha without persisting states to disk.
- **Logic Details**:
  - Fetches fresh open positions from the broker on start-up.
  - Cross-references open "short" NFO margin positions against open/pending SL (Buy) orders from the broker.
  - Calculates unprotected quantities and immediately places correct SL-Limit or SL-M orders to cover any unhedged exposure.
  - Uses strictly in-memory mapping `_position_states` to prevent stale files from executing false entries.
- **Observations**: Critical for risk management. Avoiding file-persistence for live state is a robust architectural decision to prevent desynchronization between the local algo state and the actual broker account.

### `strategy/pre_market_data.py`
- **Purpose**: Fetches GIFT Nifty, US markets, and the Nifty previous close to calculate the pre-market gap sentiment.
- **Logic Details**:
  - Primarily depends on the `yfinance` library.
  - Caches data locally using `json` under a `cache` directory to prevent rate limiting and speed up start-up checks.
  - Uses changes in SGX Nifty and US indices to establish an overall sentiment (Bullish, Bearish, Neutral) and a trading recommendation (Trade, Reduce, Skip).
- **Observations**: Caching mechanism is implemented properly. Using a fallback strategy if `yfinance` is missing ensures the system doesn't crash completely.

### `strategy/survivor.py`
- **Purpose**: The core "Survivor" strategy. A trend-following, options selling strategy that captures theta decay by placing trades dynamically based on configured gaps from the index price.
- **Logic Details**:
  - Extends tracking logic for indicators (RSI, EMA, ADX, ATR).
  - Actively uses `PositionManager` at start-up to reconcile SLs, and handles trailing stop losses dynamically.
  - Checks if pre-market gaps block trading.
  - Dynamically calculates the strike distance using ATR if enabled (`enable_atr_strike_selection`), or falls back to fixed gaps.
  - **Trade execution**: On ticks, evaluates distance from reference points (`nifty_pe_last_value`, `nifty_ce_last_value`). Places short positions, logs trades to `daily_trades`, and updates reference values based on `pe_reset_gap` or `ce_reset_gap`.
  - Places linked SL orders directly after a fresh position is sold. Checks profit targets (default 60% of premium).
- **Observations**: Logic is extremely thorough. It successfully implements technical filters, dynamic gaps, explicit error handling, continuous SL trailing, profit booking, and WebSocket integration logic.

### `strategy/wave.py`
- **Purpose**: Another trading strategy that places simultaneous buy and sell limit orders based on predefined gaps, managing a "wave" style grid tracking.
- **Logic Details**:
  - Relies heavily on the `mibian` library for Greeks calculation (Delta) based on active portfolio components.
  - Enforces portfolio delta limits (e.g., `min_nifty_delta`, `max_nifty_delta`) to block/allow fresh buy or sell sides dynamically based on overall directional exposure.
  - Implements a multiplier scale (`_generate_multiplier_scale`) that scales gap requirements based on position imbalance (e.g., if already heavy on the buy side, stretches the buy gap further away).
- **Observations**: Highly mathematical approach to grid trading. Using `mibian` for live delta tracking ensures the strategy doesn't become overly skewed in a trending market.

## 2. Web Backend
### `web/backend/app/main.py`
- **Purpose**: Entry point for the FastAPI application. Provides API routes and real-time WebSocket capabilities.
- **Logic Details**:
  - Implements an `asynccontextmanager` `lifespan` for application startup/shutdown. Background tasks (`heartbeat_loop`, `state_broadcast_loop`) push the strategy state and Nifty prices to all connected clients over WebSocket every 1 second.
  - Includes routers for `strategy`, `config`, `positions`, `market`, `analysis`, `greeks`, `auth`, `monitoring`, and `visual`.
- **Observations**: Modern, async-first implementation. Clean separation of concerns with sub-routers and dedicated WebSocket managers.

### `web/backend/app/services/broker_service.py`
- **Purpose**: A facade handling communication with the underlying `BrokerGateway` instance (Zerodha, etc.).
- **Logic Details**:
  - Normalizes responses for the frontend (converting broker-specific structures to standard Pydantic models).
  - Fetches positions, calculates percentage PnL on the fly, fetches orders, trades, Nifty index data, and available account funds.
  - Includes a crucial error sanitation mechanism that checks for authentication failures (e.g., missing API keys) and gracefully returns an error schema instead of throwing 500 server errors.
- **Observations**: Clean facade pattern. Error handling ensures the frontend receives actionable error messages (like prompting for Re-Authentication) rather than raw tracebacks.

### `web/backend/app/routes/strategy.py`
- **Purpose**: API endpoints to control the main trading strategy (Start, Stop, Restart, Status).
- **Logic Details**:
  - Validates authentication state before allowing the strategy to start.
  - Uses the `strategy_manager` service, which spins up or stops the background `multiprocessing` worker.
- **Observations**: Concise and functional. The pre-flight auth check is a smart inclusion to prevent background process crashes due to invalid broker states.

## 3. Web Frontend
### `web/frontend/app/layout.tsx` & `web/frontend/app/page.tsx`
- **Purpose**: The main Next.js App Router layout and Dashboard UI.
- **Logic Details**:
  - Uses React hooks (`useState`, `useEffect`, `useCallback`) to manage state and fetch API data every 10 seconds.
  - Implements a custom `useWebSocket` hook to handle live `StrategyState` and `PriceUpdate` payloads efficiently without hammering the REST endpoints.
  - Renders interactive components for controlling the strategy (Start/Stop), viewing active positions (with sortable columns for symbol, quantity, price, PnL), tracking account funds, and rendering complex visual components (`VisualEngine`, `GreeksDisplay`, `PayoffChart`).
- **Observations**: Excellent React architecture. Combines polling (for robust state) with WebSockets (for real-time ticks). Clean UI separation utilizing TailwindCSS.

## Summary
The automated trading system is robust, modular, and built with modern frameworks (FastAPI for backend, Next.js for frontend, multiprocessing for strategy isolation). The risk management logic—particularly the in-memory SL reconciliation and the day-of-week gap evaluation—is exceptionally well-thought-out to protect against Black Swan events and persistent stale state issues. The separation of concerns between broker drivers, execution strategies, REST APIs, and background processes is exemplary.
