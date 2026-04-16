# Code Review Report: NIFTY Gap Handling Automated Trading Strategy

This report contains a detailed code review of the Python-based automated trading strategy, encompassing the core strategy logic, the FastAPI backend, and the Next.js frontend. The Sensibull folder has been explicitly excluded from this review.

## 1. Strategy Logic (`strategy/` directory)

The `strategy/` directory forms the core execution logic of the trading bot. It primarily contains the `Survivor` and `Wave` strategies, along with essential risk management modules.

### 1.1 `strategy/survivor.py`
**Overview:** A trend-following options selling strategy on NIFTY index that sells Puts (PE) on rise and Calls (CE) on fall to capture theta decay.

**Strengths:**
* **Comprehensive Risk Management:** Implements multiple layers of risk control including a Gap Risk Manager (day-of-week multipliers, pre-market gap checks), max daily loss limits, consecutive loss tracking, and position limits.
* **Technical Filters:** Uses RSI, EMA, and ADX for entry filtering to avoid selling into strong adverse trends or overbought/oversold extremes.
* **Volatility Adjustment:** Dynamically scales gaps and strike distances using ATR (Average True Range) and reduces position sizes in high volatility regimes.
* **Robust SL Handling:** Automatically places broker-level Stop Loss (SL) orders immediately after fresh orders. Fallback manual SL monitoring is in place if the broker order is cancelled.
* **Trailing SL & Profit Targets:** Built-in trailing SL activation and configurable profit booking (e.g., 60% of premium).
* **Startup Reconciliation:** Integrates well with `PositionManager` to reconcile active positions with pending SL orders upon system restarts.

**Areas for Improvement / Risks:**
* **Blocking/Abrupt Exits:** In `_place_order()`, if order placement fails (`order_id == -1`), it calls `exit()`. This forcefully kills the strategy process without proper teardown or notification. It should raise an exception or gracefully stop instead.
* **Polling Interval:** The `_check_profit_targets()` runs on a periodic polling interval (default 300s / 5 mins). In fast-moving options markets, a 5-minute delay could mean missing the exact profit target. Using Limit orders at the broker level for profit booking might be safer.
* **Indicator Warm-up:** The `_load_historical_data()` pulls a chunk of 5m candles. However, the indicators (like EMA) update based on live tick price without fully tracking tick-level close properly (just appending `current_price` to `self.price_history`).

### 1.2 `strategy/wave.py`
**Overview:** The Wave strategy places simultaneous buy and sell limit orders offset by `buy_gap` and `sell_gap` thresholds, aiming to capture intraday oscillations.

**Strengths:**
* **Dynamic Scaling:** Adjusts gaps and multipliers dynamically based on the current net position (`current_diff_scale`) to avoid accumulating too much on one side.
* **Delta Neutrality Checks:** Uses the `mibian` library to calculate live portfolio Greeks (Delta) based on Black-Scholes. Restricts new buy/sell orders if the total portfolio delta breaches the defined `min_nifty_delta` and `max_nifty_delta` thresholds.
* **Order Tracking:** Maintains robust tracking of associated limit orders, cancelling the counterpart when one executes.

**Areas for Improvement / Risks:**
* **Blocking Sleeps:** Uses `time.sleep(self.cool_off_time)` inside `_prepare_final_prices` and `time.sleep(3)` in the main execution flow. This blocks the entire thread, preventing WebSocket ticks and order updates from being processed concurrently. An async approach or event-driven timer would be significantly better.
* **Dictionary Deletion during Iteration:** In `check_and_enforce_restrictions_on_active_orders`, it safely uses `list(self.orders.items())` to avoid `RuntimeError`, which is good. However, state variables like `sell_price` and `buy_price` are overwritten in loops without clear disambiguation if multiple orders exist.

### 1.3 `strategy/position_manager.py`
**Overview:** Reconciles Stop Loss orders for the Survivor strategy at startup.

**Strengths:**
* **In-Memory Source of Truth:** Wisely avoids file persistence for position states. Fetching live positions and open orders directly from the broker avoids stale data and false duplicate orders.
* **Intelligent Order Matching:** Matches active broker positions with pending BUY orders to calculate "unprotected" quantities. Places new SL orders only for the delta.
* **Clean Design:** Separation of concerns is excellent here.

**Areas for Improvement / Risks:**
* **Broker Coupling:** Contains hardcoded checks specifically for `Exchange.NFO` and `ProductType.MARGIN`. If the strategy ever expands to equities or intraday (`MIS`), this logic will need refactoring.

### 1.4 `strategy/gap_risk_manager.py`
**Overview:** Implements rules to protect against overnight gap risks.

**Strengths:**
* **Nuanced Day-of-Week Rules:** Adjusts position sizes (e.g., Monday 0.5x, Friday 0.4x) and implements a hard rule to wait until 10:00 AM on Mondays for gap settlement.
* **Thresholds:** Clear logic to SKIP trading if the gap > 1.5% and REDUCE if > 1.0%.

**Areas for Improvement:**
* Hardcoding the rules makes them slightly less configurable, but the logic is sound.

### 1.5 `strategy/pre_market_data.py`
**Overview:** Service to fetch overnight market data (GIFT Nifty, US Markets, Asian Markets).

**Strengths:**
* **Caching:** Implements a time-based cache (15 mins) and file persistence to prevent API rate limiting.
* **Graceful Fallback:** Catches exceptions and returns neutral data instead of crashing the strategy if the data provider (e.g., `yfinance`) is down.

**Areas for Improvement:**
* `yfinance` is slow and sometimes unreliable for production systems. Depending on it for live trading decisions without a solid institutional data backup could lead to missed market context.

## 2. Backend Architecture (`web/backend/` directory)

The backend is built with **FastAPI** (Python 3.11+) and is responsible for managing strategy execution, broker connections, API routing, and WebSocket streaming to the frontend.

### 2.1 FastAPI Application (`app/main.py`)
**Overview:** Entry point for the FastAPI server defining routes, middleware, and the application lifecycle.

**Strengths:**
* **Lifespan Management:** Uses modern `asynccontextmanager` for clean startup and teardown of background tasks (heartbeat, state broadcast) and stopping the strategy process safely.
* **CORS & Modularity:** Cleanly imports and mounts routers via `app.include_router()`. CORS is configured appropriately.
* **WebSocket Integration:** Exposes a single `/ws` endpoint that acts as a central hub for all real-time messaging, using a dedicated `connection_manager`.

**Areas for Improvement:**
* **Error Handling:** In `websocket_endpoint`, a generic `Exception` is caught and the connection is disconnected. Logging the stack trace explicitly would be helpful for debugging unexpected disconnects.

### 2.2 Strategy Manager (`app/services/strategy_manager.py`)
**Overview:** A crucial service utilizing Python's `multiprocessing` library to run trading strategies in isolated background processes.

**Strengths:**
* **Process Isolation:** Running the strategy via `multiprocessing.Process` ensures that blocking I/O or heavy computation in the trading loop does not block the FastAPI asyncio event loop.
* **Inter-process Communication (IPC):** Uses `multiprocessing.Queue` to stream strategy state updates back to the main API process seamlessly.
* **Unified Registry:** Supports both single strategy (legacy backward compatible) and multi-strategy modes, dynamically loading strategy classes via `StrategyRegistry`.
* **State Caching:** Maintains a robust cache of the current visual state for the dashboard, refreshing it via the IPC queue.

**Areas for Improvement / Risks:**
* **Process Termination:** The `stop()` method attempts a graceful `.terminate()`, waits 10 seconds, then uses `.kill()`. However, abrupt process termination might leave broker WebSockets open or orphan background threads initiated within the strategy (like the `dispatcher` thread).

### 2.3 Broker Service (`app/services/broker_service.py`)
**Overview:** A facade that wraps broker driver operations (e.g., Zerodha) and handles API error states gracefully.

**Strengths:**
* **Singleton Pattern:** Uses `__new__` to ensure only one instance handles broker credentials across the app.
* **Robust Error Handling:** Methods like `get_positions`, `get_orders`, and `get_trades` catch exceptions and return empty responses rather than causing HTTP 500s. It specifically catches authentication errors and surfaces user-friendly messages.
* **PnL Calculation:** Centralizes the math for extracting real PnL vs percentage PnL based on position value.

### 2.4 Live Data Manager (`app/services/live_data_manager.py`)
**Overview:** Streams active positions and orders continuously from the broker to connected WebSockets.

**Strengths:**
* **In-Memory Volatility:** Explicitly avoids writing live data to disk, drastically reducing I/O bottlenecks and ensuring no stale state persists across restarts.
* **Async Concurrency:** The streaming loop (`_stream_loop`) runs continuously using `asyncio.sleep()`, yielding control back to the event loop.
* **Backoff & Resilience:** Tracks consecutive error counts (`_error_count`) and stops the stream if errors exceed `_max_errors`, preventing infinite crash loops against rate-limited broker APIs.

**Areas for Improvement:**
* Currently updates via a polling mechanism (`await asyncio.sleep(1)`) reading from `get_positions()` and `get_orderbook()`. While safe, a purely WebSocket-driven approach from the broker for order updates might be more efficient, though harder to normalize.

## 3. Frontend Architecture (`web/frontend/` directory)

The frontend is built with **Next.js 14** using the App Router, **React**, **Tailwind CSS**, and **Recharts**. It provides a real-time dashboard for strategy monitoring and visual analytics.

### 3.1 WebSocket Hook (`hooks/useWebSocket.ts`)
**Overview:** A custom React hook managing the WebSocket connection to the FastAPI backend.

**Strengths:**
* **Resilience:** Implements auto-reconnection logic (`reconnectInterval`, `maxReconnectAttempts`) gracefully handling temporary network drops.
* **Ref-based Callbacks:** Uses `useRef` for callbacks (like `onMessage`, `onPriceUpdate`) to ensure the WebSocket event listeners always have the latest closure scope without triggering unnecessary re-renders or reconnection loops.
* **Clean Cleanup:** Properly cleans up the connection (`ws.close()`) and timeouts when the component unmounts (`isUnmountedRef`).

### 3.2 Visual Components
**Overview:** A suite of React components for visualizing portfolio health and strategy parameters.

#### `GreeksDisplay.tsx`
**Strengths:**
* **Clear Hierarchy:** Breaks down portfolio-level Greeks (Delta, Gamma, Theta, Vega, Rho) and then provides a detailed per-position breakdown in a table.
* **Color Coding:** Utilizes dynamic Tailwind classes (`getGreekColor`) to visually warn users of high risk (e.g., Red for absolute Delta > 100).
* **Auto-refresh:** Implements a sensible `setInterval` (default 30s) to keep Greek values updated.

#### `PayoffChart.tsx`
**Strengths:**
* **Recharts Integration:** Effectively uses `ComposedChart` from Recharts to overlay individual position P&L lines and a combined P&L area chart.
* **Interactive Filtering:** Allows toggling individual positions on/off via `selectedPositions` Set, dynamically recalculating the combined P&L curve (`prepareChartData`).
* **Multi-view:** Provides flexible viewing options ('chart', 'table', 'both') which is great for UX.

#### `StrategySelector.tsx`
**Strengths:**
* **Confirmation Flow:** Excellent UI flow that prevents accidental strategy starts by forcing a review of differences between default and current configurations.
* **Rich State Management:** Handles multiple loading states (`initialLoading`, `loading`) and surfaces clear success/error messages.

**Areas for Improvement:**
* **Error Handling across Components:** If backend services (like `greeksAPI.getPortfolioGreeks()`) return a 500, the error state is shown as text. Adding a generalized error boundary could catch unexpected rendering errors.
