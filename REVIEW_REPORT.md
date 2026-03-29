# Comprehensive Code Review Report: Survivor Trading Strategy & Web Backend

## 1. Architecture Overview

The system is designed as a hybrid algorithmic trading platform consisting of:

1.  **Strategy Layer (`strategy/`)**: Standalone Python classes implementing trading logic (`SurvivorStrategy`, `WaveStrategy`). These strategies interact with a broker via a `BrokerGateway` abstraction.
2.  **Web Backend (`web/backend/`)**: A FastAPI application that manages the lifecycle of strategy processes (`StrategyManager`), provides REST APIs for frontend interaction, and handles real-time data via WebSockets.
3.  **Frontend (`web/frontend/`)**: A Next.js application providing a dashboard for monitoring, configuration, and control.
4.  **Broker Integration (`brokers/`)**: A facade pattern (`BrokerGateway`) abstracting broker-specific implementations (Zerodha, Fyers).

### Assessment
*   **Strengths**:
    *   **Separation of Concerns**: Good decoupling between strategy logic, API service, and broker implementation.
    *   **Process Isolation**: Strategies run in separate processes (`multiprocessing`), preventing strategy crashes from bringing down the API server.
    *   **Real-time Capabilities**: Extensive use of WebSockets for tick data and state updates.
*   **Weaknesses**:
    *   **State Persistence**: Strategy state (e.g., `nifty_pe_last_value`) is largely in-memory. While `orders.py` and `position_manager.py` have some persistence, a crash of the strategy process might lose critical context like "consecutive losses" or "daily PnL" for risk limits.
    *   **Complexity**: The interaction between `StrategyManager`, `StrategyRegistry`, and the running strategy process involves queue-based communication which can be fragile if not handled robustly (e.g., queue full scenarios, serialization errors).

---

## 2. Strategy Analysis (`strategy/`)

### `SurvivorStrategy` (`strategy/survivor.py`)

**Logic Correctness:**
*   **Trend Following**: The core logic uses a reference price (`nifty_pe_last_value`) that shifts as the market moves, attempting to capture trends by selling options away from the trend (selling PE on rise). This is a standard "trailing entry" mechanism.
*   **Strike Selection**: `_find_nifty_symbol_from_gap` correctly calculates target strikes. The fallback to `lot_size` adjustment if premium is too low is a good heuristic.
*   **Gap Logic**: The `pe_gap` / `ce_gap` logic with dynamic adjustment based on ATR (`_get_dynamic_gap`) is sound for adapting to volatility.

**Risk Management:**
*   **Stop Loss**: The strategy implements both "Broker SL" (placing SL orders immediately) and "Manual SL" (monitoring price).
    *   *Critique*: The `_check_stop_losses` method iterates over positions but relies on `self.broker.get_quote` inside the loop. For many positions, this might hit rate limits or introduce latency.
*   **Position Limits**: `_check_position_limits` correctly checks `max_positions_per_side` and `max_consecutive_losses`.
*   **Capital Protection**: `_check_daily_loss_limit` squares off all positions if the loss limit is hit.

**Theta Decay Optimization:**
*   The strategy sells options (Credit strategy).
*   **Entry Filters**: The use of RSI, EMA, and ADX (`_check_entry_filters`) helps avoid entering trades against strong momentum or in low-volatility chop, which is crucial for theta decay strategies.

**Code Quality & Issues:**
*   **Infinite Loop Risk**: The `while True` loops in `_handle_pe_trade` (lines 400+) and `_handle_ce_trade` rely on finding an instrument. If `temp_gap` adjustment fails to find a valid instrument eventually, this could hang. *Recommendation: Add a max retry counter.*
*   **Blocking Calls**: `time.sleep` usage in main loops (implied or explicit) or synchronous broker calls can block tick processing.
*   **Hardcoded Values**: Some values like `tick_size = 0.05` are hardcoded. While standard for NIFTY, it reduces portability.

### `PositionManager` (`strategy/position_manager.py`)

*   **Purpose**: Reconciles SL orders at startup. This is critical for system reliability after restarts.
*   **Implementation**: It fetches open orders and positions, matches them, and places missing SL orders.
*   **Assessment**: This is a robust feature. It correctly identifies "unprotected" positions.

### `WaveStrategy` (`strategy/wave.py`)

*   **Logic**: A different approach using "waves" (buy/sell gaps).
*   **Observation**: It seems to be a separate, perhaps experimental, strategy. It shares the same `BrokerGateway` but has its own logic.
*   **Issue**: `time.sleep(self.cool_off_time)` inside `_prepare_final_prices` blocks the strategy execution thread, which might delay reaction to market moves.

---

## 3. Web Backend Analysis (`web/backend/`)

### API Design (`app/routes/`)
*   **FastAPI**: Good choice for performance and async support.
*   **Endpoints**: Comprehensive endpoints for Status, Config, Positions, Orders, and Auth.
*   **Strategy Control**: `/api/strategy/start`, `/stop` endpoints effectively manage the background process.

### Process Management (`app/services/strategy_manager.py`)
*   **Multiprocessing**: Uses `multiprocessing.Process` to run the strategy.
*   **State Sync**: Uses a `multiprocessing.Queue` (`_state_queue`) to send updates from the strategy process back to the API.
    *   *Risk*: If the queue fills up or the API doesn't consume it fast enough, the strategy might block (though `put_nowait` or timeouts usually mitigate this).
    *   *Error Handling**: Captures exceptions in the strategy process and sends `ERROR` messages to the queue.

### Authentication (`app/routes/auth.py`)
*   **Env File Manipulation**: `_save_access_token_to_env` modifies the `.env` file at runtime.
    *   *Critique*: Modifying source/config files at runtime is generally bad practice (race conditions, file permissions, Docker container immutability).
    *   *Recommendation*: Use a database (SQLite/Redis) or a dedicated writable config file for session tokens.

### Greeks Calculation (`app/services/greeks_calculator.py`)
*   **Implementation**: Standard Black-Scholes implementation.
*   **Performance**: Calculates Greeks for the portfolio on demand.
*   **Symbol Parsing**: Regex-based parsing for NIFTY/BANKNIFTY symbols is comprehensive but fragile to broker symbol format changes.

---

## 4. Recommendations

### Critical Improvements
1.  **Fix Runtime Config Modification**: Stop writing tokens to `.env`. Use a `session.json` or a lightweight DB.
2.  **Safety Limits**: Add a `max_retries` counter to the `while True` loops in `SurvivorStrategy` (strike selection) to prevent infinite loops.
3.  **Async Broker Calls**: Ensure strategy loops don't block on synchronous network calls if high-frequency tick processing is required.

### Code Quality
1.  **Type Hinting**: While present, it can be more consistent.
2.  **Logging**: Ensure logs are rotated to prevent disk fill-up (`logger.py` review needed, though not explicitly requested, implied by usage).
3.  **Testing**: Add unit tests for the complex `_check_entry_filters` and `Greeks` calculation logic.

### Feature Enhancements
1.  **Backtesting**: The current setup runs live/paper. Integrating a backtesting engine (like `backtrader` or custom) using the same strategy logic would be valuable.
2.  **Dashboard**: The frontend looks comprehensive, but ensure the WebSocket connection auto-reconnects robustly (handled in `useWebSocket`).

---

## 5. Conclusion

The system is a well-structured, sophisticated trading bot. It moves beyond simple scripts by adding a robust web management layer and process isolation. The `SurvivorStrategy` logic is financially sound (trend-following option selling with risk checks). The main risks are technical implementation details regarding state persistence and runtime file modification.
