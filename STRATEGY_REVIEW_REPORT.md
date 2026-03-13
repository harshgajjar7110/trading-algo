# Strategy Code Review Report

This report provides a detailed code review of the Python-based automated trading strategy, specifically focusing on the `strategy` and `web` directories. The strategy is designed to sell options on the NIFTY index to generate profit through theta decay, utilizing the Zerodha broker integration.

## 1. `strategy/` Directory Review

### 1.1 `strategy/survivor.py`
The `SurvivorStrategy` is the core trend-following options selling strategy. It incorporates several advanced features:
- **Technical Indicators**: Calculates RSI, EMA, ADX, and ATR using historical data from the broker.
- **Entry Logic**: Sells PE on market rise (using `pe_gap` and `pe_reset_gap`) and CE on market fall (using `ce_gap` and `ce_reset_gap`). It dynamically adjusts gap thresholds based on ATR (VolatilityRegime).
- **Risk Management**:
  - Automatically places broker-level SL orders immediately after opening a position.
  - Implements trailing stop-loss logic.
  - Supports time-based exits (`square_off_time`).
  - Implements daily loss limits (`max_daily_loss_percent`).
- **Profit Booking**: Periodically checks if a target profit percentage (e.g., 60%) is hit and automatically exits the position.
- **SL Reconciliation**: At startup, it fetches open positions and ensures they have active SL orders, protecting against system restarts or crashes.

**Observations & Review Points:**
- **Robustness**: The strategy handles order tracking and SL order placements very explicitly, catching errors and tracking them in `order_tracker`.
- **Initialization**: The `initialize_positions_from_broker` method fetches existing positions to track profit booking correctly, even if SL is disabled. It appropriately ignores non-short or non-NFO positions.
- **Duplicate Prevention**: The code checks `self.positions` before placing new trades to prevent duplicate order placement for the same symbol.
- **Dynamic Gaps**: The ATR-based dynamic gap adjustments (`_get_dynamic_gap` and `_get_atr_strike_distance`) correctly widen gaps in high volatility, adjusting risk.
- **Indicator Calculations**: `_calculate_rsi`, `_calculate_ema`, and `_calculate_atr` are standard implementations. However, `_calculate_rsi` uses standard Wilders smoothing, avoiding SMA.
- **Filter Checks**: The `_check_entry_filters` logic gracefully handles rejection reporting.
- **Error Handling**: Uses proper exception catching during order execution and logs them cleanly without crashing the main loop.
- **Potential Issues**:
  - `strike_difference` calculation in `_get_strike_difference` assumes the top 2 sorted strikes represent the standard step. If there are irregular strikes, it might compute an incorrect difference. It correctly falls back to returning 50.
  - In `_place_order_enhanced`, `self.last_profit_check_time` is reset so new positions are checked immediately. This is a clever fix to ensure PT is tracked immediately.

### 1.2 `strategy/base_strategy.py`
This module provides the `BaseStrategy` abstract class and `StrategyConfig` dataclass.
- **Structure**: It successfully abstracts common initialization (instrument downloading, logger setup, state tracking) from specific strategy logic.
- **Configuration**: Uses a comprehensive `StrategyConfig` dataclass with `from_dict`, standardizing configurations across strategies.
- **Utility Methods**: Includes generic methods for order placement (`place_market_order`, `place_limit_order`, `place_sl_order`), position management, and risk checks (`is_max_loss_reached`).

**Observations & Review Points:**
- **Code Reuse**: Excellent separation of concerns. Inheriting strategies only need to implement `initialize()` and `on_tick()`.
- **Order Handling**: The `place_order` method correctly translates strategy-level requests into `BrokerGateway` `OrderRequest` objects.
- **Note**: `SurvivorStrategy` currently implements its own initializations heavily, though it seems it migrated away from `BaseStrategy` or uses a different pattern. The `BaseStrategy` is well-written for general use.

### 1.3 `strategy/gap_risk_manager.py`
The `GapRiskManager` adjusts position sizes and trading permissions based on overnight gaps and the day of the week.
- **Rules**: Reduces positions on Mondays (0.5x) and Fridays (0.4x). Suspends trading if pre-market gaps exceed 1.5%. Delays Monday entry until 10:00 AM.
- **Integration**: Accessed via a global singleton (`get_gap_risk_manager()`).

**Observations & Review Points:**
- **Risk Mitigation**: This is a very prudent risk management layer, especially for options selling where overnight gap risk is substantial.
- **Clear Logic**: The `assess_gap_risk` and `can_trade_now` functions have explicit, easy-to-follow logic.

### 1.4 `strategy/position_manager.py`
The `PositionManager` focuses on SL Order Reconciliation, specifically for the Survivor strategy on Zerodha.
- **In-Memory Tracking**: Explicitly avoids persisting state to disk to prevent stale data issues.
- **Reconciliation**: On startup, it matches broker positions against open orders to detect unprotected positions and places missing SL orders.

**Observations & Review Points:**
- **State Management**: The decision to rely entirely on fresh broker data rather than a local file state is a very robust design choice for live trading systems.
- **Reconciliation Logic**: `_calculate_protected_quantities` correctly sums up quantities of active `BUY` orders to determine if a short position is fully protected.
- **Broker Specificity**: The documentation notes it's designed for Zerodha. The logic assumes SL orders are regular `STOP_LIMIT` or `STOP` orders.

## 2. `web/` Directory Review (Backend)

The backend is built with FastAPI and manages strategy execution, broker connections, and API endpoints for the frontend.

### 2.1 `app/services/strategy_manager.py`
Manages the lifecycle of trading strategies (start, stop, monitor) using Python's `multiprocessing`.
- **Process Isolation**: Strategy instances run in isolated processes (`_run_strategy_process`). The manager uses queues for state updates.
- **Multi-Strategy Support**: Supports running different strategies via a registry, though default is 'survivor'.
- **Monitoring**: `_monitor_state` runs as an asyncio task, pulling from a multiprocessing queue. Updates are broadcasted and Telegram notifications are dispatched for significant events (Gap Risk, Errors, Stops).

**Observations & Review Points:**
- **Architecture**: The use of `multiprocessing.Process` ensures that the heavy computation and synchronous blocking calls of the strategy do not block the asynchronous FastAPI event loop.
- **Shutdown**: Uses a robust `_stop_on_error` implementation, attempting a clean `terminate()` before escalating to `kill()`.
- **Initialization Flow**: It specifically calls `reconcile_sl_at_startup`, `initialize_positions_from_broker`, and gap risk assessment when firing up the strategy process.
- **Concurrency Issue**: `get_visual_state` utilizes `_visual_state_cache`. Since `visual_state` isn't fully synchronized across processes, the manager constructs a basic version or retrieves the cache.

### 2.2 `app/services/broker_service.py`
Provides an abstraction layer around broker operations.
- **Features**: Wraps `get_positions`, `get_orders`, `get_trades`, `get_quote`, and `get_funds`.
- **Caching**: The broker instance is lazily initialized via `_ensure_broker()`.

**Observations & Review Points:**
- **Error Handling**: `get_positions` has specific handling to detect unauthenticated states (by checking funds if positions are empty) and returning a clean `PositionsResponse` with an error message instead of raising an exception. This is good for UI stability.
- **Position PnL Calculation**: Calculates `pnl_percent` based on the value of the short position (`average_price * abs(quantity_total)`), which is accurate for options selling.
- **Reinitialization**: A clean `reinitialize()` method allows the system to reload broker credentials without restarting the server.

### 2.3 API Routes

#### `app/routes/strategy.py` & `strategy_selector.py`
Handles strategy lifecycle API requests.
- **Status Checks**: Includes pre-flight checks before starting a strategy to ensure the broker is authenticated (`broker.get_funds()`).
- **Validation**: `preview_strategy_config` runs config validation through the registry before execution.

#### `app/routes/market.py`
Provides quotes and funds data.
- **Integration**: The `/nifty` endpoint cleverly injects `nifty_pe_last_value` and `nifty_ce_last_value` from the `strategy_manager` state, allowing the frontend to see the distance to trigger.

#### `app/routes/positions.py`
Handles positions, orders, and trades.
- **Live Stream Integration**: Prioritizes `live_data_manager.get_positions()` if streaming is active; otherwise falls back to polling `broker_service.get_positions()`.

#### `app/routes/analysis.py` (Payoff Analysis)
Generates payoff curves for open option positions.
- **Parsing**: `parse_option_symbol` extracts strike and type from NIFTY, BANKNIFTY, FINNIFTY, etc.
- **Calculation**: Computes Max Profit/Loss and Breakeven points. Since options are sold (Short Strangle), it accurately accounts for limited profit (premium) and unlimited loss.
- **Curve Generation**: Generates PnL points ±10% around the current NIFTY price for graphing.

#### `app/routes/greeks.py`
Calculates option Greeks (Delta, Gamma, Theta, Vega, Rho) using the Black-Scholes model.
- **Implementation**: Uses `scipy.stats.norm` in `PortfolioGreeksCalculator` for precise Greek computations.
- **Scenario Analysis**: The `/scenario` endpoint estimates P&L changes across a matrix of price movements (-5% to +5%) and volatility shifts.

#### `app/routes/auth.py`
Handles authentication flows for Zerodha and Fyers.
- **Token Management**: Writes access tokens directly to the `.env` file (`_save_access_token_to_env`) and reinitializes the broker service. This allows persistent sessions across restarts without requiring a database.
- **Multi-Broker**: Checks `BROKER_NAME` to switch between `_handle_zerodha_callback` and `_handle_fyers_callback`.

#### `app/routes/visual.py` & `monitoring.py`
Provide telemetry and visual state data for the frontend dashboard.
- **Health Checks**: Checks stream status, strategy state, and broker connectivity.
- **Session Tracking**: Integrates with `local_monitor` to log win rates, PnL, and active trades.

## Conclusion

The architecture of this trading strategy and its web backend is very sound and well-structured for an automated options selling bot.
1. **Separation of Concerns**: The separation between the API (FastAPI async layer) and the Trading Logic (Synchronous multiprocessing) is an excellent design choice.
2. **Risk First**: Risk management is heavily prioritized. Automatic SL placement, SL reconciliation on startup, Gap Risk checks, and profit taking are all core components, which is vital for options selling.
3. **Black-Scholes Integration**: The inclusion of a custom Greeks calculator and Payoff Analysis shows a high degree of mathematical rigor tailored to options trading.
4. **Broker Resilience**: Catching unauthenticated states gracefully prevents server crashes and alerts the UI appropriately.

**Suggested Improvements:**
- The `strategy/survivor.py` is quite large. Moving the specific filter logic or trade execution functions into helper classes could improve maintainability.
- Parsing option symbols (seen in `analysis.py` and `greeks_calculator.py`) involves complex regex that is duplicated in a few places. A single `OptionSymbolParser` utility could centralize this logic.
