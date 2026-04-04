# Comprehensive Code Review Report: Automated Trading Strategy

This report provides a detailed code review of the Python-based automated trading strategy, specifically focusing on its use of the Zerodha broker and its core mechanism of options selling to generate profit via theta decay.

The review is divided into two primary sections: the `strategy` folder (containing the core trading logic) and the `web` folder (containing the backend management and services).

## Part 1: Strategy Logic (`strategy/`)

This section reviews the core algorithmic trading logic, focusing on how positions are taken, risk is managed, and theta decay is captured.

### 1. `survivor.py` (SurvivorStrategy)

**Overview:** This is the primary options selling strategy. It's a trend-following strategy designed to sell Call Options (CE) on downtrends and Put Options (PE) on uptrends, capturing premium (theta decay).

**Strengths:**
*   **Robust Theta Decay Focus:** The strategy explicitly sells options (`TransactionType.SELL`) and waits for the premium to decay. The profit target feature (`profit_target_percent`, typically 60%) is a standard and effective way to book profits on short options before gamma risk increases near expiry.
*   **Comprehensive Entry Filters:** The inclusion of RSI, EMA, and ADX filters (`_check_entry_filters`) is excellent. It prevents selling PE (bullish) when the market is overbought (RSI > max) or in a downtrend (EMA), and vice-versa for CE.
*   **Dynamic Risk Management:** The `dynamic_gap` calculation based on the Volatility Regime (using ATR) is a sophisticated approach. Widening gaps in high volatility prevents premature entries during whipsaws.
*   **Extensive SL Mechanisms:** The strategy employs multiple layers of stop-loss protection:
    *   Broker-level SL (`_place_sl_order`): Crucial for protecting against sudden spikes, especially important for option sellers.
    *   Trailing SL (`_check_enhanced_trailing_sl`): Locks in profits as theta decays.
    *   Reconciliation (`reconcile_sl_at_startup`): A strong safety net to ensure positions aren't left naked if the algorithm restarts.
*   **Position Sizing:** `_get_position_size` correctly reduces position sizes in high volatility or based on the day of the week (e.g., lower on Fridays due to weekend gap risk).

**Areas for Improvement / Potential Issues:**
*   **Market Order Usage (`OrderType.MARKET` in `_place_order_enhanced`):** Selling options using market orders can lead to significant slippage, particularly in illiquid strikes. It is highly recommended to use Limit orders (`OrderType.LIMIT`) with a price slightly favorable or equal to the current bid to ensure a minimum premium is collected.
*   **Strike Selection Logic:** `_get_strike_difference` assumes the top 2 CE strikes sorted by strike price will give the interval. While usually true, it might be safer to hardcode intervals for known indices (e.g., 50 for NIFTY, 100 for BANKNIFTY) or calculate the mode of differences across the chain.
*   **ATR Calculation Limitation:** The `_calculate_atr` method is somewhat simplified. True ATR requires previous close data, which it attempts to use, but depends heavily on the structure of `high_low_close` dictionary.

### 2. `wave.py` (WaveStrategy)

**Overview:** A trend-following strategy that places simultaneous buy and sell orders offset by configured gaps. It manages delta exposure to keep the portfolio relatively neutral or within defined bounds.

**Strengths:**
*   **Delta Hedging/Management:** The `_get_portfolio_greeks` method using the `mibian` library is a standout feature. It calculates the aggregate delta of the portfolio (futures + options) and restricts new trades if the delta exceeds configured minimum/maximum bounds (`min_nifty_delta`, `max_nifty_delta`). This is advanced risk management for a portfolio of short options.
*   **Dynamic Gap Scaling:** `_get_scaled_gaps` increases the gap for subsequent orders if the position becomes unbalanced (e.g., if you are very long, the buy gap increases significantly, making further buys less likely).
*   **Margin Calculation:** It attempts to estimate margin requirements (`calculate_margin_requirement`) based on spreads and naked positions, which is useful for preventing order rejections due to insufficient funds.

**Areas for Improvement / Potential Issues:**
*   **Complexity in Order Management:** The logic surrounding `check_and_enforce_restrictions_on_active_orders` and `handle_order_update` is highly complex and relies on tracking associated orders. This state management is prone to race conditions if WebSocket updates arrive out of order or are missed.
*   **Hardcoded Values:** Values in `_generate_multiplier_scale` (e.g., `[1.3, 1.7, 2.5...]`) and `calculate_margin_requirement` (dividing by 75) are hardcoded. These should ideally be configurable parameters.

### 3. `gap_risk_manager.py` (GapRiskManager)

**Overview:** Manages overnight gap risk, a primary concern for option sellers.

**Strengths:**
*   **Multi-factor Assessment:** Considers both the actual overnight gap and indicators like GIFT Nifty (`assess_gap_risk`).
*   **Day-of-Week Multipliers:** Applying different risk profiles based on the day (e.g., `Friday: 0.4` for weekend risk, `Monday: 0.5` for settlement volatility) is a very sound practice for options selling.
*   **Monday Entry Delay:** Waiting until 10:00 AM on Mondays (`can_trade_now`) avoids the often erratic opening volatility as weekend news is digested.

### 4. `position_manager.py` (PositionManager)

**Overview:** Specifically designed to reconcile Stop-Loss orders at startup for the Survivor strategy on Zerodha.

**Strengths:**
*   **Stateless Design:** Explicitly avoiding file persistence for positions and relying entirely on fresh broker data (`_fetch_broker_positions`) is the correct architectural choice to prevent issues with stale state after crashes or manual interventions.
*   **Reconciliation Logic:** `_calculate_protected_quantities` accurately matches open BUY orders (SLs for short options) against open short positions to determine if a position is naked and needs a new SL order.

**Areas for Improvement:**
*   **Broker Specificity:** The module explicitly mentions it is for Zerodha only. While functional, abstracting the order status checks might make it easier to support other brokers in the future.

### 5. `base_strategy.py` (BaseStrategy)

**Overview:** Provides common functionality for strategies.

**Strengths:**
*   **Standardization:** Enforces a common configuration structure (`StrategyConfig`) and interface (`initialize`, `on_tick`).
*   **Unified Broker Access:** Provides wrapper methods for order placement (`place_order`, `place_sl_order`) that handle tagging and basic error logging.

## Part 2: Backend Services (`web/backend/app/services/`)

This section reviews the services responsible for managing strategy execution, broker communication, and data processing.

### 1. `strategy_manager.py` (StrategyManager)

**Overview:** Manages the lifecycle (start, stop, monitor) of strategies, running them in isolated processes.

**Strengths:**
*   **Process Isolation:** Using `multiprocessing.Process` (`_run_strategy_process`) to run strategies is excellent. It ensures that a crash in the strategy logic does not take down the main FastAPI backend, providing high availability for the UI/API.
*   **Inter-Process Communication (IPC):** Uses `multiprocessing.Queue` (`_state_queue`) effectively to send state updates, error messages, and risk alerts from the isolated strategy process back to the main manager for monitoring and alerting.
*   **Comprehensive Notifications:** Integrates closely with `telegram_notifier` to alert the user of critical events like strategy starts/stops, gap risks, and insufficient funds errors (`_stop_on_error`).

**Areas for Improvement / Potential Issues:**
*   **Process Termination:** While `_stop_on_error` and `stop` attempt graceful termination (`terminate()` then `join(timeout)`), killing a process abruptly might leave background threads (like broker websockets within the strategy) in an undefined state. Ensuring the strategy itself has a shutdown hook would be cleaner.

### 2. `broker_service.py` (BrokerService)

**Overview:** Acts as a facade/wrapper over the underlying `BrokerGateway`, providing standardized responses to API routes.

**Strengths:**
*   **Standardized Schemas:** Maps raw broker responses (which might vary depending on the underlying driver, e.g., Zerodha vs. Fyers) to standardized internal Pydantic schemas (`Position`, `Order`, `Quote`). This abstracts broker-specific details away from the API layer.
*   **Error Handling and Graceful Degradation:** Catches exceptions during API calls (e.g., `get_positions`) and returns structured error messages or empty lists instead of crashing, ensuring the UI remains functional even if the broker API drops.
*   **PnL Calculation:** Implements custom logic to calculate PnL percentages specifically tailored for options selling margins (`pnl_percent = (pnl / position_value) * 100`).

### 3. `greeks_calculator.py` (BlackScholesGreeks & PortfolioGreeksCalculator)

**Overview:** Implements the Black-Scholes model to calculate Option Greeks (Delta, Gamma, Theta, Vega, Rho).

**Strengths:**
*   **In-House Calculation:** Calculating Greeks internally using `scipy.stats.norm` rather than relying entirely on broker APIs ensures Greeks are always available for risk management (like in the Wave strategy), even if the broker doesn't provide them reliably.
*   **Robust Symbol Parsing:** `parse_option_symbol` handles a wide variety of NSE/NFO symbol formats (monthly, weekly, 3-digit and 4-digit numeric dates) to extract strike, type, and expiry accurately, which is essential for accurate time-to-expiry (T) calculations.
*   **Portfolio Aggregation:** `PortfolioGreeksCalculator` correctly aggregates Greeks across multiple positions, allowing the strategy to assess total portfolio risk (e.g., total Delta exposure).
*   **Scenario Analysis:** The `get_scenario_analysis` method is a powerful tool for estimating PnL changes based on potential shifts in price, time (theta), and volatility.

### 4. `live_data_manager.py` (LiveDataManager)

**Overview:** Manages the continuous polling/streaming of positions and orders to feed the frontend via WebSockets.

**Strengths:**
*   **In-Memory Storage:** Strictly adheres to the requirement of not persisting this fast-moving data to disk, using async locks (`_lock`) to update in-memory dictionaries safely.
*   **Background Task:** Uses `asyncio.create_task` to run the polling loop (`_stream_loop`) continuously without blocking the main event loop.
*   **WebSocket Broadcasting:** Integrates with `connection_manager` to push updates (`POSITIONS_UPDATE`, `ORDERS_UPDATE`) to connected clients in real-time.

### 5. `strategy_registry.py` (StrategyRegistry)

**Overview:** Implements a registry pattern to manage available strategies and their configurations.

**Strengths:**
*   **Dynamic Loading:** Uses `importlib` in `load_strategy_class` to dynamically load strategy classes based on configuration. This makes adding new strategies easy without modifying core manager code.
*   **Validation:** Provides `validate_config` to ensure required parameters (like gap values and quantities being positive) are correct before a strategy is allowed to start.

## Conclusion

The architecture is robust and well-thought-out for automated options selling.

**Key Highlights:**
1.  **Risk Management First:** The system heavily emphasizes risk management with features like Gap Risk Assessment, ATR-based dynamic gaps, multiple Stop-Loss layers, and Delta monitoring.
2.  **Process Isolation:** Running strategies in isolated processes ensures the backend API remains stable.
3.  **Stateless Design:** Relying on live broker data rather than state files for positions prevents synchronization issues and false order generation.

**Main Recommendations:**
1.  **Order Types:** Consider migrating from Market Orders to Limit Orders for options selling to minimize slippage, which can significantly eat into theta profits.
2.  **Hardcoded Values:** Extract hardcoded scaling factors and margin divisors (seen in `wave.py`) into the YAML configuration files.
