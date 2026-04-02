# Code Review Report: Automated Trading Strategy System

## 1. Introduction
This document provides a comprehensive code review of the Python-based automated trading strategy system, specifically focusing on the `web` folder (FastAPI backend and Next.js frontend context) and the `strategy` folder (Survivor and Wave strategies).
The system primarily uses Zerodha as a broker to sell options and capture theta decay.

## 2. Strategy Directory (`strategy/`)

### 2.1. `base_strategy.py`
- **Purpose**: Provides `BaseStrategy`, an abstract base class for all trading strategies, handling common functionalities such as broker interaction, order tracking, position management, logging setup, risk management (SL, profit targets), and configuration parsing (`StrategyConfig`).
- **Strengths**:
  - Standardizes strategy parameters through a dataclass `StrategyConfig`.
  - Implements boilerplate methods for broker calls (`place_order`, `get_positions`, etc.) to enforce consistency.
- **Review & Suggestions**:
  - `is_max_loss_reached()` implementation correctly calculates percentage against `starting_capital`. However, `starting_capital` defaults to `0.0` and its initialization is missing or happens outside the class. If `starting_capital` remains `0.0`, the max loss check returns `False`, effectively disabling this crucial risk mechanism.
  - Subclasses must implement `initialize()` and `on_tick()`, enforcing a robust inheritance contract via Python's `abc` module.

### 2.2. `survivor.py`
- **Purpose**: Implements the main "Survivor" trend-following option selling strategy. Sells PE on market rise, and CE on market fall, capturing theta decay. Includes technical indicator filters (RSI, EMA, ADX), dynamic gaps (ATR), position sizing based on volatility, and automatic broker-level Stop Loss (SL) order placement.
- **Strengths**:
  - Very comprehensive risk management. Combines technical filter checks (`_check_entry_filters`), gap risk (`_check_gap_risk`), dynamic position limits, and trailing SL capabilities.
  - The integration of `PositionManager` to run SL reconciliation at startup (`reconcile_sl_at_startup`) is robust, ensuring open positions left from unexpected crashes are protected.
  - Uses `TechnicalIndicators` dataclass effectively to clean up parameter passing.
- **Review & Suggestions**:
  - **Refactor `__init__`**: Dynamically adding configuration variables as attributes (`setattr(self, f"strat_var_{k}", v)`) makes static typing and IDE introspection impossible. It's better to strictly use the `StrategyConfig` object initialized in the base class.
  - **`_nifty_quote` call frequency**: It continuously calls the broker API for NIFTY quotes, which might hit rate limits depending on the frequency of `on_ticks_update`. Passing the underlying's tick data if available from the stream could optimize this.
  - **Order Tracking Dependency**: SL order placement logic assumes Zerodha returns order IDs reliably. If the broker is slow, there's a minor race condition where the strategy expects the SL order ID immediately. It currently handles this gracefully, but an async confirmation might be safer.
  - **Bug risk in SL order type**: The strategy relies on `ProductType.MARGIN`. Zerodha typically uses `ProductType.NRML` or `ProductType.MIS` for F&O. Fyers/Fyrodha might use `MARGIN`. This needs to be consistent.

### 2.3. `wave.py`
- **Purpose**: A wave trading strategy that places simultaneous buy and sell limit orders at dynamic gaps from the market price.
- **Strengths**:
  - Uses the `mibian` library for precise Option Greeks calculation to dynamically adjust order restrictions based on portfolio Delta.
  - Generates dynamic gaps based on position imbalance (`_generate_multiplier_scale`).
- **Review & Suggestions**:
  - The implementation of `handle_order_update` tracks order updates using a boolean flag dictionary (`handle_order_update_call_tracker`). This feels a bit brittle compared to a state machine approach for each order.
  - There are multiple commented-out lines and `TODO` tags (e.g., `TODO: Variety Supported is only Regular for now`, `TODO: Check this with Vibhu`). These should be resolved to reduce technical debt.
  - The `place_wave_order` has sleep statements (`time.sleep(3)`) which will block the entire strategy execution if run in the main thread. It's acceptable since strategies run in isolated processes via `StrategyManager`, but an async implementation would be more scalable.
  - `_execute_orders` places dependent orders. If a buy order fails, it immediately cancels the associated sell order. This is a good safety net.

### 2.4. `position_manager.py`
- **Purpose**: Handles detection of open positions at startup and reconciles SL orders to ensure protection. Specifically tailored for Survivor Strategy on Zerodha.
- **Strengths**:
  - Correctly avoids disk persistence for positions. Fetching directly from the broker prevents stale data issues—a major architectural advantage.
  - Gracefully handles partial protection (`protected_qty`, `unprotected_qty`), meaning it doesn't blindly place duplicate SL orders if the user manually placed one.
- **Review & Suggestions**:
  - Hardcoded for Survivor (`TransactionType.BUY` for shorts, NFO MARGIN product). It would be beneficial to make it generic so other strategies can utilize it for SL reconciliation.

### 2.5. `gap_risk_manager.py`
- **Purpose**: Evaluates gap risk to adjust position sizing or suspend trading (e.g., skipping Monday pre-10 AM, reducing size on large gaps).
- **Strengths**:
  - Clear `DayRiskProfile` logic. Great implementation of day-based multipliers.
  - Effectively calculates gap between `overnight_gap_percent` and `gift_nifty_gap`.
- **Review & Suggestions**:
  - Uses a global instance `gap_risk_manager = GapRiskManager()`. Since the system uses `multiprocessing` for strategies, a global instance will be instantiated separately per process. This is fine, but caching mechanism within it should be aware it's not shared globally across the backend.

## 3. Web Backend Directory (`web/backend/app/`)

### 3.1. Routes (`routes/`)
- **`strategy.py` & `strategy_selector.py`**: Clean FastAPI endpoints. Effectively handles starting, stopping, getting status, and previewing strategies. Uses standard Pydantic models. Good error handling around broker authentication pre-flight checks.
- **`market.py` & `positions.py`**: Good use of `live_data_manager` when streaming is active to reduce API load, with a fallback to direct `broker_service` calls when not streaming.
- **`greeks.py` & `analysis.py`**: Robust. Good implementation of scenario analysis and portfolio payoffs. The `parse_option_symbol` regex handling in `analysis.py` covers both weekly and monthly expiries gracefully.

### 3.2. Services (`services/`)
- **`strategy_manager.py`**:
  - **Purpose**: Orchestrates strategy processes. Uses Python's `multiprocessing` module.
  - **Strengths**: Uses isolated processes for strategies, preventing a crash in a strategy from taking down the FastAPI backend. Implements communication via `multiprocessing.Queue` (`state_queue`), which is robust.
  - **Review & Suggestions**:
    - The `_stop_on_error` gracefully handles critical errors like "insufficient funds".
    - `live_data_manager.start_stream()` is called here. Note that WebSocket streams are notoriously tricky when crossing process boundaries, but here the streaming happens in the FastAPI backend, and data is pushed to a `DataDispatcher` Queue which the Strategy process consumes. This is a very solid architecture.

## 4. Overall Architecture and Safety Measures
1. **Safety Measures**: Strong focus on risk mitigation. `sl_reconcile_on_start`, `max_daily_loss_percent`, `max_consecutive_losses`, and dynamic `GapRiskManager` provide excellent downside protection for option selling strategies.
2. **Event-Driven**: The strategy's `on_ticks_update` provides low latency reaction to market data, supported by the `multiprocessing.Queue` dispatch pattern.
3. **Broker Abstraction**: `BrokerGateway` is used effectively to mask the underlying broker driver (Zerodha, Fyers, etc.), making the system broker-agnostic.

## 5. Summary & Recommendations
The system is exceptionally well-architected for automated trading. It isolates risks, avoids stale local file states for critical positions, and features comprehensive risk management logic explicitly built for Option Selling.

**Immediate Action Items**:
1. Remove `TODO` comments in `wave.py` by finalizing the logic.
2. Review the use of `ProductType.MARGIN` vs `ProductType.NRML` depending on the broker integration to avoid unexpected order rejections.
3. In `base_strategy.py`, explicitly initialize `starting_capital` by fetching account balances via `broker.get_funds()` at startup to ensure `is_max_loss_reached()` functions properly.
