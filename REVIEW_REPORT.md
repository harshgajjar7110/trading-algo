# Automated Trading Strategy Code Review Report
*Generated on: 2026-03-25 10:29:10*

## Overview
This report provides a detailed code review of the automated trading strategy system, specifically focusing on the `web` and `strategy` folders, while strictly excluding the `sensibull` directory. The system integrates with the Zerodha broker and implements option selling strategies (like Survivor and Wave) to profit from theta decay.

### Scope of Review
- **Strategy Components**: Review of core trading algorithms, position sizing, risk management, and indicator calculations.
- **Backend Infrastructure**: Review of FastAPI services, WebSocket communication, strategy management, and broker integration.
- **Code Quality**: Analysis of complexity, potential bugs, security issues (like hardcoded secrets), and adherence to best practices.

## 1. Strategy Module Review (`strategy/`)

### 1.1 Survivor Strategy (`strategy/survivor.py`)
**Purpose**: Trend-following options selling strategy (sells PE on rise, CE on fall) based on NIFTY index movements to capture theta decay.
**Architecture**: Large class (`SurvivorStrategy`) with 3194 lines of code, managing everything from indicator calculation to order execution and risk management.
**Key Findings & Recommendations**:
- **Complexity**: The file is monolithic. Several methods exhibit high cyclomatic complexity (e.g., `initialize_positions_from_broker`, `_calculate_entry_predictions`, `_check_stop_losses`). Consider refactoring by extracting responsibilities (like indicator calculation, signal generation, and order execution) into separate classes or modules.
- **Logging vs. Print**: There is a `print` statement at line 280. All standard output should be routed through the configured logger for consistency and observability in production.
- **State Management**: The strategy relies heavily on in-memory state. While `PositionManager` handles SL reconciliation, ensure robust handling of connection drops to prevent state desynchronization with the broker.
- **Error Handling**: Deeply nested `try-except` blocks are present. Ensure exceptions are granular and don't silently swallow critical trading errors.

### 1.2 Wave Strategy (`strategy/wave.py`)
**Purpose**: Places simultaneous buy and sell orders offset from the market price by configured thresholds, utilizing the Black-Scholes model for options pricing via the `mibian` library.
**Key Findings & Recommendations**:
- **Logging vs. Print**: Significant use of `print` statements (over 60 instances found between lines 1096-1313). This is a critical code smell for production trading systems. Replace all `print` calls with appropriate `logger.info()`, `logger.debug()`, or `logger.error()` calls.
- **Complexity**: The `check_and_enforce_restrictions_on_active_orders` method has a very high complexity score (~39). This logic is difficult to test and maintain. It should be broken down into smaller, composable rule-check functions.
- **Dynamic Strategy Loading**: The code contains hardcoded references to symbol classes (`nifty`, `banknifty`). Consider making these configurable parameters rather than hardcoded string checks.

### 1.3 Risk Management (`strategy/position_manager.py`, `strategy/gap_risk_manager.py`)
**Purpose**: Handles startup SL order reconciliation (`PositionManager`) and pre-market gap analysis (`GapRiskManager`).
**Key Findings & Recommendations**:
- **Clean Implementation**: These modules are well-separated from the main strategy logic, which is good practice.
- **PositionManager**: Designed specifically for Zerodha to track SL order IDs without file persistence. Ensure it gracefully handles cases where order IDs are lost or modified directly in the broker UI.
- **GapRiskManager**: Effectively adjusts position sizing based on the day of the week and pre-market gap limits. Consider adding unit tests specifically for edge cases (e.g., exact boundary values of gap percentages).

### 1.4 Core Framework (`strategy/base_strategy.py`)
**Purpose**: Provides the `BaseStrategy` abstract class to consolidate common functionality.
**Key Findings & Recommendations**:
- **Extensibility**: Good use of inheritance. Ensure all new strategies adhere to this contract.
- **Configuration Management**: The `StrategyConfig` class provides a structured way to handle settings. Consider using Pydantic for robust validation of these configurations before initialization.

## 2. Backend Module Review (`web/backend/`)

### 2.1 Overall Architecture (`web/backend/app/main.py`, `web/backend/app/config.py`)
**Design**: FastAPI application serving as the control plane for strategies, providing REST APIs and WebSocket streams for the frontend.
**Key Findings**:
- **Configuration**: Uses `BaseSettings` for environment variables, which is standard and secure.
- **Service Registry**: The `StrategyRegistry` effectively manages dynamic loading of strategy classes based on configuration.

### 2.2 WebSocket Communication (`web/backend/app/websocket/manager.py`)
**Purpose**: Manages real-time data streaming (prices, orders, state) to connected clients.
**Key Findings**:
- **Implementation**: Solid connection manager implementation.
- **Scalability**: Currently relies on in-memory connection tracking. If scaling to multiple backend instances, consider a Redis Pub/Sub backplane to distribute WebSocket messages.

### 2.3 Strategy Execution (`web/backend/app/services/strategy_manager.py`)
**Purpose**: Manages the lifecycle (start, stop, restart) of strategy instances using Python's `multiprocessing` library.
**Key Findings & Recommendations**:
- **Process Isolation**: Using isolated processes for strategies prevents a crash in the trading logic from bringing down the FastAPI server. This is an excellent design choice.
- **Complexity**: The `_run_strategy_process` method is quite complex (~29). Managing inter-process communication (IPC) and signal handling is tricky. Ensure zombie processes are handled correctly during unexpected shutdowns.
- **State Synchronization**: State updates from the strategy process back to the main API application must be carefully serialized and de-serialized.

### 2.4 Broker Service (`web/backend/app/services/broker_service.py`)
**Purpose**: Facade for interacting with underlying broker drivers (e.g., `ZerodhaDriver`).
**Key Findings & Recommendations**:
- **Error Handling**: The `_sanitize_error` method is crucial for security (redacting API keys). Ensure it covers all edge cases of exceptions thrown by the underlying drivers.
- **Resilience**: Methods like `get_positions` catch exceptions and return empty responses to maintain stability. While this prevents crashes, ensure appropriate logging and alerts are triggered so operational issues aren't silently ignored.

### 2.5 Analytics (`web/backend/app/services/greeks_calculator.py`)
**Purpose**: Calculates option Greeks using the Black-Scholes model via `scipy`.
**Key Findings**:
- **Logic**: Implements `_bs_price`, `_bs_vega`, etc. Parsing option symbols (`parse_option_symbol`) has moderate complexity due to various expiry formats.
- **Optimization**: If analyzing large portfolios frequently, consider vectorizing these calculations using `numpy` arrays instead of iterating over individual positions.

## 3. General Recommendations & Action Plan
1. **Remove Print Statements**: Immediately replace all `print` calls in `strategy/wave.py` and `strategy/survivor.py` with proper logging. This is crucial for production monitoring.
2. **Refactor Complex Methods**: Prioritize refactoring `wave.py` (`check_and_enforce_restrictions_on_active_orders`) and `survivor.py` (`_calculate_entry_predictions`, `initialize_positions_from_broker`) to reduce cyclomatic complexity. This will drastically improve maintainability and testability.
3. **Code Duplication**: Notice that `parse_option_symbol` exists in both `greeks_calculator.py` and `routes/analysis.py`. Centralize this utility function to avoid inconsistent parsing logic.
4. **Testing**: Given the financial risk involved, ensure high test coverage, especially for the complex logic identified above. Use `pytest-mock` to mock broker responses and test edge cases like partial fills or API timeouts.
5. **Type Hinting**: Enforce strict type hinting across the codebase (using `mypy`) to catch potential bugs early, especially when dealing with nested dictionaries returned by broker APIs.