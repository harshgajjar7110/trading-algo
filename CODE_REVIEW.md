# Code Review Report: Automated Trading Strategy

## 1. Overview
This codebase implements an automated options-selling trading strategy written in Python, primarily leveraging the Zerodha broker API. Its main goal is to generate profit from Theta decay by systematically selling Call (CE) and Put (PE) options. The system comprises a FastAPI backend for configuration/execution, a frontend (assumed Next.js based on previous context, outside this review scope), and core trading logic encapsulated in the `strategy/` directory.

## 2. Architecture & Design

### 2.1 Backend Services
- **`BrokerService` (`broker_service.py`)**: Acts as a robust abstraction layer over the broker APIs (Zerodha, Fyers, etc.). It standardizes data schemas (e.g., `Quote`, `Position`, `Order`) and handles API errors via sanitized logging to prevent exposing sensitive tokens.
- **`StrategyManager` (`strategy_manager.py`)**: Responsible for process management. Crucially, it uses Python's `multiprocessing` to isolate trading strategies from the main API loop. This ensures that synchronous broker operations or heavy computations do not block the web server. It supports both single and multi-strategy deployments through a registry pattern (`strategy_registry.py`).
- **`LiveDataManager` (`live_data_manager.py`)**: Maintains an in-memory representation of positions and orders, fed via WebSocket streams. This prevents excessive polling to the broker REST API and ensures low-latency state representation on the dashboard.
- **`GreeksCalculator` (`greeks_calculator.py`)**: Implements the Black-Scholes model utilizing `scipy` to compute Option Greeks (Delta, Gamma, Theta, Vega, Rho). Crucial for dynamic risk monitoring (like portfolio delta neutralization in the Wave strategy).

### 2.2 Routing & WebSocket
- **Routes (`strategy.py`, `market.py`, `positions.py`)**: Provide clean, RESTful interfaces. `positions.py` correctly toggles between fetching from `LiveDataManager` (when the strategy is active) and direct broker calls (when inactive), optimizing resource usage.
- **WebSocket Manager (`manager.py`)**: Broadcasts real-time events (`StrategyState`, quotes, position updates) to the Next.js frontend, enabling a reactive UI. It implements a basic publish/subscribe model per symbol.

## 3. Trading Strategies Implementation

### 3.1 Base Strategy (`base_strategy.py`)
Provides a strong foundation `BaseStrategy` class that handles boilerplate initialization, configuration management via `StrategyConfig`, and logging. It mandates standard interfaces (e.g., `on_tick`, `initialize`), enforcing consistency across specific implementations.

### 3.2 Survivor Strategy (`survivor.py`)
- **Logic**: An advanced trend-following option selling algorithm. It evaluates market conditions to sell PEs on market rise and CEs on market fall.
- **Filters**: Employs technical indicators (RSI, EMA, ADX) to avoid entering trades in extreme conditions. (e.g., preventing selling PEs when RSI > `rsi_max`).
- **Execution Flow**: Operates sequentially on ticks (`on_ticks_update`). It calculates dynamic gaps adjusted by ATR (Average True Range) to account for market volatility.
- **Order Placement**: Focuses on placing fresh short orders followed immediately by GTT OCO (Good Till Triggered - One Cancels Other) limit orders for automated Stop Loss (SL) and Profit Booking (PT).

### 3.3 Wave Strategy (`wave.py`)
- **Logic**: Operates on a "scraper" model, placing simultaneous buy and sell offset orders around the market price.
- **Delta Management**: Uses `mibian` library for Greek calculations to actively monitor and restrict orders if the portfolio Delta exceeds predefined thresholds (`min_nifty_delta`, `max_nifty_delta`), ensuring a delta-neutral stance.

## 4. Risk Management Modules

### 4.1 Position Manager (`position_manager.py`)
A highly specialized component that handles the reconciliation of open positions with active Stop Loss orders, particularly at strategy startup. If the system restarts, it queries the broker, detects unprotected open positions, and automatically places missing SL orders. This is a critical safety feature for a distributed/restarting system.

### 4.2 Gap Risk Manager (`gap_risk_manager.py`)
Evaluates pre-market gaps (e.g., using GIFT Nifty via `yfinance` in `pre_market_data.py`). If the overnight gap exceeds configured thresholds (e.g., 1.5%), it suspends trading or reduces position sizing to protect capital from extreme overnight volatility. It also includes day-of-week multipliers (e.g., reducing sizing on Monday and Friday).

## 5. Potential Improvements & Vulnerabilities

### 5.1 Code Deduplication
- **Greek Calculations**: `wave.py` uses `mibian` for Greeks calculation internally, while the backend has a dedicated `GreeksCalculator` using `scipy.stats`. Consolidating all Greek calculations to use the backend `GreeksCalculator` would improve consistency and reduce dependency weight.

### 5.2 Error Handling & Resilience
- **GTT Placement Reliability**: While the `SurvivorStrategy` places GTT orders immediately after execution, a network failure between the fresh execution and the GTT request could leave a position unprotected. The `PositionManager` reconciliation helps on restart, but an active, continuous background reconciliation loop (checking if `len(positions) == len(gtt_orders)`) would be safer.
- **Time/Date Management**: Broker APIs (Zerodha) are notoriously strict regarding Date formats (e.g., requiring `YYYY-MM-DD` instead of full timestamps). Ensure all `get_history` calls strictly cast datetime objects to the correct string representation to avoid `unconverted data remains` exceptions.

### 5.3 Asynchronous vs. Multiprocessing
- The `StrategyManager` uses `multiprocessing` to isolate strategies. However, inside those isolated processes, strategies like `wave.py` use synchronous broker calls. If network latency spikes, the entire tick processing loop in that process blocks, potentially causing the strategy to miss subsequent ticks. Transitioning the core broker drivers to fully `async` implementations (e.g., using `aiohttp` for REST calls within the strategy processes) would drastically improve throughput.

## 6. Conclusion
The codebase is well-structured, modular, and incorporates robust risk-management paradigms essential for automated option selling. The separation of the API service layer from the strategy execution processes via `multiprocessing` is an excellent architectural choice. Focusing on consolidating duplicate logic (Greeks) and transitioning to asynchronous broker execution within the strategy loops will elevate the system to institutional-grade reliability.