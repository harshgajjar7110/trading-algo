# Comprehensive Review Report: Survivor Automated Trading Algorithm

This document outlines a detailed review of the Python-based automated trading system, with a specific focus on the FastAPI backend (`web/backend`) and the Trading Strategies (`strategy`). The system aims to generate profit via theta decay by executing options selling strategies using the Zerodha broker.

*Note: As per instructions, the `sensibull` folder was completely excluded from this review.*

---

## 1. Architectural Overview & Strengths

The architecture cleanly decouples the frontend (Next.js) from the backend (FastAPI + Python Multiprocessing), maintaining strategy processes in isolation.

### 1.1 Backend (`web/backend`)
*   **FastAPI & Uvicorn Setup:** Well-structured API using the `lifespan` context manager for startup/shutdown events (`app/main.py`), replacing the deprecated `@app.on_event`.
*   **WebSocket Integration:** Strong implementation (`websocket/manager.py`) for streaming live ticks (`PRICE_UPDATE`), position updates, and strategy states (`STRATEGY_STATE`). Heartbeat mechanisms ensure connection vitality.
*   **Strategy Manager:** (`app/services/strategy_manager.py`) Uses `multiprocessing` to isolate strategy execution from the API thread, which prevents blocking operations from degrading API performance. Support for legacy single-strategy and new multi-strategy formats provides flexibility.
*   **Broker Gateway:** The `BrokerService` (`app/services/broker_service.py`) correctly handles error sanitization, removing tokens and API keys before logging, and gracefully catches driver exceptions to ensure the backend does not crash.
*   **Strategy Registry:** (`app/services/strategy_registry.py`) Provides dynamic loading of strategies using `importlib`, enabling easy extensibility for new algorithms without cluttering the main API logic.

### 1.2 Strategy Layer (`strategy`)
*   **Base Strategy Abstraction:** `base_strategy.py` consolidates order tracking, configuration parsing, and utility functions nicely.
*   **Survivor Strategy (`survivor.py`):** The core strategy is robust. Key features include:
    *   **GTT OCO Integration:** Automates risk management by placing 30% Stop-Loss and 60% Take-Profit GTT orders. It wisely places orders *before* writing to the local trade logger.
    *   **Indicator Logic:** Correctly implements Wilder's Smoothing for RSI rather than using a Simple Moving Average.
    *   **Retry Mechanisms:** Incorporates max retry loops (default 20) during instrument selection to avoid infinite hanging when scraping lists.
    *   **Timestamp Handling:** Uses `exchange_timestamp` from ticks for accurate bar building instead of local machine time.
*   **Position Manager (`position_manager.py`):** Outstanding feature that handles "SL Order Reconciliation" on system startup. It prevents orphan positions by checking broker open positions and re-attaching missing GTT SL orders.
*   **Pre-Market & Gap Risk (`gap_risk_manager.py`, `pre_market_data.py`):** Excellent implementation to suspend trading or reduce position sizing (e.g., Mondays) based on GIFT Nifty opening gaps > 1.5%.

---

## 2. Identified Issues & Vulnerabilities

### 2.1 Code Quality & Duplication
*   **Duplicate Dataclasses:** Both `base_strategy.py` and `survivor.py` appear to redefine `PositionInfo` or similar dataclasses, causing duplicate code violations (detected via Pylint).
*   **Line Lengths:** Multiple PEP8 warnings (`E501`) regarding line lengths exceeding 79/96 characters in `strategy/__init__.py`, `base_strategy.py`, and `survivor.py`.
*   **Unused Imports:** `base_strategy.py` contains several unused imports (e.g., `os`, `sys`, `timedelta`, `Tuple`, `Callable`).

### 2.2 Broker Driver Inconsistencies
*   **Timestamp Format:** The `BrokerGateway.get_history` method expects `YYYY-MM-DD` strings, but depending on the upstream calls, full datetime timestamps might still be passed, triggering `"unconverted data remains"` errors.
*   **Missing 'oi' Handling:** While the `ZerodhaDriver` explicitly checks the `oi` (Open Interest) parameter in historical responses, other broker drivers (Fyers, Fyrodha) must adhere strictly to the signature `(symbol, interval, start, end, oi=False)` to prevent strategy crashes when switching brokers.

### 2.3 Trading Logic Risks
*   **Multiprocessing Queue Congestion:** The `LiveDataManager` and `StrategyManager` use `multiprocessing.Queue` to dispatch ticks. During periods of extremely high volatility, if the strategy execution loop (`on_ticks_update`) becomes slower than the tick ingestion rate, the queue could bloat, causing latency in execution.
*   **Greeks Calculation Overhead:** Delta/Gamma/Theta are calculated using `scipy`/`mibian` locally. Running this on every single tick for multiple strikes might overwhelm the CPU. Consider throttling greek calculations to 1-second or 5-second intervals.
*   **Hardcoded "Zerodha-Only" Mentions:** The `PositionManager` states it is specifically designed for Zerodha. If a user switches to Fyers (which the project supports), the Stop-Loss reconciliation might silently fail or behave unpredictably.

---

## 3. Recommended Improvements

### 3.1 Refactoring & Cleanup
1.  **Consolidate Models:** Move `PositionInfo`, `TrendBias`, `VolatilityRegime`, and `StrategyConfig` into `web/backend/app/models/` or a dedicated `strategy/models.py` to eliminate duplication between `base_strategy.py`, `survivor.py`, and the backend.
2.  **Linting Fixes:** Run `black` or `autopep8` across the `strategy` and `web/backend` directories to resolve spacing, line-length, and unused import warnings.

### 3.2 Performance & Execution
1.  **Asynchronous Ticks:** Shift the `on_ticks` handler within the strategy process to use `asyncio.Queue` (if the process runs an async loop) to handle tick dispatching more efficiently.
2.  **Greeks Throttling:** Introduce a timestamp check in `_get_portfolio_greeks` to only recalculate Greeks every 2-3 seconds rather than on every raw tick.
3.  **Graceful Degration of Pre-Market Data:** Ensure that if `yfinance` fails to fetch the GIFT Nifty data (e.g., network timeout, Yahoo API change), the strategy doesn't block indefinitely but falls back to a safe default (e.g., halving position size).

### 3.3 Broker Integration
1.  **Abstract SL Reconciliation:** Update `PositionManager` to query the `BrokerGateway` for supported features (e.g., `broker.supports_gtt()`). If the broker (like Fyers) uses standard stop-loss orders instead of GTT OCO, the reconciliation logic should adapt automatically.
2.  **Strong Typing for Dates:** Use Python's `datetime.date` strictly for `get_history` API bounds and format internally within the broker driver, ensuring no string-parsing errors leak to the strategy layer.

---

## 4. Conclusion

The codebase is highly mature for an algorithmic trading system. The integration of FASTAPI websockets with separate multiprocessing strategy workers provides an excellent, non-blocking user experience on the Next.js frontend.

The advanced risk-management features—specifically the Gap Risk assessment and Stop-Loss Reconciliation—demonstrate a deep understanding of live trading edge cases (like system reboots and overnight gaps). Addressing the minor code duplications and ensuring cross-broker compatibility in the `PositionManager` will elevate the system to institutional-grade stability.
