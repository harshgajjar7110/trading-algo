# Architectural & Code Review Report
## Automated Options Selling Trading System (Survivor & Wave)

### 1. Executive Summary

The project is an automated algorithmic trading system focused on selling options (NIFTY/Bank NIFTY) to profit from theta decay. It integrates with Indian brokers (primarily Zerodha, with Fyers/Fyrodha support) to execute dynamic, gap-based, and technical indicator-filtered strategies.

The system is split into two primary domains:
*   **Strategy Engine** (`strategy/`): Contains the core logic (`SurvivorStrategy`, `WaveStrategy`), risk management (`GapRiskManager`), and position tracking (`PositionManager`).
*   **Web Services** (`web/`): A FastAPI backend and Next.js frontend serving as the control interface, streaming market data, positions, and order states via WebSockets.

### 2. Strategy Engine Review

#### `strategy/survivor.py`
**Concept:** Sells OTM/ATM Put Options (PE) on market rise and Call Options (CE) on market fall, using gap thresholds to trigger entries.
**Key Features & Logic Review:**
*   **Technical Filters:** Implements RSI, EMA, and ADX correctly. Using Wilder's Smoothing for RSI (standard definition) instead of Simple Moving Average is financially accurate and mathematically sound.
*   **GTT & Stop-Loss:** Excellent usage of GTT (Good Till Triggered) OCO (One Cancels the Other) orders for robust risk management. Selling options carries infinite theoretical risk; using a fixed 30% SL to 60% Target OCO order is structurally solid.
*   **Execution Flow:** `on_ticks_update` listens to the broker websocket continuously. Latency is minimized by using `exchange_timestamp` rather than local machine time for building standard minute-bars.
*   **Strengths:** Safety loops (e.g., maximum retry limit for option instrument selection) prevent infinite execution errors when contracts expire or are unavailable.

#### `strategy/wave.py`
**Concept:** Places simultaneous buy and sell offset orders around the market price to scrape profits via theta decay and mean reversion.
**Key Features & Logic Review:**
*   **Greeks Pricing:** Uses the `mibian` library for delta limits and Black-Scholes calculations to find the appropriate strikes to sell. This provides a mathematical edge compared to purely point-based offset systems.
*   **Dynamic Gap Scaling:** Uses a multiplier scale `[1.3, 1.7, 2.5, 3, 10, ...]` based on position imbalances. This is a very aggressive, quasi-martingale approach that can lead to large drawdowns if the market trends strongly without reverting.
*   **Recommendation:** Enhance position sizing limits so that wave entries do not cascade into outsized margin requirements on strong trend days.

#### `strategy/position_manager.py` (Survivor Specific)
**Concept:** Handles in-memory mapping of open positions to SL orders and reconciles unprotected positions on startup.
**Key Features & Logic Review:**
*   **No Persistence Design:** Relying purely on the broker's API as the source of truth rather than a local file (JSON/DB) prevents "ghost positions" and state mismatch. This is a high-quality architectural decision for trading systems.
*   **Reconciliation:** Safely protects positions that were left open if the system crashed and rebooted.

#### `strategy/gap_risk_manager.py`
**Concept:** Prevents trading against severe overnight market gaps.
**Key Features & Logic Review:**
*   **Contextual Risk Scaling:** Scales down position sizes on high-risk days (Monday 0.5x, Friday 0.4x). Suspends trading entirely if overnight gaps exceed 1.5%.
*   **Pre-Market Parsing:** Integrates GIFT Nifty and US Market data to anticipate opening conditions.
*   **Strengths:** Very robust logic. Option selling strategies are historically blown up by overnight gaps. Suspending trades on >1.5% gaps is a life-saving mechanism.

### 3. Web & Infrastructure Review

#### Backend (`web/backend/app/main.py` & `websocket/manager.py`)
*   **Framework:** FastAPI correctly chosen for its asynchronous capability, vital for high-throughput market data handling.
*   **WebSocket Management:** `ConnectionManager` efficiently routes market ticks (`PRICE_UPDATE`), system state (`STRATEGY_STATE`), and order updates (`ORDER_UPDATE`) to the frontend.
*   **Process Isolation:** The `StrategyManager` utilizes Python's `multiprocessing` to isolate strategy execution from the API server. This is a critical architectural choice, as any blocking I/O on the web server will not delay the strategy's microsecond trade execution.
*   **Message Queues:** Incoming broker ticks are passed to the strategy process via a thread-safe main worker queue (`dispatcher.py`), safely crossing process boundaries.

#### Broker Integration (`brokers/integrations/zerodha/driver.py` & `core/gateway.py`)
*   **Abstraction Layer:** `BrokerGateway` ensures the system isn't tightly coupled to Zerodha. If a switch to Fyers is needed, the `Gateway` proxies the requests natively.
*   **Sanitization:** Catching and redacting API keys from log exceptions prevents catastrophic security leaks during system crashes.

### 4. Areas for Improvement & Potential Risks

1.  **Testing Environment Missing Scipy:** During test suite execution (`uv run pytest tests/`), `scipy` was missing for `greeks_calculator.py`. Ensure that `web/backend/requirements.txt` and `pyproject.toml` are perfectly synchronized.
2.  **Wave Strategy Margin Risk:** As noted, `wave.py` uses scaling multipliers. Implement an absolute hard stop or margin limit check before blindly issuing the 4th/5th tier multiplier orders.
3.  **Fail-over Mechanism:** If the WebSocket to Zerodha disconnects, the system currently handles reconnection gracefully, but ensure that in-flight GTT OCO orders are verified upon reconnection to prevent double-ordering.
4.  **yfinance Rate Limits:** `pre_market_data.py` relies on `yfinance` as a fallback. For production, relying on an unauthenticated free API like Yahoo Finance is risky due to rate limits or IP bans. A paid API for SGX/GIFT Nifty is highly recommended.

### 5. Conclusion
The codebase is exceptionally well-structured for a retail algorithmic trading system. The separation of concerns between process-isolated strategy execution, an in-memory position reconciliation model, and a responsive async API makes it highly resilient. The mathematical modeling in technical filters and the strict gap-risk management are strong indicators of a professional-grade automated trading setup.