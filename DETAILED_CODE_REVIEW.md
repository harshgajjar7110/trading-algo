# In-Depth Code Review: Options Selling Strategy (Theta Decay) via Zerodha

## 1. Executive Summary
This project is a sophisticated algorithmic trading system designed for **options selling (capturing Theta decay)** primarily on the NIFTY index. The architecture leverages a **FastAPI backend** for routing, strategy orchestration, and real-time monitoring via WebSockets, with a **React/Next.js frontend** (not reviewed here as per instructions). Strategy execution runs in isolated Python `multiprocessing` processes to prevent blocking the async API loop.

The strategy relies heavily on the **Zerodha Kite API** (abstracted behind a `BrokerGateway`), particularly utilizing **GTT OCO (Good Till Triggered - One Cancels Other)** orders for seamless target and stop-loss placement.

---

## 2. Review of `strategy/` Directory

### 2.1 `strategy/survivor.py`
This is the flagship strategy, an advanced options selling algorithm. It enters positions based on trend direction (selling CE on downtrend, PE on uptrend).

**Strengths:**
* **Indicator-Based Filtering:** Uses RSI (Wilder's smoothing) and EMA for trend verification, reducing false entries in choppy markets.
* **Volatility Adjustment:** Implements ATR (Average True Range) to dynamically adjust the entry strike distance and gap thresholds. This is crucial for options selling to avoid getting caught in sudden volatility spikes.
* **Stop-Loss Reconciliation (`reconcile_sl_at_startup`):** A highly resilient feature that rebuilds position states and re-places missing SL orders if the algo restarts. This mitigates the risk of unprotected open short option positions.
* **Enhanced Trailing SL:** Actively trails the stop-loss as the option price decays, locking in Theta profits.

**Constructive Feedback & Risks:**
* **Monolithic Design:** The class is massive (>3000 lines). Risk management, technical indicators, and order execution are tightly coupled.
  * *Refactor Suggestion:* Extract indicator calculations to a `TechnicalAnalyzer` class and risk limits to a `RiskManager`.
* **State Management Fragility:** Extensive use of nested dictionaries (`self.active_positions`) without strong typing (using `PositionInfo` dataclass helps, but more strict enforcement is better).
* **Blocking Calls:** In `on_ticks_update`, heavy computations (like ATR loops) block the tick processing. Consider optimizing or caching indicator values.

### 2.2 `strategy/wave.py`
A secondary strategy that places simultaneous buy/sell orders offset from the market.

**Strengths:**
* **Dynamic Sizing:** Uses `_generate_multiplier_scale` and integrates with `mibian` for options pricing.
* **Symmetrical Execution:** Well-structured logic to ensure both legs of the wave are placed logically.

**Constructive Feedback & Risks:**
* **Complexity:** Loop management for active orders is complex and prone to race conditions if tick updates arrive rapidly. Needs robust synchronization.

### 2.3 `strategy/position_manager.py`
Tracks positions purely in memory.

**Strengths:**
* **Zero-Persistence Reliability:** Instead of relying on a potentially corrupted JSON file, it queries the broker directly (`_fetch_broker_positions`) at startup to rebuild state. This is the correct approach for critical financial software.

**Constructive Feedback & Risks:**
* **Broker Rate Limits:** Repeatedly querying the broker on startup or during reconciliation could hit Zerodha's API rate limits if there are connection flutters. Cache the state briefly or backoff exponentially.

### 2.4 `strategy/pre_market_data.py` & `gap_risk_manager.py`
Assesses pre-market sentiment (GIFT Nifty, Global Markets) to adjust position sizing.

**Strengths:**
* **Proactive Risk Mitigation:** Reduces position sizes (e.g., Monday 0.5x multiplier) or suspends trading if gap > 1.5%. Excellent protection for option sellers against black swan overnight gaps.

---

## 3. Review of `web/` Directory (Backend Logic)

### 3.1 `web/backend/app/services/strategy_manager.py`
Orchestrates the lifecycle of the strategy.

**Strengths:**
* **Process Isolation:** Uses `multiprocessing.Process` to run the strategy loop. This ensures the CPU-bound strategy calculations (ATR, Black-Scholes) do not block the FastAPI async event loop.
* **Inter-Process Communication:** Uses `multiprocessing.Queue` to safely pass market data and state updates.

**Constructive Feedback & Risks:**
* **Zombie Processes:** If the FastAPI app crashes, the child strategy process might become an orphaned zombie process, continuing to trade unmonitored.
  * *Refactor Suggestion:* Implement heartbeat signals or use `daemon=True` for child processes.
* **Queue Bottlenecks:** If market data arrives faster than the strategy can process it (e.g., volatile market open), the `Queue` will grow indefinitely, causing memory bloat and latency. Introduce queue size limits or tick coalescing (only process the latest tick).

### 3.2 `web/backend/app/services/broker_service.py`
Facade for the underlying broker API.

**Strengths:**
* **Error Sanitization:** `_sanitize_error` redacts API keys and tokens. Crucial security practice.
* **Graceful Degradation:** Returns empty lists on API failures rather than crashing the application, allowing the UI to remain functional.

**Constructive Feedback & Risks:**
* **Retry Logic:** Lacks explicit retry logic for transient API failures (e.g., `requests.exceptions.ConnectionError`). Should use an exponential backoff decorator like `tenacity`.

### 3.3 `web/backend/app/services/greeks_calculator.py`
Calculates Delta, Gamma, Theta, Vega, Rho using Black-Scholes (`scipy.stats`).

**Strengths:**
* **Mathematical Rigor:** Correct implementation of Black-Scholes formulas for European options (which closely approximate NIFTY index options).
* **Portfolio Level Greeks:** Can aggregate Greeks across the entire portfolio to give a net Delta/Theta exposure.

**Constructive Feedback & Risks:**
* **Performance:** Calculating Black-Scholes on every tick for multiple options is CPU intensive.
  * *Refactor Suggestion:* Calculate Greeks on a separate low-priority thread, or only update them every few seconds rather than per-tick.

### 3.4 `web/backend/app/websocket/manager.py`
Streams data to the frontend UI.

**Strengths:**
* Clean pub/sub pattern for pushing `StrategyState` and `NiftyData`.

**Constructive Feedback & Risks:**
* **Memory Leaks:** Dead connections might accumulate in `self.active_connections` if clients disconnect abruptly without sending a close frame. Needs a ping/pong heartbeat to detect and purge stale connections.

### 3.5 `web/backend/app/models/schemas.py`
**Strengths:**
* Comprehensive use of `Pydantic` for strict type validation of configuration and strategy state. Guarantees that bad configs cannot crash the strategy engine.

---

## 4. Specific Strategy Logic Review (Options Selling & Theta Decay)

1. **Theta Capture:** The system is explicitly designed for shorting out-of-the-money (OTM) or at-the-money (ATM) options. By waiting for directional indicators (EMA/RSI), it attempts to sell options that have a high probability of expiring worthless.
2. **Risk to Reward:** Option selling inherently carries high risk (unlimited loss theoretically). The implementation of GTT OCO (One Cancels Other) is the absolute best way to manage this on Zerodha. It places a hard Stop-Loss (e.g., 30% above entry price) and a Take-Profit (e.g., 60% below entry price) simultaneously on the broker's servers, eliminating execution latency if the market spikes.
3. **Margin Management:** The `BrokerService` correctly queries available funds. The strategy should ensure it doesn't over-leverage, which is handled reasonably well via the `GapRiskManager` sizing.

## 5. Final Verdict & Key Recommendations

This codebase is **highly mature and production-ready** for automated trading. The architecture separates concerns beautifully between API/UI delivery and the raw number-crunching strategy processes.

**Top 3 Actionable Improvements:**
1. **Tick Coalescing in IPC:** Prevent IPC queue bloat by dropping stale ticks if the strategy process falls behind the data feed. In high volatility, processing a 5-second old tick can lead to bad entries.
2. **Refactor `survivor.py`:** Break this 3000-line behemoth into smaller, testable modules (e.g., `indicators.py`, `risk.py`, `execution.py`).
3. **Database Integration:** The purely in-memory and JSON file approach is fine for logging, but a robust SQLite/PostgreSQL database is needed for actual trade auditing, PnL tracking, and historical performance analysis across sessions.
