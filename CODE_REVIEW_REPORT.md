# Comprehensive Code Review Report: Automated Options Trading Strategy

This report details a thorough review of the `web/` and `strategy/` folders of the automated Python-based options trading strategy using the Zerodha broker. The strategy revolves around selling NIFTY options to capture Theta decay and includes advanced risk management and a multi-service web dashboard.

---

## 1. High-Level Architecture & Design

### Overview
The system relies on a central Python algorithm executing option-selling trades. The broker integration is abstracted via `BrokerGateway`, allowing multiple broker drivers. The architecture includes a FastAPI backend (`web/backend`) orchestrating strategies via isolated processes (`multiprocessing.Process`), communicating via queues (`multiprocessing.Queue`). A Next.js frontend (`web/frontend`) consumes real-time market data, strategy state, positions, and Greek calculations via WebSocket and REST APIs.

### Strengths
- **Service Isolation:** Running the strategies in isolated background processes (via `StrategyManager`) prevents main thread blocking and separates the FastAPI web server from intensive trading loops.
- **Pluggable Architecture:** `BrokerGateway` acts as an adapter, seamlessly switching between `zerodha`, `fyers`, etc.
- **Configurability:** Strategies load configurations dynamically via YAML (`StrategyRegistry`), allowing dynamic tuning.

### Areas for Improvement
- **Singleton Sprawl & State Synchronization:**
  The codebase uses multiple singletons representing similar state (`StrategyManager`, `LiveDataManager`, `PositionManager`, `LocalMonitor`). The `PositionManager` tracks SL and option positions natively inside `survivor.py`, while `LiveDataManager` independently polls and broadcasts via WebSockets. Centralizing state into a single cohesive manager or a lightweight fast cache (e.g., Redis) would reduce duplication.
- **Multiprocessing Communication:**
  The Queue used to pass state from the strategy process to `StrategyManager` (`self._state_queue.put(...)`) is effective but could cause backpressure if the API thread doesn't consume it fast enough.
- **Event Loop Management:**
  Spawning asyncio tasks from a synchronous function (`StrategyManager.start()`) inside an instantiated web server using `asyncio.create_task` relies on the running event loop. This is generally fine in FastAPI, but during tests, managing async context across boundaries can lead to event loop mismatch errors.

---

## 2. Review of `strategy/` Folder

### `survivor.py` & `base_strategy.py`
The flagship "Survivor" strategy implements trend-following NIFTY option selling (selling PE on a rise, CE on a fall).
- **Logic & Filter Implementation:**
  - It successfully integrates Technical Indicators (RSI, EMA, ADX, ATR). The calculation approach using list history (`np.diff`, `pd.Series.ewm`) is accurate but can be CPU-heavy on every tick. The history size is bounded to 1000, which avoids infinite memory growth.
  - Using Wilder’s smoothing for RSI (`adjust=False`) aligns well with standard trading terminals.
- **Gap & Risk Management (`gap_risk_manager.py`):**
  - Very robust logic suspending trading on excessive overnight gaps (fetching data using `pre_market_data.py`).
  - *Risk:* `_check_gap_risk()` caches results for 30 minutes. If the market aggressively dumps right after the 30-minute interval and pre-market fails to fetch (`yfinance` error), it falls back to "trading allowed with default multiplier." This could inadvertently permit a bad trade.
- **Stop Loss Handling (`position_manager.py`):**
  - Stop Loss reconciliation at startup (`reconcile_sl_at_startup`) matches open NFO MARGIN short positions to existing pending SL (BUY) orders. This is a critical safety net against restarts.
  - *Risk:* In `_place_sl_order`, the code rounds to `0.05` ticks. This is accurate for NIFTY, but hardcoding `0.05` tick size will fail if the script is ever pointed to an asset class with a different tick size (e.g., currency derivatives).
- **Profit Target:**
  - A polling mechanism checks profit targets every 60 seconds (`profit_check_interval`). If a rapid spike happens between polls, the profit target might be missed. Ideally, profit targets should be placed natively as GTT (Good Till Triggered) or Limit orders directly at the broker level, similar to how SL is handled.

### `wave.py`
Implements a wave-based strategy with configured buy/sell gaps.
- **Logic Review:**
  - Employs `mibian` for option Greeks to dynamically adjust allowed trading actions based on the portfolio's Delta exposure limits (`min_nifty_delta` / `max_nifty_delta`). This is an excellent dynamic hedging capability.
  - *Bug Potential:* In `_execute_orders`, it passes `final_buy_price` and `final_sell_price` directly to `Broker.place_order` as LIMIT orders without ensuring they meet the tick size requirement (`0.05`). Rejected orders due to invalid tick intervals might occur.

---

## 3. Review of `web/backend/` Folder

### `app/services/strategy_manager.py`
Manages the `multiprocessing.Process` of the active strategy.
- **Process Management:** Correctly joins and kills processes during `stop()`.
- **WebSocket Streaming:** Starts/Stops `live_data_manager.start_stream()` concurrently.
- *Recommendation:* Error handling auto-stops the strategy if insufficient funds are detected (`_stop_on_error`). This is smart. However, the error keywords search is case-sensitive inside the loop unless `.lower()` is correctly applied to both. The codebase does use `lower()` correctly here.

### `app/services/live_data_manager.py`
Streams positions and orders every second and broadcasts via `connection_manager`.
- **Data Freshness:** Pulls directly from the broker using a background async loop (`_stream_loop`).
- *Bug Potential:* If `broker.get_positions()` hangs or takes >1 second, the async loop could stack up pending requests or block other WebSocket broadcasts if not properly yielding. It currently awaits properly (`await self._fetch_and_update()`), ensuring sequential execution.

### `app/services/broker_service.py`
- Exposes `get_positions`, `get_orders`, `get_trades` gracefully.
- Exception handling intercepts "unauthenticated" errors safely and returns an empty schema with an error message instead of causing a 500 API crash. Excellent design.

### `app/services/greeks_calculator.py`
- Implements `scipy.stats.norm` to compute Black-Scholes Greeks.
- **Symbol Parsing (`parse_option_symbol`):** Contains extensive Regex to handle weekly/monthly expirations.
  - *Risk:* `NIFTY2630224750PE` (Numeric date parsing) regex assumes strict formatting. It correctly parses 3-digit MDD (March 02). This is robust but highly specific to NSE's naming convention changes.

### `app/routes/` & `app/main.py`
- Implements modern FastAPI routers.
- The `lifespan` context manager handles WebSocket heartbeats cleanly.
- `CORSMiddleware` is configured, allowing seamless connection from Next.js on port 3000.

---

## 4. Review of `web/frontend/` Folder

The frontend uses Next.js 14 App Router, Tailwind CSS, and Recharts.

### Structure & Components
- **Hooks (`useWebSocket.ts`):**
  - Standard WebSocket integration with automatic reconnection.
  - Listens to `STRATEGY_STATE`, `POSITIONS_UPDATE`, `PRICE_UPDATE`.
- **Charting (`PayoffChart.tsx`):**
  - Aggregates portfolio payoff logic to display max profit/loss and breakeven points natively using `recharts`.
- **Visual Engine (`VisualEngine/`):**
  - Displays Entry Radar and Strategy Status. Very modern UI.
- *Recommendation:* Ensure WebSocket disconnections gracefully show a "Reconnecting..." banner to the user to prevent stale UI interactions (trading decisions based on frozen data).

---

## 5. Security & Pre-commit Safety
- **Environment Variables:** `BrokerGateway` uses API keys properly segregated via `.env`.
- **Sanitization:** `_sanitize_error` is mentioned in the memory log as a way to redact API keys from logs, ensuring secrets do not leak into the `local_monitor.py` or terminal output.
- **Error States:** Exceptions in background tasks (like `_stream_loop`) are isolated and increment an `_error_count` rather than crashing the application entirely.

---

## 6. Summary of Recommendations

1. **Tick Size Validation:** Ensure all `price`, `trigger_price`, and `limit_price` variables (especially in `wave.py`) are strictly rounded to `0.05` before placing limit/stop orders.
2. **Profit Book Execution:** Consider converting the manual polling Profit Target logic (`profit_check_interval` in `survivor.py`) to native Broker Limit Orders / GTT to avoid missing momentary price wicks.
3. **Queue Backpressure:** Monitor `self._state_queue` in `StrategyManager`. If the queue grows indefinitely because the consumer event loop hangs, it may crash the process due to Memory constraints. Adding `queue.put(..., timeout=1)` or a `maxsize` to the Queue is advised.
4. **State Consolidation:** Future iterations should merge the tracking properties of `LiveDataManager` and `PositionManager` to prevent duplicate polling to the Broker API, which might hit rate limits (`ratelimit` library is installed).

## Conclusion
The codebase exhibits excellent architectural maturity, employing isolated processes, robust fault-tolerant broker API wrapping, advanced gap analysis, and responsive Next.js frontend displays. The strategic approach to selling options natively accounts for Greek risks and system restart recoveries.
