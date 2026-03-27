# Automated Trading Strategy Code Review Report

This report contains a detailed code review of the automated trading strategy built on Python with FastAPI (backend) and Zerodha Broker integration for an options selling (theta decay) approach. The review is broken down by folders and files, focusing on logic correctness, risk management, execution robustness, and overall architecture.

## 1. Strategy Module (`strategy/`)

### 1.1 `strategy/survivor.py`
### 1.2 `strategy/wave.py`
### 1.3 `strategy/base_strategy.py`
### 1.4 `strategy/position_manager.py`
### 1.5 `strategy/gap_risk_manager.py`
### 1.6 `strategy/pre_market_data.py`

## 2. Web Backend Services (`web/backend/app/services/`)

### 2.1 `broker_service.py`
### 2.2 `strategy_manager.py`
### 2.3 `greeks_calculator.py`
### 2.4 `live_data_manager.py`
### 2.5 `strategy_registry.py`
### 2.6 `telegram_notifier.py`
### 2.7 `local_monitor.py`

## 3. Web Backend Routes, Models, and Config (`web/backend/app/`)

### 3.1 `websocket/manager.py`
### 3.2 `models/schemas.py`
### 3.3 `routes/` (auth, market, strategy, greeks, positions, etc.)
### 3.4 `main.py` & `config.py`

## Summary and Recommendations


## 1. Strategy Module (`strategy/`)

### 1.1 `strategy/survivor.py`
**Overview:**
The `SurvivorStrategy` is an advanced NIFTY options selling strategy designed to capture theta decay while utilizing a variety of filters (RSI, EMA, ADX), dynamic gap adjustments (ATR), and automatic Stop-Loss (SL) placement via the broker interface.

**Strengths:**
- **Risk Management:** Extremely comprehensive. Includes SL orders placed immediately upon entry, trailing SL, max positions per side, consecutive loss limits, time-based exit, and gap risk blocks.
- **Dynamic Adjustments:** Uses ATR to adjust strike distances dynamically, ensuring the strategy adapts to current volatility regimes.
- **Robust SL Reconciliation:** It specifically queries the broker orderbook during initialization to reconcile existing SLs and avoid duplicate SL placement, recovering gracefully from restarts.

**Areas for Improvement / Risks:**
- **Code Size & Complexity:** The file is very large (> 1700 lines). Extracting the technical indicator logic into a separate `indicators.py` utility would clean up the class significantly.
- **Blocking Calls in Ticks:** The `on_ticks_update` method repeatedly calls `_get_technical_indicators` which performs computations like EMA/RSI. Since `on_ticks` is driven by high-frequency websocket callbacks, heavy processing here could create latency, leading to queue buildup or delayed order execution.
- **Hardcoded Ticks Size Check:** Tick size rounding (`tick_size = 0.05`) is hardcoded in several places (`_calculate_sl_prices`, `_place_order_enhanced`). It would be better to fetch this from the `instruments` dataframe dynamically, as different exchanges/segments can vary.
- **Broker Call in Loop:** Inside `_check_stop_losses` and `_check_profit_targets`, `broker.get_quote` is called for every position during every tick/interval. A batch quote or using the live tick stream to update position prices in-memory would dramatically reduce API calls and latency.

### 1.2 `strategy/wave.py`
**Overview:**
The `WaveStrategy` is designed to place simultaneous buy and sell orders offset from the market price. It balances delta across the portfolio for both NIFTY and BANKNIFTY.

**Strengths:**
- **Greek-based Restrictions:** Dynamically restricts buying/selling based on calculated Delta bounds (using Black-Scholes).
- **Position Imbalance Scaling:** Adjusts order distance gaps dynamically based on the current position imbalance (`_generate_multiplier_scale`).

**Areas for Improvement / Risks:**
- **Blocking Operations:** `time.sleep(self.cool_off_time)` inside `_prepare_final_prices` and `time.sleep(3)` inside `place_wave_order` block the entire execution thread. In an asynchronous or event-driven architecture, this is an anti-pattern and can cause missed websocket ticks.
- **Delta Calculation:** In `_get_portfolio_greeks`, a Black-Scholes calculation loop runs over all open positions. This happens synchronously and can be computationally expensive if the position count grows.
- **Hardcoded Scaling Arrays:** The `buy_scale` and `sell_scale` arrays are hardcoded in `_generate_multiplier_scale`. These should ideally be driven by configuration.

### 1.3 `strategy/base_strategy.py`
**Overview:**
Provides `BaseStrategy` and `StrategyConfig` to consolidate common functionality like broker initialization, basic risk management limits, and order placing helpers.

**Strengths:**
- **Standardization:** Enforces standard interfaces (`on_tick`, `initialize`) across strategies.
- **Typed Configurations:** `StrategyConfig` handles default values and parses raw dicts elegantly.

**Areas for Improvement / Risks:**
- **Inconsistent adoption:** `survivor.py` manually initializes a lot of state that overlaps with `base_strategy.py`. `SurvivorStrategy` currently does NOT inherit from `BaseStrategy` (it is a standalone class). To realize the benefits, `SurvivorStrategy` and `WaveStrategy` should inherit from `BaseStrategy`.

### 1.4 `strategy/position_manager.py`
**Overview:**
Manages in-memory tracking of open positions and reconciles Stop-Loss orders with the broker on startup. Specifically handles Zerodha logic.

**Strengths:**
- **Stateless Persistence:** Intentionally avoids saving state to disk to prevent stale data reading, strictly relying on the broker's source of truth.
- **Orderbook Matching:** Correctly matches existing open BUY orders to short options positions to calculate "protected quantity".

**Areas for Improvement / Risks:**
- **Single Broker Specific:** Although the app uses a `BrokerGateway`, `PositionManager` makes assumptions about the Orderbook statuses and order types (e.g., `STOP_LIMIT`, string matching "AMO REQ RECEIVED") which might be strictly Zerodha-specific and could fail if Fyers or another broker is used.

### 1.5 `strategy/gap_risk_manager.py`
**Overview:**
Adjusts position sizes based on the day of the week and gap magnitude (difference between previous close and open, or GIFT Nifty).

**Strengths:**
- **Clear Rulesets:** Separates Monday delay logic, day multipliers, and gap thresholds cleanly.

**Areas for Improvement / Risks:**
- **Tight Coupling:** The day multipliers are hardcoded inside the class (`DAY_MULTIPLIERS`), though they are also fetched from config in `SurvivorStrategy`. It would be better to pass these entirely via configuration.

### 1.6 `strategy/pre_market_data.py`
**Overview:**
Fetches pre-market indices like GIFT Nifty and US Markets to determine gap risk.

**Strengths:**
- **Caching Mechanism:** Implements a sensible 15-minute file cache so it doesn't spam APIs.

**Areas for Improvement / Risks:**
- **yfinance dependency:** Uses `yfinance` to fetch live SGX NIFTY. `yfinance` can be unstable or rate-limited in production server environments. A dedicated API (like a paid data vendor) is highly recommended for live trading.

## 2. Web Backend Services (`web/backend/app/services/`)

### 2.1 `broker_service.py`
**Overview:**
A singleton wrapper service around the `BrokerGateway` driver to expose API-friendly schemas (`Position`, `Order`, `Quote`) to the FastAPI routes.

**Strengths:**
- **Encapsulation:** Protects the FastAPI routes from the raw broker driver responses.
- **Resilience:** Catches exceptions gracefully during calls (`get_positions`, `get_orders`, `get_trades`) and returns empty lists rather than crashing the API.
- **Dynamic Re-initialization:** The `reinitialize` method allows the application to cleanly reload broker instances if credentials change.

**Areas for Improvement / Risks:**
- **Implicit Error Masking:** Returning an empty list on `Exception` in `get_orders` and `get_trades` masks actual API failures. It's difficult to distinguish between "No trades" and "Broker API is down". It should ideally log the error traceback or return an error field in the response schema.

### 2.2 `strategy_manager.py`
**Overview:**
Responsible for orchestrating strategy lifecycles. It uses `multiprocessing` to spawn isolated Python processes for each active strategy, connecting them to the main API process via `multiprocessing.Queue`.

**Strengths:**
- **Process Isolation:** Excellent architectural decision to run trading strategies in a separate `Process` rather than thread or async task. This ensures blocking operations (like Pandas calculations) do not stall the FastAPI event loop.
- **Event-Driven Feedback:** The `_monitor_state` asyncio task listens to the queue from the child process and updates the singleton state, triggering Telegram alerts and WebSocket updates.
- **Graceful Shutdown:** Implements `terminate()` followed by a timeout and `kill()` to guarantee cleanup.

**Areas for Improvement / Risks:**
- **Heavy Startup Routine:** Inside `_run_strategy_process`, the strategy is instantiated, SLs are reconciled, positions initialized, profit targets checked, and gap risk assessed before connecting the websocket. This takes time, and any unhandled exception here silently kills the strategy process, though it logs an error via the queue.
- **Global instances in spawned process:** `live_data_manager` and `telegram_notifier` are referenced from both the main FastAPI app and potentially within the strategy context. Careful attention must be paid to `multiprocessing` memory boundaries; child processes get copies of memory, so singletons like `live_data_manager` will diverge between the API process and the strategy process.

### 2.3 `greeks_calculator.py`
**Overview:**
Calculates Option Greeks (Delta, Gamma, Theta, Vega, Rho) using the Black-Scholes formula.

**Strengths:**
- **Robust Parsing:** Very robust regex-based option symbol parser (`parse_option_symbol`) that handles standard monthly, weekly, and abbreviated symbol formats correctly across various indices.
- **No External Dependencies for Pricing:** It implements Black-Scholes natively using `math` and `scipy.stats.norm`, avoiding heavy quantitative libraries.
- **Scenario Analysis:** Includes a `get_scenario_analysis` function to simulate PnL impacts of price and volatility shifts.

**Areas for Improvement / Risks:**
- **Loop Efficiency:** Similar to the Wave strategy, calculating Greeks for a large portfolio synchronously could be optimized by utilizing `numpy` vectorized operations instead of standard python loops.
- **Hardcoded Rates:** `risk_free_rate` defaults to 0.10 (10%). For accurate Greek calculations, this should be configurable or fetched live (e.g., India 10-Year Bond Yield is currently ~7%).

### 2.4 `live_data_manager.py`
**Overview:**
An asyncio background task that continuously polls the broker for open positions and orders every second (`asyncio.sleep(1)`) while streaming is active, and broadcasts the data over WebSockets to clients.

**Strengths:**
- **Real-Time UI Updates:** Pushes updates directly to frontend clients via `connection_manager.broadcast`, making the UI snappy.
- **Error Backoff:** Implements an error counter (`_max_errors = 5`) and backs off (`asyncio.sleep(2)`) to avoid spamming the broker when API limits are hit or the broker is down.

**Areas for Improvement / Risks:**
- **Polling vs. WebSocket:** It polls `broker.get_positions()` and `broker.get_orderbook()` every 1 second. Most brokers (like Zerodha) have strict rate limits on HTTP API calls (e.g., 3 requests/sec). Polling orderbook and positions every second is highly likely to trigger `429 Too Many Requests` bans in live environments. It is better to rely on the broker's order update websocket callback instead of HTTP polling.

### 2.5 `strategy_registry.py`
**Overview:**
Maintains a registry of available strategies, handling dynamic class loading and configuration validation.

**Strengths:**
- **Extensible Architecture:** Makes adding new strategies trivial by defining a `StrategyInfo` metadata object and using `importlib` to load classes dynamically.
- **Validation:** Implements `validate_config` to catch basic configuration errors before starting a strategy.

**Areas for Improvement / Risks:**
- **Hardcoded Default Paths:** Uses relative directory traversal (`os.path.dirname` x 5) to locate `survivor.yml`. While it works, it is brittle if the project structure changes.

### 2.6 `telegram_notifier.py`
**Overview:**
Async service wrapping the Telegram Bot API (`aiohttp`) for real-time trade execution and error alerts.

**Strengths:**
- **Async HTTP:** Uses `aiohttp.ClientSession` properly, ensuring message dispatching doesn't block the trading loop or API.
- **Rich Formatting:** Leverages HTML parse mode to send clean, visually distinct alerts with emojis.

### 2.7 `local_monitor.py`
**Overview:**
A basic in-memory tracking tool for session trades, calculating Win Rate, PnL, Max Drawdown, etc., and printing them to the console.

**Strengths:**
- **Simplicity:** Provides quick, glanceable terminal output without needing a frontend or database.

## 3. Web Backend Routes, Models, and Config (`web/backend/app/`)

### 3.1 `websocket/manager.py`
**Overview:**
Standard `FastAPI` Connection Manager for WebSocket handling, subscriptions, and broadcasts.

**Strengths:**
- **Symbol Subscriptions:** Implements logic so clients only receive updates for symbols they care about (`broadcast_to_subscribers`).
- **Resilience:** Gracefully removes disconnected clients instead of raising unhandled exceptions in the broadcast loops.

**Areas for Improvement / Risks:**
- **Scaling:** Operates strictly in-memory (`self._active_connections = []`). If the backend ever scales horizontally to multiple Uvicorn workers or servers, WebSocket broadcasts won't propagate across instances. (A Redis pub/sub backplane would be needed in the future).

### 3.2 `models/schemas.py`
**Overview:**
Pydantic schemas describing API requests/responses and internal state objects.

**Strengths:**
- **Comprehensive:** Models everything from API responses to strategy state and visual dashboard formats.
- **Type Safety:** Heavily leverages Enums and Pydantic validation.

**Areas for Improvement / Risks:**
- **`StrategyConfigUpdate` Maintenance:** The schema is massive and reflects the configuration of the Survivor Strategy specifically. As the system moves towards a multi-strategy platform (with the `StrategyRegistry`), having a single massive configuration schema might break or confuse frontend logic for other strategies.

### 3.3 `routes/` (auth, market, strategy, greeks, positions, etc.)
**Overview:**
FastAPI routers providing HTTP endpoints for the Next.js frontend to communicate with the Python backend.

**Strengths:**
- **Decoupling:** Routes correctly delegate complex business logic to services (`broker_service`, `strategy_manager`).
- **NIFTY context endpoint:** Features a specific `/market/nifty` route that injects strategy variables (like PE/CE gaps and references) directly alongside the quote, making frontend calculations trivial.

### 3.4 `main.py` & `config.py`
**Overview:**
`main.py` handles the FastAPI app creation, CORS setup, Lifespan context, and background broadcasting loops. `config.py` maps environment variables via Pydantic BaseSettings.

**Strengths:**
- **Async Lifespan:** Modern FastAPI lifespan usage (`@asynccontextmanager`) ensuring strategy stops gracefully when the server is killed.
- **Broadcast Loop decoupling:** `state_broadcast_loop` runs independently, polling state once per second and broadcasting it, keeping the WebSocket responsive.

**Areas for Improvement / Risks:**
- **Silent Exception Swallowing:** In `state_broadcast_loop`, NIFTY quote errors are silently ignored (`pass`). This is dangerous if the broker API drops, as the system won't alert the user or log the issue.
- **`config.py` hardcoding:** The `STRATEGY_CONFIG_PATH` is deeply hardcoded to the survivor strategy yaml relative to `__file__`. As the registry pattern grows, this should refer to a more dynamic configurations folder.

## 4. Overall Architecture Summary and Recommendations

### 4.1 Architecture Strengths
- **Isolation:** Separating the long-running, blocking strategy computations (e.g., indicator calculations in Survivor, Option pricing in Wave) into `multiprocessing.Process` completely isolates them from the FastAPI HTTP/WebSocket event loop.
- **Robust Risk Management:** The strategies, particularly `SurvivorStrategy`, feature excellent risk controls, gap handling, and SL logic. Integrating SL protection natively at the broker level ensures positions are safe even if the local server crashes.
- **Modularity:** The codebase is well-organized. Separating business logic into `services/` makes testing and replacement straightforward.

### 4.2 Recommendations & Key Vulnerabilities
1. **API Rate Limiting & WebSocket Polling:** The `LiveDataManager` service currently polls the broker for positions and orders every second using HTTP APIs. Most retail brokers (Zerodha, Fyers) severely rate-limit these endpoints. The architecture should shift to subscribing to the broker's own Order Update websocket stream and maintaining state locally, rather than polling via HTTP.
2. **Strategy Process Error Handling:** If the `multiprocessing` strategy crashes silently due to an unhandled exception before the `try/except` catches it (e.g., memory exhaustion or C-extension segfault from Pandas/Scipy), the API backend won't know. Implementing a heartbeat mechanism between the API and the strategy process is recommended.
3. **Synchronous Greek Calculations:** The `BlackScholesGreeks` and portfolio greek calculations loop over items sequentially. For options tracking, vectorizing these calculations using `numpy` arrays will vastly improve performance.
4. **Blocking Calls in Event Loops:** The `WaveStrategy` contains hardcoded `time.sleep(3)` calls which block the executing thread. Refactoring the strategies to use standard async event loops (`asyncio.sleep()`) or state-machine architectures instead of blocking sleeps is highly recommended to prevent missing critical market ticks.
5. **Multi-Strategy Configuration:** The codebase has remnants of being hardcoded to the Survivor strategy (e.g., `StrategyConfig` schema in `models/schemas.py`). To fully support `Wave` and future strategies cleanly, configuration objects should be generalized, and the frontend should dynamically render settings based on the strategy selected via `StrategyRegistry`.

**Conclusion:** The codebase represents a robust, well-thought-out automated trading system. Resolving the API polling loop and blocking operations will upgrade it from an excellent prototype to a highly reliable production trading engine.
