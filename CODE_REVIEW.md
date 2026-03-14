# Comprehensive Code Review & Recommendations Report

## Overview
This report provides a detailed code review of the automated options selling strategy system, covering the `strategy/` directory (trading logic) and `web/` directory (FastAPI backend and React/Next.js frontend). The system aims to capture theta decay by selling NIFTY options using the Zerodha broker.

In addition to the review, this report outlines the steps needed to **consolidate the web folder** and **make the application ready for AWS Runner** (e.g., AWS App Runner, ECS, or Elastic Beanstalk).

---

## 1. Strategy Folder (`strategy/`)

### `survivor.py`
The Survivor strategy is a trend-following option selling strategy using dynamic gaps, technical filters (RSI, EMA, ADX), and trailing stop-losses.

**Strengths:**
- Advanced risk management logic including gap risk checks, daily PnL limits, consecutive loss tracking, and position sizing.
- Comprehensive technical indicator calculations utilizing `numpy` and `pandas`.
- Well-structured state tracking (`PositionInfo`, `TrailingSLState`).
- Robust SL order reconciliation on startup.

**Areas for Improvement / Risks:**
- **Indicator Calculations (Performance):** The indicator calculations (`_calculate_rsi`, `_calculate_ema`, `_calculate_atr`) are performed on every tick using `pandas` EWMA and raw arrays. On high tick frequency, `pandas` operations could introduce latency. It's recommended to maintain a running calculation (e.g., using `ta-lib` or a simple stateful EMA/RSI calculator) rather than recalculating the entire history.
- **Infinite Loop Risk in Strike Selection:** In `_handle_pe_trade` and `_handle_ce_trade`, the `while True:` loop decreases `temp_gap` until a premium condition is met. If the minimum premium condition is impossible to meet, it could loop until `temp_gap` reaches zero or negative, potentially picking an ITM strike or hanging. A safeguard (max retries) should be strictly enforced.
- **`time.sleep()` in Async/Event-Driven Flow:** Not explicitly present in `survivor.py`, but it's important to ensure tick processors don't block the data dispatcher.

### `wave.py`
The Wave strategy places simultaneous buy/sell orders based on configured gaps.

**Strengths:**
- Uses `mibian` for options pricing and Greeks calculations.
- Portfolio delta exposure constraints (`min_nifty_delta`, `max_nifty_delta`).

**Areas for Improvement / Risks:**
- **Blocking Calls:** Uses `time.sleep(self.cool_off_time)` and `time.sleep(3)` in the execution path. This is a severe anti-pattern in a real-time tick-processing system. The thread will block, potentially missing critical market data or delaying other operations. This logic should be converted to an asynchronous state machine or a timer-based check in the main loop.
- **Dictionary Size Modification:** Modifies `self.orders` inside a loop using `list(self.orders.items())`. While safe from `RuntimeError`, it is a bit fragile if concurrent modifications happen (though Python GIL protects mostly, if threaded it might fail).

### `position_manager.py` & `gap_risk_manager.py`
- **Position Manager:** Good logic to prevent duplicate SL orders by parsing the broker's open orders and cross-referencing.
- **Gap Risk Manager:** Sound logic for avoiding weekend/overnight gap risks (e.g., reducing positions on Fridays, skipping trades on >1.5% gaps). It relies on `PreMarketDataService`.

### `pre_market_data.py`
- Relies heavily on `yfinance` to fetch US markets and SGX Nifty (GIFT Nifty).
- **Risk:** Yahoo Finance is notorious for rate-limiting or returning delayed/incorrect data. For a live trading system, depending on a free scraper API for critical risk decisions is dangerous. Consider integrating a paid, reliable data feed for GIFT Nifty.

---

## 2. Web Folder (`web/backend/`)

### Backend Architecture
Built with FastAPI. It handles routing, WebSocket communication, and background strategy execution using Python's `multiprocessing`.

**Strengths:**
- Modular routing (`routes/` directory).
- Clean separation of broker logic (`broker_service.py`) and strategy lifecycle (`strategy_manager.py`).
- Real-time updates pushed via WebSockets to the frontend.

**Areas for Improvement / Risks:**
- **Multiprocessing State Synchronization:** The `StrategyManager` spawns the strategy in a separate process (`multiprocessing.Process`) and uses a `multiprocessing.Queue` to receive state updates. This is generally good for isolating the strategy from the web server. However, if the strategy process crashes silently, the manager might not clean up properly. `_monitor_state` handles `Queue` messages, but there should be a robust process health check (e.g., `process.is_alive()`).
- **Global Instances:** Heavy use of singleton instances (`strategy_manager`, `broker_service`, `live_data_manager`). While standard for FastAPI, ensure that these singletons don't hold stale state if the broker connection drops.
- **Logging Configuration:** The application sets `access_log=False` in `uvicorn.run`. This removes vital request tracing. For production/AWS, access logs are critical for debugging.

---

## 3. Consolidation & AWS Runner Readiness

### Issue: "Consolidate code under /web folder"
The user requested consolidating the code and making it ready for AWS Runner. AWS App Runner requires a containerized application (Docker).

**Action Plan for Consolidation & Dockerization:**
1. **Containerize the Backend:** Create a `Dockerfile.backend` to package the FastAPI app, install dependencies (using `uv` or `pip`), and set the entrypoint to start Uvicorn.
2. **Containerize the Frontend:** Create a `Dockerfile.frontend` to package the Next.js app (using multi-stage builds for smaller image size).
3. **Root Integration:** A single `docker-compose.yml` to orchestrate both the frontend, backend, and potentially Redis/Postgres if needed in the future.
4. **AWS App Runner config:** Write an `apprunner.yaml` or instructions to deploy the container to AWS App Runner.

**Security for AWS:**
- Use AWS Secrets Manager or Parameter Store to inject `BROKER_API_KEY`, `BROKER_API_SECRET`, and `BROKER_ACCESS_TOKEN` into the environment. The `auth.py` script currently writes to `.env` which is an anti-pattern for ephemeral containers (App Runner instances are stateless).
- **CRITICAL FIX in `auth.py`:** Writing the generated `access_token` to `.env` inside a container will lose the token when the container restarts. You must store the token in a persistent datastore (like a small SQLite DB, AWS Parameter Store, or DynamoDB) if you want it to survive container restarts.

---

## 4. Specific Action Items to Implement Next

1. **Fix `auth.py` State Persistence:** Refactor `_save_access_token_to_env` to optionally use a mounted volume, a database, or at least keep it in-memory / prompt the user to use AWS Secrets for persistence.
2. **Create Docker Structure:** Add `Dockerfile.backend`, `Dockerfile.frontend`, and `docker-compose.yml` to the root or `web/` directory.
3. **App Runner Configuration:** Prepare the build specifications and ensure `host=0.0.0.0` is used in FastAPI so the container can receive traffic.
4. **Remove Blocking calls:** Document the `time.sleep` issue in `wave.py`.

*End of Report*