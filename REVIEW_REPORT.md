# Code Review Report: Automated Options Selling Strategy (Theta Decay)

I have completed a comprehensive review of your Python-based automated trading strategy architecture, focusing on the `web/backend` and `strategy` directories. The review evaluates the codebase against your primary objective: an automated option selling strategy generating theta decay profit via the Zerodha broker.

---

## 🌟 1. Architecture & Design Overview

The overall architecture is robust and well-thought-out, employing modern Python practices to separate concerns:
- **FastAPI Backend (`web/backend`)**: Handles user interface interactions, API routing, configuration management, and WebSocket streaming.
- **Multiprocessing Strategy Engine (`StrategyManager`)**: Trading strategies run in isolated processes via the `multiprocessing` module, ensuring that blocking API calls or web traffic do not interrupt the critical path of trading execution.
- **Service Abstractions**: Strong use of registries (`StrategyRegistry`), managers (`LiveDataManager`), and broker facades (`BrokerGateway`) makes the codebase highly modular and easy to extend.

### Strengths:
- **Excellent Risk Management Framework:** The modularization of risk management into `GapRiskManager` (weekend and overnight gaps) and `PositionManager` (SL reconciliation at startup) is a massive positive. It prevents common pitfalls in algorithmic trading.
- **Comprehensive Analysis Tools:** Built-in portfolio Greeks calculations (`greeks_calculator.py`) and payoff scenario analysis (`analysis.py`) are top-tier additions for an options selling system.
- **Resilience:** Features like SL order reconciliation on startup ensure that open positions remain protected even if the server restarts or crashes.

---

## ⚠️ 2. Critical Findings & Areas for Improvement

While the architecture is strong, there are a few critical discrepancies and areas for improvement, particularly regarding how option selling risk is managed and API rate limits.

### A. Missing GTT OCO Implementation for Stop-Loss & Target
**File:** `strategy/survivor.py`
**Issue:** The strategy currently uses `OrderType.STOP_LIMIT` or `OrderType.STOP` for stop-loss orders (`_place_sl_order`) and implements a manual profit-booking mechanism that runs on an interval (`_check_profit_targets`).
**Why it matters:** Based on system memory and standard option selling practices, combining Stop Loss (e.g., Entry * 1.30) and Profit Booking (e.g., Entry * 0.40) into a **single GTT OCO (One Cancels Other) order** is far more reliable and requires less active monitoring. The `ZerodhaDriver` supports `place_gtt_oco_order`, but the strategy is not utilizing it.
**Recommendation:**
- Update `_place_order_enhanced` to immediately issue a GTT OCO order using the `place_gtt_oco_order` method from your broker driver right after the entry order executes. This offloads the SL and Target execution entirely to Zerodha's servers, eliminating the risk of network latency missing a manual target hit.

### B. Synchronous API Calls in Trading Loop
**File:** `strategy/survivor.py` (`_check_profit_targets` and `_check_stop_losses`)
**Issue:** The strategy iterates over `self.positions.items()` and calls `self.broker.get_quote(f"NFO:{symbol}")` synchronously for every open position on every interval or tick.
**Why it matters:** Network I/O inside the main strategy loop (especially fetching quotes) can cause significant latency. If the Zerodha API is slow to respond, the entire tick processing loop blocks.
**Recommendation:**
- Subscribe to the specific option instrument tokens via WebSockets (`symbols_to_subscribe`).
- Keep a local, continuously updating dictionary of live prices updated via `on_ticks_update`.
- Use this local dictionary to check current prices against SL and Profit Targets without making active `get_quote` API calls.

### C. Potential API Rate Limit Breaches
**File:** `web/backend/app/services/live_data_manager.py`
**Issue:** The `_stream_loop` fetches `broker.get_positions()` and `broker.get_orderbook()` every 1 second (`await asyncio.sleep(1)`).
**Why it matters:** Zerodha Kite API has strict rate limits for orderbook and position fetches (usually a few requests per second, with overall daily limits). Polling these endpoints every second will quickly exhaust rate limits or lead to temporary IP bans.
**Recommendation:**
- Instead of polling every second, rely on the broker's WebSocket order update callbacks (`on_order_update`) to push state changes to your application.
- Only poll the API as a fallback (e.g., every 30-60 seconds) to reconcile state.

### D. Multiprocessing Connection Safety
**File:** `web/backend/app/services/strategy_manager.py`
**Issue:** The broker connection is initialized, but objects like WebSocket connections or HTTP sessions might not be safe to pass across process boundaries.
**Why it matters:** Currently, `_run_strategy_process` re-initializes the broker and creates a new WebSocket connection inside the child process, which is correct! However, be very careful that the main FastAPI process and the Strategy process don't step on each other's toes regarding WebSocket limits per API app on Zerodha (Zerodha allows up to 3 concurrent WebSocket connections per API key).

---

## 🛠️ 3. Detailed Component Review

### `strategy/survivor.py` (Main Options Selling Strategy)
- **Logic:** Beautiful implementation of an options selling system capturing theta decay. Filtering entries via RSI, EMA, and ADX helps prevent selling puts in a downtrend or selling calls in an uptrend.
- **Dynamic Gaps:** Utilizing ATR (`_get_dynamic_gap`) to adjust strike selection based on market volatility is a highly professional touch.
- **Bug Alert:** In `_check_stop_losses`, if `self.sl_enabled` is true, the strategy queries the broker for the order status (`self.broker.get_order()`). As mentioned earlier, this introduces synchronous API latency. It should rely on WebSocket `on_order_update` events to track order statuses.

### `strategy/position_manager.py` & `strategy/gap_risk_manager.py`
- **Position Manager:** Reconciling SL orders at startup is brilliant for disaster recovery. It correctly identifies unprotected quantities and places fresh SLs.
- **Gap Risk Manager:** Reducing sizing by 50% on Monday mornings and 60% on Fridays (weekend risk), plus suspending trading if the overnight gap is >1.5%, perfectly aligns with the defensive posture required for systematic option selling.

### `web/backend/app/routes/auth.py`
- **Environment Management:** The route dynamically updates the `.env` file and `os.environ` upon successful broker login. While slightly unconventional for 12-factor apps, it is a pragmatic solution for a desktop/server-deployed trading bot to maintain session state across restarts.

### `web/backend/app/services/greeks_calculator.py`
- **Math & Accuracy:** The Black-Scholes implementation is clean and handles both weekly and monthly expiries via regex parsing (`_parse_option_symbol`).
- **Efficiency:** The calculation runs on-demand via the `/greeks` endpoint, so it won't slow down the main trading loop.

---

## 📋 4. Actionable Next Steps (Execution Plan)

If you would like me to implement fixes based on this review, here is the suggested priority list:

1. **Implement GTT OCO Orders:** Refactor `survivor.py` to use `place_gtt_oco_order` for combined 30% Stop-Loss and 60% Profit Booking, delegating the execution to the broker.
2. **Optimize Tick Processing:** Remove synchronous `get_quote` calls from `_check_stop_losses` and `_check_profit_targets` in `survivor.py`. Replace them with local state checks using the WebSocket tick data.
3. **Fix Rate Limits:** Adjust `live_data_manager.py` to poll less frequently (e.g., every 30 seconds) and rely more on WebSocket `on_order_update` events for real-time frontend updates.

Please let me know if you would like me to proceed with implementing these improvements!