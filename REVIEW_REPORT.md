# Automated Trading Strategy Code Review Report

## 1. Overview and Architecture

The application is an automated trading system designed to execute a theta-decay strategy (specifically, "Survivor") on the Indian stock market (NIFTY 50) using the Zerodha broker. The architecture is cleanly divided into:
*   **Backend (FastAPI):** Handles API requests, WebSocket streaming, background tasks, and manages the execution of isolated strategy processes using Python's `multiprocessing`.
*   **Strategy Layer (`strategy/`):** Contains the core trading logic, risk management, and pre-market analysis.
*   **Frontend (Next.js):** Provides a comprehensive dashboard for real-time monitoring, configuration management, and strategy control.

**Overall Design Assessment:**
The separation of concerns is commendable. Utilizing `multiprocessing` for the trading strategy (`StrategyManager`) ensures that the core trading loop is not blocked by the asynchronous API event loop, which is critical for a low-latency trading system. The unified base classes (`BaseStrategy`) and modular services (`BrokerService`, `LiveDataManager`) promote reusability and maintainability.

---

## 2. Strategy Logic (`strategy/` folder)

### 2.1. `survivor.py` (Main Theta Decay Strategy)
The "Survivor" strategy is the core options selling system. It sells OTM Put (PE) options when the market rises and Call (CE) options when the market falls, capturing premium through theta decay.

**Strengths:**
*   **Comprehensive Filtering:** Incorporates RSI (momentum), EMA (trend), and ADX (trend strength) filters to prevent selling options against strong momentum.
*   **Dynamic Adjustments:** Uses ATR (Average True Range) to dynamically scale entry gaps based on current market volatility, ensuring wider safety nets during high volatility.
*   **Profit Booking & Stop Loss:** Implements a strict percentage-based Profit Target (default 60% of premium) and a robust Stop-Loss mechanism (default 60% above entry) placed directly on the broker's side.
*   **Position Sizing:** Dynamically adjusts position sizing based on the day of the week (e.g., smaller sizes on Monday/Friday due to gap risks).

**Areas for Improvement / Risks:**
*   **Execution Risk:** The strategy relies heavily on `MARKET` orders for entries (`place_order_enhanced` with `OrderType.MARKET`). In illiquid options or highly volatile moments, this can lead to significant slippage. *Recommendation:* Consider using limit orders with a small buffer or checking bid/ask spreads before execution.
*   **Indicator Calculation:** Technical indicators (RSI, EMA, ATR) are calculated manually using list comprehensions and NumPy/Pandas. While functionally correct, relying heavily on historical 5-minute candles appended tick-by-tick can introduce slight inaccuracies if ticks are missed.
*   **State Management:** Reference values (`nifty_pe_last_value`, `nifty_ce_last_value`) dictate the core logic. If the system restarts mid-day, these values are re-initialized based on `start_point` configs or the current LTP. This could cause the strategy to lose context of the day's prior movements.

### 2.2. Risk & Position Management (`gap_risk_manager.py`, `position_manager.py`)
*   **Gap Risk Manager:** Excellently mitigates overnight risk by analyzing previous closes, GIFT Nifty, and global markets. The logic to reduce position sizes or halt trading entirely on >1.5% gaps is a very sound defensive measure for option sellers.
*   **Position Manager:** The `reconcile_at_startup` method is a critical safety feature. It fetches live broker positions and active SL orders to ensure no short position is left naked upon system restart.
    *   *Positive:* It explicitly avoids file-based state persistence, mitigating issues with stale data.
    *   *Risk:* It assumes all existing `BUY` orders for short positions are SL orders. If manual limit buy orders were placed for profit booking, they might be misinterpreted.

### 2.3. Greeks & Options Pricing (`wave.py` & `greeks_calculator.py`)
*   The `BlackScholesGreeks` class calculates theoretical Greeks. It parses complex Indian market option symbols cleanly.
*   The `WaveStrategy` calculates real-time portfolio Delta to restrict trades if the portfolio becomes too directionally biased (e.g., Delta < `min_nifty_delta`). This is an excellent feature for managing directional risk in a predominantly non-directional (theta) strategy.

---

## 3. Backend Implementation (`web/backend/`)

### 3.1. API and WebSocket (`main.py`, `routes/`, `websocket/`)
*   **FastAPI Setup:** Standard, clean implementation.
*   **WebSocket streaming:** The `connection_manager` broadcasts real-time prices, strategy state, and position updates. The implementation includes heartbeat mechanisms to keep connections alive, which is best practice.
*   **Background Tasks:** `state_broadcast_loop` pushes data out efficiently at 1-second intervals.

### 3.2. Services (`services/`)
*   **`strategy_manager.py`:** A robust lifecycle manager. It spawns the strategy as a separate process and communicates via a `multiprocessing.Queue`. It includes automated Telegram error reporting and emergency stops for "Insufficient Funds", which is a highly practical failsafe.
*   **`broker_service.py`:** Acts as a solid facade over the underlying broker driver. It properly catches and sanitizes exceptions (preventing API keys from leaking to the frontend).
*   **`live_data_manager.py`:** Handles real-time syncing of the broker's order book and position book into memory, broadcasting changes via WebSockets.

**Backend Risks:**
*   **Multiprocessing Queue Congestion:** If the strategy generates state updates faster than the `_monitor_state` asyncio loop consumes them, the queue could grow unbounded. A bounded queue size or batching might be safer.
*   **Rate Limits:** Polling `broker.get_positions()` and `broker.get_orderbook()` every 1 second in `LiveDataManager._fetch_and_update()` is very aggressive. Depending on the Zerodha Kite Connect API tier, this could lead to `429 Too Many Requests` bans. *Recommendation:* Rely more on the broker's WebSocket order updates (`on_order_update`) rather than aggressive REST polling.

---

## 4. Frontend Implementation (`web/frontend/`)

The frontend is a modern Next.js 14 application using React, Tailwind CSS, and Recharts.

*   **Architecture:** Clean component hierarchy. Pages fetch initial data via REST and hydrate with live data via the `useWebSocket` hook.
*   **Components:** `VisualEngine`, `GreeksDisplay`, and `PayoffChart` provide a highly sophisticated view of the trading system. The `VisualEngine` specifically maps strategy state (like RSI/EMA filters and entry predictions) into a digestible UI.
*   **Robustness:** The custom `useWebSocket` hook implements automatic reconnection logic with exponential backoff (up to 10 attempts), ensuring resilience against network blips.

---

## 5. Summary & Key Recommendations

The codebase is exceptionally well-structured for an automated trading system. The implementation of specific defensive mechanisms (startup reconciliation, gap risk management, volatility-based sizing) shows a deep understanding of the risks associated with option selling.

**Critical Recommendations for Production:**
1.  **Broker API Rate Limiting:** The `LiveDataManager` polling the broker REST API every 1 second is extremely dangerous for Zerodha API limits. Switch to updating internal state purely via the `on_order_update` WebSocket callback, and only poll REST as a fallback (e.g., every 30-60 seconds) to reconcile state.
2.  **Market Order Slippage:** Replace `MARKET` orders in `survivor.py` with `LIMIT` orders priced aggressively (e.g., LTP + 5% for buys, LTP - 5% for sells) to protect against freak trades or momentary illiquidity, which is common in NFO options.
3.  **State Persistence:** While relying on live broker state for positions is correct, the strategy's *internal* reference levels (`nifty_pe_last_value`) should optionally be persisted to a fast datastore (like Redis or a simple JSON file) so that intraday restarts do not completely reset the strategy's grid logic.
4.  **Error Handling for Greeks:** The Newton-Raphson implied volatility calculator in `greeks_calculator.py` can fail to converge. Ensure that downstream logic (like `WaveStrategy`) gracefully handles `None` values for volatility and Greeks.