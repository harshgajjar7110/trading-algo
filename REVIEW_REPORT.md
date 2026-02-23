# Detailed Code Review Report

This report provides a comprehensive review of the Python-based automated trading strategy codebase, focusing on the `web/` and `strategy/` directories.

## Executive Summary

The project implements an algorithmic trading platform with a FastAPI backend, a Next.js frontend, and multiple trading strategies integrated with Zerodha (Kite Connect). The primary strategy, "Survivor", is an option selling strategy designed to capture premium decay (Theta) by entering positions based on NIFTY index movements.

Overall, the codebase is well-structured, modular, and demonstrates good practices in separating concerns (strategies vs. broker integration vs. web interface). However, there are areas for improvement in error handling, race condition management, and security.

---

## 1. Strategies Review (`strategy/` folder)

### 1.1 `SurvivorStrategy` (`strategy/survivor.py`)
**Type:** Trend-Following Option Selling (Short Strangle/Straddle variation)

*   **Logic:**
    *   Monitors NIFTY price via WebSocket ticks.
    *   **PE Entry:** Sells PE options when NIFTY rises by a configurable `pe_gap` (selling into strength/momentum).
    *   **CE Entry:** Sells CE options when NIFTY falls by a configurable `ce_gap` (selling into weakness/momentum).
    *   **Sizing:** Uses a "multiplier" based on the magnitude of the move (e.g., if price moves 2x the gap, sell 2x the quantity).
    *   **Reset:** Adjusts reference levels when the market moves favorably to trail the price and lock in the range.
*   **Theta Decay:**
    *   By selling OTM/ATM options and holding them, the strategy naturally benefits from Theta decay as time passes, provided the market doesn't move adversely beyond the premium received.
*   **Critique:**
    *   **Trend Logic:** Selling PE on a rise is counter-intuitive for a standard "fade" strategy (usually you sell PE on a dip). However, as a "momentum" short selling strategy, it bets that the trend will continue or stabilize above the strike.
    *   **Execution:** Uses `MARKET` orders. In illiquid options or fast-moving markets, this can lead to significant slippage.
    *   **Risk:** No built-in Stop Loss in the base class. It relies on the "Reset" mechanism for profit trailing but lacks a hard exit for adverse moves.
    *   **Loop:** The `while True` loop in `_handle_pe_trade` for finding strikes has a potential infinite loop if `temp_gap` adjustments fail to find a valid instrument.

### 1.2 `EnhancedSurvivorStrategy` (`strategy/survivor_enhanced.py`)
**Type:** Advanced Version of Survivor

*   **Enhancements:**
    *   **Filters:** Adds RSI, EMA, ADX, and ATR filters to validate entries (e.g., don't sell PE if RSI is overbought).
    *   **Dynamic Gaps:** Adjusts `pe_gap`/`ce_gap` based on ATR (Volatility), which is a robust feature for adapting to market regimes.
    *   **Risk Management:**
        *   **Stop Loss:** Implements automatic broker-level SL orders (`STOP` or `STOP_LIMIT`) immediately after entry.
        *   **Profit Target:** Exits positions when a configurable profit % (e.g., 60%) is reached. This effectively captures Theta/Delta decay.
        *   **Time Exit:** Squares off at a specific time (e.g., 15:15).
        *   **Reconciliation:** Checks for open positions on startup and places missing SL orders (`PositionManager`).
*   **Critique:**
    *   **Logic:** The addition of filters and SL makes this strategy much more robust than the base version.
    *   **State Management:** Uses a local JSON file to persist state (`PositionManager`). This is good for recovery but can get out of sync if the file is corrupted or deleted.
    *   **Concurrency:** The `_place_order_enhanced` method has a check-then-act race condition where it checks `self.positions` before placing an order. In a fast-ticking market, multiple threads/processes aren't an issue here (standard Python GIL/single process), but care is needed if moved to async or multi-threaded.

### 1.3 `WaveStrategy` (`strategy/wave.py`)
**Type:** Grid/Wave Trading (Directional/Scalping)

*   **Logic:**
    *   Places BUY and SELL orders around the current price based on `buy_gap` and `sell_gap`.
    *   Uses a multiplier scale based on position imbalance.
    *   Monitors Portfolio Delta (using Black-Scholes via `mibian`) and restricts trades if delta limits are breached.
*   **Theta Decay:**
    *   Less focused on pure Theta decay compared to Survivor. It seems to be trying to capture small price oscillations (scalping).
*   **Critique:**
    *   **Complexity:** Managing both buy and sell orders with dynamic gaps and delta limits is complex.
    *   **Risk:** "Martingale-like" scaling (multipliers) can be dangerous if the market trends strongly in one direction without reverting.

---

## 2. Web Backend Review (`web/backend/`)

### 2.1 Architecture
*   **Framework:** FastAPI (Python).
*   **Structure:** Service-oriented (`services/`, `routes/`, `models/`).
*   **Process Management:** Uses `multiprocessing` to run strategies in separate processes. This is excellent for isolation; if a strategy crashes, the web server stays alive.
*   **Communication:** Uses `multiprocessing.Queue` to send state updates from strategy to backend.

### 2.2 Key Components
*   **`StrategyManager`:**
    *   Handles starting/stopping the strategy process.
    *   Monitors strategy health and errors (e.g., "Insufficient Funds").
    *   Loads/Saves configuration to YAML.
*   **`BrokerService`:**
    *   Wraps `BrokerGateway` (Zerodha).
    *   Handles authentication checks (token validity).
    *   Normalizes data (Positions, Orders, Quotes) for the frontend.
*   **WebSocket:**
    *   Broadcasts real-time `PRICE_UPDATE` (NIFTY) and `STRATEGY_STATE`.
    *   Allows the frontend to be reactive without polling.

### 2.3 Issues & Recommendations
*   **Error Handling:** While `BrokerService` catches exceptions, some raw error messages might leak to the frontend.
*   **Security:**
    *   **API Keys:** Ensure `kite.trade` (Zerodha) credentials are stored securely (Env vars) and not logged.
    *   **Authentication:** The backend checks for broker auth, but the web app itself seems to rely on the broker's session. Ensure the backend endpoints are protected if exposed publicly.

---

## 3. Web Frontend Review (`web/frontend/`)

### 3.1 Architecture
*   **Framework:** Next.js (React).
*   **UI:** Tailwind CSS for styling.
*   **State:** React Context/Hooks for managing WebSocket connections and data fetching.

### 3.2 Features
*   **Dashboard:** Displays NIFTY price, PnL, Positions, and Strategy Status.
*   **Strategy Selector:** Allows choosing between strategies (`Survivor`, `Enhanced`, etc.) and configuring them before start.
*   **Real-time:** Uses `useWebSocket` hook to update UI components live.

### 3.3 Critique
*   **User Experience:** The UI provides good visibility into what the bot is doing. The "Confirmation" dialog in `StrategySelector` is a good safety feature.
*   **Robustness:** Handles connection loss (WebSocket disconnects) and displays status clearly.

---

## 4. Zerodha Integration

*   **Library:** Uses `kiteconnect` (via `BrokerGateway`/`ZerodhaDriver` in `brokers/`).
*   **Orders:** Supports `MARKET`, `LIMIT`, `SL`, `SL-M` (Stop Loss Market).
*   **Data:** Uses WebSocket (`KiteTicker`) for real-time ticks.
*   **Authentication:** Relies on an external auth flow (likely manual login or TOTP based on `BROKER_TOTP_ENABLE` env var).
*   **Rate Limits:** The strategy loops run on ticks. Ensure that API calls (like `get_quote`, `place_order`) inside the loop do not exceed Zerodha's rate limits (3 requests/sec for some endpoints). `SurvivorStrategy` places orders on ticks, which could trigger rate limits in a volatile market if not throttled.

---

## 5. Theta Decay & Option Logic

*   **Implementation:** The core logic of selling options and holding them is sound for Theta capture.
*   **Profit Booking:** `EnhancedSurvivor`'s logic to exit at X% profit is crucial for Theta strategies to lock in gains and recycle capital.
*   **Strike Selection:** The logic `ltp + gap` ensures options are sold OTM (Out of The Money) or ATM (At The Money) depending on the gap size, which is optimal for Theta decay (ATM has highest Theta, OTM has safer Delta).

## 6. Recommendations

1.  **Risk Controls:**
    *   Implement a **Global Kill Switch** in the backend that cancels all orders and closes all positions immediately in case of emergency.
    *   Add **Max Order Frequency** check to prevent runaway algos from placing thousands of orders in seconds (loop bug).

2.  **Code Improvements:**
    *   **SurvivorStrategy:** Replace `while True` loops with bounded loops (max retries) to prevent getting stuck.
    *   **Logging:** Ensure no sensitive data (auth tokens) is logged in `strategy_logger`.

3.  **Testing:**
    *   Add **Backtesting** support. The current code seems designed for live/paper trading. Integrating `backtrader` or a custom backtester using historical data would help validate the "Gap" logic before risking capital.

4.  **Deployment:**
    *   Ensure the server (FastAPI) and strategy process run in a stable environment (e.g., Docker) with auto-restart capabilities for the web server, but *manual* restart for the strategy (safety first).
