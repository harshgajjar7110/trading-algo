# Code Review Report: Survivor & Wave Strategies and Web Backend

## 1. Executive Summary

This code review assesses the Python-based automated trading strategies (`survivor` and `wave`) and their integration with the FastAPI backend (`web/backend`) for a Zerodha-based options selling system.

**Overall Assessment:** The system has a robust architectural foundation, implementing the `StrategyManager` using Python `multiprocessing` to isolate strategy execution from the FastAPI web server. The code handles real-time WebSocket communication, dynamic configuration, and extensive risk management well. However, there are significant areas for improvement, particularly regarding error handling consistency, race conditions in multiprocessing, type safety, and the removal of legacy/commented-out code.

---

## 2. Architecture & Design

### Strengths
*   **Separation of Concerns:** The backend effectively separates route definitions (`app/routes`), service business logic (`app/services`), WebSocket handling (`app/websocket`), and core strategy implementations (`strategy/`).
*   **Multiprocessing Strategy Isolation:** The `StrategyManager` uses `multiprocessing.Process` to run strategies. This is a vital design choice, ensuring that heavy strategy computations or blocking calls do not halt the FastAPI asyncio event loop.
*   **Registry Pattern:** The `StrategyRegistry` allows for dynamic loading and configuration validation of different strategies (`survivor`, `wave`), making the system extensible.

### Areas for Improvement / Risks
*   **Process Management:** In `StrategyManager.stop()`, the process termination sequence uses `.terminate()` followed by a 10-second timeout, then `.kill()`. However, abrupt termination of the strategy process might leave the broker connection or order state hanging. Strategies should ideally catch a termination signal (like a `multiprocessing.Event` flag or `KeyboardInterrupt`) and run their internal `cleanup()` methods before exiting.
*   **Singleton Pattern Enforcement:** Services like `StrategyManager`, `BrokerService`, and `StrategyRegistry` are implemented as singletons using `__new__`. While this works, ensuring thread safety within these singletons (e.g., locking access to `_current_instance` or `_status`) when accessed concurrently by different FastAPI routes is critical.
*   **Inter-process Communication (IPC):** State updates from the strategy process back to the main API process are handled via a `multiprocessing.Queue`. The API process periodically polls this queue (`_monitor_state`). If the strategy process generates updates too quickly, this queue could fill up or consume excessive memory. Bounding the queue size (e.g., `multiprocessing.Queue(maxsize=...)`) is recommended.

---

## 3. Survivor Strategy Specifics (`strategy/survivor.py`)

The Survivor strategy is a trend-following options seller (sells PE on rise, CE on fall) using technical indicators and ATR-based dynamic gaps.

### Strengths
*   **Comprehensive Risk Management:** Implements daily loss limits, consecutive loss checks, dynamic gap adjustments based on ATR, and day-of-week position sizing multipliers.
*   **SL Order Reconciliation:** The `reconcile_sl_at_startup` method effectively guards against unprotected positions persisting after system restarts.
*   **Trailing Stop Loss:** A well-structured `_check_enhanced_trailing_sl` method dynamically locks in profits.

### Areas for Improvement / Risks
*   **Market Order Price Assumptions:** The strategy primarily places `MARKET` orders for entries (`price=0`). For options, especially illiquid strikes, market orders can lead to severe slippage. It is highly recommended to use `LIMIT` orders based on the current `quote.last_price` with a slight buffer, or verify liquidity before placing market orders.
*   **Duplicate Order Prevention Logic Risk:** In `_place_order_enhanced`, there is logic to prevent duplicate orders: `if symbol in self.positions: return False`. However, if an order is placed but the position update fails or is delayed, this could lead to issues. It relies on in-memory state tracking which is wiped on restart, hence why `initialize_positions_from_broker` is crucial but also susceptible to timing bugs.
*   **Timer Reset Hack:** In `_place_order_enhanced`: `self.last_profit_check_time = datetime.now() - timedelta(seconds=self.profit_check_interval + 1)`. While commented as a "BUG FIX" to force immediate checking, it forces a check for *all* positions, not just the new one, which might violate the intended interval logic.
*   **Exception Handling in Loops:** The `while True` loop finding instruments (`_find_nifty_symbol_from_gap`) has a mechanism to decrement the gap (`temp_gap -= self.lot_size`) if the premium is too low. However, there is no hard circuit breaker (e.g., max 20 iterations). If premium is low across all strikes, this could result in an infinite loop blocking the strategy.

---

## 4. Wave Strategy Specifics (`strategy/wave.py`)

The Wave strategy handles simultaneous offset buy/sell orders and relies heavily on delta restrictions.

### Strengths
*   **Options Greeks Calculation:** Utilizes the `mibian` library correctly to calculate Delta dynamically for the portfolio.
*   **Dynamic Restrictions:** `_get_dynamic_restrictions` smartly restricts buy/sell sides based on whether the total portfolio delta breaches configured NIFTY/BankNifty minimums/maximums.

### Areas for Improvement / Risks
*   **`self.quantity` vs `self.sell_quantity`:** In the initialization, `self.quantity = self.sell_quantity` is explicitly marked with `# TODO: Confirm with Vibhu`. Later in `place_wave_order`, the diff scale comment says `# Quantity should be removed and not used`. This ambiguity in position sizing mathematics needs resolution to prevent unintended exposure.
*   **State Tracking Deletion Risk:** The code relies on deleting items from dictionaries while tracking orders (e.g., `del self.orders[order_id]`). While `list(self.orders.items())` is used in loops to prevent `RuntimeError: dictionary changed size during iteration`, this is brittle if state updates come asynchronously from the broker callback `handle_order_update`.
*   **Broker Dependency:** Uses `from brokers.zerodha import ZerodhaBroker` in commented code but relies on the `BrokerGateway`. It seems partially migrated to the new gateway architecture. Needs complete verification that all broker calls (e.g., `cancel_order`) map perfectly to the gateway.
*   **Associated Orders Logic:** The linkage between `buy_order_id` and `sell_order_id` via `associated_order` is complex. If a partial fill occurs on one leg, cancelling the associated leg might leave the portfolio delta imbalanced.

---

## 5. Web Backend & Services (`web/backend/app/services/`)

### Strengths
*   **LiveDataManager:** Efficiently caches positions and orders in-memory and pushes updates via WebSockets.
*   **Strategy Status Transparency:** Generates a rich "Visual State" (`get_visual_state`) detailing exactly why a strategy is blocking trades (e.g., RSI overbought, Gap Risk). This is excellent for debugging and user trust.

### Areas for Improvement / Risks
*   **Broker Authentication Leakage:** In `BrokerService.get_positions`, there is error sanitization (`if "api_key" in error_msg.lower()`). However, `broker.get_positions()` might return an empty list when unauthenticated instead of raising an error, which the code tries to catch by calling `get_funds()`. This indicates the underlying `BrokerGateway` should standardize its authentication error raising rather than relying on the service layer to guess based on empty lists.
*   **WebSocket State Bottleneck:** The `state_broadcast_loop` in `main.py` broadcasts the strategy state and NIFTY price every 1 second continuously. If there are many connected clients, this could become a bottleneck. Using a push mechanism (broadcasting only when state actually changes) rather than a continuous poll-and-broadcast loop would be more efficient.

---

## 6. Recommendations & Action Items

1.  **Implement Circuit Breakers:** Add a maximum retry limit to the `while True` loop inside `SurvivorStrategy._handle_pe_trade_enhanced` and `_handle_ce_trade_enhanced` to prevent infinite loops when searching for strikes.
2.  **Graceful Process Termination:** Modify `StrategyManager.stop()` to send a stop signal to the strategy queue, allowing the strategy to exit its `while True` loop, cancel open limit orders, and close cleanly before `process.join()` times out.
3.  **Review Order Types:** Migrate from `MARKET` to `LIMIT` orders for option selling entries in `SurvivorStrategy` to prevent catastrophic slippage.
4.  **Resolve Wave Strategy "TODOs":** Clear up the logic regarding `self.quantity` vs `self.buy_quantity/sell_quantity` in `wave.py` to ensure position sizing calculations are mathematically sound.
5.  **Thread Safety on Dictionaries:** Ensure that `self.orders` in `wave.py` and `self.positions` in `survivor.py` are protected by a lock (e.g., `threading.Lock`) if the broker's WebSocket callbacks (`on_order_update`) execute on a different thread than the main strategy loop.
6.  **Type Hinting:** Increase the use of strict type hinting across the strategy implementations to catch variable state errors early (e.g., ensuring `price` is always `float` and not sometimes `int` or `None`).
