# Code Review Report: Survivor Automated Trading System

## 1. Executive Summary

This is a comprehensive code review of the Python-based automated trading strategy focused on theta decay via options selling on the NIFTY index using the Zerodha broker. The review strictly covers the `web` and `strategy` folders, evaluating system architecture, strategy logic, risk management, and backend services.

The system is a sophisticated multi-component architecture utilizing a FastAPI backend, websockets for real-time state, and Python `multiprocessing` to isolate trading loops. The trading intelligence is primarily driven by `SurvivorStrategy` which shows robust options selling features.

---

## 2. Architecture & System Design

### Strengths
- **Process Isolation**: The `StrategyManager` utilizes Python's `multiprocessing` to spawn trading strategies in isolated processes. This ensures heavy strategy calculations or blocking I/O (like tick loop processing) do not block the FastAPI main event loop.
- **WebSocket Broadcasting**: `main.py` uses modern FastAPI `asynccontextmanager` lifespans for managing background broadcast tasks (`state_broadcast_loop` and `heartbeat_loop`).
- **In-Memory Streaming**: The `LiveDataManager` successfully uses an in-memory storage approach that streams positions and orders via broker fetches, ensuring state is decoupled from disk persistence.

### Areas for Improvement
- **WebSocket Error Recovery**: `live_data_manager.py` restricts error streaming loops to a `_max_errors` limit (5), after which it stops entirely. This could leave the UI stale without automatic reconnection if network connectivity temporarily drops.
- **Process Communication overhead**: High-frequency tick data is passed across process boundaries using a `multiprocessing.Queue`. For extremely fast tick bursts, this could introduce serialization latency.

---

## 3. Strategy Logic & Risk Management (`SurvivorStrategy`)

The `SurvivorStrategy` class (`strategy/survivor.py`) is the core engine for generating options selling orders based on theta decay.

### Strengths
- **Technical Filters**: Incorporates well-structured filters (RSI, EMA, ADX) to avoid entering trades in highly adverse trends (e.g., selling a Call Option during a strong uptrend).
- **Dynamic Gap Sizing**: Leverages ATR (Average True Range) to adjust entry distances. Wider gaps in high-volatility environments reduce the likelihood of immediate stop-loss hits.
- **Comprehensive Risk Controls**: Implements multiple layers of protection:
  - Broker-level Stop-Loss (OCO-like behavior placing SL limits right after a fresh order).
  - Configurable profit target checking every interval.
  - Max consecutive losses tracking.
  - Gap Risk Management integration: Suspends trading if morning overnight gap > 1.5%.
  - SL Reconciliation on startup: Identifies unprotected short positions and places SL orders automatically if the system restarted.

### Areas for Improvement
- **Profit Target Loop Delay**: Profit targets are checked by default every 5 minutes (`profit_check_interval = 300`). In a volatile market, an option could hit the 60% profit target and bounce back before the interval hits. A continuous stream-check or OCO limit order for profit would be safer.
- **File Length & Complexity**: `survivor.py` is over 3,100 lines long. Methods like `_handle_pe_trade_enhanced` and `_handle_ce_trade_enhanced` share almost identical logic and could be heavily refactored/DRY'd out into generic functions.

---

## 4. Backend Services & Routes

### Strengths
- **Strategy Registry Pattern**: `StrategyRegistry` effectively manages the metadata, validation, and dynamic class loading of different strategies (e.g., `survivor`, `wave`). This makes extending the system highly maintainable.
- **Error Sanitization**: `BrokerService` appropriately catches exceptions and ensures empty arrays or sanitized responses are returned to the API so frontend components do not crash.
- **Extensive API Surface**: `strategy_selector.py` provides excellent configuration preview endpoints (`/strategy/preview-config`) and configuration validations before a user actually starts a process.

### Areas for Improvement
- **Error Handling in Main API**: In `main.py`'s websocket endpoint, raw `json.JSONDecodeError`s are handled, but specific exceptions inside `handle_websocket_message` could potentially surface and terminate the connection loop unexpectedly if not wrapped tightly.

---

## 5. `WaveStrategy` Review (`strategy/wave.py`)

### Strengths
- The `WaveStrategy` establishes basic boundary setups by implementing generic `buy_gap` and `sell_gap` limit configurations.

### Areas for Improvement
- **Configuration Validation**: The script has a highly verbose manual check script directly inside the file for configuration overrides. This logic should ideally be offloaded to the centralized `strategy_registry.py` validation.

---

## 6. Recommendations & Action Items

1. **Refactor `SurvivorStrategy`**: Extract the indicator calculations (`_calculate_rsi`, `_calculate_ema`) into a separate `technical_indicators.py` utility module. Extract duplicate PE/CE trade handling logic into a unified `_handle_trade(option_type: str, current_price, indicators)` method.
2. **Optimize Profit Target Execution**: Move profit booking from a periodic 5-minute interval check to either a per-tick evaluation or place explicit Limit orders at the broker level immediately after placing the SL orders to guarantee execution.
3. **Enhance Live Data Reconnects**: Implement an exponential backoff retry mechanism in `LiveDataManager` rather than permanently halting the stream after 5 errors.
4. **Reduce IPC Overhead**: Consider batching tick data inside `dispatcher.py` if tick velocity increases, to reduce the overhead of passing thousands of individual dictionaries across multiprocessing queues.