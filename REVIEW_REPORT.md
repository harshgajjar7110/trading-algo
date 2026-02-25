# Code Review Report: Automated Trading Strategy

**Date:** February 2025
**Reviewer:** Automated Code Review Agent
**Scope:** `web/` folder, `strategy/` folder (excluding `sensibull/`)
**Primary Focus:** Logic correctness, performance, safety, and code quality.

---

## Executive Summary

The codebase implements an algorithmic trading platform primarily focused on options selling strategies (Survivor) and delta-neutral strategies (Wave). It features a Python-based backend using FastAPI (`web/backend`) and a frontend (Next.js, not deeply reviewed but noted). The architecture separates strategy logic (`strategy/`) from execution and management (`web/backend/app/services`), using `multiprocessing` for isolation.

Overall, the codebase is functional and demonstrates good separation of concerns. The strategies include sophisticated risk management features like dynamic gaps, technical filters (RSI, EMA, ADX), and automatic stop-loss reconciliation. However, there are some critical bugs in configuration management, potential stability issues with error handling, and some code duplication.

## Critical Findings (High Priority)

### 1. Configuration Reset Bug (`web/backend/app/routes/config.py`)
In `web/backend/app/routes/config.py`, the `reset_configuration` endpoint calls a non-existent method:
```python
@router.post("/reset", response_model=StrategyConfig)
async def reset_configuration():
    strategy_manager._load_config()  # CRITICAL: This method does not exist
    return strategy_manager.get_config()
```
The `StrategyManager` class (in `strategy_manager.py`) defines `_load_legacy_config()`, not `_load_config()`. This will cause an `AttributeError` at runtime when this endpoint is hit.
**Recommendation:** Rename the call to `_load_legacy_config()`.

### 2. Error Swallowing in Broker Service (`web/backend/app/services/broker_service.py`)
The `BrokerService.get_positions` method catches all exceptions and returns an empty list/error message. While this prevents the API from crashing, it can mask critical connectivity or authentication issues, making debugging difficult.
```python
except Exception as e:
    error_msg = str(e)
    # ... checks for auth errors ...
    return PositionsResponse(positions=[], ..., error=error_msg)
```
**Recommendation:** Ensure all exceptions are logged with full tracebacks (which is currently done via `print`, but structured logging is preferred). Consider raising specific exceptions for authentication failures so the frontend can prompt the user properly.

### 3. Hardcoded System Paths (`strategy/survivor.py`)
The strategy files use `sys.path.append` to modify the Python path dynamically:
```python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```
This is fragile and depends on the specific directory structure.
**Recommendation:** Use proper package installation (e.g., `pip install -e .`) or run modules with `python -m strategy.survivor` to handle imports cleanly.

## Detailed Module Review

### 1. Strategy Module (`strategy/`)

#### `strategy/survivor.py` (Survivor Strategy v2.0)
*   **Strengths:**
    *   **Risk Management:** comprehensive features including SL reconciliation (`reconcile_sl_at_startup`), dynamic gaps based on ATR, and technical filters.
    *   **State Management:** Good handling of restart/recovery scenarios (reconciling SL orders).
    *   **Logic:** The core logic for gap-based trading and resetting reference values appears sound.
    *   **Safety:** Iteration over collections is generally handled safely (e.g., iterating over `list(self.positions.items())` or collecting keys before modification).
*   **Weaknesses:**
    *   **Logging:** `logger.critical` is used for method entry (`INITIALIZING POSITIONS FROM BROKER`), which is inappropriate severity for normal operation.
    *   **Complexity:** The class is quite large (~1000 lines). Could benefit from further splitting (e.g., moving technical indicators to a separate utility class).
    *   **Time-Based Logic:** `check_profit_targets_at_startup` relies on `PositionManager` state. If the state is stale or time-dependent logic is involved, it might behave unexpectedly on restarts.

#### `strategy/wave.py` (Wave Strategy)
*   **Strengths:**
    *   **Concept:** Implements a delta-neutral approach with dynamic adjustments.
    *   **Greeks Calculation:** Integrates `mibian` for real-time Greeks calculation.
*   **Weaknesses:**
    *   **Code Quality:** Contains several `TODO` comments and commented-out code blocks.
    *   **Iteration Safety:** `check_and_enforce_restrictions_on_active_orders` correctly iterates over a copy of items (`list(self.orders.items())`), which is good. However, `check_is_any_order_active` iterates directly, which is safe only because it doesn't modify the dict.
    *   **Blocking Calls:** `time.sleep(3)` in `place_wave_order` blocks the main execution loop. In a high-frequency context, this is undesirable, though acceptable for this specific low-frequency strategy.

#### `strategy/position_manager.py`
*   **Strengths:**
    *   **Responsibility:** Cleanly separates SL order management from the main strategy logic.
    *   **Safety:** Explicitly avoids file persistence to prevent stale state, relying on live broker data.
*   **Weaknesses:**
    *   **Coupling:** Tightly coupled to Zerodha's specific order status strings.

### 2. Web Backend (`web/backend/`)

#### `app/services/strategy_manager.py`
*   **Strengths:**
    *   **Architecture:** Uses `multiprocessing` to isolate strategy execution, preventing a strategy crash from bringing down the API.
    *   **Flexibility:** Supports both single-strategy (legacy) and multi-strategy modes.
*   **Weaknesses:**
    *   **Process Management:** `_stop_on_error` uses `process.terminate()`/`kill()`. This is aggressive and might leave open file handles or zombie processes if not handled by the OS correctly.
    *   **State Synchronization:** Uses `multiprocessing.Queue` for state updates. If the queue fills up (not consumed fast enough), the strategy process could block. Currently, it consumes with `get_nowait`, which is good.

#### `app/routes/config.py`
*   **Issues:**
    *   **Duplicate Validation:** Validates configuration manually (e.g., `symbol_initials` length, `pe_gap` positive). This logic duplicates validation that should be central (e.g., in `StrategyRegistry` or Pydantic models).
    *   **Bug:** The `reset_configuration` bug mentioned above.

#### `app/services/broker_service.py`
*   **Issues:**
    *   **Error Handling:** Relies on string matching (`"unauthenticated"`, `"access_token"`) to detect auth errors. This is brittle.
    *   **Refactoring:** `reinitialize` method sets `_broker = None` but doesn't immediately re-init. This is lazy loading, which is fine, but might cause a delay on the next request.

## Recommendations

1.  **Fix Critical Bugs:**
    *   Correct the method call in `reset_configuration` to `strategy_manager._load_legacy_config()`.
2.  **Improve Error Handling:**
    *   Implement a custom exception hierarchy for Broker errors (`AuthenticationError`, `NetworkError`).
    *   Update `BrokerService` to raise these exceptions instead of returning generic error strings/empty lists.
3.  **Refactor Configuration:**
    *   Centralize configuration validation in Pydantic models (in `app/models/schemas.py`) to ensure consistency between the API and the backend logic.
    *   Remove duplicate validation code in `app/routes/config.py`.
4.  **Enhance Logging:**
    *   Review log levels. Use `INFO` or `DEBUG` for operational events, and `CRITICAL` only for application-crashing events.
5.  **Code Cleanup:**
    *   Remove commented-out code and resolve `TODO`s in `strategy/wave.py`.
    *   Standardize imports in strategy files to avoid `sys.path` manipulation.

## Conclusion

The platform is well-structured for an algorithmic trading system. The core logic for `SurvivorStrategy` is robust regarding risk management. The main areas for improvement are in the "plumbing" (configuration, error handling, logging) rather than the trading logic itself. Addressing the critical configuration bug is the immediate priority.
