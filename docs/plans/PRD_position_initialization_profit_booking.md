# PRD: Position Initialization and Profit Booking Fix

## Problem Statement
Profit booking is not working because `self.positions` is empty when the strategy starts with existing broker positions.

### Root Causes Identified
1. **Primary**: `self.positions` dictionary is never populated from broker positions at startup
2. **Secondary**: `sl_enabled: false` prevents PositionManager creation, so reconciliation is skipped entirely
3. **Tertiary**: Position initialization is coupled with SL reconciliation logic

## Current Flow Issues
```
Current Execution Flow:
1. WebSocket connects (ticks start arriving)
2. 10-second sleep
3. Strategy initialized (self.positions = {})
4. reconcile_sl_at_startup() called
   - Returns early if sl_enabled=false OR PositionManager is None
   - Never populates self.positions
5. check_profit_targets_at_startup() called
   - Exits early because self.positions is empty
6. Main loop starts
   - on_ticks_update() called
   - _check_profit_targets() skipped because self.positions is empty
```

## Requirements

### Functional Requirements
1. **R1**: Load positions from actual broker at startup (not just state file)
2. **R2**: Match broker positions with pending orders to determine protection status
3. **R3**: Populate `self.positions` regardless of SL or profit booking settings
4. **R4**: Profit booking check should run independently of SL reconciliation
5. **R5**: Remove debug print statements and duplicate code

### Non-Functional Requirements
1. **N1**: Maintain backward compatibility with existing SL reconciliation
2. **N2**: Ensure no duplicate SL orders are placed
3. **N3**: Comprehensive logging for debugging

## Proposed Solution

### Architecture Changes

#### 1. New Method: `initialize_positions_from_broker()`
**Purpose**: Load positions directly from broker and populate `self.positions`

**Logic**:
```python
def initialize_positions_from_broker(self) -> int:
    1. Fetch positions from broker (broker.get_positions())
    2. Fetch pending orders from broker (broker.get_orderbook())
    3. For each position:
       a. Extract symbol, quantity, average_price
       b. Determine option_type from symbol (CE/PE)
       c. Check if SL order exists in pending orders
       d. Calculate profit_target_price
       e. Create PositionInfo and add to self.positions
    4. Return count of initialized positions
```

#### 2. Modified Execution Flow
```
New Execution Flow:
1. WebSocket connects
2. 10-second sleep
3. Strategy initialized (self.positions = {})
4. **initialize_positions_from_broker()** ← NEW
   - Fetches real positions from broker
   - Populates self.positions
   - Runs regardless of flags
5. check_profit_targets_at_startup()
   - Now has positions to check
6. reconcile_sl_at_startup() (if sl_enabled)
   - Updates existing positions with SL info
   - Places SL orders for unprotected positions
7. Main loop starts
   - on_ticks_update() has positions to work with
```

#### 3. Code Cleanup
- Remove debug print statements (lines 1110-1112)
- Consolidate duplicate logic between methods
- Remove `initialize_positions_from_state()` (redundant with broker-based approach)

## Implementation Plan

### Phase 1: Code Audit and Cleanup
- [ ] Remove debug print statements
- [ ] Identify duplicate code between reconcile_sl_at_startup and initialize_positions_from_state

### Phase 2: Core Implementation
- [ ] Implement `initialize_positions_from_broker()` method
- [ ] Add broker position fetching logic
- [ ] Add pending order matching logic
- [ ] Integrate into main execution flow

### Phase 3: Testing
- [ ] Test with sl_enabled=false, profit_target_enabled=true
- [ ] Test with both enabled
- [ ] Verify no duplicate SL orders

## File Changes

### 1. strategy/survivor_enhanced.py
- Remove `initialize_positions_from_state()` method (redundant)
- Add `initialize_positions_from_broker()` method
- Modify main execution flow (around line 1570)
- Remove debug prints (lines 1110-1112)

### 2. strategy/position_manager.py
- May need to expose `_fetch_broker_positions()` as public method
- Or duplicate the logic in survivor_enhanced.py

## Rollback Plan
If issues occur:
1. Revert to original `initialize_positions_from_state()` approach
2. Keep the new method as fallback

## Success Criteria
1. `self.positions` is populated at startup regardless of `sl_enabled` setting
2. Profit booking works when `profit_target_enabled=true` and `sl_enabled=false`
3. No duplicate SL orders are placed
4. All existing tests pass
