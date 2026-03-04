# Survivor Strategy Memory Optimization Plan

## Current Memory Issues Identified

### 1. Instruments DataFrame (~5-10 MB per strategy instance)
**Current Behavior:**
- Downloads entire master contract (50,000+ instruments × 12 columns)
- Filters to symbol_initials but keeps all columns
- Stores as pandas DataFrame for entire strategy lifetime
- Repeated `.to_dict()` calls create copies

**Memory Impact:**
- DataFrame overhead: ~60 bytes per cell
- 50k rows × 12 cols × 60 bytes ≈ 36 MB raw data
- Plus pandas overhead: ~50-100 MB per strategy

### 2. Historical Data Storage (~2-5 MB per strategy)
**Current Behavior:**
- `history_data` list stores up to 2000 candles as dictionaries
- Each candle: `{'ts': float, 'open': float, 'high': float, 'low': float, 'close': float}`
- List `pop(0)` is O(n) - causes periodic lag spikes

**Memory Impact:**
- 2000 candles × 5 fields × overhead ≈ 2-5 MB
- Fragmentation from dict-per-candle approach

### 3. Indicator Calculations (~5-15 MB per calculation)
**Current Behavior:**
- Every `_calculate_indicators()` call creates new DataFrame: `df = pd.DataFrame(self.history_data)`
- Creates intermediate columns (up_move, down_move, plus_dm, minus_dm, tr, atr, etc.)
- DataFrame discarded after each calculation

**Memory Impact:**
- 2000 rows × 15+ intermediate columns × 60 bytes ≈ 1.8 MB per calc
- Frequent garbage collection pressure

### 4. Strike Symbol Lookups (~1-2 MB per lookup)
**Current Behavior:**
- `_find_instrument_for_strike()` filters DataFrame on each call
- Returns `.to_dict(orient="records")[0]` - creates copy
- Called frequently during trading hours

## Proposed Optimizations

### Optimization 1: Lightweight Instrument Storage
```python
# BEFORE: Keep full DataFrame
self.instruments = self.broker.get_instruments()
self.instruments = self.instruments[self.instruments['symbol'].str.contains(...)]

# AFTER: Extract only needed fields into dict
instruments_df = self.broker.get_instruments()
filtered = instruments_df[instruments_df['symbol'].str.contains(...)]
self._instrument_cache = {
    row['symbol']: {
        'strike': row['strike'],
        'instrument_type': row['instrument_type'],
        'lot_size': row['lot_size'],
        'token': row['instrument_token']
    }
    for _, row in filtered.iterrows()
}
del instruments_df, filtered  # Free DataFrame memory
```

**Expected Savings:** ~40-80 MB per strategy

### Optimization 2: Deque-Based History with Fixed Capacity
```python
# BEFORE: List with manual size management
self.history_data = []
# ... append ...
if len(self.history_data) > 2000:
    self.history_data.pop(0)  # O(n)!

# AFTER: Deque with maxlen
from collections import deque
self.history_data = deque(maxlen=2000)
# ... append ...
# Automatic O(1) eviction at maxlen
```

**Expected Savings:** More predictable memory + better performance

### Optimization 3: Pre-allocated Numpy Arrays for Indicators
```python
# BEFORE: New DataFrame each calculation
df = pd.DataFrame(self.history_data)
df['ema'] = df['close'].ewm(span=period, adjust=False).mean()

# AFTER: Rolling calculation with state preservation
# Keep only last N values in numpy arrays
self._closes = np.zeros(maxlen)
self._ema = np.zeros(maxlen)
# Update incrementally instead of full recalc
```

**Expected Savings:** ~5-10 MB per tick, faster execution

### Optimization 4: Strike Lookup Cache
```python
# BEFORE: DataFrame filter every time
df = self.instruments[...]
best = df.sort_values('target_strike_diff').iloc[0]
return best.to_dict()

# AFTER: Pre-built strike-indexed cache
# Build once at init: {strike_price: instrument_dict}
# Lookup via binary search or nearest-key dict
```

**Expected Savings:** ~1-2 MB per lookup, faster execution

### Optimization 5: Memory-Efficient Candle Storage
```python
# BEFORE: List of dicts
{'ts': 1234567890.0, 'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5}

# AFTER: Namedtuple or simple class
from collections import namedtuple
Candle = namedtuple('Candle', ['ts', 'open', 'high', 'low', 'close'])
# Or use __slots__ for even less memory
```

**Expected Savings:** ~30-40% reduction in history_data memory

## Implementation Phases

### Phase 1: Instrument Storage (High Impact, Low Risk)
- Modify `__init__` to extract minimal fields
- Update `_find_instrument_for_strike` to use cache
- Clear DataFrame references after init

### Phase 2: History Data Structure (Medium Impact, Low Risk)
- Replace list with deque
- Update `_update_history` and `_calculate_indicators`
- Maintain backward compatibility for history access

### Phase 3: Indicator Optimization (High Impact, Medium Risk)
- Implement incremental RSI/ADX calculations
- Add rolling EMA with state
- Requires thorough testing

### Phase 4: Monitoring (Low Impact, Low Risk)
- Add memory usage logging
- Configurable limits
- Optional tracemalloc integration

## Testing Strategy

1. **Memory Profiling:**
   ```python
   import tracemalloc
   tracemalloc.start()
   # Run strategy for simulated trading day
   current, peak = tracemalloc.get_traced_memory()
   ```

2. **Behavior Verification:**
   - Same trades generated before/after
   - Same indicator values calculated
   - Same order placements

3. **Stress Testing:**
   - 8-hour trading session simulation
   - High-frequency tick processing (1000+ ticks/minute)
   - Multiple strategy instances

## Expected Total Memory Reduction

| Component | Before | After | Savings |
|-----------|--------|-------|---------|
| Instruments | 80 MB | 5 MB | 75 MB (94%) |
| History Data | 5 MB | 3 MB | 2 MB (40%) |
| Indicators | 10 MB/tick | 0.5 MB/tick | 9.5 MB (95%) |
| Lookups | 2 MB | 0.1 MB | 1.9 MB (95%) |
| **Total** | **~97 MB** | **~8.6 MB** | **~88 MB (91%)** |
