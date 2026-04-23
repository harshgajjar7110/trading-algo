# Survivor Strategy Resilience Redesign

**Date:** 2026-04-23  
**Status:** Approved for implementation  
**Scope:** Full restructure of `strategy/survivor.py` into three collaborating components

---

## Problem Statement

Current strategy has three critical failure modes:

1. **Overnight gap risk** — Naked short options with GTT OCO as only protection. Gap open beyond 30% option price move causes market-order fill at gap price, not SL price. Loss unbounded structurally.
2. **Volatility overtrading** — 40pt fixed gap fires many trades on volatile days. No trade count cap. No dynamic widening.
3. **No daily loss circuit breaker** — MTM loss can compound all day with no halt.

**Capital context:** Small (<₹5L margin), max acceptable loss ₹5–10k/day.

---

## Architecture

```
WebSocket tick
      │
      ▼
 SurvivorStrategy (signal generation only)
      │  ── gap trigger detection (PE / CE)
      │  ── reference value management
      │  ── reset logic
      │  ── calls RiskManager.evaluate() per signal
      │  ── calls PositionLifecycleManager.on_tick() each tick
      │
      ▼
 RiskManager.evaluate(signal_type, current_price)
      │  ── daily loss limit check
      │  ── trade count cap check
      │  ── dynamic gap check (VIX-scaled)
      │
   ALLOW / BLOCK
      │ (if ALLOW)
      ▼
 PositionLifecycleManager.open_spread(signal)
      │  ── place sell leg (SELL MARKET)
      │  ── place hedge buy leg (BUY MARKET, OTM + 300pts)
      │  ── if hedge fails → reverse short leg immediately
      │  ── persist to artifacts/positions.json
      │
      ▼
 BrokerGateway (unchanged)
```

**On startup (before trading loop):**
```
PositionLifecycleManager.reconcile_on_open()
      │  ── load positions.json
      │  ── get_quote for each short leg
      │  ── if price > 2× entry_price → immediate buy-back at market
      │  ── update positions.json
```

---

## Component 1: RiskManager

**File:** `strategy/risk_manager.py`  
**Pattern:** Stateless evaluator with mutable daily counters (reset at midnight / strategy start)

### Responsibilities

- Evaluate whether a new trade signal is allowed
- Track daily MTM loss (read from `PositionLifecycleManager`)
- Track daily trade count (PE + CE combined)
- Compute effective gap (base gap scaled by VIX)

### Interface

```python
class RiskManager:
    def __init__(self, config: dict, broker: BrokerGateway, position_manager: PositionLifecycleManager):
        ...

    def evaluate(self, signal_type: str, current_price: float) -> tuple[bool, str]:
        """
        Returns (allowed: bool, reason: str).
        Checks in order:
          1. daily_loss_limit
          2. max_trades_per_day
          3. effective_gap (dynamic)
        """

    def get_effective_gap(self, base_gap: float) -> float:
        """
        Fetches India VIX quote. Scales base_gap by VIX ratio.
        vix_ratio = current_vix / vix_baseline
        effective_gap = base_gap * vix_ratio
        Clamps to [base_gap, base_gap * vix_max_multiplier]
        """

    def reset_daily_counters(self):
        """Called at strategy start and midnight."""
```

### Configuration (added to survivor.yml)

```yaml
risk:
  daily_loss_limit: 8000          # rupees MTM loss, halts new trades
  max_trades_per_day: 6           # PE + CE combined
  vix_symbol: "NSE:INDIA VIX"     # symbol for VIX quote
  vix_baseline: 15.0              # VIX level at which gap = base_gap
  vix_max_multiplier: 3.0         # max gap scale factor (e.g. VIX 45 → 3× gap)
  vix_enabled: true               # false = skip VIX fetch, use base gap always
```

### Dynamic Gap Logic

```
effective_gap = base_gap * clamp(current_vix / vix_baseline, 1.0, vix_max_multiplier)

Example:
  base_gap = 40, vix_baseline = 15
  VIX = 15  → effective_gap = 40   (normal day)
  VIX = 30  → effective_gap = 80   (volatile day, gap doubled)
  VIX = 45  → effective_gap = 120  (capped at 3× = 120)
```

VIX fetched once per gap-trigger evaluation (not every tick). Cached with 60-second TTL to avoid rate limiting.

### Daily Loss Limit

MTM loss computed as:
```
loss = sum(
    (position.short_entry_price - current_quote.last_price) * position.quantity
    for position in open_short_positions
    where current_quote.last_price > position.short_entry_price
)
```

If `loss >= daily_loss_limit` → `evaluate()` returns `(False, "daily_loss_limit_breached")`. No new trades. Existing positions continue running (GTT OCO / lifecycle manager handles exits).

---

## Component 2: PositionLifecycleManager

**File:** `strategy/position_manager.py`  
**Pattern:** Stateful manager, persists to JSON, owns all order placement

### Responsibilities

- Place spread legs atomically on entry
- Persist all positions with full metadata
- Reconcile open positions at startup (gap-open bailout)
- Monitor each open position on every tick (SL / target check)
- Mark positions closed when exited

### Position Schema

```python
@dataclass
class SpreadPosition:
    spread_id: str              # uuid
    signal_type: str            # "PE" or "CE"
    short_symbol: str           # e.g. "NIFTY26127P24000"
    hedge_symbol: str           # e.g. "NIFTY26127P23700"  (300pts further OTM)
    short_order_id: str
    hedge_order_id: str
    short_entry_price: float    # premium collected
    hedge_entry_price: float    # premium paid
    net_credit: float           # short_entry_price - hedge_entry_price
    quantity: int
    index_price_at_entry: float
    sl_trigger: float           # short_entry_price * 1.30
    target_trigger: float       # short_entry_price * 0.40
    timestamp: str              # ISO8601
    status: str                 # "open" | "sl_hit" | "target_hit" | "manual_exit" | "gap_bailout"
```

Persisted to `artifacts/positions.json` as list of SpreadPosition dicts.

### Entry: open_spread()

```python
def open_spread(self, signal_type, short_symbol, current_price, quantity) -> SpreadPosition | None:
    """
    1. Find hedge symbol (short_symbol_gap + 300pts further OTM)
    2. Get quotes for both legs
    3. Place short leg (SELL, MARKET)
    4. Place hedge leg (BUY, MARKET)
    5. If hedge placement fails → immediately buy back short leg, log error, return None
    6. Build SpreadPosition, persist to JSON
    7. Return SpreadPosition
    """
```

**Atomicity note:** Broker has no native atomic spread order for NFO. Best-effort: short first, hedge immediately after. If hedge fails, short is bought back. This is the safest ordering (short creates risk, hedge removes it — so hedge failure must reverse short).

### Startup Reconciliation: reconcile_on_open()

```python
def reconcile_on_open(self):
    """
    Called once at strategy startup, before trading loop.
    1. Load positions.json
    2. Filter status == "open"
    3. For each open position:
       a. get_quote(short_symbol)
       b. if quote.last_price > 2 * short_entry_price:
            place BUY market order for short leg
            place SELL market order for hedge leg (close hedge)
            update status = "gap_bailout"
            log with spread_id, loss amount
    4. Save updated positions.json
    """
```

### Tick Monitoring: on_tick()

```python
def on_tick(self, current_index_price: float):
    """
    Called every tick (after signal evaluation).
    For each open SpreadPosition:
      - get_quote(short_symbol)  [cached per-tick, not per-position]
      - if quote.last_price >= sl_trigger:
            exit spread (buy short, sell hedge)
            status = "sl_hit"
      - elif quote.last_price <= target_trigger:
            exit spread (buy short, sell hedge)
            status = "target_hit"
    """
```

**Performance note:** Quotes for all open short symbols fetched in batch using `broker.get_quotes()` (existing interface, single API call for multiple symbols). Avoids N calls per tick.

### Spread Width Configuration

```yaml
spread:
  pe_hedge_gap: 300    # points further OTM for PE hedge leg
  ce_hedge_gap: 300    # points further OTM for CE hedge leg
```

---

## Component 3: SurvivorStrategy (slimmed)

**File:** `strategy/survivor.py` (modified, not replaced)

**Removes:** All order placement code (`_place_order`, `_place_gtt_oco`, GTT logic)  
**Keeps:** Gap trigger detection, reference value management, reset logic, entry filters (EMA/RSI/ADX), instrument lookup  
**Adds:** Calls to `RiskManager.evaluate()` and `PositionLifecycleManager.open_spread()`

### Revised on_ticks_update()

```python
def on_ticks_update(self, ticks):
    current_price = ticks.get('last_price') or ticks.get('ltp')
    self._update_history(current_price)
    self.position_manager.on_tick(current_price)   # monitor existing positions
    self._handle_pe_trade(current_price)
    self._handle_ce_trade(current_price)
    self._reset_reference_values(current_price)
```

### Revised _handle_pe_trade() / _handle_ce_trade()

```python
# Before placing any order:
allowed, reason = self.risk_manager.evaluate("PE", current_price)
if not allowed:
    logger.info(f"PE trade blocked by RiskManager: {reason}")
    return

# Get effective gap (replaces fixed pe_gap):
effective_gap = self.risk_manager.get_effective_gap(self.strat_var_pe_gap)
if price_diff <= effective_gap:
    return

# Instrument selection unchanged (find OTM strike with adequate premium)
# Order placement delegated:
spread = self.position_manager.open_spread("PE", instrument['symbol'], current_price, total_quantity)
if spread:
    self.pe_reset_gap_flag = 1
```

---

## Data Flow: Startup Sequence

```
1. Load config (YAML + CLI overrides)
2. Authenticate broker
3. Instantiate PositionLifecycleManager
4. PositionLifecycleManager.reconcile_on_open()   ← gap-open bailout
5. Instantiate RiskManager
6. Instantiate SurvivorStrategy(broker, config, risk_manager, position_manager)
7. Connect WebSocket
8. Enter trading loop
```

---

## Data Flow: Trade Entry

```
Tick arrives
  → SurvivorStrategy detects gap trigger
  → RiskManager.evaluate() → ALLOW
  → RiskManager.get_effective_gap() → 80pts (VIX = 30)
  → price_diff > effective_gap → proceed
  → _check_entry_filter() → pass (EMA/RSI/ADX if configured)
  → _find_nifty_symbol_from_gap("PE", price, 700) → short_instrument
  → PositionLifecycleManager.open_spread("PE", short_symbol, price, qty)
      → find hedge_symbol at 1000pts OTM
      → SELL short_symbol MARKET
      → BUY hedge_symbol MARKET
      → persist SpreadPosition to JSON
  → pe_reset_gap_flag = 1
```

---

## Files Changed

| File | Change |
|------|--------|
| `strategy/survivor.py` | Remove order placement, add RiskManager + PositionLifecycleManager calls |
| `strategy/risk_manager.py` | New file |
| `strategy/position_manager.py` | New file |
| `strategy/configs/survivor.yml` | Add `risk:` and `spread:` sections |
| `orders.py` | Kept as-is (OrderTracker still used by PositionLifecycleManager internally) |
| `dispatcher.py` | Unchanged |
| `brokers/` | Unchanged |

---

## What This Fixes

| Risk | Before | After |
|------|--------|-------|
| Overnight gap | GTT only, unbounded loss | Spread = max loss = 300pts × qty |
| Gap-open ITM short | No check | reconcile_on_open() exits if price > 2× entry |
| Volatile day overtrading | No cap | max_trades_per_day cap |
| Fixed gap on volatile day | Always 40pt | Dynamic: VIX 30 → 80pt gap |
| Daily loss runaway | No halt | daily_loss_limit halts new trades |
| Hedge placement failure | N/A | Short leg auto-reversed if hedge fails |

---

## Out of Scope

- Multi-expiry management
- Greeks-based position sizing
- Broker-agnostic GTT removal (GTT removed for Zerodha; other brokers unaffected)
- Backtesting framework
- UI / dashboard
