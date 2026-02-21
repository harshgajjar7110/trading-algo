# Enhanced Survivor Strategy - Complete Guide

## Overview

The **Enhanced Survivor Strategy** builds upon the base Survivor Strategy with advanced risk management, technical analysis filters, and dynamic position sizing. This guide explains all enhancements and how to configure them.

---

## 📊 Comparison: Base vs Enhanced

| Feature | Base Strategy | Enhanced Strategy |
|---------|---------------|-------------------|
| Gap-based triggers | ✅ | ✅ |
| Reset mechanism | ✅ | ✅ |
| Technical filters | ❌ | ✅ RSI, EMA, ADX |
| Dynamic gaps | ❌ | ✅ ATR-based |
| Position limits | ❌ | ✅ Max per side/total |
| Stop-loss | ❌ | ✅ Premium-based |
| Trailing stop | ❌ | ✅ Optional |
| Time-based exit | ❌ | ✅ Square off time |
| Daily loss limit | ❌ | ✅ Percentage-based |
| Volatility sizing | ❌ | ✅ Reduce in high vol |
| Trade journaling | ❌ | ✅ Detailed logging |

---

## 🚀 Key Enhancements Explained

### 1. Technical Indicator Filters

Control when trades are executed based on market conditions.

#### RSI Filter (Relative Strength Index)
```yaml
entry_filter_type: "RSI"
rsi_period: 14
rsi_min: 30      # Don't sell CE below RSI 30 (oversold)
rsi_max: 70      # Don't sell PE above RSI 70 (overbought)
```

**Logic:**
- **PE (Put) Sells**: Blocked if RSI > 70 (market overbought, might reverse down)
- **CE (Call) Sells**: Blocked if RSI < 30 (market oversold, might reverse up)

**When to use:** Avoid selling options when market is at extremes

#### EMA Filter (Trend Following)
```yaml
entry_filter_type: "EMA"
ema_period: 20
```

**Logic:**
- **PE Sells**: Only allowed when price > EMA (uptrend)
- **CE Sells**: Only allowed when price < EMA (downtrend)

**When to use:** Trade with the trend, not against it

#### ADX Filter (Trend Strength)
```yaml
entry_filter_type: "ADX"
adx_period: 14
adx_threshold: 25
```

**Logic:**
- Only trade when ADX > 25 (strong trend)
- Prevents trading in choppy/sideways markets

**When to use:** Avoid low-volatility chop

#### Combined Filter
```yaml
entry_filter_type: "ALL"
```

Requires ALL conditions to pass for trade entry.

---

### 2. Dynamic Gap Adjustment

Automatically adjusts trigger gaps based on market volatility.

```yaml
enable_dynamic_gaps: true
atr_multiplier_pe: 2.0
atr_multiplier_ce: 2.0
```

**How it works:**
```
If ATR increases (high volatility):
  → Gaps widen (fewer trades, safer)

If ATR decreases (low volatility):
  → Gaps tighten (more trades)
```

**Example:**
- Base `pe_gap`: 40 points
- Normal ATR: 30 points → Gap = 40
- High ATR: 60 points → Gap = 60 (50% wider)

---

### 3. Position Limits

Prevent over-trading and limit exposure.

```yaml
max_positions_per_side: 3     # Max 3 PE or 3 CE positions
max_total_positions: 5        # Max 5 combined
max_consecutive_losses: 2     # Stop after 2 losses
```

**Why this matters:**
- Prevents runaway position buildup in choppy markets
- Forces cooldown after losing streaks
- Caps maximum risk exposure

---

### 4. Stop-Loss Management

Protect against unlimited losses on short options.

#### Fixed Stop-Loss
```yaml
stop_loss_multiplier: 2.0
```
- Exit if premium doubles (100% loss on short position)
- Example: Sold at ₹50, exit if price reaches ₹100

#### Trailing Stop (Optional)
```yaml
trailing_stop_enabled: true
trailing_stop_distance: 0.5
```
- Captures profits if market reverses
- Example: Position profit peaks at ₹2000, exit if drops to ₹1000

---

### 5. Time-Based Risk Controls

```yaml
square_off_time: "15:15"
max_daily_loss_percent: -3.0
```

**Benefits:**
- Close positions before market close (avoid overnight risk)
- Stop trading if daily loss exceeds 3%
- Prevents revenge trading

---

### 6. Volatility-Based Position Sizing

```yaml
volatility_sizing: true
high_vol_size_reduction: 0.5
```

**Logic:**
- High volatility (ATR > 150% of average): Trade 50% size
- Normal volatility: Trade 100% size
- Low volatility (ATR < 50% of average): Trade 110% size

---

## 📈 Preset Configurations

### Conservative (Lower Risk)
```yaml
entry_filter_type: "ALL"
max_positions_per_side: 2
stop_loss_multiplier: 1.5
pe_gap: 50
```

**Best for:** New traders, low risk tolerance, choppy markets

### Aggressive (Higher Risk)
```yaml
entry_filter_type: "EMA"
max_positions_per_side: 5
stop_loss_multiplier: 3.0
pe_gap: 25
enable_dynamic_gaps: false
```

**Best for:** Experienced traders, trending markets

### Trend Following
```yaml
entry_filter_type: "ALL"
adx_threshold: 30
enable_dynamic_gaps: true
```

**Best for:** Strong directional markets

---

## 🛠 Usage

### Step 1: Copy Configuration
```bash
cp strategy/configs/survivor_enhanced.yml strategy/configs/survivor.yml
```

### Step 2: Update Symbol
Edit the config file and update:
```yaml
symbol_initials: "NIFTY26FEB"  # Change to current expiry
```

### Step 3: Choose Filter Mode
```yaml
# For no filters (like base strategy):
entry_filter_type: "NONE"

# For trend following:
entry_filter_type: "EMA"

# For maximum protection:
entry_filter_type: "ALL"
```

### Step 4: Run the Strategy
```bash
python strategy/survivor_enhanced.py
```

---

## 📊 Monitoring & Statistics

The enhanced strategy provides detailed statistics:

```python
# Get current stats
stats = strategy.get_strategy_stats()

print(f"Active Positions: {stats['positions']['total']}")
print(f"Daily P&L: ₹{stats['daily_stats']['pnl']}")
print(f"Trades Taken: {stats['daily_stats']['trades_taken']}")
print(f"Trades Rejected: {stats['daily_stats']['trades_rejected']}")
```

### Log Output Example
```
2024-02-18 10:30:15 - INFO: Indicators - RSI: 62.5, EMA: 22450.30, Trend: bullish, ATR: 45.20, Vol: normal
2024-02-18 10:30:15 - INFO: PE trade filters passed: RSI 62.5, Trend bullish
2024-02-18 10:30:16 - INFO: Execute PE sell @ NIFTY26FEB22500PE × 65, Premium: ₹28.50
2024-02-18 11:15:22 - INFO: CE trade rejected: RSI 24.3 < 30
```

---

## ⚠️ Important Notes

### 1. Historical Data Requirement
Technical indicators require historical data. The strategy will:
- Load 5 days of 5-minute data on startup
- Use default values (RSI=50, Neutral trend) until sufficient data

### 2. Broker Compatibility
The enhanced strategy uses the same broker interface as the base strategy.

### 3. Performance Impact
- Additional calculations have minimal latency impact
- Historical data loading may take 5-10 seconds on startup

### 4. Backtesting Recommendation
Test configurations in simulation before live trading:
```yaml
# Start with conservative settings
entry_filter_type: "RSI"  # Single filter first
max_positions_per_side: 2
stop_loss_multiplier: 2.0
```

---

## 🔧 Troubleshooting

### Issue: No trades executing
**Check:**
1. `entry_filter_type` - Try "NONE" to verify base logic works
2. Indicator values in logs - May be waiting for trend/RSI conditions
3. Position limits - May have reached max positions

### Issue: Too many rejected trades
**Solutions:**
1. Widen RSI range: `rsi_min: 20, rsi_max: 80`
2. Lower ADX threshold: `adx_threshold: 20`
3. Use fewer filters: `entry_filter_type: "EMA"`

### Issue: Stop-loss not triggering
**Check:**
1. Ensure positions are being tracked (check logs)
2. Verify `stop_loss_multiplier` is set
3. Check broker order placement permissions

---

## 📚 Further Enhancements (Future)

Potential additions:
1. **VIX Integration** - Direct volatility index usage
2. **Greeks-based Strike Selection** - Delta-neutral adjustments
3. **Machine Learning Filters** - Pattern recognition
4. **Correlation Filters** - Check Bank Nifty correlation
5. **News-based Pauses** - Pause before major events

---

## Summary

The Enhanced Survivor Strategy provides:
- ✅ **Better Entry Quality** via technical filters
- ✅ **Risk Protection** via stop-losses and limits
- ✅ **Adaptive Behavior** via dynamic gaps
- ✅ **Disciplined Trading** via time/loss controls

Start with `entry_filter_type: "RSI"` and gradually add complexity as you understand each component.
