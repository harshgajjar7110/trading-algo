# Enhanced Survivor Strategy - Quick Reference

## 🎯 One-Page Setup Guide

### Minimal Configuration (Get Started in 5 Minutes)

```yaml
symbol_initials: "NIFTY26FEB"    # CHANGE THIS
index_symbol: "NSE:NIFTY 50"

# Base gaps
pe_gap: 40
ce_gap: 40
pe_symbol_gap: 800
ce_symbol_gap: 800

# Position sizing
pe_quantity: 65
ce_quantity: 65

# ONE filter to start with
entry_filter_type: "RSI"         # Start here
rsi_min: 30
rsi_max: 70

# Essential risk management
max_positions_per_side: 3
stop_loss_multiplier: 2.0
square_off_time: "15:15"
```

---

## 🎛 Filter Selection Guide

| Market Condition | Recommended Filter | Why |
|------------------|-------------------|-----|
| Trending Up | `EMA` | Only sell PE in uptrend |
| Trending Down | `EMA` | Only sell CE in downtrend |
| Choppy/Sideways | `RSI` | Avoid extremes |
| Strong trends | `ADX` | Only trade strong moves |
| Unsure | `ALL` | Maximum protection |
| Testing | `NONE` | Base strategy behavior |

---

## 📏 Parameter Quick-Adjust

### Gap Settings by Volatility

| VIX Level | pe_gap/ce_gap | pe_symbol_gap/ce_symbol_gap |
|-----------|---------------|----------------------------|
| < 15 (Low) | 30 | 600 |
| 15-20 (Normal) | 40 | 800 |
| 20-25 (High) | 50 | 1000 |
| > 25 (Very High) | 60 | 1200 |

### Position Sizing by Capital

| Capital | Per-Lot Margin | Max Lots | pe_quantity |
|---------|---------------|----------|-------------|
| ₹5L | ₹1L | 4 | 50 |
| ₹10L | ₹1L | 8 | 50 |
| ₹20L | ₹1L | 16 | 75 |
| ₹50L | ₹1L | 40 | 100 |

---

## 🚨 Risk Settings by Profile

### Conservative (Capital Preservation)
```yaml
max_positions_per_side: 2
max_total_positions: 3
stop_loss_multiplier: 1.5
max_daily_loss_percent: -2.0
pe_gap: 50
entry_filter_type: "ALL"
```

### Moderate (Balanced)
```yaml
max_positions_per_side: 3
max_total_positions: 5
stop_loss_multiplier: 2.0
max_daily_loss_percent: -3.0
pe_gap: 40
entry_filter_type: "RSI"
```

### Aggressive (Higher Returns, Higher Risk)
```yaml
max_positions_per_side: 5
max_total_positions: 8
stop_loss_multiplier: 3.0
max_daily_loss_percent: -5.0
pe_gap: 25
entry_filter_type: "EMA"
```

---

## 🔍 Common Patterns & Solutions

### Pattern 1: Strategy Not Taking Trades
```
Symptom: No trades for hours
Check: RSI filter too restrictive
Fix: rsi_min: 20, rsi_max: 80 (widen range)
```

### Pattern 2: Too Many Losses
```
Symptom: Stop-losses hitting frequently
Check: Gaps too tight for volatility
Fix: Increase pe_gap/ce_gap by 50%
```

### Pattern 3: Missing Good Moves
```
Symptom: Market trends, no entries
Check: ADX threshold too high
Fix: adx_threshold: 20 (lower)
```

### Pattern 4: Over-trading
```
Symptom: Too many positions built up
Check: Position limits not set
Fix: max_positions_per_side: 2
```

---

## 📊 Daily Checklist

### Pre-Market (8:45 AM)
- [ ] Update `symbol_initials` to current expiry
- [ ] Check VIX, adjust gaps if > 20
- [ ] Verify account balance for margin
- [ ] Check for news/events (RBI, earnings)

### During Market
- [ ] Monitor log for filter rejections
- [ ] Watch position count vs limits
- [ ] Check P&L vs daily loss limit
- [ ] Monitor around 3:15 PM for square-off

### Post-Market
- [ ] Review daily_stats from logs
- [ ] Check rejected_trades count
- [ ] Note any stop-loss hits
- [ ] Plan adjustments for tomorrow

---

## 💡 Pro Tips

1. **Start Simple**: Begin with RSI filter only, add more gradually

2. **Monday Effect**: Markets often gap on Monday, widen gaps by 20%

3. **Expiry Week**: Reduce position sizes by 50% in expiry week

4. **Budget Day**: Disable strategy or use `NONE` filter (high volatility)

5. **Correlation Check**: If Bank Nifty and Nifty diverge >1%, be cautious

6. **Win Rate vs Profit**: 60% win rate with 1:2 risk-reward beats 80% with 1:1

---

## 🐛 Emergency Commands

```python
# Check current positions
stats = strategy.get_strategy_stats()
print(stats['positions'])

# Manual square off
strategy._square_off_all_positions()

# Reset consecutive losses
strategy.consecutive_losses = 0

# Check why trades rejected
print(stats['filters']['rejection_reasons'])
```

---

## 📞 Decision Flowchart

```
NIFTY Moves → Gap Triggered?
    ↓
YES → Check Position Limits
    ↓
OK → Check Filter (RSI/EMA/ADX)
    ↓
PASS → Check Volatility Regime
    ↓
ADJUST → Calculate Size & Gap
    ↓
EXECUTE → Place Order
    ↓
TRACK → Monitor Stop-Loss
    ↓
EXIT? → Stop hit / Time / Manual
```

---

## 🎓 Learning Path

### Week 1: Base Strategy
```yaml
entry_filter_type: "NONE"
max_positions_per_side: 5
# Just observe how gaps work
```

### Week 2: Add RSI Filter
```yaml
entry_filter_type: "RSI"
rsi_min: 30
rsi_max: 70
# Learn to read RSI
```

### Week 3: Add Position Limits
```yaml
max_positions_per_side: 3
stop_loss_multiplier: 2.0
# Learn risk management
```

### Week 4: Full Configuration
```yaml
entry_filter_type: "ALL"
enable_dynamic_gaps: true
volatility_sizing: true
# Complete system
```

---

## 🔗 Quick Links

- Full Guide: `ENHANCEMENTS_GUIDE.md`
- Config File: `configs/survivor_enhanced.yml`
- Base Strategy: `survivor.py`
- Enhanced Strategy: `survivor_enhanced.py`

---

**Remember**: Paper trade each configuration for at least 1 week before going live!
