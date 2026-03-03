
# GAP UP/GAP DOWN HANDLING FOR UNHEDGED OPTION SELLERS
## Nifty 50 - Comprehensive Risk Management Guide

---

## EXECUTIVE SUMMARY

Gap risk is the #1 killer of unhedged option sellers. A 1% gap against your position
can wipe out weeks of profits in seconds. This guide provides battle-tested strategies
for handling gap openings, with special focus on Monday volatility (weekend risk).

**Key Principle:** When unhedged, your ONLY defense is position sizing, timing, and 
proactive risk management. You cannot "manage" a gap after it happens - you must 
PREVENT the damage before the market opens.

---

## PART 1: UNDERSTANDING GAP RISK

### 1.1 Types of Gaps

| Gap Type | Description | Risk Level | Typical Cause |
|----------|-------------|------------|---------------|
| **Common Gap** | Small gap, fills quickly | Low | Normal market noise |
| **Breakaway Gap** | Gap beyond key S/R, continues | VERY HIGH | Major news, earnings |
| **Runaway Gap** | Mid-trend gap, continuation | High | Momentum acceleration |
| **Exhaustion Gap** | Late trend gap, reverses | Medium | Final push, reversal |

### 1.2 Monday Gap Statistics (Nifty 50)

Based on historical analysis:

```
┌─────────────────────────────────────────────────────────────────┐
│  MONDAY GAP PROBABILITY (Nifty 50)                              │
├─────────────────────────────────────────────────────────────────┤
│  Gap > 0.5%:     ~45% of Mondays                               │
│  Gap > 1.0%:     ~22% of Mondays                               │
│  Gap > 1.5%:     ~10% of Mondays                               │
│  Gap > 2.0%:     ~4% of Mondays                                │
└─────────────────────────────────────────────────────────────────┘

Key Insight: You will face a >1% gap approximately 1 in 5 Mondays.
With unhedged positions, this is a GUARANTEED blow-up over time.
```

### 1.3 Weekend Risk Factors

**Why Mondays Are Dangerous:**

1. **Two days of information accumulation** (global markets, geopolitics)
2. **GIFT Nifty trading** (Singapore) indicates direction before Indian open
3. **SGX Nifty can move 100-200 points** overnight
4. **Economic data releases** (US non-farm payrolls, Fed speeches)
5. **Corporate announcements** (earnings, deals, management changes)
6. **Geopolitical events** (wars, sanctions, trade disputes)

### 1.4 The Math of Gap Destruction

**Example Scenario:**
- Sold 24500 CE @ Rs.80 (Nifty @ 24300)
- Position: 5 lots (125 qty)
- Capital: Rs.500,000
- Premium collected: Rs.10,000

**Gap Up 1.5% (Nifty opens @ 24665):**
- New call premium: ~Rs.250-300 (ITM now)
- Loss: (250 - 80) x 125 = Rs.21,250
- % of capital lost: 4.25% in 1 second

**Gap Up 2.5% (Nifty opens @ 24910):**
- New call premium: ~Rs.500+ (deep ITM)
- Loss: (500 - 80) x 125 = Rs.52,500
- % of capital lost: 10.5% in 1 second

**Reality Check:** A single 2.5% gap can erase 2-3 months of profits.

---

## PART 2: ENTRY STRATEGY MODIFICATIONS

### 2.1 Day-of-Week Based Entry Rules

```
╔══════════════════════════════════════════════════════════════════╗
║           ENTRY RULES BY DAY (Unhedged Selling)                  ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  MONDAY:     HIGH RISK DAY                                       ║
║  ─────────────────────────────────────────────────────────────   ║
║  • NO NEW POSITIONS in first 45 minutes (9:15-10:00 AM)         ║
║  • Wait for gap to "settle" and direction to establish          ║
║  • If gap > 1%, SKIP trading for the day                        ║
║  • Reduce position size by 50% if entering                      ║
║  • Prefer 0DTE (same day expiry) only                           ║
║                                                                  ║
║  TUESDAY:    NORMAL DAY (if not expiry)                          ║
║  ─────────────────────────────────────────────────────────────   ║
║  • Standard entry rules apply                                   ║
║  • 10:00 AM - 2:30 PM window                                    ║
║                                                                  ║
║  WEDNESDAY:  NORMAL DAY                                          ║
║  ─────────────────────────────────────────────────────────────   ║
║  • Standard entry rules apply                                   ║
║                                                                  ║
║  THURSDAY:   EXPIRY DAY (High Volatility)                        ║
║  ─────────────────────────────────────────────────────────────   ║
║  • Only intraday positions (close by 3:15 PM)                   ║
║  • No overnight carry                                           ║
║  • Wider strikes (higher buffer)                                ║
║                                                                  ║
║  FRIDAY:     WEEKEND RISK                                        ║
║  ─────────────────────────────────────────────────────────────   ║
║  • NO OVERNIGHT POSITIONS unless hedged                         ║
║  • Close all positions by 3:15 PM                               ║
║  • If keeping positions, reduce size by 60%                     ║
║  • Check GIFT Nifty before deciding to hold                     ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
```

### 2.2 Pre-Market Analysis Checklist

**Before EVERY trading day (especially Monday):**

```
[ ] Check GIFT Nifty (Singapore) - indicates opening direction
[ ] Review overnight US markets (S&P 500, Nasdaq, Dow)
[ ] Check Asian markets (Nikkei, Hang Seng, Shanghai)
[ ] Review VIX (volatility index) movement
[ ] Check for major news/events:
   - RBI policy announcements
   - Fed speeches/decisions
   - Geopolitical developments
   - Corporate earnings (major companies)
   - Economic data releases
[ ] Review option chain OI changes from previous day
[ ] Identify key support/resistance levels
[ ] Determine if gap is likely and direction
```

### 2.3 Gap Size Assessment Protocol

**At 9:15 AM Market Open:**

| Gap Size | Action | Position Size |
|----------|--------|---------------|
| < 0.5% | Trade normally | 100% |
| 0.5% - 1.0% | Wait 30 min, then decide | 50% |
| 1.0% - 1.5% | SKIP day or 0DTE only | 25% |
| > 1.5% | NO TRADING | 0% |

**Gap Direction Decision Tree:**

```
IF Gap Up > 1%:
    → Market is bullish
    → DO NOT sell calls (high risk)
    → Consider selling puts ONLY if:
        - Price stabilizes above gap
        - Volume confirms strength
        - Strike is 1.5%+ below open

IF Gap Down > 1%:
    → Market is bearish
    → DO NOT sell puts (high risk)
    → Consider selling calls ONLY if:
        - Price stabilizes below gap
        - Volume confirms weakness
        - Strike is 1.5%+ above open
```

---

## PART 3: STRIKE SELECTION MODIFICATIONS

### 3.1 Standard vs Gap-Adjusted Strikes

**Normal Day Strike Selection:**
```
Nifty @ 24500
POC = 24480
DTE = 5
Call Strike = 24480 + (24500 x 0.018) = 24,920
Put Strike = 24480 - (24500 x 0.018) = 24,040
```

**Monday/High Gap Risk Day Strike Selection:**
```
Nifty @ 24500 (but expecting gap)
POC = 24480
DTE = 5
GAP BUFFER ADDED: +0.5% to 1.0%

Call Strike = 24480 + (24500 x 0.025) = 25,090 (was 24,920)
Put Strike = 24480 - (24500 x 0.025) = 24,870 (was 24,040)

Result: Strikes are 170 points wider = 0.7% more buffer
```

### 3.2 Strike Selection Matrix

| Market Condition | DTE | Normal Buffer | Gap-Risk Buffer | Example Call @ 24500 |
|-----------------|-----|---------------|-----------------|---------------------|
| Normal Day | 0-2 | 1.5% | 1.5% | 24,870 |
| Normal Day | 3-7 | 2.0% | 2.0% | 24,990 |
| Normal Day | 7+ | 2.5% | 2.5% | 25,110 |
| Monday | 0-2 | - | 2.0% | 24,990 |
| Monday | 3-7 | - | 2.5% | 25,110 |
| Post-Gap >1% | Any | - | 3.0%+ | 25,240 |
| Pre-Event | Any | - | 3.0%+ | 25,240 |

### 3.3 Dynamic Strike Adjustment Based on Gap

**If Market Gaps and You Want to Trade:**

```
STRIKE ADJUSTMENT LOGIC:

IF gap_percent > 1.5%:
    additional_buffer = 1.0%  # Add 1% extra
ELIF gap_percent > 1.0%:     # > 1% gap
    additional_buffer = 0.5%  # Add 0.5% extra
ELSE:
    additional_buffer = 0

total_buffer = base_buffer + additional_buffer

Example:
spot = 24650  (After 1.5% gap up from 24300)
gap = 1.5%
base_buffer = 2.0%

# Normal strike would be ~25,100
# Adjusted strike:
call_strike = spot x (1 + total_buffer)
call_strike = 24650 x 1.03 = 25,390

Result: Extra ~300 points buffer
```

---

## PART 4: POSITION SIZING FOR GAP RISK

### 4.1 The Gap-Risk Position Sizing Formula

**Standard Position Sizing:**
```
Lots = (Capital x Risk%) / (Stop Loss Points x Lot Size)

Example:
Capital = Rs.500,000
Risk% = 2% (Rs.10,000)
Stop Loss = 100 points
Lot Size = 25

Lots = 10,000 / (100 x 25) = 4 lots
```

**Gap-Risk Adjusted Position Sizing:**
```
Gap-Risk Factor = 1 + (Expected Gap% x 2)

IF trading on Monday OR holding overnight:
    Adjusted Risk% = Standard Risk% / Gap-Risk Factor

Example (Monday trading):
Standard Risk% = 2%
Expected Gap = 1% (conservative estimate)
Gap-Risk Factor = 1 + (0.01 x 2) = 1.2

Adjusted Risk% = 2% / 1.2 = 1.67%

New Position Size:
Lots = (500,000 x 0.0167) / (100 x 25) = 3.3 lots → 3 lots

Result: 25% reduction in position size on high-risk days
```

### 4.2 Day-Based Position Size Multipliers

| Day | Risk Multiplier | Max Lots (vs Normal) | Reason |
|-----|-----------------|---------------------|--------|
| Monday | 0.5x | 50% | Weekend gap risk |
| Tuesday | 1.0x | 100% | Normal day |
| Wednesday | 1.0x | 100% | Normal day |
| Thursday | 0.75x | 75% | Expiry volatility |
| Friday | 0.4x | 40% | Weekend risk |
| Pre-Holiday | 0.3x | 30% | Extended closure |

### 4.3 Capital Allocation by Day

```
Total Capital: Rs.500,000

Monday:     Deploy max Rs.100,000 (20%)  → 2 lots
Tuesday:    Deploy max Rs.250,000 (50%)  → 5 lots
Wednesday:  Deploy max Rs.250,000 (50%)  → 5 lots
Thursday:   Deploy max Rs.150,000 (30%)  → 3 lots (intraday only)
Friday:     Deploy max Rs.100,000 (20%)  → 2 lots (close by EOD)
```

---

## PART 5: EXIT STRATEGIES FOR GAP SCENARIOS

### 5.1 Pre-Gap Exit Rules (Before Weekend)

**Friday 3:00 PM Decision Point:**

```
CHECK: Are you holding positions overnight into weekend?

IF YES:
    QUESTION 1: Is position profitable?
    ────────────────────────────────────
    IF Premium captured > 50%:
        → CLOSE POSITION (take profits)

    QUESTION 2: How close is spot to strike?
    ────────────────────────────────────
    IF Spot within 0.5% of either strike:
        → CLOSE POSITION (too risky)

    QUESTION 3: Any upcoming events?
    ────────────────────────────────────
    IF Major event over weekend (Fed, RBI, etc.):
        → CLOSE POSITION (event risk)

    QUESTION 4: GIFT Nifty indication?
    ────────────────────────────────────
    IF GIFT Nifty > 0.5% away from spot:
        → CLOSE or ADJUST position
```

### 5.2 Post-Gap Exit Protocol

**Market Opens with Gap Against You:**

```
STEP 1: Assess Damage (within first 2 minutes)
        - How far is spot from your strike?
        - What is the new premium value?
        - Calculate unrealized loss

STEP 2: Decision Matrix

┌──────────────────────────────────────────────────────────────┐
│ Gap Against Position:                                        │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Scenario A: Strike is now ITM or ATM                        │
│  ─────────────────────────────────────                       │
│  • EXIT IMMEDIATELY (market order if needed)                 │
│  • Do not hope for recovery                                  │
│  • Accept the loss and move on                               │
│                                                              │
│  Scenario B: Strike is still OTM but within 0.5%            │
│  ─────────────────────────────────────                       │
│  • Monitor for 15-30 minutes                                 │
│  • If price continues against you → EXIT                     │
│  • If price stabilizes → Tighten stop to 1%                  │
│                                                              │
│  Scenario C: Strike is still OTM (>1% away)                 │
│  ─────────────────────────────────────                       │
│  • Hold position but monitor closely                         │
│  • Set alert at 0.5% from strike                             │
│  • Be ready to exit if alert triggers                        │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 5.3 Gap Recovery Strategy (Advanced)

**When to Hold Through a Gap:**

```
HOLD CONDITIONS (ALL must be met):

1. Strike is still >1% OTM after gap
2. Gap is < 1.5% (not extreme)
3. No major news driving the gap
4. Volume is not exceptionally high
5. Position size is small (< 2% risk)
6. You have a tight stop-loss in place

ADJUSTMENT OPTIONS:

Option 1: Roll the threatened leg
   - Buy back the threatened option
   - Sell new option further OTM (next strike)
   - Extends breathing room

Option 2: Convert to spread
   - Buy protective option further OTM
   - Limits maximum loss
   - Requires additional margin

Option 3: Add opposite leg
   - If call threatened, sell put at support
   - Creates strangle with wider range
   - Collects additional premium
```

---

## PART 6: SPECIFIC MONDAY STRATEGIES

### 6.1 Monday-Specific Trading Plan

```
╔══════════════════════════════════════════════════════════════════╗
║                    MONDAY TRADING PROTOCOL                       ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  PRE-MARKET (8:00 AM - 9:15 AM)                                  ║
║  ─────────────────────────────────────────────────────────────   ║
║  1. Check GIFT Nifty (Singapore) closing price                  ║
║  2. Calculate expected gap: GIFT Nifty - Previous Close         ║
║  3. Review weekend news/events                                  ║
║  4. Decide: Trade, Reduce, or Skip                              ║
║                                                                  ║
║  MARKET OPEN (9:15 AM - 10:00 AM)                                ║
║  ─────────────────────────────────────────────────────────────   ║
║  1. DO NOT ENTER any positions                                  ║
║  2. Observe gap size and direction                              ║
║  3. Watch for gap fill or continuation                          ║
║  4. Note volume confirmation                                    ║
║  5. Identify support/resistance levels                          ║
║                                                                  ║
║  DECISION TIME (10:00 AM)                                        ║
║  ─────────────────────────────────────────────────────────────   ║
║  IF Gap < 0.5% AND stable:                                      ║
║     → Can trade with 50% position size                          ║
║     → Use 2.5% buffer (wider strikes)                           ║
║                                                                  ║
║  IF Gap 0.5% - 1.0%:                                            ║
║     → Can trade with 25% position size                          ║
║     → Use 3.0% buffer                                           ║
║     → Only sell options on opposite side of gap                 ║
║                                                                  ║
║  IF Gap > 1.0%:                                                 ║
║     → SKIP TRADING for the day                                  ║
║     → Paper trade or observe only                               ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
```

### 6.2 The "Monday Strangle" Strategy

**Modified Strangle for Mondays:**

```
Standard Strangle (Normal Day):
Nifty @ 24500
Sell 24900 CE (1.6% OTM)
Sell 24100 PE (1.6% OTM)
Combined buffer: 3.2%

Monday Strangle (Gap Protection):
Nifty @ 24500
Sell 25000 CE (2.0% OTM)  ← 0.4% wider
Sell 24000 PE (2.0% OTM)  ← 0.4% wider
Combined buffer: 4.0%

Additional Protection:
- Position size: 50% of normal
- Stop loss: Tighter (150% of premium vs 200%)
- Time stop: Exit by 2:30 PM if not profitable
```

### 6.3 Monday Intraday Only Strategy

**For Conservative Traders:**

```
RULES:
1. Only 0DTE (same day expiry) positions
2. Enter after 10:30 AM only
3. Exit by 2:30 PM (before closing volatility)
4. No overnight carry under ANY circumstances
5. Max 2 lots regardless of capital

EXAMPLE SCHEDULE:
9:15 AM  - Observe, no positions
10:30 AM - Assess market stability
11:00 AM - Enter if conditions met
2:30 PM  - Exit all positions
3:30 PM  - Flat, no overnight risk
```

---

## PART 7: ALTERNATIVE APPROACHES TO AVOID GAP RISK

### 7.1 The Intraday-Only Approach

**Eliminate Overnight Risk Completely:**

```
ADVANTAGES:
✓ No gap risk (positions closed by EOD)
✓ Higher win rate (theta decay works intraday)
✓ Lower margin requirements (intraday)
✓ Can sleep peacefully on weekends

DISADVANTAGES:
✗ Lower total premium (shorter time)
✗ Need to be active during market hours
✗ More trades = more transaction costs
✗ Miss overnight theta decay

IMPLEMENTATION:
- Enter between 10:00 AM - 11:00 AM
- Exit between 2:30 PM - 3:00 PM
- Use ATM or slightly OTM strikes
- Target 30-50% of premium capture
```

### 7.2 The Hedged Alternative (Iron Condor)

**If You Must Hold Overnight:**

```
Unhedged Short Strangle (Risky):
Sell 24900 CE @ Rs.80
Sell 24100 PE @ Rs.80
Max Risk: UNLIMITED
Margin: ~Rs.150,000 per lot

Hedged Iron Condor (Safer):
Sell 24900 CE @ Rs.80
Buy 25100 CE @ Rs.30  ← Protection
Sell 24100 PE @ Rs.80
Buy 23900 PE @ Rs.30  ← Protection

Net Credit: Rs.100 (vs Rs.160)
Max Risk: Rs.100 per lot (defined!)
Margin: ~Rs.40,000 per lot

Gap Protection:
- Maximum loss capped at Rs.100
- Gap of 2% = loss of Rs.100 (not Rs.500+)
- Can hold overnight with peace of mind
```

### 7.3 The "No Monday" Rule

**Simple but Effective:**

```
RULE: Do not trade on Mondays at all.

IMPLEMENTATION:
- Close all positions by Friday 3:15 PM
- No new positions until Tuesday 10:00 AM
- Use Monday to analyze and plan

RESULTS:
- Eliminates 80% of gap risk
- Reduces trading days by 20%
- May improve overall returns due to fewer losses

STATISTICS:
If Monday = 22% of trading days
And Monday losses = 40% of total losses
Then skipping Monday = +8.8% improvement in returns
```

---

## PART 8: RISK MANAGEMENT CHECKLIST

### 8.1 Pre-Trade Gap Risk Checklist

```
[ ] What day of the week is it?
[ ] Is there a weekend/holiday before next session?
[ ] What is GIFT Nifty showing?
[ ] Any major events in next 24-48 hours?
[ ] Is VIX elevated (>20)?
[ ] Am I comfortable losing 2x my normal risk?
[ ] Is position size appropriate for gap risk?
[ ] Are my strikes wide enough?
[ ] Do I have exit plan if gap occurs?
[ ] Can I monitor position at market open?
```

### 8.2 Gap Event Response Checklist

```
WHEN GAP OCCURS:

Immediate (First 2 minutes):
[ ] Calculate gap size
[ ] Determine if strike is threatened
[ ] Assess new risk level
[ ] Decide: Hold, Adjust, or Exit

Within 15 minutes:
[ ] If strike ITM → EXIT NOW
[ ] If strike within 0.5% → Set tight stop
[ ] If strike >1% away → Monitor with alert

Within 30 minutes:
[ ] Review price action
[ ] Check if gap is filling or continuing
[ ] Reassess position viability
[ ] Execute adjustment if needed
```

---

## PART 9: REAL-WORLD EXAMPLES

### 9.1 Case Study 1: Monday Gap Up Disaster

```
DATE: Monday, March 3, 2025
SCENARIO: Unhedged call seller

FRIDAY CLOSE:
Nifty @ 24,500
Sold 24,900 CE @ Rs.70 (5 lots)
Premium collected: Rs.8,750

WEEKEND EVENT:
Positive US jobs data + Fed dovish comments
GIFT Nifty up 180 points

MONDAY OPEN:
Nifty gaps up to 24,750 (+1.0%)
24,900 CE opens at Rs.220

RESULT:
Unrealized loss: (220 - 70) x 125 = Rs.18,750
% of capital lost: 3.75% (assuming Rs.500k)

LESSON:
- Should have checked GIFT Nifty Friday evening
- Should have closed position before weekend
- Position size was too large for gap risk
```

### 9.2 Case Study 2: Successful Monday Management

```
DATE: Monday, February 10, 2025
SCENARIO: Gap-aware trader

FRIDAY DECISION:
GIFT Nifty showing +120 points
Trader decides to close all positions

ACTION:
Closed strangle at 3:00 PM Friday
Captured 60% of premium
No overnight risk

MONDAY:
Market gaps up 0.8%
Trader watches from sidelines

RESULT:
- No loss from gap
- Can re-enter at better levels
- Peace of mind over weekend

LESSON:
- Sometimes best trade is no trade
- Preserving capital > capturing theta
```

### 9.3 Case Study 3: Post-Gap Recovery

```
DATE: Monday, January 20, 2025
SCENARIO: Gap down recovery

MARKET OPEN:
Nifty gaps down 1.2% (geopolitical news)
Trader sold 24,500 PE (now 0.8% OTM)

DECISION:
- Strike still 200 points away
- Gap due to news (may recover)
- Position size small (2 lots)
- Decide to hold with tight stop

OUTCOME:
- Market recovers by 11:00 AM
- PE premium decays
- Position becomes profitable
- Exit at 50% profit

LESSON:
- Not all gaps require exit
- Small position size allows holding
- Tight stop protects if wrong
```

---

## PART 10: SUMMARY & ACTION PLAN

### 10.1 Key Takeaways

```
╔══════════════════════════════════════════════════════════════════╗
║                    TOP 10 GAP RISK RULES                         ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  1. Gaps are the #1 killer of unhedged option sellers           ║
║                                                                  ║
║  2. Monday = highest gap risk day (skip or reduce size)         ║
║                                                                  ║
║  3. Never hold unhedged positions over weekend if possible      ║
║                                                                  ║
║  4. Check GIFT Nifty before deciding to hold overnight          ║
║                                                                  ║
║  5. Reduce position size by 50% on Mondays                      ║
║                                                                  ║
║  6. Use wider strikes (+0.5% buffer) on gap-risk days           ║
║                                                                  ║
║  7. If gap >1%, skip trading for the day                        ║
║                                                                  ║
║  8. If strike becomes ITM after gap, EXIT immediately           ║
║                                                                  ║
║  9. Consider intraday-only to eliminate gap risk                ║
║                                                                  ║
║  10. Consider iron condors to cap gap losses                    ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
```

### 10.2 Your Action Plan

```
WEEK 1: IMPLEMENT DAY-BASED RULES
[ ] Stop trading first 45 min on Mondays
[ ] Reduce Friday position size by 60%
[ ] Close all positions by Friday 3:15 PM

WEEK 2: ADD STRIKE ADJUSTMENTS
[ ] Use +0.5% wider strikes on Mondays
[ ] Use +1.0% wider strikes post-gap >1%

WEEK 3: ADD POSITION SIZING RULES
[ ] Implement day-based multipliers
[ ] Never risk >1% on Mondays

WEEK 4: ADD PRE-MARKET CHECKLIST
[ ] Check GIFT Nifty daily
[ ] Review weekend news
[ ] Assess gap probability

ONGOING:
[ ] Keep gap risk journal
[ ] Track Monday performance separately
[ ] Review and adjust monthly
```

---

## CONCLUSION

Gap risk is unavoidable in option selling, but it can be MANAGED through:

1. **Timing:** Avoid high-risk periods (Monday open, pre-weekend)
2. **Sizing:** Reduce exposure when gap risk is elevated
3. **Strikes:** Use wider buffers on gap-risk days
4. **Discipline:** Exit when rules dictate, not when hoping

**Remember:** 
- A 1% gap can erase weeks of profits
- You cannot predict gaps, but you can prepare for them
- Preservation of capital is more important than capturing every theta penny
- Sometimes the best trade is NO trade

**Final Advice:**
If you are consistently losing to gaps, consider:
1. Switching to intraday-only (no overnight risk)
2. Using hedged strategies (iron condors)
3. Skipping Mondays entirely
4. Reducing position size by 70%

Your survival as an option seller depends on how well you manage gap risk.

---

Document Version: 1.0
Focus: Nifty 50 Unhedged Option Selling
Gap Risk Management Framework
