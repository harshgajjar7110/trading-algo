# Visual Engine Design Document

## Overview
A real-time visualization dashboard that shows algorithm status, active filters, entry signals, and prediction timing.

## Architecture

```mermaid
flowchart TB
    subgraph Backend
        A[Strategy Process] -->|State Updates| B[Strategy Manager]
        B -->|Visual State| C[/api/strategy/visual-state]
        B -->|WebSocket| D[Real-time Updates]
    end
    
    subgraph Frontend
        E[VisualEngine Component] --> F[Filter Status Panel]
        E --> G[Entry Prediction Chart]
        E --> H[Signal Timeline]
        E --> I[Algo Status Card]
    end
    
    C --> E
    D --> E
```

## 1. Backend API Design

### New Endpoint: GET /api/strategy/visual-state

**Response Schema:**
```typescript
interface StrategyVisualState {
  // Algo Status
  is_running: boolean;
  uptime_seconds: number;
  current_strategy: string;
  last_update: string;
  
  // Active Filters with Current Values
  filters: {
    entry_filter_type: 'NONE' | 'RSI' | 'EMA' | 'ADX' | 'ALL';
    items: FilterStatus[];
  };
  
  // Entry Predictions
  predictions: {
    pe: EntryPrediction | null;  // Put Entry
    ce: EntryPrediction | null;  // Call Entry
  };
  
  // Recent Signals
  signals: EntrySignal[];
  
  // Market Context
  market_context: {
    nifty_price: number | null;
    trend: 'BULLISH' | 'BEARISH' | 'NEUTRAL';
    volatility_regime: 'LOW' | 'NORMAL' | 'HIGH';
    gap_assessment: {
      can_trade: boolean;
      message: string;
    };
  };
  
  // Daily Stats
  daily_stats: {
    trades_taken: number;
    trades_rejected: number;
    pnl: number;
    consecutive_losses: number;
  };
}

interface FilterStatus {
  name: string;           // e.g., "RSI Filter"
  type: 'RSI' | 'EMA' | 'ADX' | 'TREND' | 'GAP';
  enabled: boolean;
  current_value: number | string;
  threshold: number | string;
  status: 'PASS' | 'FAIL' | 'PENDING' | 'BLOCKED';
  message: string;        // e.g., "RSI 65.2 < 70 (Pass)"
}

interface EntryPrediction {
  side: 'PE' | 'CE';
  status: 'READY' | 'WAITING' | 'BLOCKED' | 'COOLDOWN';
  current_price: number | null;
  trigger_price: number | null;      // When price reaches this, entry triggers
  distance_to_trigger: number | null; // Points away from trigger
  distance_percent: number | null;
  estimated_time: string | null;      // "~5 min" based on volatility
  blocking_reasons: string[];         // Why entry is blocked
  next_check: string;
}

interface EntrySignal {
  timestamp: string;
  side: 'PE' | 'CE';
  type: 'ENTRY' | 'REJECTION' | 'EXIT';
  price: number;
  reason: string;
  filters_passed: string[];
  filters_failed: string[];
}
```

## 2. Frontend Component Design

### VisualEngine Dashboard Layout

```
┌─────────────────────────────────────────────────────────────┐
│  🤖 ALGO STATUS          🟢 RUNNING (2h 15m)               │
│  Strategy: Survivor v2.0    Uptime: 02:15:30               │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────┐  ┌─────────────────────────────────────┐
│  📊 ENTRY RADAR     │  │  📈 PRICE CHART & SIGNALS           │
│                     │  │                                     │
│   PE (Put)          │  │   Nifty: 22,450 ▲ +0.5%            │
│   ┌─────────────┐   │  │                                     │
│   │  WAITING    │   │  │   [Chart showing price line]       │
│   │  22,400     │   │  │   [Trigger levels marked]          │
│   │  ▼ 50 pts   │   │  │   [Entry signals as dots]          │
│   └─────────────┘   │  │                                     │
│                     │  │   PE Trigger: ──────── 22,400      │
│   CE (Call)         │  │   CE Trigger: ──────── 22,500      │
│   ┌─────────────┐   │  │   Current:    🔴 22,450            │
│   │  BLOCKED    │   │  │                                     │
│   │  Gap Risk   │   │  └─────────────────────────────────────┘
│   │  ▲ 50 pts   │   │
│   └─────────────┘   │
└─────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  🔍 ACTIVE FILTERS (Entry Filter Type: ALL)                 │
├─────────────────────────────────────────────────────────────┤
│  ✅ RSI     65.2 / 70       PASS   "Not overbought"        │
│  ✅ EMA     BULLISH         PASS   "Trend is up"           │
│  ✅ ADX     28.5 / 25       PASS   "Strong trend"          │
│  ❌ GAP     1.8% / 1.5%     BLOCK  "Gap too large"         │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  📋 RECENT SIGNALS                                          │
├─────────────────────────────────────────────────────────────┤
│  10:45:23  PE ENTRY    @22,350  RSI=62, EMA=Up, ADX=30     │
│  10:30:15  CE REJECTED @22,400  Gap Risk > 1.5%            │
│  10:15:00  PE EXIT     @22,300  Profit Target Hit          │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  📊 TODAY'S STATS                                           │
├─────────────────────────────────────────────────────────────┤
│  Trades Taken: 3    Rejected: 5    P&L: +₹2,450            │
│  Consecutive Losses: 0                                      │
└─────────────────────────────────────────────────────────────┘
```

### Component Structure

```typescript
// VisualEngine.tsx - Main container
<VisualEngine>
  <AlgoStatusCard />
  <div className="grid grid-cols-1 lg:grid-cols-3">
    <EntryRadar />
    <PriceChart className="lg:col-span-2" />
  </div>
  <FilterPanel />
  <SignalTimeline />
  <DailyStats />
</VisualEngine>
```

## 3. Implementation Plan

### Phase 1: Backend - Extend Strategy State

1. **Modify `strategy/survivor.py`:**
   - Add `get_visual_state()` method that returns current filter values, predictions
   - Track recent signals in a circular buffer (last 20)
   - Calculate distance to trigger levels

2. **Modify `web/backend/app/services/strategy_manager.py`:**
   - Add `_visual_state_cache` to store latest visual state
   - Update cache when processing state updates from strategy
   - Add `get_visual_state()` method

3. **Create `web/backend/app/models/schemas.py` additions:**
   - Add `StrategyVisualState`, `FilterStatus`, `EntryPrediction`, `EntrySignal` schemas

4. **Create `web/backend/app/routes/visual.py`:**
   - New endpoint `GET /api/strategy/visual-state`
   - Returns 503 if strategy not running

### Phase 2: Frontend - Visual Engine Component

1. **Add types to `web/frontend/types/index.ts`:**
   - Add StrategyVisualState and related interfaces

2. **Create `web/frontend/components/VisualEngine/`:**
   - `index.tsx` - Main container
   - `AlgoStatusCard.tsx` - Status display
   - `EntryRadar.tsx` - PE/CE prediction cards
   - `FilterPanel.tsx` - Active filters grid
   - `SignalTimeline.tsx` - Recent signals list
   - `DailyStats.tsx` - Today's statistics

3. **Add API to `web/frontend/lib/api.ts`:**
   - `strategyAPI.getVisualState()`

4. **Integrate into dashboard (`web/frontend/app/page.tsx`):**
   - Add VisualEngine tab or section
   - Auto-refresh every 5 seconds when strategy running

### Phase 3: Real-time Updates (Optional)

1. **Extend WebSocket in `web/backend/app/websocket/manager.py`:**
   - Add `visual_state` message type
   - Broadcast on significant changes

2. **Update frontend to listen for visual state updates**

## 4. Key Features

### Entry Prediction Logic
```python
# Calculate distance to trigger
def calculate_prediction(side, current_nifty, pe_value, ce_value, gaps):
    if side == 'PE':
        trigger = current_nifty - pe_value - gaps['pe_gap']
        distance = current_nifty - trigger
    else:
        trigger = current_nifty + ce_value + gaps['ce_gap']
        distance = trigger - current_nifty
    
    # Estimate time based on ATR/volatility
    atr = get_current_atr()
    estimated_minutes = (distance / atr) * 5  # Assuming 5 min per ATR move
    
    return {
        'trigger_price': trigger,
        'distance': distance,
        'estimated_time': f"~{estimated_minutes:.0f} min"
    }
```

### Filter Status Display
- Green checkmark = Pass
- Red X = Fail
- Yellow clock = Waiting for data
- Gray block = Filter disabled

### Signal Timeline
- Shows last 10-20 signals
- Color-coded by type (Entry=Green, Rejection=Red, Exit=Blue)
- Click to expand details

## 5. API Response Example

```json
{
  "is_running": true,
  "uptime_seconds": 8100,
  "current_strategy": "Survivor (enhanced)",
  "last_update": "2026-03-04T10:45:23Z",
  "filters": {
    "entry_filter_type": "ALL",
    "items": [
      {
        "name": "RSI Filter",
        "type": "RSI",
        "enabled": true,
        "current_value": 65.2,
        "threshold": 70,
        "status": "PASS",
        "message": "RSI 65.2 < 70 (Not overbought)"
      },
      {
        "name": "Gap Risk",
        "type": "GAP",
        "enabled": true,
        "current_value": "1.8%",
        "threshold": "1.5%",
        "status": "BLOCKED",
        "message": "Gap 1.8% exceeds 1.5% threshold"
      }
    ]
  },
  "predictions": {
    "pe": {
      "side": "PE",
      "status": "WAITING",
      "current_price": 22450,
      "trigger_price": 22400,
      "distance_to_trigger": 50,
      "distance_percent": 0.22,
      "estimated_time": "~8 min",
      "blocking_reasons": ["Gap risk too high"],
      "next_check": "2026-03-04T10:46:00Z"
    },
    "ce": {
      "side": "CE",
      "status": "BLOCKED",
      "current_price": 22450,
      "trigger_price": 22500,
      "distance_to_trigger": 50,
      "distance_percent": 0.22,
      "estimated_time": null,
      "blocking_reasons": ["Gap risk too high", "Trend is BULLISH"],
      "next_check": "2026-03-04T10:46:00Z"
    }
  },
  "signals": [...],
  "market_context": {
    "nifty_price": 22450,
    "trend": "BULLISH",
    "volatility_regime": "NORMAL",
    "gap_assessment": {
      "can_trade": false,
      "message": "Gap 1.8% exceeds threshold"
    }
  },
  "daily_stats": {
    "trades_taken": 3,
    "trades_rejected": 5,
    "pnl": 2450,
    "consecutive_losses": 0
  }
}
```

## 6. Development Tasks

| Task | File | Description |
|------|------|-------------|
| 1 | `strategy/survivor.py` | Add `get_visual_state()` method |
| 2 | `web/backend/app/models/schemas.py` | Add visual state schemas |
| 3 | `web/backend/app/services/strategy_manager.py` | Cache and expose visual state |
| 4 | `web/backend/app/routes/visual.py` | Create new API endpoint |
| 5 | `web/frontend/types/index.ts` | Add TypeScript interfaces |
| 6 | `web/frontend/lib/api.ts` | Add API client method |
| 7 | `web/frontend/components/VisualEngine/` | Create component directory |
| 8 | `web/frontend/app/page.tsx` | Integrate into dashboard |

---

**Ready for implementation?** Switch to Code mode to start building.