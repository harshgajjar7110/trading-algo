# Multi-Strategy System Guide

## Overview

The trading platform now supports running **multiple strategies** with different parameters. You can select which strategy to run, review its configuration, and confirm before starting.

---

## 🎯 Key Features

1. **Strategy Selection**: Choose between Survivor and Enhanced Survivor strategies
2. **Configuration Preview**: Review all parameters before starting
3. **Parameter Confirmation**: See differences from defaults and confirm
4. **Strategy Tracking**: Know which strategy is running at any time
5. **Risk Indicators**: See risk level and recommended capital for each strategy

---

## 📊 Available Strategies

| Strategy | ID | Risk Level | Description |
|----------|-----|------------|-------------|
| **Survivor** | `survivor` | Medium | Classic gap-based options selling. Simple and reliable. |
| **Enhanced Survivor** | `enhanced_survivor` | Medium-High | Advanced with RSI/EMA filters, dynamic gaps, stop-losses. |

---

## 🚀 How to Use

### 1. Select Strategy

Go to the dashboard and find the **Strategy Selection** section:

```
┌─────────────────────────────────────────────────────┐
│ Strategy Selection                                  │
├─────────────────────────────────────────────────────┤
│ Select Strategy: [Choose a strategy...        ▼]   │
│                                                     │
│ Strategies:                                         │
│ • Survivor Strategy (medium risk)                  │
│ • Enhanced Survivor Strategy (medium-high risk)    │
└─────────────────────────────────────────────────────┘
```

### 2. Review Strategy Details

After selecting, you'll see:
- **Description**: What the strategy does
- **Risk Level**: Low/Medium/Medium-High/High
- **Recommended Capital**: Minimum suggested capital
- **Tags**: Features like "technical-indicators", "risk-management"

### 3. Preview Configuration

Click **"Review & Start Strategy"** to see:

```
┌─────────────────────────────────────────────────────┐
│ ⚠️ Confirm Strategy Configuration                   │
├─────────────────────────────────────────────────────┤
│ Key Parameters:                                     │
│   Symbol: NIFTY26FEB                               │
│   PE Gap: 40                                        │
│   CE Gap: 40                                        │
│   PE Qty: 65                                        │
│   CE Qty: 65                                        │
│   Filter: RSI                                       │
├─────────────────────────────────────────────────────┤
│ Custom Parameters (changed from defaults):          │
│   pe_gap:     25 → 40  ✏️                          │
│   rsi_max:    70 → 65  ✏️                          │
├─────────────────────────────────────────────────────┤
│ Risk Level: medium-high | Capital: ₹10L+           │
├─────────────────────────────────────────────────────┤
│ [Confirm & Start]  [Go Back]                       │
└─────────────────────────────────────────────────────┘
```

### 4. Confirm & Start

Review the configuration and click **"Confirm & Start"** to run the strategy.

---

## 🔍 Finding Which Strategy is Running

### Dashboard Status Card

The **Strategy Status** card now shows:

```
┌─────────────────────────────────────────────────────┐
│ Strategy Status                          RUNNING   │
├─────────────────────────────────────────────────────┤
│ Running: Enhanced Survivor Strategy               │
│ Uptime: 2h 15m                                     │
├─────────────────────────────────────────────────────┤
│ [Stop] [Restart]                                   │
└─────────────────────────────────────────────────────┘
```

### API Endpoint

```bash
# Get current strategy
curl http://localhost:8000/api/strategy/current

{
  "running": true,
  "id": "enhanced_survivor_20260219_103045",
  "strategy_id": "enhanced_survivor",
  "name": "Enhanced Survivor Strategy",
  "status": "RUNNING",
  "started_at": "2026-02-19T10:30:45",
  "pid": 12345,
  "config_summary": {
    "symbol_initials": "NIFTY26FEB",
    "pe_gap": 40,
    "ce_gap": 40,
    "entry_filter_type": "RSI"
  }
}
```

---

## ⚙️ API Endpoints

### List Available Strategies
```bash
GET /api/strategy/available
```

### Get Strategy Details
```bash
GET /api/strategy/{strategy_id}/details
```

### Preview Configuration
```bash
POST /api/strategy/preview-config
{
  "strategy_id": "enhanced_survivor",
  "config_override": {
    "pe_gap": 50,
    "rsi_max": 65
  }
}
```

### Start Strategy (with confirmation flow)
```bash
# Step 1: Request without confirmation
POST /api/strategy/start
{
  "strategy_id": "enhanced_survivor",
  "confirmed": false
}

# Response: Requires confirmation
{
  "success": false,
  "requires_confirmation": true,
  "preview": { ... }
}

# Step 2: Confirm and start
POST /api/strategy/start
{
  "strategy_id": "enhanced_survivor",
  "confirmed": true
}
```

### Stop Strategy
```bash
POST /api/strategy/stop
```

---

## 📝 Configuration Files

Each strategy has its own config file:

| Strategy | Config File |
|----------|-------------|
| Survivor | `strategy/configs/survivor.yml` |
| Enhanced Survivor | `strategy/configs/survivor_enhanced.yml` |

### Example Config Structure

```yaml
default:
  symbol_initials: "NIFTY26FEB"
  pe_gap: 40
  ce_gap: 40
  pe_quantity: 65
  ce_quantity: 65
  # ... other params
```

---

## 🔧 Adding a New Strategy

To add a new strategy:

1. **Create strategy class** in `strategy/my_strategy.py`
2. **Create config file** at `strategy/configs/my_strategy.yml`
3. **Register in registry** (`web/backend/app/services/strategy_registry.py`):

```python
self.register(StrategyInfo(
    id="my_strategy",
    name="My Strategy",
    description="Description here...",
    class_path="strategy.my_strategy:MyStrategyClass",
    config_path=os.path.join(project_root, "strategy", "configs", "my_strategy.yml"),
    risk_level="medium",
    recommended_capital="₹5L+",
    tags=["options", "selling"],
))
```

---

## 🎨 Frontend Components

| Component | Purpose |
|-----------|---------|
| `StrategySelector.tsx` | Dropdown to select and start strategies |
| `StrategyConfirmationDialog` | Shows config preview and asks for confirmation |
| Dashboard Status Card | Shows which strategy is currently running |

---

## 🐛 Troubleshooting

### Strategy not appearing in dropdown
- Check that strategy is registered in `strategy_registry.py`
- Verify strategy class exists and imports correctly
- Check backend logs for errors

### "Failed to load strategy details" error
- Verify config file exists at correct path
- Check YAML syntax in config file
- Look for import errors in backend logs

### Strategy shows "UNKNOWN" expiry
- Update Greeks calculator symbol parsing for your symbol format
- Check symbol format matches expected patterns

---

## 📁 Files Created/Modified

### Backend
- `web/backend/app/services/strategy_registry.py` - Strategy registry
- `web/backend/app/services/strategy_manager_v2.py` - Multi-strategy manager
- `web/backend/app/routes/strategy_selector.py` - API endpoints
- `web/backend/app/models/schemas.py` - Updated StrategyState model
- `web/backend/app/main.py` - Added new router

### Frontend
- `web/frontend/components/StrategySelector.tsx` - Strategy selection UI
- `web/frontend/types/index.ts` - Added strategy types
- `web/frontend/lib/api.ts` - Added strategySelectorAPI
- `web/frontend/app/page.tsx` - Integrated StrategySelector

---

## 🔄 Migration from Old System

The old strategy API still works for backward compatibility:
- `/api/strategy/start` (old) - Starts default Survivor strategy
- `/api/strategy/start` (new) - Accepts `strategy_id` parameter

To use the new system, pass `strategy_id` in requests.
