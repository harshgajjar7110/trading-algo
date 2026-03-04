# Portfolio Greeks - Implementation Guide

## Overview

The Greeks calculation module provides real-time risk metrics for your options portfolio using the **Black-Scholes model**. It calculates Delta, Gamma, Theta, Vega, and Rho for each position and aggregates them at the portfolio level.

---

## 📐 What are Greeks?

### Delta (Δ)
- **Measures**: Sensitivity to underlying price changes
- **Unit**: ₹ per 1 point move in NIFTY
- **Example**: Delta = +50 means portfolio gains ₹50 for every 1 point NIFTY rises
- **Range**: -∞ to +∞ (practically -500 to +500 for most portfolios)

### Gamma (Γ)
- **Measures**: Rate of change of Delta (convexity)
- **Unit**: Delta change per 1 point move
- **Example**: Gamma = 0.5 means Delta increases by 0.5 for each point NIFTY rises
- **Range**: -∞ to +∞

### Theta (Θ)
- **Measures**: Time decay (daily)
- **Unit**: ₹ per day
- **Example**: Theta = -100 means portfolio loses ₹100 per day from time decay
- **Note**: Short options have positive theta (benefit from time decay)

### Vega (V)
- **Measures**: Sensitivity to volatility changes
- **Unit**: ₹ per 1% change in implied volatility
- **Example**: Vega = 200 means portfolio gains ₹200 if IV increases by 1%
- **Note**: Short options have negative vega (hurt by volatility increases)

### Rho (ρ)
- **Measures**: Sensitivity to interest rate changes
- **Unit**: ₹ per 1% change in risk-free rate
- **Example**: Rho = 50 means portfolio gains ₹50 if rates increase by 1%
- **Note**: Usually smallest impact for short-term options

---

## 🚀 API Endpoints

### 1. Get Portfolio Greeks
```http
GET /api/greeks/portfolio
```

**Query Parameters:**
- `underlying_price` (optional): Override NIFTY price
- `risk_free_rate` (optional): Default 10% (0.10)

**Response:**
```json
{
  "positions": [
    {
      "symbol": "NIFTY24FEB24000CE",
      "option_type": "CE",
      "strike": 24000,
      "quantity": -50,
      "entry_price": 150.0,
      "delta": -25.5,
      "gamma": -0.0234,
      "theta": 45.2,
      "vega": -89.5,
      "rho": -12.3
    }
  ],
  "portfolio": {
    "delta": -45.2,
    "gamma": -0.0567,
    "theta": 125.5,
    "vega": -245.0,
    "rho": -28.5
  },
  "interpretation": {
    "delta": "Bearish bias: ₹-45 for every 1 point NIFTY rise",
    "gamma": "Short gamma: Delta decreases by -0.057 per point move",
    "theta": "Positive theta: Gaining ₹126/day from time decay",
    "vega": "Short volatility: ₹-245 per 1% vol increase",
    "rho": "Low interest rate sensitivity"
  },
  "underlying_price": 24150.0,
  "calculation_date": "2024-02-18"
}
```

### 2. Scenario Analysis
```http
GET /api/greeks/scenario
```

**Query Parameters:**
- `days_forward` (optional): Days to project (default: 1, max: 30)
- `risk_free_rate` (optional): Default 10%

**Response:**
```json
{
  "current_greeks": { ... },
  "scenarios": [
    {
      "price_change_pct": 1.0,
      "price_change_points": 241.5,
      "volatility_change": 0,
      "days_forward": 1,
      "estimated_pnl": -9850.0,
      "breakdown": {
        "delta_pnl": -10900.0,
        "gamma_pnl": 1350.0,
        "theta_pnl": 125.5,
        "vega_pnl": 0.0
      }
    }
  ],
  "underlying_price": 24150.0,
  "days_forward": 1
}
```

### 3. Get Position Greeks
```http
GET /api/greeks/position/{symbol}
```

**Example:**
```http
GET /api/greeks/position/NIFTY24FEB24000CE
```

---

## 🧮 Black-Scholes Model

### Formula
```
d1 = [ln(S/K) + (r - q + σ²/2) * T] / (σ * √T)
d2 = d1 - σ * √T

Where:
S = Underlying price (NIFTY)
K = Strike price
r = Risk-free rate (10%)
q = Dividend yield (0% for NIFTY)
σ = Implied volatility (15% default)
T = Time to expiry (years)
```

### Greeks Formulas

**Delta:**
- Call: e^(-qT) * N(d1)
- Put: -e^(-qT) * N(-d1)

**Gamma:**
- Both: e^(-qT) * N'(d1) / (S * σ * √T)

**Theta (per year, divide by 365 for daily):**
- Call: -[S * σ * e^(-qT) * N'(d1)] / (2√T) - rKe^(-rT)N(d2) + qSe^(-qT)N(d1)
- Put: -[S * σ * e^(-qT) * N'(d1)] / (2√T) + rKe^(-rT)N(-d2) - qSe^(-qT)N(-d1)

**Vega (per 1% vol change):**
- Both: S * e^(-qT) * N'(d1) * √T / 100

**Rho (per 1% rate change):**
- Call: K * T * e^(-rT) * N(d2) / 100
- Put: -K * T * e^(-rT) * N(-d2) / 100

---

## 🎨 Frontend Components

### GreeksDisplay Component
```tsx
import GreeksDisplay from '@/components/GreeksDisplay';

// Auto-refreshes every 30 seconds
<GreeksDisplay refreshInterval={30000} />
```

**Features:**
- Real-time portfolio Greeks display
- Color-coded risk levels (green/yellow/red)
- Position breakdown table
- Human-readable interpretations
- Toggle to show/hide position details

### Color Coding
- **Green**: Low risk / Favorable
- **Yellow**: Medium risk / Caution
- **Red**: High risk / Danger

---

## 📊 Interpreting Your Greeks

### Short Strangle Example (Sell CE + Sell PE)
```
Delta: 0 to ±50       (Neutral to slight directional bias)
Gamma: Negative       (Short gamma - benefit from range-bound)
Theta: Positive       (Collecting time premium daily)
Vega: Negative        (Short volatility - hurt by spikes)
Rho: Near 0          (Minimal rate sensitivity)
```

### Risk Management Guidelines

| Greek | Safe Zone | Caution Zone | Danger Zone |
|-------|-----------|--------------|-------------|
| Delta | 0-50      | 50-100       | >100        |
| Gamma | -0.1-0.1  | -0.5-0.5     | >0.5        |
| Theta | 0-200     | 200-500      | >500        |
| Vega  | -200-200  | -500-500     | >500        |

---

## ⚙️ Configuration

### Default Parameters
```python
RISK_FREE_RATE = 0.10      # 10% annual (typical for India)
DIVIDEND_YIELD = 0.0       # NIFTY minimal dividends
DEFAULT_VOLATILITY = 0.15  # 15% implied volatility
```

### Customizing Risk-Free Rate
```http
GET /api/greeks/portfolio?risk_free_rate=0.08
```

---

## 🔄 Integration with Strategy

### Automatic Updates
The Greeks display auto-refreshes every 30 seconds to show:
- Current portfolio risk exposure
- Position-level breakdown
- Real-time interpretations

### WebSocket Integration (Future)
Real-time Greeks updates via WebSocket when positions change.

---

## 📈 Scenario Analysis

### Use Cases
1. **Stress Testing**: See P&L under ±5% NIFTY moves
2. **Volatility Planning**: Understand impact of VIX changes
3. **Expiry Planning**: Project theta decay over multiple days

### Example Scenarios
```
Price: -5% (NIFTY drops 1200 points)
Vol: +2% (IV expansion)
Days: 1
→ Estimated P&L: -₹45,000 (delta loss) + ₹500 (gamma) + ₹125 (theta) - ₹500 (vega)
```

---

## 🐛 Troubleshooting

### Issue: Greeks not calculating
**Check:**
1. Positions exist in portfolio
2. Symbol format is valid (NIFTY+YY+MMM+STRIKE+CE/PE)
3. Backend has scipy installed

### Issue: Unrealistic values
**Check:**
1. Underlying price is correct
2. Expiry date parsing is correct
3. Volatility assumption is reasonable

### Issue: Performance slow
**Optimize:**
1. Increase refresh interval (default: 30s)
2. Cache calculations for unchanged positions
3. Use WebSocket for real-time updates

---

## 📝 References

- **Black-Scholes Model**: Fischer Black & Myron Scholes (1973)
- **Greeks Definitions**: John C. Hull, "Options, Futures, and Other Derivatives"
- **NIFTY Options**: NSE India F&O Specifications

---

## 🤝 Contributing

To extend the Greeks module:
1. Add new Greeks (Vanna, Charm, Vomma)
2. Implement implied volatility surface
3. Add historical Greeks tracking
4. Create Greeks-based alerts
