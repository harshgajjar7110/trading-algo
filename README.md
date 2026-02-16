# 🚀 Survivor Trading Algorithm

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Next.js 14](https://img.shields.io/badge/Next.js-14-black.svg)](https://nextjs.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109+-green.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive **algorithmic trading system** for NIFTY options with real-time payoff analysis, multi-broker integration, and an interactive web dashboard. Built for options sellers running strangle/straddle strategies on the Indian stock market.

---

## 📋 Table of Contents

- [Features Overview](#-features-overview)
- [Technology Stack](#-technology-stack)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Project Structure](#-project-structure)
- [API Documentation](#-api-documentation)
- [Usage Examples](#-usage-examples)
- [Payoff Analysis](#-payoff-analysis)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)
- [License](#-license)
- [Contact & Support](#-contact--support)

---

## ✨ Features Overview

### 🎯 Options Trading Strategy
- **Survivor Strategy**: Automated NIFTY options selling based on PE/CE gap triggers
- **Real-time Monitoring**: Live tracking of positions, P&L, and market movements
- **Configurable Parameters**: Gap thresholds, reset points, quantity sizing, and more

### 📊 Payoff Analysis
- **Interactive Charts**: Visual payoff curves using Recharts
- **Breakeven Calculation**: Automatic breakeven points for individual and combined positions
- **Risk Assessment**: Max profit/loss calculations for options strategies
- **Short Strangle Support**: Full support for selling CE and PE options

### 🔗 Multi-Broker Integration
- **Zerodha (Kite Connect)**: Full support with TOTP authentication
- **Fyers**: OAuth-based authentication
- **Fyrodha**: Hybrid broker integration
- **Refresh Token Storage**: Automatic token persistence for seamless re-authentication

### 💻 Web Dashboard
- **Real-time Updates**: WebSocket-based live data streaming
- **Strategy Control**: Start/stop strategy from the UI
- **Position Management**: View and analyze open positions
- **Configuration Editor**: Modify strategy parameters on-the-fly

---

## 🛠 Technology Stack

| Component | Technology |
|-----------|------------|
| **Backend** | Python 3.11+, FastAPI, Uvicorn |
| **Frontend** | Next.js 14, React 18, TypeScript |
| **Charts** | Recharts, Tailwind CSS |
| **Database** | SQLite (Sensibull data) |
| **Broker APIs** | Kite Connect, Fyers API |
| **WebSocket** | FastAPI WebSocket, native browser API |
| **Package Manager** | uv (Python), npm (Node.js) |

---

## 📦 Prerequisites

### Required Software
- **Python 3.11+** - [Download](https://www.python.org/downloads/)
- **Node.js 18+** - [Download](https://nodejs.org/)
- **uv** (Python package manager) - Install: `pip install uv`

### Broker Requirements
- Active trading account with Zerodha or Fyers
- API credentials (API Key, API Secret)
- For Zerodha: TOTP authenticator app configured

---

## 🚀 Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/trading-algo.git
cd trading-algo
```

### Step 2: Set Up Python Environment

```bash
# Install uv if not already installed
pip install uv

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -r pyproject.toml
```

### Step 3: Configure Environment Variables

```bash
# Copy sample environment file
cp .sample.env .env

# Edit .env with your credentials
nano .env  # Or use your preferred editor
```

### Step 4: Install Frontend Dependencies

```bash
cd web/frontend
npm install
cd ../..
```

### Step 5: Start the Application

**Terminal 1 - Backend:**
```bash
cd web/backend
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Terminal 2 - Frontend:**
```bash
cd web/frontend
npm run dev
```

### Step 6: Access the Dashboard

Open your browser and navigate to: **http://localhost:3000**

---

## ⚙️ Configuration

### Environment Variables

Create a `.env` file in the project root with the following variables:

```bash
# =============================================================================
# Broker Configuration
# =============================================================================
BROKER_NAME=zerodha  # Options: zerodha, fyers, fyrodha
BROKER_API_KEY=your_api_key_here
BROKER_API_SECRET=your_api_secret_here

# =============================================================================
# Authentication Mode
# =============================================================================
BROKER_LOGIN_MODE=auto  # Options: auto, manual, totp

# =============================================================================
# Zerodha Specific (Required for TOTP login)
# =============================================================================
BROKER_ID=your_zerodha_user_id
BROKER_PASSWORD=your_zerodha_password
BROKER_TOTP_KEY=your_totp_secret_key

# =============================================================================
# Refresh Token (Auto-generated after first login)
# =============================================================================
BROKER_REFRESH_TOKEN=  # Leave empty, will be auto-filled
```

### Broker API Setup

#### Zerodha (Kite Connect)

1. Visit [Kite Connect Developer Portal](https://kite.trade/)
2. Create a new app to get API credentials
3. Enable TOTP in your Zerodha account security settings
4. Get your TOTP secret key from the Kite app settings

#### Fyers

1. Visit [Fyers API Dashboard](https://myapi.fyers.in/)
2. Create a new app and get API credentials
3. Set the redirect URI in your app settings

### Strategy Configuration

Edit `strategy/configs/survivor.yml` to customize trading parameters:

```yaml
# Core Parameters
index_symbol: "NIFTY 50"
symbol_initials: "NIFTY"

# Gap Triggers
pe_gap: 100          # PE strike gap from spot
ce_gap: 100          # CE strike gap from spot
pe_reset_gap: 50     # Reset trigger for PE
ce_reset_gap: 50     # Reset trigger for CE

# Position Sizing
pe_quantity: 50      # Quantity for PE options
ce_quantity: 50      # Quantity for CE options

# Risk Management
min_price_to_sell: 10.0
sell_multiplier_threshold: 3

# Order Settings
exchange: "NFO"
order_type: "MARKET"
product_type: "MIS"
```

---

## 📁 Project Structure

```
trading-algo/
├── .env                      # Environment variables (not in git)
├── .sample.env               # Sample environment file
├── pyproject.toml            # Python dependencies
├── README.md                 # This file
│
├── brokers/                  # Broker integration module
│   ├── __init__.py
│   ├── config.py             # Broker configuration
│   ├── registry.py           # Broker driver registry
│   ├── core/
│   │   ├── enums.py          # Trading enums
│   │   ├── errors.py         # Custom exceptions
│   │   ├── gateway.py        # Broker gateway abstraction
│   │   ├── interface.py      # Broker driver interface
│   │   └── schemas.py        # Data models
│   ├── integrations/
│   │   ├── zerodha/          # Zerodha (Kite Connect)
│   │   ├── fyers/            # Fyers API
│   │   └── fyrodha/          # Hybrid integration
│   └── auth/
│       ├── manual.py         # Manual auth flow
│       ├── totp.py           # TOTP authentication
│       └── tokens.py         # Token management
│
├── strategy/                 # Trading strategies
│   ├── survivor.py           # Survivor options strategy
│   ├── wave.py               # Wave strategy
│   └── configs/
│       ├── survivor.yml      # Survivor config
│       └── wave.yml          # Wave config
│
├── sensibull/                # Sensibull data scraper
│   ├── app.py                # Flask app for data
│   ├── scraper.py            # Web scraper
│   └── database.py           # SQLite operations
│
├── web/                      # Web application
│   ├── backend/              # FastAPI backend
│   │   ├── app/
│   │   │   ├── main.py       # Application entry
│   │   │   ├── config.py     # App configuration
│   │   │   ├── models/
│   │   │   │   └── schemas.py  # Pydantic models
│   │   │   ├── routes/
│   │   │   │   ├── strategy.py
│   │   │   │   ├── positions.py
│   │   │   │   ├── market.py
│   │   │   │   ├── config.py
│   │   │   │   └── analysis.py
│   │   │   ├── services/
│   │   │   │   ├── broker_service.py
│   │   │   │   └── strategy_manager.py
│   │   │   └── websocket/
│   │   │       └── manager.py
│   │   └── requirements.txt
│   │
│   └── frontend/             # Next.js frontend
│       ├── app/
│       │   ├── page.tsx      # Dashboard page
│       │   ├── layout.tsx    # Root layout
│       │   ├── config/       # Config editor
│       │   └── history/      # Trade history
│       ├── components/
│       │   └── PayoffChart.tsx  # Payoff analysis
│       ├── hooks/
│       │   └── useWebSocket.ts  # WebSocket hook
│       ├── lib/
│       │   ├── api.ts        # API client
│       │   └── utils.ts      # Utility functions
│       ├── types/
│       │   └── index.ts      # TypeScript types
│       └── package.json
│
└── tests/                    # Test files
    └── ...
```

---

## 📚 API Documentation

### REST Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/strategy/status` | GET | Get current strategy status |
| `/api/strategy/start` | POST | Start the trading strategy |
| `/api/strategy/stop` | POST | Stop the trading strategy |
| `/api/positions` | GET | Get all open positions |
| `/api/market/nifty` | GET | Get NIFTY index data |
| `/api/market/funds` | GET | Get account funds/margin |
| `/api/config` | GET | Get strategy configuration |
| `/api/config` | PUT | Update strategy configuration |
| `/api/analysis/payoff` | GET | Get payoff analysis for positions |

### WebSocket Events

Connect to `ws://localhost:8000/ws` for real-time updates.

**Server → Client Messages:**

```typescript
// Strategy State Update
{
  "type": "STRATEGY_STATE",
  "data": {
    "status": "RUNNING",
    "nifty_pe_last_value": 24000,
    "nifty_ce_last_value": 24100,
    "pe_reset_flag": false,
    "ce_reset_flag": false
  },
  "timestamp": "2024-02-16T10:30:00Z"
}

// Price Update
{
  "type": "PRICE_UPDATE",
  "data": {
    "symbol": "NSE:NIFTY 50",
    "last_price": 24050.25,
    "change": 45.50
  },
  "timestamp": "2024-02-16T10:30:01Z"
}
```

**Client → Server Messages:**

```typescript
// Subscribe to symbols
{
  "type": "SUBSCRIBE",
  "symbols": ["NSE:NIFTY 50", "NFO:NIFTY24FEB24000PE"]
}

// Heartbeat
{
  "type": "PING"
}
```

### Example API Calls

**Get Positions:**
```bash
curl http://localhost:8000/api/positions
```

**Start Strategy:**
```bash
curl -X POST http://localhost:8000/api/strategy/start
```

**Get Payoff Analysis:**
```bash
curl http://localhost:8000/api/analysis/payoff
```

---

## 💡 Usage Examples

### Starting the Strategy via API

```python
import requests

# Start the strategy
response = requests.post('http://localhost:8000/api/strategy/start')
result = response.json()

print(f"Status: {result['status']}")
print(f"Message: {result['message']}")
```

### Getting Positions with Python

```python
import requests

# Get current positions
response = requests.get('http://localhost:8000/api/positions')
data = response.json()

for position in data['positions']:
    print(f"{position['symbol']}: {position['quantity']} @ ₹{position['average_price']}")
    print(f"  P&L: ₹{position['pnl']} ({position['pnl_percent']}%)")
```

### WebSocket Client Example

```javascript
const ws = new WebSocket('ws://localhost:8000/ws');

ws.onopen = () => {
  console.log('Connected to WebSocket');
  ws.send(JSON.stringify({ type: 'SUBSCRIBE', symbols: ['NSE:NIFTY 50'] }));
};

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  
  switch (message.type) {
    case 'STRATEGY_STATE':
      updateStrategyStatus(message.data);
      break;
    case 'PRICE_UPDATE':
      updatePrice(message.data);
      break;
  }
};
```

---

## 📈 Payoff Analysis

### Understanding the Payoff Chart

The payoff analysis provides a visual representation of your options positions:

```
P&L (₹)
    ^
    |     ╱╲
    |    ╱  ╲
    |   ╱    ╲
    |  ╱      ╲
    | ╱        ╲
----+--------------> NIFTY Price
    |    BE1  BE2
    |
```

**For a SHORT STRANGLE (selling CE + PE):**
- **Maximum Profit**: Limited to premium received (middle range)
- **Maximum Loss**: Unlimited on upside (CE), Limited on downside (PE)
- **Breakeven Points**: Two points where P&L = 0

### Payoff Table Columns

| Column | Description |
|--------|-------------|
| Symbol | Option trading symbol |
| Type | CE (Call) or PE (Put) |
| Strike | Strike price of the option |
| Qty | Position quantity (negative = SHORT) |
| Premium | Price at which option was sold/bought |
| Breakeven | Price where P&L = 0 |
| BE % | Distance from current NIFTY to breakeven |
| Max Profit | Maximum possible profit |
| Max Loss | Maximum possible loss |
| Current P&L | Unrealized profit/loss |

### Example Payoff Response

```json
{
  "positions": [
    {
      "symbol": "NIFTY24FEB24000PE",
      "option_type": "PE",
      "strike_price": 24000,
      "side": "SHORT",
      "quantity": 50,
      "premium": 25.50,
      "breakeven": 23974.50,
      "breakeven_percent": -0.52,
      "max_profit": 1275.00,
      "max_loss": 1198725.00,
      "current_pnl": 350.00
    }
  ],
  "combined_breakeven_points": [23850.00, 24250.00],
  "total_max_profit": 5000.00,
  "current_nifty_price": 24100.00
}
```

---

## 🔧 Troubleshooting

### Common Issues

#### 1. "Unknown broker" Error

**Problem:** `ValueError: Unknown broker 'Zerodha'`

**Solution:** 
- Check that `BROKER_NAME` in `.env` matches exactly: `zerodha` (lowercase)
- Ensure broker driver is properly registered

#### 2. WebSocket Connection Issues

**Problem:** Frontend shows "Disconnected" status

**Solution:**
- Verify backend is running on port 8000
- Check CORS settings in `web/backend/app/main.py`
- Ensure firewall allows WebSocket connections

#### 3. Authentication Failures

**Problem:** TOTP login fails repeatedly

**Solution:**
- Verify `BROKER_TOTP_KEY` is correct (scan QR again)
- Check system time is synchronized
- Try manual login mode: `BROKER_LOGIN_MODE=manual`

#### 4. Module Import Errors

**Problem:** `ModuleNotFoundError: No module named 'brokers'`

**Solution:**
- Ensure you're in the project root directory
- Activate virtual environment: `source .venv/bin/activate`
- Reinstall dependencies: `uv pip install -r pyproject.toml`

#### 5. Position P&L Shows Double

**Problem:** P&L appears to be doubled

**Solution:**
- This was fixed in recent versions
- Ensure you're using the latest code
- The fix uses only 'net' positions, not 'day' + 'net'

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Health Check

```bash
curl http://localhost:8000/health
```

---

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

### Getting Started

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes
4. Run tests: `pytest tests/`
5. Commit changes: `git commit -m 'Add amazing feature'`
6. Push to branch: `git push origin feature/amazing-feature`
7. Open a Pull Request

### Code Style

- **Python**: Follow PEP 8 guidelines
- **TypeScript**: Use ESLint configuration
- **Commits**: Use conventional commit messages

### Testing

```bash
# Run Python tests
pytest tests/ -v

# Run frontend tests
cd web/frontend && npm test
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

```
MIT License

Copyright (c) 2024 Trading Algorithm Project

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## 📞 Contact & Support

### Documentation
- [API Documentation](http://localhost:8000/docs) - Available when backend is running
- [Strategy Configuration Guide](./docs/strategy-config.md)

### Community
- **Issues**: [GitHub Issues](https://github.com/yourusername/trading-algo/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/trading-algo/discussions)

### Disclaimer

> ⚠️ **Trading Risk Warning**
> 
> This software is for educational purposes only. Options trading involves substantial risk of loss and is not suitable for all investors. Past performance is not indicative of future results. Always do your own research and consider your risk tolerance before trading.

---

<div align="center">

**Built with ❤️ for the Indian Options Trading Community**

[⬆ Back to Top](#-survivor-trading-algorithm)

</div>
