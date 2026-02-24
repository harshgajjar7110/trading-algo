# Survivor Trading Strategy - Web UI

A modern web-based dashboard for monitoring and controlling the Survivor options trading strategy. Built with Next.js 14 (frontend) and FastAPI (backend) with real-time WebSocket support.

## Features

- **Real-time Dashboard**: Monitor strategy status, NIFTY price, positions, and P&L in real-time
- **Strategy Control**: Start, stop, and restart the trading strategy from the UI
- **Configuration Editor**: Modify strategy parameters through an intuitive form interface
- **Trade History**: View historical orders and trades with filtering options
- **WebSocket Integration**: Real-time price updates and strategy state changes

## Architecture

```
web/
├── backend/                 # FastAPI backend
│   ├── app/
│   │   ├── main.py         # Application entry point
│   │   ├── config.py       # Configuration management
│   │   ├── routes/         # API routes
│   │   │   ├── strategy.py # Strategy control endpoints
│   │   │   ├── config.py   # Configuration endpoints
│   │   │   ├── positions.py# Position/order endpoints
│   │   │   └── market.py   # Market data endpoints
│   │   ├── services/       # Business logic
│   │   │   ├── strategy_manager.py
│   │   │   └── broker_service.py
│   │   ├── websocket/      # WebSocket support
│   │   │   └── manager.py
│   │   └── models/         # Pydantic models
│   │       └── schemas.py
│   └── requirements.txt
│
└── frontend/               # Next.js frontend
    ├── app/
    │   ├── page.tsx       # Dashboard
    │   ├── config/        # Configuration editor
    │   └── history/       # Trade history
    ├── hooks/
    │   └── useWebSocket.ts
    ├── lib/
    │   ├── api.ts         # API client
    │   └── utils.ts       # Utilities
    └── types/
        └── index.ts       # TypeScript types
```

## Prerequisites

- Python 3.10+
- Node.js 18+
- npm or yarn
- Broker account (Zerodha or Fyers) with API credentials

## Setup

### 1. Environment Configuration

Ensure your main project `.env` file is configured with broker credentials:

```env
BROKER_NAME=zerodha
BROKER_API_KEY=your_api_key
BROKER_API_SECRET=your_api_secret
# ... other broker settings
```

### 2. Backend Setup

```bash
# Navigate to backend directory
cd web/backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the backend server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`
API Documentation: `http://localhost:8000/docs`

### 3. Frontend Setup

```bash
# Navigate to frontend directory
cd web/frontend

# Install dependencies
npm install

# Run development server
npm run dev
```

The UI will be available at `http://localhost:3000`

## API Endpoints

### Strategy Control

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/strategy/status` | Get current strategy status |
| POST | `/api/strategy/start` | Start the strategy |
| POST | `/api/strategy/stop` | Stop the strategy |
| POST | `/api/strategy/restart` | Restart the strategy |

### Configuration

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/config` | Get current configuration |
| PUT | `/api/config` | Update configuration |
| POST | `/api/config/validate` | Validate configuration |

### Positions & Orders

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/positions` | Get current positions |
| GET | `/api/orders` | Get order book |
| GET | `/api/trades` | Get trade history |

### Market Data

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/market/quote/{symbol}` | Get quote for symbol |
| GET | `/api/market/nifty` | Get NIFTY index data |
| GET | `/api/market/funds` | Get account funds |

## WebSocket Events

Connect to `ws://localhost:8000/ws` for real-time updates.

### Server → Client Messages

```typescript
// Price update
{
  "type": "PRICE_UPDATE",
  "data": {
    "symbol": "NSE:NIFTY 50",
    "last_price": 24500.50,
    "change": 25.5
  },
  "timestamp": "2024-01-15T10:30:00Z"
}

// Strategy state update
{
  "type": "STRATEGY_STATE",
  "data": {
    "status": "RUNNING",
    "nifty_pe_last_value": 24500,
    "nifty_ce_last_value": 24500,
    "pe_reset_flag": false,
    "ce_reset_flag": true
  },
  "timestamp": "2024-01-15T10:30:00Z"
}

// Order update
{
  "type": "ORDER_UPDATE",
  "data": {
    "order_id": "123456",
    "symbol": "NIFTY26FEB24300PE",
    "status": "COMPLETE",
    "quantity": 50,
    "price": 45.5
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Client → Server Messages

```typescript
// Subscribe to symbols
{
  "type": "SUBSCRIBE",
  "symbols": ["NSE:NIFTY 50", "NIFTY26FEB24300PE"]
}

// Unsubscribe from symbols
{
  "type": "UNSUBSCRIBE",
  "symbols": ["NIFTY26FEB24300PE"]
}

// Ping/Pong
{
  "type": "PING"
}
```

## Configuration Parameters

### Core Parameters
- `index_symbol`: Underlying index for tracking (e.g., "NSE:NIFTY 50")
- `symbol_initials`: Option series identifier (e.g., "NIFTY26FEB")

### Gap Parameters
- `pe_gap`: NIFTY upward movement threshold to trigger PE sells
- `ce_gap`: NIFTY downward movement threshold to trigger CE sells
- `pe_reset_gap`: Favorable movement threshold to reset PE reference
- `ce_reset_gap`: Favorable movement threshold to reset CE reference

### Strike Selection
- `pe_symbol_gap`: Distance below current price for PE strike selection
- `ce_symbol_gap`: Distance above current price for CE strike selection

### Position Sizing
- `pe_quantity`: Base quantity for PE option trades
- `ce_quantity`: Base quantity for CE option trades

### Risk Management
- `min_price_to_sell`: Minimum option premium threshold for execution
- `sell_multiplier_threshold`: Maximum allowed position multiplier

## Development

### Running in Development Mode

```bash
# Terminal 1: Backend
cd web/backend
uvicorn app.main:app --reload --port 8000

# Terminal 2: Frontend
cd web/frontend
npm run dev
```

### Building for Production

```bash
# Backend
cd web/backend
pip install -r requirements.txt

# Frontend
cd web/frontend
npm run build
npm start
```

## Troubleshooting

### Common Issues

1. **CORS Errors**: Ensure the backend CORS settings include your frontend URL
2. **WebSocket Connection Failed**: Check that the backend is running and accessible
3. **Broker Authentication**: Verify your broker credentials in `.env`
4. **Strategy Won't Start**: Check the backend logs for error messages

### Logs

- Backend logs are printed to console
- Frontend logs are available in browser developer tools

## Security Notes

- This UI is designed for local/development use
- For production deployment, add authentication
- Never expose the API directly to the internet without proper security measures
- Keep your broker credentials secure

## License

This project is for educational purposes. See the main project README for disclaimer.
