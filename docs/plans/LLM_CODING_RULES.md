# Trading Algorithm Project - LLM Coding Agent Rules

## Project Overview
A Python-based algorithmic trading system with broker abstractions (Fyers, Zerodha), trading strategies (Survivor, Wave), and a web interface. Uses Python 3.12+ with modern typing and async patterns.

---

## Python Standards

### General
- Use `from __future__ import annotations` in every Python file
- Target Python 3.12+; use modern syntax (e.g., `|` union types, `dict[str, Any]`)
- Use type hints for ALL function signatures and variables
- Use `@dataclass` or `NamedTuple` over raw dictionaries for data structures
- Prefer functional patterns; avoid classes when functions suffice
- Use `__slots__` for memory-critical high-frequency objects

### Naming Conventions
- **Files/Directories**: lowercase with underscores (e.g., `broker_gateway.py`, `order_routes/`)
- **Functions**: snake_case with auxiliary verbs (e.g., `is_valid_order`, `has_sufficient_funds`)
- **Variables**: snake_case with context (e.g., `order_request`, `raw_response`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `MAX_RETRY_ATTEMPTS`)
- **Classes**: PascalCase only when necessary (dataclasses, exceptions, ABCs)
- **Enums**: PascalCase with values as UPPER_SNAKE_CASE

### Imports
- Group imports: stdlib → third-party → local
- Use absolute imports for cross-module references
- Lazy import optional dependencies with try/except blocks
- Re-export key types in package `__init__.py` files

---

## Code Patterns

### Function Definitions
```python
# Use def for pure/synchronous operations
def calculate_pnl(entry_price: float, exit_price: float, quantity: int) -> float:
    return (exit_price - entry_price) * quantity

# Use async def for I/O-bound operations (network, database, file system)
async def fetch_quotes(symbols: list[str]) -> dict[str, Quote]:
    ...
```

### Data Transfer Objects (RORO Pattern)
```python
from dataclasses import dataclass, field
from typing import Any, Optional

@dataclass
class OrderRequest:
    symbol: str
    exchange: Exchange
    quantity: int
    order_type: OrderType
    transaction_type: TransactionType
    product_type: ProductType
    price: Optional[float] = None
    stop_price: Optional[float] = None
    validity: Validity = Validity.DAY
    tag: Optional[str] = None
    extras: dict[str, Any] = field(default_factory=dict)
```

### Error Handling
- Use custom exception hierarchy inheriting from base project exception
- Include context in exceptions for debugging
- Use specific exception types over generic Exception

```python
class BrokerError(Exception):
    def __init__(self, message: str, *, context: Optional[dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.context = context or {}

class AuthError(BrokerError): ...
class RateLimitError(BrokerError): ...
class ValidationError(BrokerError): ...
```

### Enums (StrEnum for JSON serialization)
```python
from enum import Enum

class Exchange(str, Enum):
    NSE = "NSE"
    BSE = "BSE"
    NFO = "NFO"
    BFO = "BFO"
    MCX = "MCX"
    CDS = "CDS"

class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"
```

### Registry Pattern (for extensibility)
```python
from typing import Callable, Dict, TypeVar

T = TypeVar("T")

class Registry:
    _registry: Dict[str, Callable[[], T]] = {}

    @classmethod
    def register(cls, name: str, factory: Callable[[], T]) -> None:
        cls._registry[name.lower()] = factory

    @classmethod
    def create(cls, name: str) -> T:
        key = name.lower()
        if key not in cls._registry:
            raise ValueError(f"Unknown key '{name}'. Registered: {list(cls._registry)}")
        return cls._registry[key]()
```

---

## Project Structure

```
trading-algo/
├── brokers/                  # Broker abstraction layer
│   ├── __init__.py          # Public API exports
│   ├── registry.py          # Broker registry
│   ├── config.py            # Configuration utilities
│   ├── core/                # Core abstractions
│   │   ├── enums.py         # Exchange, OrderType, etc.
│   │   ├── errors.py        # Exception hierarchy
│   │   ├── schemas.py       # Dataclass models (OrderRequest, Position, etc.)
│   │   ├── interface.py     # BrokerDriver ABC
│   │   └── gateway.py       # BrokerGateway facade
│   ├── integrations/        # Broker implementations
│   │   ├── fyers/
│   │   ├── zerodha/
│   │   └── fyrodha/
│   ├── mappings/            # Enum/string mappings per broker
│   ├── symbols/             # Symbol normalization/resolution
│   └── net/                 # HTTP, rate limiting
├── strategy/                # Trading strategies
│   ├── survivor.py
│   ├── wave.py
│   └── configs/
│       ├── survivor.yml
│       └── wave.yml
├── orders.py                # Order tracking/management
├── dispatcher.py            # Data routing
├── logger.py                # Centralized logging
├── web/                     # Web interface (if applicable)
│   ├── backend/
│   └── frontend/
├── utils/                   # Shared utilities
├── docs/                    # Documentation
├── pyproject.toml          # Dependencies (uv)
└── .env                    # Environment variables (not committed)
```

### File Organization Rules
1. **Exported Router/Module**: Each module exposes a clean public API via `__all__`
2. **Sub-routes**: Group related functionality in subdirectories
3. **Utilities**: Place in `utils/` or module-specific `_utils.py`
4. **Static Content**: Place in `artifacts/`, `logs/`, or appropriate directories
5. **Types/Models**: Use `schemas.py` for dataclasses, `enums.py` for enumerations

---

## Broker Integration Standards

### Driver Implementation
```python
class BrokerDriver(ABC):
    def __init__(self) -> None:
        self.capabilities: BrokerCapabilities = BrokerCapabilities()

    @abstractmethod
    def get_funds(self) -> Funds: ...

    @abstractmethod
    def place_order(self, request: OrderRequest) -> OrderResponse: ...
```

### Gateway Pattern
Use `BrokerGateway` as the facade for all broker operations:
```python
gateway = BrokerGateway.from_name("fyers")
funds = gateway.get_funds()
response = gateway.place_order(order_request)
```

### Symbol Normalization
- Use internal format: `{EXCHANGE}:{SYMBOL}` (e.g., `NSE:RELIANCE`)
- Register broker-specific resolvers in `symbols/resolvers.py`
- Convert via `symbol_registry.to_broker_symbol(broker_name, internal)`

---

## Async Patterns

### Prefer Async for I/O
```python
# Network calls, database operations, file I/O
async def fetch_historical_data(symbol: str, interval: str) -> list[Candle]:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            return [Candle(**c) for c in data]
```

### Lifespan Context Managers
```python
from contextlib import asynccontextmanager
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await initialize_connections()
    yield
    # Shutdown
    await close_connections()

app = FastAPI(lifespan=lifespan)
```

### Avoid @app.on_event
Replace with lifespan context managers for better resource management.

---

## Error Handling & Logging

### Logging Standards
```python
from logger import logger  # Use centralized logger

# Use appropriate levels
logger.debug("Detailed diagnostic information")
logger.info("General operational events")
logger.warning("Unexpected but handled situations")
logger.error("Errors that prevent specific operations")
```

### Defensive Programming
```python
# Validate early
if not symbol or not isinstance(symbol, str):
    raise ValidationError(f"Invalid symbol: {symbol}")

# Use optional chaining patterns
order_id = order.get("order_id") or order.get("id")

# Handle None gracefully
price = quote.last_price if quote else 0.0
```

---

## Configuration

### Environment Variables
- Load via `python-dotenv` (best-effort in package init)
- Use env vars for credentials, API keys, sensitive data
- Never commit `.env` files

### YAML Configuration (Strategies)
```yaml
default:
  index_symbol: "NSE:NIFTY 50"
  pe_gap: 50
  ce_gap: 50
  pe_quantity: 65
  ce_quantity: 65
```

### Type-Safe Config Access
```python
import os
from typing import Optional

def get_broker_config() -> dict[str, Optional[str]]:
    return {
        "api_key": os.getenv("BROKER_API_KEY"),
        "api_secret": os.getenv("BROKER_API_SECRET"),
        "login_mode": os.getenv("BROKER_LOGIN_MODE", "auto"),
    }
```

---

## Performance Guidelines

### Memory Optimization
```python
# Use __slots__ for high-frequency objects
class Candle:
    __slots__ = ['ts', 'open', 'high', 'low', 'close']
    
    def __init__(self, ts: float, open_price: float, high: float, low: float, close: float):
        self.ts = ts
        self.open = open_price
        self.high = high
        self.low = low
        self.close = close
```

### Lazy Loading
```python
@property
def instruments(self) -> list[Instrument]:
    if self._instruments is None:
        self._instruments = self._load_instruments()
    return self._instruments
```

### Caching
- Use `functools.lru_cache` for deterministic, expensive computations
- Use Redis/in-memory stores for API response caching

---

## Testing & Quality

### Code Style
- Run `ruff` for linting and formatting
- Use type hints throughout (checked with `mypy` or `pyright`)
- Maximum line length: 100 characters

### Test Patterns
```python
def test_order_request_validation():
    request = OrderRequest(
        symbol="RELIANCE",
        exchange=Exchange.NSE,
        quantity=100,
        order_type=OrderType.MARKET,
        transaction_type=TransactionType.BUY,
        product_type=ProductType.INTRADAY,
    )
    assert request.symbol == "RELIANCE"
    assert request.validity == Validity.DAY  # Default value
```

---

## Dependencies

### Core Stack
- **Package Manager**: `uv` (preferred) or `pip`
- **Configuration**: `pyproject.toml`
- **Data**: `pandas`, `numpy` for numerical operations
- **HTTP**: `requests` (sync), `aiohttp` (async)
- **Broker SDKs**: `fyers-apiv3`, `kiteconnect`
- **Security**: `pyotp` for TOTP authentication
- **Environment**: `python-dotenv`

### Optional Dependencies
Lazy import with graceful degradation:
```python
try:
    from fyers_apiv3 import fyersModel
except ImportError:
    fyersModel = None
```

---

## Documentation

### Docstrings
```python
def calculate_position_size(
    available_funds: float, 
    risk_per_trade: float, 
    stop_loss_points: float
) -> int:
    """Calculate the number of shares to trade based on risk management.
    
    Args:
        available_funds: Total available capital for trading
        risk_per_trade: Percentage of capital to risk per trade (0.0-1.0)
        stop_loss_points: Distance to stop loss in price points
        
    Returns:
        Number of shares to trade
        
    Raises:
        ValueError: If stop_loss_points is zero or negative
    """
    if stop_loss_points <= 0:
        raise ValueError("Stop loss must be positive")
    risk_amount = available_funds * risk_per_trade
    return int(risk_amount / stop_loss_points)
```

### Comments
- Explain WHY, not WHAT (code should be self-explanatory)
- Use comments for complex business logic or trading rules
- Keep comments concise and up-to-date

---

## Security

### Credentials
- Never hardcode API keys or secrets
- Use environment variables for all sensitive data
- Rotate credentials regularly
- Use TOTP/MFA where available

### Input Validation
```python
def validate_symbol(symbol: str) -> str:
    if not symbol or len(symbol) > 50:
        raise ValidationError(f"Invalid symbol: {symbol}")
    return symbol.upper().strip()
```

---

## Trading-Specific Rules

### Order Management
- Always validate order parameters before submission
- Track all orders with unique IDs
- Implement idempotency for order placement
- Handle partial fills and order statuses explicitly

### Risk Management
- Never risk more than configured percentage per trade
- Implement circuit breakers for unusual market conditions
- Log all trading decisions with context for audit

### Data Handling
- Normalize timestamps to UTC internally
- Handle market hours and holidays explicitly
- Validate price and quantity ranges
