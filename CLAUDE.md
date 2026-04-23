# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

**trading-algo** is a trading algorithm system that implements automated options trading strategies (Survivor Algo, Wave Algo) with a broker-agnostic abstraction layer.

Key characteristics:
- Multi-broker support (Fyers, Zerodha, Dhan) via pluggable driver architecture
- Symbol normalization and exchange mapping system
- YAML-based strategy configuration
- Live and historical data capabilities
- Educational/research project — verify thoroughly before live trading

## Architecture

### Broker Abstraction Layer (`brokers/`)

Layered design separating concerns:

- **`core/`** — Domain models and abstract interface:
  - `interface.py` — `BrokerDriver` ABC defining broker contract (orders, positions, funds, quotes, market data)
  - `schemas.py` — Typed models: `OrderRequest`, `OrderResponse`, `Position`, `Funds`, `Quote`, `Instrument`, `BrokerCapabilities`
  - `enums.py` — `Exchange`, `OrderType`, `ProductType`, `TransactionType`, `Validity` (normalized across brokers)
  - `gateway.py` — `BrokerGateway` facade (lazily instantiates driver from `BROKER_NAME` env var)

- **`integrations/`** — Broker-specific drivers:
  - `fyers/driver.py`, `zerodha/driver.py`, `dhan/driver.py` — Each implements `BrokerDriver` interface
  - REST API wrappers, WebSocket listeners, auth flows (TOTP, manual login)

- **`symbols/`** — Cross-broker symbol normalization:
  - `registry.py`, `resolvers.py` — Pluggable symbol mappings (e.g., broker token → NSE/BSE format)

- **`mappings/`** — Enum value translation per broker:
  - `registry.py`, per-broker files (e.g., `fyers.py`, `zerodha.py`, `dhan.py`)
  - Converts normalized enums → broker-specific strings/codes

- **`auth/`** — Authentication helpers:
  - `totp.py` — 2FA token generation
  - `tokens.py` — Session/access token persistence
  - `manual.py` — Manual login flows (e.g., browser-based for Zerodha)

- **`net/`** — HTTP and rate-limiting utilities:
  - `http.py` — Wrapper for requests with retries
  - `ratelimiter.py` — Per-broker request throttling

### Strategies (`strategy/`)

Standalone Python scripts (not a package) that use the broker abstraction:
- `survivor.py` — Nifty options strategy (PE/CE gap-based selling)
- `wave.py` — Wave extraction algorithm

Each reads YAML config from `strategy/configs/` for parameters (gaps, quantities, etc.) and CLI args override those.

### Utilities

- `logger.py` — Centralized logging config
- `dispatcher.py`, `orders.py` — WIP order management and data routing
- `plugins/` — Extension hooks (placeholder)

### Registry & Factory

- `registry.py` — `BrokerRegistry` maps broker names → driver factory functions
- Auto-registration of default brokers on first `.create()` call

## Setup & Commands

### Install Dependencies

```bash
uv sync
```

Or if without uv:
```bash
pip install -r requirements.txt
```

### Configure Environment

```bash
cp .sample.env .env
# Edit .env with broker credentials
```

Environment variables control broker selection and auth:
- `BROKER_NAME` — "fyers", "zerodha", or "dhan"
- `BROKER_API_KEY`, `BROKER_API_SECRET`
- `BROKER_LOGIN_MODE` — "auto" or "manual"
- `BROKER_TOTP_KEY`, `BROKER_TOTP_PIN`, `BROKER_PASSWORD` — broker-specific

### Run Survivor Strategy

```bash
cd strategy/
python survivor.py  # Uses default config from strategy/configs/survivor.yml

# With CLI overrides
python survivor.py \
    --symbol-initials NIFTY25JAN30 \
    --pe-gap 25 --ce-gap 25 \
    --pe-quantity 50 --ce-quantity 50 \
    --min-price-to-sell 15

# View resolved config
python survivor.py --show-config
```

### Run Wave Strategy

```bash
cd strategy/
python wave.py
```

### Run Tests

```bash
pytest test_dhan.py  # Test-specific
pytest                # All tests (when test suite grows)
pytest -v             # Verbose
pytest test_*.py -k "marker"  # Filter by name/marker
```

## Key Concepts

### Adding a New Broker

1. Create `brokers/integrations/{broker_name}/driver.py` implementing `BrokerDriver`
2. Create `brokers/mappings/{broker_name}.py` with enum mappings
3. Add symbol resolver in `brokers/symbols/resolvers.py` if needed
4. Register in `brokers/registry.py` via `register_default_brokers()`
5. Test via `BrokerGateway.from_name(broker_name)` or set `BROKER_NAME` env var

### Broker Driver Lifecycle

1. `BrokerGateway.from_name(name)` calls `BrokerRegistry.create(name)`
2. Registry factory instantiates driver class
3. `__init__` initializes API client, loads credentials from env, sets `capabilities`
4. Caller invokes methods like `.place_order()`, `.get_positions()`, `.get_quote()`
5. Driver translates normalized models → broker-specific API calls → back to normalized response

### Symbol Resolution

Strategies typically work with human-readable symbols (e.g., "NIFTY25JAN30C24500"). The resolver chain:
1. `symbols.registry.resolve(symbol)` → looks up registered resolvers
2. Resolver converts → broker token (numeric ID) or broker symbol format
3. Broker driver uses token/format in API calls

### Testing Integrations

Tests use real API calls (no mocks). Before running:
- Ensure `.env` configured with valid credentials
- Dhan tests in `test_dhan.py` — review before running on live account

## Dependencies & Versions

See `pyproject.toml`. Notable:
- `pandas`, `scipy` — Data analysis
- `numba` — Fast numeric computation
- `mibian`, `stumpy` — Options pricing, time-series matching
- `fyers-apiv3`, `dhanhq` — Broker SDKs
- `pyyaml` — Strategy config files
- `requests`, `ratelimit` — HTTP + throttling
- `pyotp` — TOTP 2FA

## Development Notes

- **Zerodha is primary** — tested heavily; Fyers/Dhan may need extra validation
- **Credentials in env** — `.env` must not commit (use `.env.example` or `.sample.env`)
- **No live trading without verification** — strategies are educational; test in sandbox/paper first
- **Rate limiting** — brokers have API limits; `net/ratelimiter.py` enforces per-broker caps
- **Symbol mapping drift** — exchange symbols change; resolvers must stay synced

## Common Tasks

**Debug a broker call:**
- Add `logging.basicConfig(level=logging.DEBUG)` in strategy
- Set `BROKER_NAME=zerodha` in `.env`
- Run strategy with `--show-config` to confirm settings
- Check `brokers/integrations/{name}/driver.py` for actual API call

**Add a new strategy:**
1. Create `strategy/mystrategy.py`
2. Use `BrokerGateway.from_name(...)` to get driver
3. Call `.get_quote()`, `.place_order()`, etc.
4. Create `strategy/configs/mystrategy.yml` for params
5. Parse CLI args to override YAML (see `survivor.py` for pattern)

**Add enum value (e.g., new OrderType):**
1. Update `brokers/core/enums.py`
2. Update all mappings in `brokers/mappings/*.py`
3. Update driver implementations if they reference that enum

**Check broker capabilities:**
```python
from brokers import BrokerGateway
driver = BrokerGateway.from_name("zerodha")
caps = driver.get_capabilities()
print(f"Supports options: {caps.supports_options}")
```
