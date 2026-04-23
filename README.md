# trading-algo
Code for certain trading strategies
1. [Survivor Algo](https://raahibhushan.substack.com/p/the-survivor-an-algorithm-for-when)
2. [Wave Algo](https://raahibhushan.substack.com/p/the-wave-extractor-algo-2)

### Note
Please test the code on your own setup and verify that it works as expected.  
If you encounter any issues or unexpected behavior, feel free to [open an issue](../../issues) with relevant details so it can be addressed promptly.

## Disclaimer:
This algorithm is provided for **educational** and **informational purposes** only. Trading in financial markets involves substantial risk, and you may lose all or more than your initial investment. By using this algorithm, you acknowledge that all trading decisions are made at your own risk and discretion. The creators of this algorithm assume no liability or responsibility for any financial losses or damages incurred through its use. **Always do your own research and consult with a qualified financial advisor before trading.**


## Setup

### 1. Install Dependencies

To insall uv, use:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
or


```bash
pip install uv
```

This uses `uv` for dependency management. Install dependencies:
```bash
uv sync
```

Or if you prefer using pip:

```bash
pip install -r requirements.txt  # You may need to generate this from pyproject.toml
```

### 2. Environment Configuration

1. Copy the sample environment file:
   ```bash
   cp .sample.env .env
   ```

2. Edit `.env` and fill in your broker credentials:
   ```bash
    # should be one of -  fyers, zeodha
    BROKER_NAME=<INPUT_YOUR_BROKER_NAME>
    BROKER_API_KEY=<INPUT_YOUR_API_KEY>
    BROKER_API_SECRET=<INPUT_YOUR_API_SECRET>
    BROKER_LOGIN_MODE='auto' # manual or auto - fyers only with 'auto' for now
    BROKER_ID=<INPUT_YOUR_BROKER_ID>
    BROKER_TOTP_REDIDRECT_URI=<INPUT_YOUR_TOTP_REDIRECT_URI>
    BROKER_TOTP_KEY=<INPUT_YOUR_TOTP_KEY>
    BROKER_TOTP_PIN=<INPUT_YOUR_TOTP_PIN> # Required for fyers, not zerodha
    BROKER_PASSWORD=<INPUT_YOUR_BROKER_PASSWORD> # Required for zerodha, not fyers
   ```

### 3. Running Strategies

Strategies should be placed in the `strategy/` folder.

#### Running the Survivor Strategy


**Basic usage (using default config):**
```bash
cd strategy/
python survivor.py
```

**With custom parameters:**
```bash
cd strategy/
python survivor.py \
    --symbol-initials NIFTY25JAN30 \
    --pe-gap 25 --ce-gap 25 \
    --pe-quantity 50 --ce-quantity 50 \
    --min-price-to-sell 15
```

**View current configuration:**
```bash
cd strategy/
python survivor.py --show-config
```

### 4. Available Brokers

- **Fyers**: Supports REST API for historical data, quotes, and WebSocket for live data
- **Zerodha**: Supports KiteConnect API with order management and live data streaming

### 5. Core Components

- `brokers/`: Broker implementations (Fyers, Zerodha, Dhan)
- `dispatcher.py`: Data routing and queue management - WIP
- `orders.py`: Order management utilities - WIP
- `logger.py`: Logging configuration
- `strategy/`: Place your trading strategies here

## Survivor Strategy Architecture (Resilience Redesign)

The Survivor Strategy has been redesigned to address overnight gap risk, volatility overtrading, and missing risk controls. It now consists of three collaborating components:

### Component 1: RiskManager (`strategy/risk_manager.py`)

Handles trade validation and dynamic risk adjustment:
- **Daily Loss Limit**: Halts new trades if MTM loss exceeds configured limit (default ₹8,000)
- **Trade Count Cap**: Prevents overtrading with max trades per day (default 6 PE + CE combined)
- **VIX-Based Dynamic Gaps**: Scales trade trigger gaps based on market volatility
  - Base gap × clamp(current_vix / vix_baseline, 1.0, vix_max_multiplier)
  - Example: VIX 30 with baseline 15 and max 3.0 → gap doubles

```yaml
risk:
  daily_loss_limit: 8000          # rupees MTM loss, halts new trades
  max_trades_per_day: 6           # PE + CE combined
  vix_symbol: "NSE:INDIA VIX"     # symbol for VIX quote
  vix_baseline: 15.0              # VIX level at which gap = base_gap
  vix_max_multiplier: 3.0         # max gap scale factor
  vix_enabled: true               # false = skip VIX fetch, use base gap always
```

### Component 2: PositionLifecycleManager (`strategy/position_manager.py`)

Manages spread position lifecycle with full resilience:
- **Atomic Spread Entry**: Places short and hedge legs atomically; reverses short leg if hedge fails
- **Hedge Spreads**: Buys protective hedge 300pts further OTM to cap overnight loss
  - Short leg: Collect premium (e.g., NIFTY26127P24000)
  - Hedge leg: Buy protection (e.g., NIFTY26127P23700 for PE, 300pts lower)
  - Max overnight loss = 300pts × quantity
- **Gap-Open Bailout**: On startup, exits any position where short premium doubled overnight
- **SL/Target Monitoring**: Checks each tick:
  - SL trigger: short_entry_price × 1.30
  - Target trigger: short_entry_price × 0.40
- **State Persistence**: All positions saved to `artifacts/positions.json` for recovery

```yaml
spread:
  pe_hedge_gap: 300    # points further OTM for PE hedge leg
  ce_hedge_gap: 300    # points further OTM for CE hedge leg
```

### Component 3: SurvivorStrategy (`strategy/survivor.py`)

Simplified to focus on signal generation:
- **Gap Trigger Detection**: Monitors NIFTY movements against reference values
- **Reference Value Management**: Tracks PE and CE reference prices with reset logic
- **Entry Filters**: Optional RSI, ADX, EMA confirmation (configurable)
- **Delegates Execution**: Calls RiskManager.evaluate() and PositionLifecycleManager for all order placement

### Data Flow

```
Tick arrives
  → SurvivorStrategy detects gap trigger
  → RiskManager.evaluate() → ALLOW or BLOCK
  → RiskManager.get_effective_gap() → dynamic gap
  → SurvivorStrategy checks entry filters
  → PositionLifecycleManager.open_spread() → place short + hedge atomically
  → PositionLifecycleManager.on_tick() → monitor SL/target

Startup
  → PositionLifecycleManager.reconcile_on_open() → gap bailout
  → RiskManager initialized with daily counters
  → SurvivorStrategy starts trading loop
```

### Resilience Features

| Risk | Mitigation |
|------|-----------|
| Overnight gap | Mandatory hedge spread (max loss = 300pts × qty) |
| Gap-open ITM short | reconcile_on_open() exits if price > 2× entry |
| Volatile day overtrading | Trade count cap (default 6/day) |
| Fixed gap on volatile day | Dynamic VIX-scaled gap (up to 3× base) |
| Daily loss runaway | MTM loss limit halts new trades |
| Hedge placement failure | Short leg auto-reversed if hedge fails |

### Example Usage

```python
from brokers import BrokerGateway
from position_manager import PositionLifecycleManager
from risk_manager import RiskManager
from survivor import SurvivorStrategy
import yaml

# Load config
with open('configs/survivor.yml') as f:
    config = yaml.safe_load(f)['default']

# Initialize broker
broker = BrokerGateway.from_name("zerodha")

# Initialize components (in order)
position_manager = PositionLifecycleManager(broker, config)
position_manager.reconcile_on_open()  # Gap bailout check

risk_manager = RiskManager(config, broker, position_manager)

strategy = SurvivorStrategy(broker, config, risk_manager, position_manager)

# Connect and trade
broker.connect_websocket()
# ... tick updates flow to strategy.on_ticks_update()
```

### Example Usage

```python
# ==================
# IMPORTANT - Strategies are tested with Zerodha, although it should work with fyers as well 
# testing might be required to make sure the results are as expected
# ==================
from brokers import BrokerGateway
broker = BrokerGateway.from_name(os.getenv("BROKER_NAME")) # fyers or zerodha
# Get historical data, place orders, etc.
```

For more details, check the individual broker implementations and example strategies in the `strategy/` folder.
