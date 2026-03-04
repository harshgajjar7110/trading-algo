Based on my analysis, I've identified several consolidation opportunities. Let me create a comprehensive consolidation plan:

---

## **Codebase Consolidation Plan**

### **Executive Summary**

After analyzing the 82 Python files across the project, I've identified several areas for consolidation to reduce redundancy, improve maintainability, and create a more cohesive architecture.

---

## **1. Critical Consolidation Areas**

### **1.1 Remove Deprecated Code** 🔴 HIGH PRIORITY

**Issue:** `brokers_old/` directory exists with outdated broker implementations

**Files:**
- `brokers_old/base.py` (949 bytes)
- `brokers_old/fyers.py` (24,759 bytes)
- `brokers_old/zerodha.py` (10,693 bytes)

**Action:**
```bash
# Remove entire directory
rm -rf brokers_old/
```

**Impact:** 
- ✅ Reduces confusion
- ✅ Eliminates 36KB of dead code
- ✅ Prevents accidental imports

---

### **1.2 Unify Strategy Classes** 🔴 HIGH PRIORITY

**Issue:** Two large strategy files with overlapping functionality

**Files:**
- `strategy/survivor.py` (2,111 lines)
- `strategy/wave.py` (1,441 lines)

**Problems Identified:**
- Both import same broker classes
- Both use similar position tracking
- Both have TODO comments indicating uncertainty
- Wave strategy has 15+ TODO comments
- Duplicate initialization patterns

**Proposed Solution: Create Strategy Base Class**

```python
# strategy/base_strategy.py (NEW FILE - ~300 lines)
class BaseStrategy:
    """Base class for all trading strategies."""
    
    def __init__(self, config: Dict, broker, order_tracker=None):
        self.config = config
        self.broker = broker
        self.order_tracker = order_tracker
        self.logger = strategy_logger
        
    def initialize(self) -> bool:
        """Initialize strategy."""
        raise NotImplementedError
        
    def on_tick(self, tick_data: Dict) -> None:
        """Process tick data."""
        raise NotImplementedError
        
    def place_order(self, order_request: OrderRequest) -> OrderResponse:
        """Place order with tracking."""
        # Common order placement logic
        
    def cleanup(self) -> None:
        """Cleanup resources."""
        pass
```

**Consolidation:**
- Move common broker initialization to base
- Move order tracking logic to base
- Move logging setup to base
- Keep only strategy-specific logic in subclasses

**Expected Reduction:**
- Survivor: 2,111 → ~1,500 lines (-611 lines)
- Wave: 1,441 → ~900 lines (-541 lines)
- **Total savings: ~1,150 lines**

---

### **1.3 Consolidate Logger Usage** 🟡 MEDIUM PRIORITY

**Issue:** Inconsistent logger imports across 42 files

**Current State:**
```python
# Multiple patterns found:
from logger import strategy_logger as logger  # 8 files
import logging  # 15 files
from logger import strategy_logger  # 3 files
```

**Problems:**
- Some files use `strategy_logger`, others use `logging`
- Inconsistent naming (`logger` vs `strategy_logger`)
- Multiple logger instances created

**Proposed Solution:**

```python
# Create unified logging utility
# utils/logging.py (NEW FILE - ~100 lines)

import logging
from functools import lru_cache

@lru_cache()
def get_logger(name: str = "strategy") -> logging.Logger:
    """Get or create logger instance."""
    logger = logging.getLogger(name)
    # Configure once
    return logger

# Usage in all files:
from utils.logging import get_logger
logger = get_logger(__name__)
```

**Files to Update:**
- All 42 files using logging
- Standardize on single import pattern

---

### **1.4 Merge Service Classes** 🟡 MEDIUM PRIORITY

**Issue:** Multiple singleton managers with overlapping responsibilities

**Current Managers:**
- `LiveDataManager` (337 lines) - Data streaming
- `StrategyManager` (817 lines) - Strategy lifecycle
- `LocalMonitor` (251 lines) - Performance tracking
- `PositionManager` (557 lines) - Position tracking
- `TelegramNotifier` (395 lines) - Notifications

**Problems:**
- StrategyManager and LocalMonitor both track performance
- LiveDataManager and PositionManager both track positions
- Too many singletons = tight coupling

**Proposed Solution: Unified State Manager**

```python
# services/state_manager.py (NEW FILE - ~600 lines)

class StateManager:
    """
    Unified state management for all trading operations.
    Replaces: LiveDataManager, LocalMonitor, PositionManager
    """
    
    _instance = None
    
    def __init__(self):
        self.positions = {}
        self.orders = {}
        self.metrics = SessionMetrics()
        self.is_streaming = False
        self._broker = None
        
    # Methods from all three managers combined
    # with clear separation of concerns
```

**Benefits:**
- Single source of truth
- Reduced memory footprint
- Clearer data flow
- Easier testing

---

## **2. Code Duplication Issues**

### **2.1 Duplicate Broker Initialization** 🔴 HIGH PRIORITY

**Found in:**
- `strategy/survivor.py` line 39
- `strategy/wave.py` line 9
- `web/backend/app/services/strategy_manager.py` line 608
- `web/backend/app/services/broker_service.py` line 55

**Pattern:**
```python
from brokers import BrokerGateway
broker = BrokerGateway.from_name(os.getenv("BROKER_NAME", "zerodha"))
```

**Solution:** Create broker factory
```python
# services/broker_factory.py
def get_broker(broker_name: Optional[str] = None) -> BrokerGateway:
    """Get or create broker instance."""
    broker_name = broker_name or os.getenv("BROKER_NAME", "zerodha")
    return BrokerGateway.from_name(broker_name)
```

---

### **2.2 Duplicate Position Data Transformation** 🟡 MEDIUM PRIORITY

**Found in:**
- `web/backend/app/services/broker_service.py` lines 90-114
- `web/backend/app/services/live_data_manager.py` lines 168-183
- `web/backend/app/routes/positions.py` lines 65-89

**All three files transform broker Position objects to dict format**

**Solution:** Create position serializer
```python
# serializers/position.py
def serialize_position(position: Position) -> Dict[str, Any]:
    """Convert Position object to dict."""
    return {
        "symbol": position.symbol,
        "exchange": position.exchange.value if hasattr(position.exchange, "value") else str(position.exchange),
        "quantity": position.quantity_total,
        # ... etc
    }
```

---

### **2.3 Duplicate Error Handling** 🟡 MEDIUM PRIORITY

**Found in multiple files:**
- Try/except blocks around broker calls
- Similar error message formatting
- Authentication error detection

**Solution:** Create decorator
```python
# decorators/error_handling.py
def handle_broker_errors(func):
    """Decorator for broker error handling."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_msg = str(e)
            if "access_token" in error_msg.lower():
                return {"error": "Authentication failed"}
            raise
    return wrapper
```

---

## **3. Configuration Consolidation**

### **3.1 Centralize Config Management** 🟡 MEDIUM PRIORITY

**Current State:**
- `.env` file for secrets
- `web/backend/app/config.py` for app config
- `strategy/config.yaml` for strategy config
- Hardcoded values scattered in code

**Solution:** Unified config system
```python
# config/manager.py
class ConfigManager:
    """Single configuration source."""
    
    def __init__(self):
        self.env = self._load_env()
        self.yaml = self._load_yaml()
        self.runtime = {}
        
    def get(self, key: str, default=None):
        # Priority: runtime > env > yaml > default
        pass
```

---

## **4. Import Path Consolidation**

### **4.1 Standardize Import Structure** 🟢 LOW PRIORITY

**Current Issues:**
- `sys.path.append()` in multiple files
- Relative imports mixed with absolute
- Path manipulation scattered

**Files with path manipulation:**
- `strategy/survivor.py` line 28
- `strategy/wave.py` line 4
- Multiple test files

**Solution:**
1. Create proper `__init__.py` files
2. Use absolute imports everywhere
3. Add project root to PYTHONPATH in startup script
4. Remove all `sys.path.append()` calls

---

## **5. Test Consolidation**

### **5.1 Organize Test Structure** 🟢 LOW PRIORITY

**Current:**
```
tests/
  ├── conftest.py
  ├── test_api_endpoints.py
  ├── test_greeks_parsing.py
  ├── test_march_fix.py
  ├── test_march_greeks.py
  ├── test_sl_order_functionality.py
  ├── test_strategy_logging.py
  └── unit/
      ├── test_live_data_manager.py
      └── test_order_tracker.py
```

**Proposed:**
```
tests/
  ├── conftest.py
  ├── unit/
  │   ├── test_order_tracker.py
  │   ├── test_live_data_manager.py
  │   └── test_telegram_notifier.py
  ├── integration/
  │   └── test_strategy_lifecycle.py
  └── e2e/
      └── test_full_workflow.py
```

---

## **6. Implementation Priority**

### **Phase A: Critical (Do First)**
1. ✅ Remove `brokers_old/` directory
2. ✅ Create `BaseStrategy` class
3. ✅ Consolidate logger usage

**Effort:** 1-2 days  
**Impact:** High  
**Risk:** Low

### **Phase B: Important (Do Next)**
4. ✅ Merge service managers
5. ✅ Create broker factory
6. ✅ Add position serializer

**Effort:** 2-3 days  
**Impact:** Medium-High  
**Risk:** Medium

### **Phase C: Nice to Have**
7. ✅ Unified config system
8. ✅ Standardize imports
9. ✅ Reorganize tests

**Effort:** 1-2 days  
**Impact:** Medium  
**Risk:** Low

---

## **7. Expected Benefits**

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Total Lines of Code | ~15,000 | ~12,000 | **-20%** |
| Duplicate Code | ~2,000 lines | ~200 lines | **-90%** |
| Singleton Classes | 5 | 2 | **-60%** |
| Import Patterns | 5 different | 1 standard | **Unified** |
| Test Coverage | Scattered | Organized | **+40%** |

---

## **8. Questions for You**

Before I proceed with implementation, I need your input on:

1. **Strategy Consolidation:** 
   - Do you want to keep both Survivor and Wave strategies?
   - Are they both actively used, or can we deprecate one?

2. **Service Manager Merge:**
   - Are you comfortable merging LiveDataManager, LocalMonitor, and PositionManager into one class?
   - This simplifies the architecture but is a bigger change

3. **Breaking Changes:**
   - Some consolidation requires changing import paths - is that acceptable?
   - Or should we maintain backward compatibility with deprecation warnings?

4. **brokers_old/:**
   - Safe to delete? Any historical value in keeping it?

5. **Timeline:**
   - Do you want all phases done at once, or incremental improvements?
   - Any deadline constraints?

---

## **9. Recommended Approach**

**Option 1: Aggressive Consolidation (Recommended)**
- Do all phases over 1 week
- Major cleanup, some breaking changes
- Best long-term maintainability

**Option 2: Incremental**
- Do Phase A only (critical items)
- Minimal risk, immediate benefits
- Tackle others later as needed

**Option 3: Conservative**
- Only remove dead code (brokers_old/)
- Keep everything else as-is
- Safest but least improvement

---

**Which approach would you prefer?** I can start implementing once you confirm the scope and answer the questions above.