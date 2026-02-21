# Logging Configuration Guide

## Overview

The project now uses **separate loggers** for different components:

| Logger | File | Console Output | Purpose |
|--------|------|----------------|---------|
| `system_logger` | `logs/system.log` | Optional (disabled by default) | System-level logs |
| `strategy_logger` | `logs/strategy.log` | **No** | Strategy execution logs only |

---

## Strategy Logging (File Only)

### Usage in Strategy Code

```python
from logger import strategy_logger as logger

# In your strategy:
logger.info("Strategy started")
logger.debug(f"NIFTY price: {price}")
logger.warning("Position limit reached")
logger.error("Order failed")
```

### Log File Location

```
logs/strategy.log              # Current day's log
logs/strategy.log.2026-02-19   # Rotated daily logs (30 days retention)
```

### Log Format

```
2026-02-19 10:39:48 - INFO - survivor.py:245 - on_ticks_update - Strategy tick processed
│                    │     │            │    │              │
│                    │     │            │    │              └── Message
│                    │     │            │    └── Function name
│                    │     │            └── Line number
│                    │     └── Filename
│                    └── Log level
└── Timestamp
```

### Features

- **Daily rotation**: New log file created at midnight
- **30-day retention**: Old logs automatically deleted after 30 days
- **No console output**: All strategy logs go to file only (no CMD clutter)
- **Thread-safe**: Can be used from multiple threads

---

## System Logging

### Usage

```python
from logger import logger  # or strategy_logger for strategy code

logger.info("System event")
```

### Enable Console Output

By default, system logs also go to file only. To enable console output:

```bash
# Windows
set ENABLE_CONSOLE_LOG=true
python strategy/survivor.py

# Linux/Mac
export ENABLE_CONSOLE_LOG=true
python strategy/survivor.py
```

---

## Viewing Logs in Real-Time

### Windows (PowerShell)

```powershell
# Watch strategy log in real-time
Get-Content logs/strategy.log -Wait

# View last 50 lines
Get-Content logs/strategy.log -Tail 50
```

### Linux/Mac

```bash
# Watch strategy log in real-time
tail -f logs/strategy.log

# View last 50 lines
tail -n 50 logs/strategy.log
```

---

## Files Modified

| File | Change |
|------|--------|
| `logger.py` | Added `setup_strategy_logging()` function, separate strategy_logger |
| `strategy/survivor.py` | Uses `strategy_logger` instead of regular logger |
| `strategy/survivor_enhanced.py` | Uses `strategy_logger` instead of regular logger |
| `strategy/wave.py` | Uses `strategy_logger` instead of regular logger |
| `orders.py` | Uses `strategy_logger` instead of regular logger |
| `dispatcher.py` | Uses `strategy_logger` instead of regular logger |
| `brokers_old/zerodha.py` | Uses `strategy_logger` instead of regular logger |

---

## Troubleshooting

### Logs not appearing in file

1. Check if `logs/` directory exists and is writable
2. Verify import: `from logger import strategy_logger as logger`
3. Check file permissions

### Console still showing logs

1. Ensure you imported `strategy_logger` not `logger`
2. Check that `strategy_logger.propagate = False` is set (done in logger.py)

### Log file too large

Logs rotate daily with 30-day retention. To adjust:

```python
# In logger.py, modify backupCount
file_handler = logging.handlers.TimedRotatingFileHandler(
    log_file, when="midnight", interval=1, backupCount=7  # Keep 7 days only
)
```

---

## Example Output

```
2026-02-19 10:45:22 - INFO - survivor.py:120 - __init__ - Strategy initialized
2026-02-19 10:45:23 - DEBUG - survivor.py:245 - on_ticks_update - NIFTY price: 24150.5
2026-02-19 10:45:25 - INFO - survivor.py:310 - _handle_pe_trade - PE gap triggered, diff: 45.2
2026-02-19 10:45:25 - INFO - survivor.py:410 - _place_order - Order placed: NIFTY26FEB24200PE x 50
2026-02-19 10:45:26 - ERROR - survivor.py:415 - _place_order - Order failed: Insufficient margin
```
