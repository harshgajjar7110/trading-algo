"""Test strategy logging to file only (no console output)"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from logger import strategy_logger

print("Testing strategy logger...")
print("Check logs/strategy.log for the following messages:")
print("-" * 60)

# These should go to file only, not console
strategy_logger.debug("This is a DEBUG message from strategy")
strategy_logger.info("This is an INFO message from strategy")
strategy_logger.warning("This is a WARNING message from strategy")
strategy_logger.error("This is an ERROR message from strategy")

print("-" * 60)
print("If you see log lines ABOVE this line, console output is enabled.")
print("Strategy logs should ONLY be in logs/strategy.log")
print()
print("Verify by checking: logs/strategy.log")

# Show the log file path
package_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(os.path.dirname(package_dir), "logs", "strategy.log")
print(f"Log file location: {log_file}")
