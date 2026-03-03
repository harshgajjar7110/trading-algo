"""
Unified logging utility for the trading algorithm.
Provides a single source of truth for logger configuration across the codebase.
"""

import logging
import logging.handlers
import os
from functools import lru_cache
from typing import Optional


def _get_log_dir() -> str:
    """Get or create the log directory."""
    package_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(package_dir, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    return log_dir


@lru_cache()
def get_logger(name: str = "strategy") -> logging.Logger:
    """
    Get or create a logger instance with the specified name.
    
    This function uses LRU cache to ensure the same logger instance
    is returned for the same name, preventing duplicate handlers.
    
    Args:
        name: Logger name. Use __name__ for module-level loggers.
              Common names: 'strategy', 'system', 'broker'
    
    Returns:
        Configured logger instance
    
    Example:
        >>> from utils.logging import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("This is an info message")
    """
    logger = logging.getLogger(name)
    
    # If already configured, return as-is
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    
    # Prevent propagation to avoid duplicate logs
    logger.propagate = False
    
    log_dir = _get_log_dir()
    
    # File handler with daily rotation
    log_file = os.path.join(log_dir, f"{name}.log")
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=30, encoding='utf-8'
    )
    file_handler.suffix = "%Y-%m-%d"
    
    # Formatter with timestamp, level, file:line, function name
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Optional console handler (disabled by default)
    enable_console = os.getenv("ENABLE_CONSOLE_LOG", "false").lower() == "true"
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        # Set UTF-8 encoding for console to handle Unicode characters like ₹
        if hasattr(console_handler.stream, 'reconfigure'):
            console_handler.stream.reconfigure(encoding='utf-8')
        console_formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger


@lru_cache()
def get_strategy_logger(strategy_name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger specifically configured for strategy logging.
    
    Args:
        strategy_name: Optional strategy name to include in logger name.
                      If provided, logger will be named 'strategy.{strategy_name}'
    
    Returns:
        Configured strategy logger
    
    Example:
        >>> from utils.logging import get_strategy_logger
        >>> logger = get_strategy_logger("survivor")
        >>> logger.info("Strategy initialized")
    """
    name = f"strategy.{strategy_name}" if strategy_name else "strategy"
    return get_logger(name)


# Legacy compatibility: provide strategy_logger for existing imports
strategy_logger = get_strategy_logger()


# For backward compatibility with existing code
def setup_logging():
    """Legacy function for backward compatibility."""
    return get_logger("system")


def setup_strategy_logging():
    """Legacy function for backward compatibility."""
    return get_strategy_logger()
