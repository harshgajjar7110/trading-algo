import os, sys
import logging
import logging.handlers


def setup_logging():
    # Determine package root directory and log directory
    package_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(package_dir, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create the root logger for the package
    logger = logging.getLogger("system")
    logger.setLevel(logging.DEBUG)

    # Create a TimedRotatingFileHandler: a new log file every day
    log_file = os.path.join(log_dir, "system.log")
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=7
    )
    file_handler.suffix = "%Y-%m-%d"

    # Define a detailed formatter: time, logger name, level, filename:line, function, process ID, message
    formatter = logging.Formatter(
        fmt="%(levelname)s - %(filename)s:%(lineno)d - %(funcName)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Optionally add a console handler at a higher level (e.g., INFO)
    # Set to False to disable console output
    enable_console = os.getenv("ENABLE_CONSOLE_LOG", "false").lower() == "true"
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - PID:%(process)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    logger.debug("Logging is set up.")
    return logger


def setup_strategy_logging():
    """
    Setup separate logging for strategy that only writes to file (no console output).
    Strategy logs go to logs/strategy.log with daily rotation.
    """
    package_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(package_dir, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create strategy logger
    strategy_logger = logging.getLogger("strategy")
    strategy_logger.setLevel(logging.DEBUG)
    
    # Prevent propagation to parent loggers (avoids duplicate output)
    strategy_logger.propagate = False

    # Check if handlers already exist (avoid duplicates on re-import)
    if strategy_logger.handlers:
        return strategy_logger

    # Create a TimedRotatingFileHandler: a new log file every day
    log_file = os.path.join(log_dir, "strategy.log")
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=30  # Keep 30 days
    )
    file_handler.suffix = "%Y-%m-%d"

    # Define formatter with timestamp
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    strategy_logger.addHandler(file_handler)

    # No console handler - strategy logs only go to file
    strategy_logger.info("Strategy logging initialized.")
    return strategy_logger


# Initialize and export the loggers
logger = setup_logging()
strategy_logger = setup_strategy_logging()
