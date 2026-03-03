"""
Pytest Configuration and Fixtures

Shared fixtures and configuration for all tests.
"""

import asyncio
import os
import sys
from pathlib import Path

import pytest

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "web" / "backend"))


@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables for testing."""
    monkeypatch.setenv("BROKER_NAME", "zerodha")
    monkeypatch.setenv("BROKER_API_KEY", "test_api_key")
    monkeypatch.setenv("BROKER_ACCESS_TOKEN", "test_access_token")
    yield


@pytest.fixture
def reset_live_data_manager():
    """Reset LiveDataManager singleton before and after test."""
    from app.services.live_data_manager import LiveDataManager

    # Reset before test
    LiveDataManager._instance = None

    yield

    # Clean up after test
    if LiveDataManager._instance:
        manager = LiveDataManager()
        if manager.is_streaming:
            asyncio.run(manager.stop_stream())
        LiveDataManager._instance = None


@pytest.fixture
def mock_broker():
    """Create a mock broker instance."""
    from tests.utils.mock_broker import MockBrokerGateway

    broker = MockBrokerGateway()
    yield broker

    # Clean up
    broker.clear_positions()
    broker.clear_orders()


@pytest.fixture
def sample_position():
    """Create a sample position for testing."""
    return {
        "symbol": "NIFTY26MAR22500CE",
        "exchange": "NFO",
        "quantity": -50,
        "average_price": 50.0,
        "pnl": 1000.0,
        "product_type": "NRML",
    }


@pytest.fixture
def sample_order():
    """Create a sample order for testing."""
    return {
        "order_id": "ORDER001",
        "symbol": "NIFTY26MAR22500CE",
        "exchange": "NFO",
        "transaction_type": "SELL",
        "quantity": 50,
        "price": 50.0,
        "status": "PENDING",
        "order_type": "LIMIT",
        "product_type": "NRML",
        "timestamp": "2026-03-03T10:00:00",
        "tag": "TEST",
    }
