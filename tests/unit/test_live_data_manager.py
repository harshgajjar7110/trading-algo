"""
Unit Tests for LiveDataManager

Tests the LiveDataManager service for live data streaming.
Uses mock broker to avoid real API calls.
"""

import asyncio
import sys
import os
import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock, AsyncMock

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../web/backend"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from app.services.live_data_manager import LiveDataManager
from tests.utils.mock_broker import (
    MockBrokerGateway,
    create_mock_position,
    create_mock_order,
)


class TestLiveDataManager(unittest.TestCase):
    """Test cases for LiveDataManager."""

    def setUp(self):
        """Set up test fixtures."""
        # Create fresh instance for each test
        LiveDataManager._instance = None
        self.manager = LiveDataManager()
        self.mock_broker = MockBrokerGateway()

    def tearDown(self):
        """Clean up after tests."""
        # Stop any running streams
        if self.manager.is_streaming:
            asyncio.run(self.manager.stop_stream())

    def test_singleton_pattern(self):
        """Test that LiveDataManager is a singleton."""
        manager1 = LiveDataManager()
        manager2 = LiveDataManager()

        self.assertIs(manager1, manager2)

    def test_initial_state(self):
        """Test initial state of manager."""
        self.assertFalse(self.manager.is_streaming)
        self.assertEqual(len(self.manager.get_positions()), 0)
        self.assertEqual(len(self.manager.get_orders()), 0)
        self.assertIsNone(self.manager.last_update)

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_start_stream_success(self, mock_ensure_broker):
        """Test starting stream successfully."""
        mock_ensure_broker.return_value = self.mock_broker

        result = asyncio.run(self.manager.start_stream())

        self.assertTrue(result)
        self.assertTrue(self.manager.is_streaming)

        # Clean up
        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_start_stream_already_running(self, mock_ensure_broker):
        """Test starting stream when already running."""
        mock_ensure_broker.return_value = self.mock_broker

        # Start first time
        asyncio.run(self.manager.start_stream())

        # Try to start again
        result = asyncio.run(self.manager.start_stream())

        self.assertTrue(result)  # Should return True (already running)

        # Clean up
        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_stop_stream_clears_data(self, mock_ensure_broker):
        """Test that stopping stream clears all data."""
        mock_ensure_broker.return_value = self.mock_broker

        # Add some data
        self.mock_broker.add_position(create_mock_position())
        self.mock_broker.add_order(create_mock_order())

        # Start and let it fetch once
        asyncio.run(self.manager.start_stream())
        asyncio.run(asyncio.sleep(0.1))  # Let it fetch

        # Stop
        asyncio.run(self.manager.stop_stream())

        self.assertFalse(self.manager.is_streaming)
        self.assertEqual(len(self.manager.get_positions()), 0)
        self.assertEqual(len(self.manager.get_orders()), 0)

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_get_positions_when_not_streaming(self, mock_ensure_broker):
        """Test getting positions when not streaming returns empty."""
        positions = self.manager.get_positions()

        self.assertEqual(len(positions), 0)

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_get_orders_when_not_streaming(self, mock_ensure_broker):
        """Test getting orders when not streaming returns empty."""
        orders = self.manager.get_orders()

        self.assertEqual(len(orders), 0)

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_get_positions_returns_live_data(self, mock_ensure_broker):
        """Test that positions are fetched from broker."""
        mock_ensure_broker.return_value = self.mock_broker

        # Add mock positions
        self.mock_broker.add_position(create_mock_position(symbol="NIFTY26MAR22500CE"))
        self.mock_broker.add_position(create_mock_position(symbol="NIFTY26MAR22600PE"))

        # Start stream
        asyncio.run(self.manager.start_stream())

        # Let it fetch once
        asyncio.run(asyncio.sleep(0.1))

        positions = self.manager.get_positions()

        self.assertEqual(len(positions), 2)

        # Clean up
        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_get_orders_returns_live_data(self, mock_ensure_broker):
        """Test that orders are fetched from broker."""
        mock_ensure_broker.return_value = self.mock_broker

        # Add mock orders
        self.mock_broker.add_order(create_mock_order(order_id="ORDER001"))
        self.mock_broker.add_order(create_mock_order(order_id="ORDER002"))

        # Start stream
        asyncio.run(self.manager.start_stream())

        # Let it fetch once
        asyncio.run(asyncio.sleep(0.1))

        orders = self.manager.get_orders()

        self.assertEqual(len(orders), 2)

        # Clean up
        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_get_specific_position(self, mock_ensure_broker):
        """Test getting specific position by symbol."""
        mock_ensure_broker.return_value = self.mock_broker

        self.mock_broker.add_position(create_mock_position(symbol="TEST_SYMBOL"))

        asyncio.run(self.manager.start_stream())
        asyncio.run(asyncio.sleep(0.1))

        position = self.manager.get_position("TEST_SYMBOL")

        self.assertIsNotNone(position)
        self.assertEqual(position["symbol"], "TEST_SYMBOL")

        # Clean up
        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_get_specific_order(self, mock_ensure_broker):
        """Test getting specific order by ID."""
        mock_ensure_broker.return_value = self.mock_broker

        self.mock_broker.add_order(create_mock_order(order_id="TEST_ORDER"))

        asyncio.run(self.manager.start_stream())
        asyncio.run(asyncio.sleep(0.1))

        order = self.manager.get_order("TEST_ORDER")

        self.assertIsNotNone(order)
        self.assertEqual(order["order_id"], "TEST_ORDER")

        # Clean up
        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_force_refresh(self, mock_ensure_broker):
        """Test force refresh functionality."""
        mock_ensure_broker.return_value = self.mock_broker

        self.mock_broker.add_position(create_mock_position())

        asyncio.run(self.manager.start_stream())
        asyncio.run(asyncio.sleep(0.1))

        result = asyncio.run(self.manager.force_refresh())

        self.assertTrue(result)

        # Clean up
        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_force_refresh_when_not_streaming(self, mock_ensure_broker):
        """Test force refresh when not streaming fails."""
        result = asyncio.run(self.manager.force_refresh())

        self.assertFalse(result)

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_get_summary(self, mock_ensure_broker):
        """Test getting summary."""
        mock_ensure_broker.return_value = self.mock_broker

        self.mock_broker.add_position(create_mock_position())
        self.mock_broker.add_order(create_mock_order())

        asyncio.run(self.manager.start_stream())
        asyncio.run(asyncio.sleep(0.1))

        summary = self.manager.get_summary()

        self.assertTrue(summary["is_streaming"])
        self.assertEqual(summary["positions_count"], 1)
        self.assertEqual(summary["orders_count"], 1)
        self.assertIsNotNone(summary["last_update"])

        # Clean up
        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_error_handling_stops_after_max_errors(self, mock_ensure_broker):
        """Test that stream stops after max consecutive errors."""
        mock_ensure_broker.return_value = self.mock_broker

        # Make broker fail
        self.mock_broker.set_should_fail(True, "Test error")

        asyncio.run(self.manager.start_stream())

        # Wait for errors to accumulate
        asyncio.run(asyncio.sleep(15))  # 5 errors * 2 seconds wait + buffer

        # Stream should have stopped
        self.assertFalse(self.manager.is_streaming)

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_data_updates_on_fetch(self, mock_ensure_broker):
        """Test that data is updated when fetched."""
        mock_ensure_broker.return_value = self.mock_broker

        # Add initial position
        self.mock_broker.add_position(create_mock_position(symbol="POS1"))

        asyncio.run(self.manager.start_stream())
        asyncio.run(asyncio.sleep(0.5))

        # Verify initial data
        self.assertEqual(len(self.manager.get_positions()), 1)

        # Add new position
        self.mock_broker.add_position(create_mock_position(symbol="POS2"))

        # Wait for next fetch
        asyncio.run(asyncio.sleep(1.5))

        # Verify updated data
        self.assertEqual(len(self.manager.get_positions()), 2)

        # Clean up
        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_last_update_timestamp(self, mock_ensure_broker):
        """Test that last_update timestamp is set."""
        mock_ensure_broker.return_value = self.mock_broker

        self.mock_broker.add_position(create_mock_position())

        asyncio.run(self.manager.start_stream())
        asyncio.run(asyncio.sleep(0.1))

        self.assertIsNotNone(self.manager.last_update)
        self.assertIsInstance(self.manager.last_update, datetime)

        # Clean up
        asyncio.run(self.manager.stop_stream())


class TestLiveDataManagerEdgeCases(unittest.TestCase):
    """Test edge cases for LiveDataManager."""

    def setUp(self):
        """Set up test fixtures."""
        LiveDataManager._instance = None
        self.manager = LiveDataManager()
        self.mock_broker = MockBrokerGateway()

    def tearDown(self):
        """Clean up after tests."""
        if self.manager.is_streaming:
            asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_empty_positions_and_orders(self, mock_ensure_broker):
        """Test handling empty positions and orders."""
        mock_ensure_broker.return_value = self.mock_broker

        asyncio.run(self.manager.start_stream())
        asyncio.run(asyncio.sleep(0.1))

        self.assertEqual(len(self.manager.get_positions()), 0)
        self.assertEqual(len(self.manager.get_orders()), 0)

        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_get_nonexistent_position(self, mock_ensure_broker):
        """Test getting position that doesn't exist."""
        mock_ensure_broker.return_value = self.mock_broker

        asyncio.run(self.manager.start_stream())
        asyncio.run(asyncio.sleep(0.1))

        position = self.manager.get_position("NONEXISTENT")

        self.assertIsNone(position)

        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_get_nonexistent_order(self, mock_ensure_broker):
        """Test getting order that doesn't exist."""
        mock_ensure_broker.return_value = self.mock_broker

        asyncio.run(self.manager.start_stream())
        asyncio.run(asyncio.sleep(0.1))

        order = self.manager.get_order("NONEXISTENT")

        self.assertIsNone(order)

        asyncio.run(self.manager.stop_stream())

    @patch.object(LiveDataManager, "_ensure_broker")
    def test_multiple_start_stop_cycles(self, mock_ensure_broker):
        """Test multiple start/stop cycles."""
        mock_ensure_broker.return_value = self.mock_broker

        for i in range(3):
            self.mock_broker.add_position(create_mock_position(symbol=f"POS{i}"))

            asyncio.run(self.manager.start_stream())
            asyncio.run(asyncio.sleep(0.5))

            self.assertTrue(self.manager.is_streaming)
            self.assertEqual(len(self.manager.get_positions()), i + 1)

            asyncio.run(self.manager.stop_stream())

            self.assertFalse(self.manager.is_streaming)
            self.assertEqual(len(self.manager.get_positions()), 0)

            self.mock_broker.clear_positions()


def run_tests():
    """Run all LiveDataManager tests."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    suite.addTests(loader.loadTestsFromTestCase(TestLiveDataManager))
    suite.addTests(loader.loadTestsFromTestCase(TestLiveDataManagerEdgeCases))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 70)
    print("LIVE DATA MANAGER TEST SUMMARY")
    print("=" * 70)
    print(f"Tests Run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
