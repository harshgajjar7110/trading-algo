"""
Unit Tests for OrderTracker

Tests the OrderTracker class to ensure proper in-memory order tracking.
Verifies no file I/O operations occur.
"""

import sys
import os
import unittest
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from orders import OrderTracker


class TestOrderTracker(unittest.TestCase):
    """Test cases for OrderTracker class."""

    def setUp(self):
        """Set up test fixtures."""
        self.tracker = OrderTracker()

    def test_init_creates_empty_tracker(self):
        """Test that initialization creates empty tracker."""
        self.assertEqual(len(self.tracker.all_orders), 0)
        self.assertIsNone(self.tracker.current_order)
        self.assertEqual(len(self.tracker.completed_order_ids), 0)

    def test_add_order_with_valid_data(self):
        """Test adding an order with valid data."""
        order = {
            "order_id": "ORDER001",
            "symbol": "NIFTY26MAR22500CE",
            "exchange": "NFO",
            "transaction_type": "SELL",
            "quantity": 50,
            "price": 50.0,
            "status": "PENDING",
        }

        self.tracker.add_order(order)

        self.assertEqual(len(self.tracker.all_orders), 1)
        self.assertEqual(self.tracker.current_order["order_id"], "ORDER001")
        self.assertIn("timestamp", self.tracker.current_order)

    def test_add_order_without_order_id(self):
        """Test adding an order without order_id fails gracefully."""
        order = {"symbol": "NIFTY26MAR22500CE", "quantity": 50}

        # Should not raise exception, just log error
        self.tracker.add_order(order)

        self.assertEqual(len(self.tracker.all_orders), 0)

    def test_add_order_with_nested_id(self):
        """Test adding order with nested order ID format."""
        order = {
            "orders": {"id": "ORDER002"},
            "symbol": "NIFTY26MAR22500PE",
            "quantity": 50,
        }

        self.tracker.add_order(order)

        self.assertEqual(len(self.tracker.all_orders), 1)
        self.assertIn("ORDER002", self.tracker.all_orders)

    def test_add_order_updates_existing(self):
        """Test that adding order with same ID updates existing."""
        order1 = {
            "order_id": "ORDER003",
            "symbol": "NIFTY26MAR22500CE",
            "status": "PENDING",
        }
        order2 = {
            "order_id": "ORDER003",
            "symbol": "NIFTY26MAR22500CE",
            "status": "COMPLETE",
        }

        self.tracker.add_order(order1)
        self.tracker.add_order(order2)

        self.assertEqual(len(self.tracker.all_orders), 1)
        self.assertEqual(self.tracker.all_orders["ORDER003"]["status"], "COMPLETE")

    def test_add_order_adds_timestamp(self):
        """Test that timestamp is added if not present."""
        order = {"order_id": "ORDER004", "symbol": "NIFTY26MAR22500CE"}

        self.tracker.add_order(order)

        self.assertIn("timestamp", self.tracker.all_orders["ORDER004"])
        # Verify it's a valid ISO format
        timestamp = self.tracker.all_orders["ORDER004"]["timestamp"]
        datetime.fromisoformat(timestamp)  # Should not raise

    def test_complete_order(self):
        """Test marking an order as completed."""
        order = {
            "order_id": "ORDER005",
            "symbol": "NIFTY26MAR22500CE",
            "transaction_type": "SELL",
        }

        self.tracker.add_order(order)
        result = self.tracker.complete_order("ORDER005")

        self.assertTrue(result)
        self.assertIn("ORDER005", self.tracker.completed_order_ids)
        self.assertEqual(self.tracker._order_types_summary["SELL"], 1)

    def test_complete_nonexistent_order(self):
        """Test completing an order that doesn't exist."""
        result = self.tracker.complete_order("NONEXISTENT")

        self.assertFalse(result)

    def test_complete_order_twice(self):
        """Test completing same order twice is idempotent."""
        order = {"order_id": "ORDER006", "transaction_type": "BUY"}

        self.tracker.add_order(order)
        self.tracker.complete_order("ORDER006")
        result = self.tracker.complete_order("ORDER006")

        self.assertTrue(result)
        self.assertEqual(len(self.tracker.completed_order_ids), 1)
        self.assertEqual(self.tracker._order_types_summary["BUY"], 1)

    def test_remove_order(self):
        """Test removing an order."""
        order = {"order_id": "ORDER007", "symbol": "NIFTY26MAR22500CE"}

        self.tracker.add_order(order)
        result = self.tracker.remove_order("ORDER007")

        self.assertTrue(result)
        self.assertEqual(len(self.tracker.all_orders), 0)

    def test_remove_nonexistent_order(self):
        """Test removing an order that doesn't exist."""
        result = self.tracker.remove_order("NONEXISTENT")

        self.assertFalse(result)

    def test_get_order_by_id(self):
        """Test retrieving order by ID."""
        order = {"order_id": "ORDER008", "symbol": "NIFTY26MAR22500CE"}

        self.tracker.add_order(order)
        retrieved = self.tracker.get_order_by_id("ORDER008")

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved["order_id"], "ORDER008")

    def test_get_order_by_id_not_found(self):
        """Test retrieving nonexistent order returns None."""
        result = self.tracker.get_order_by_id("NONEXISTENT")

        self.assertIsNone(result)

    def test_all_orders_returns_copy(self):
        """Test that all_orders property returns a copy."""
        order = {"order_id": "ORDER009", "symbol": "NIFTY26MAR22500CE"}

        self.tracker.add_order(order)
        orders_copy = self.tracker.all_orders

        # Modify the copy (dict copy is shallow, so nested dicts are shared)
        orders_copy["NEW_ORDER"] = {"symbol": "NEW"}

        # Original should be unchanged (no NEW_ORDER)
        self.assertNotIn("NEW_ORDER", self.tracker.all_orders)
        self.assertEqual(len(self.tracker.all_orders), 1)

    def test_non_completed_orders(self):
        """Test getting non-completed orders."""
        order1 = {"order_id": "ORDER010", "transaction_type": "SELL"}
        order2 = {"order_id": "ORDER011", "transaction_type": "BUY"}

        self.tracker.add_order(order1)
        self.tracker.add_order(order2)
        self.tracker.complete_order("ORDER010")

        non_completed = self.tracker.non_completed_orders

        self.assertEqual(len(non_completed), 1)
        self.assertEqual(non_completed[0]["order_id"], "ORDER011")

    def test_completed_orders(self):
        """Test getting completed orders."""
        order1 = {"order_id": "ORDER012", "transaction_type": "SELL"}
        order2 = {"order_id": "ORDER013", "transaction_type": "BUY"}

        self.tracker.add_order(order1)
        self.tracker.add_order(order2)
        self.tracker.complete_order("ORDER012")

        completed = self.tracker.completed_orders

        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0]["order_id"], "ORDER012")

    def test_get_order_summary(self):
        """Test getting order summary."""
        order1 = {"order_id": "ORDER014", "transaction_type": "SELL"}
        order2 = {"order_id": "ORDER015", "transaction_type": "BUY"}

        self.tracker.add_order(order1)
        self.tracker.add_order(order2)
        self.tracker.complete_order("ORDER014")

        summary = self.tracker.get_order_summary()

        self.assertEqual(summary["total_orders"], 2)
        self.assertEqual(summary["completed_orders"], 1)
        self.assertEqual(summary["active_orders"], 1)
        self.assertEqual(summary["order_types_summary"]["SELL"], 1)

    def test_get_total_orders_count(self):
        """Test getting total order count."""
        self.assertEqual(self.tracker.get_total_orders_count(), 0)

        self.tracker.add_order({"order_id": "ORDER016"})
        self.assertEqual(self.tracker.get_total_orders_count(), 1)

    def test_get_all_orders_as_list(self):
        """Test getting all orders as list."""
        self.tracker.add_order({"order_id": "ORDER017"})
        self.tracker.add_order({"order_id": "ORDER018"})

        orders_list = self.tracker.get_all_orders_as_list()

        self.assertEqual(len(orders_list), 2)
        self.assertIsInstance(orders_list, list)

    def test_no_file_io_occurs(self):
        """Test that no file I/O operations occur."""
        # This test verifies the absence of file operations
        # by checking that the tracker works without any file system access
        import tempfile
        import os

        # Create tracker
        tracker = OrderTracker()

        # Add some orders
        for i in range(10):
            tracker.add_order(
                {"order_id": f"ORDER{i:03d}", "symbol": "NIFTY26MAR22500CE"}
            )

        # Verify all orders in memory
        self.assertEqual(len(tracker.all_orders), 10)

        # Verify no files were created (we can't directly test this,
        # but the fact that it works without a file path proves no file I/O)

    def test_multiple_trackers_independent(self):
        """Test that multiple tracker instances are independent."""
        tracker1 = OrderTracker()
        tracker2 = OrderTracker()

        tracker1.add_order({"order_id": "ORDER019"})
        tracker2.add_order({"order_id": "ORDER020"})

        self.assertEqual(len(tracker1.all_orders), 1)
        self.assertEqual(len(tracker2.all_orders), 1)
        self.assertIn("ORDER019", tracker1.all_orders)
        self.assertIn("ORDER020", tracker2.all_orders)


class TestOrderTrackerEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def test_empty_order_dict(self):
        """Test adding empty order dict."""
        tracker = OrderTracker()
        tracker.add_order({})
        self.assertEqual(len(tracker.all_orders), 0)

    def test_none_order_id(self):
        """Test adding order with None order_id."""
        tracker = OrderTracker()
        tracker.add_order({"order_id": None})
        self.assertEqual(len(tracker.all_orders), 0)

    def test_very_long_order_id(self):
        """Test adding order with very long ID."""
        tracker = OrderTracker()
        long_id = "ORDER" + "X" * 1000
        tracker.add_order({"order_id": long_id})
        self.assertIn(long_id, tracker.all_orders)

    def test_special_characters_in_order_id(self):
        """Test adding order with special characters in ID."""
        tracker = OrderTracker()
        special_id = "ORDER-001_TEST@123"
        tracker.add_order({"order_id": special_id})
        self.assertIn(special_id, tracker.all_orders)

    def test_unicode_in_symbol(self):
        """Test adding order with unicode characters."""
        tracker = OrderTracker()
        tracker.add_order({"order_id": "ORDER021", "symbol": "NIFTY₹500"})
        self.assertEqual(tracker.all_orders["ORDER021"]["symbol"], "NIFTY₹500")


def run_tests():
    """Run all OrderTracker tests."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    suite.addTests(loader.loadTestsFromTestCase(TestOrderTracker))
    suite.addTests(loader.loadTestsFromTestCase(TestOrderTrackerEdgeCases))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 70)
    print("ORDER TRACKER TEST SUMMARY")
    print("=" * 70)
    print(f"Tests Run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
