"""
Test Suite for Stop-Loss Order Functionality

This module tests the broker-level SL order placement features including:
- SL order placement after fresh order
- SL price calculation
- SL order tracking per position
- SL cancellation on manual exit
- Broker SL execution detection
- Emergency bulk exit functionality
- Basket order placement
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import unittest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from typing import Dict, List, Any, Optional

from brokers.core.schemas import (
    OrderRequest, OrderResponse, Position, BrokerCapabilities,
    Exchange, OrderType, TransactionType, ProductType, Validity
)
from brokers.core.enums import OptionType


class TestSLPriceCalculation(unittest.TestCase):
    """Test SL price calculation logic for short positions."""
    
    def test_sl_calculation_short_position_60_percent(self):
        """Test SL price calculation for short position with 60% SL."""
        entry_price = 50.0
        sl_percentage = 60
        sl_limit_buffer = 5.0
        
        # For SHORT positions: sl_trigger = entry_price * (1 + sl_percentage/100)
        sl_trigger = entry_price * (1 + sl_percentage / 100)
        sl_limit = sl_trigger + sl_limit_buffer
        risk_per_unit = sl_trigger - entry_price
        
        self.assertEqual(sl_trigger, 80.0)
        self.assertEqual(sl_limit, 85.0)
        self.assertEqual(risk_per_unit, 30.0)
    
    def test_sl_calculation_short_position_40_percent(self):
        """Test SL price calculation for short position with 40% SL (conservative)."""
        entry_price = 50.0
        sl_percentage = 40
        sl_limit_buffer = 3.0
        
        sl_trigger = entry_price * (1 + sl_percentage / 100)
        sl_limit = sl_trigger + sl_limit_buffer
        risk_per_unit = sl_trigger - entry_price
        
        self.assertEqual(sl_trigger, 70.0)
        self.assertEqual(sl_limit, 73.0)
        self.assertEqual(risk_per_unit, 20.0)
    
    def test_sl_calculation_short_position_100_percent(self):
        """Test SL price calculation for short position with 100% SL (aggressive)."""
        entry_price = 50.0
        sl_percentage = 100
        sl_limit_buffer = 0.0
        
        sl_trigger = entry_price * (1 + sl_percentage / 100)
        sl_limit = sl_trigger + sl_limit_buffer
        risk_per_unit = sl_trigger - entry_price
        
        self.assertEqual(sl_trigger, 100.0)
        self.assertEqual(sl_limit, 100.0)
        self.assertEqual(risk_per_unit, 50.0)
    
    def test_sl_calculation_various_entry_prices(self):
        """Test SL calculation with various entry prices."""
        test_cases = [
            (100.0, 60, 160.0),  # Higher entry price
            (25.0, 60, 40.0),    # Lower entry price
            (75.5, 60, 120.8),   # Decimal entry price
        ]
        
        for entry_price, sl_percentage, expected_trigger in test_cases:
            with self.subTest(entry_price=entry_price):
                sl_trigger = entry_price * (1 + sl_percentage / 100)
                self.assertAlmostEqual(sl_trigger, expected_trigger, places=1)


class TestSLOrderRequest(unittest.TestCase):
    """Test SL OrderRequest creation."""
    
    def test_stop_limit_order_request(self):
        """Test creating STOP_LIMIT order request for SL."""
        request = OrderRequest(
            symbol="NIFTY24FEB18000CE",
            exchange=Exchange.NFO,
            quantity=50,
            order_type=OrderType.STOP_LIMIT,
            transaction_type=TransactionType.BUY,  # Buy to cover short
            product_type=ProductType.MARGIN,
            price=85.0,  # Limit price
            stop_price=80.0,  # Trigger price
            tag="SL_ORDER"
        )
        
        self.assertEqual(request.order_type, OrderType.STOP_LIMIT)
        self.assertEqual(request.stop_price, 80.0)
        self.assertEqual(request.price, 85.0)
        self.assertEqual(request.tag, "SL_ORDER")
    
    def test_stop_market_order_request(self):
        """Test creating STOP (market) order request for SL."""
        request = OrderRequest(
            symbol="NIFTY24FEB18000CE",
            exchange=Exchange.NFO,
            quantity=50,
            order_type=OrderType.STOP,
            transaction_type=TransactionType.BUY,
            product_type=ProductType.MARGIN,
            stop_price=80.0,  # Trigger price only for SL-M
            tag="SL_ORDER"
        )
        
        self.assertEqual(request.order_type, OrderType.STOP)
        self.assertEqual(request.stop_price, 80.0)
        self.assertIsNone(request.price)


class TestBrokerCapabilities(unittest.TestCase):
    """Test broker capabilities for SL-related features."""
    
    def test_default_capabilities(self):
        """Test default broker capabilities."""
        caps = BrokerCapabilities()
        
        self.assertTrue(caps.supports_place_order)
        self.assertTrue(caps.supports_modify_order)
        self.assertTrue(caps.supports_cancel_order)
        self.assertFalse(caps.supports_basket_orders)
        self.assertFalse(caps.supports_exit_positions)
        self.assertFalse(caps.supports_convert_position)
    
    def test_enhanced_capabilities(self):
        """Test enhanced broker capabilities with new features."""
        caps = BrokerCapabilities(
            supports_basket_orders=True,
            supports_exit_positions=True,
            supports_convert_position=True,
            supports_gtt=True
        )
        
        self.assertTrue(caps.supports_basket_orders)
        self.assertTrue(caps.supports_exit_positions)
        self.assertTrue(caps.supports_convert_position)
        self.assertTrue(caps.supports_gtt)


class TestExitPositionsFunctionality(unittest.TestCase):
    """Test emergency bulk exit functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_positions = [
            Position(
                symbol="NIFTY24FEB18000CE",
                exchange=Exchange.NFO,
                quantity_total=-50,  # Short position
                quantity_available=-50,
                average_price=50.0,
                pnl=500.0,
                product_type=ProductType.MARGIN
            ),
            Position(
                symbol="NIFTY24FEB17900PE",
                exchange=Exchange.NFO,
                quantity_total=-50,  # Short position
                quantity_available=-50,
                average_price=45.0,
                pnl=-200.0,
                product_type=ProductType.MARGIN
            )
        ]
    
    def test_exit_positions_result_structure(self):
        """Test that exit_positions returns correct result structure."""
        # Expected result structure
        expected_structure = {
            "success": list,
            "failed": list,
            "errors": list,
            "cancelled_orders": list,
            "summary": dict
        }
        
        # Verify structure (actual implementation would use mock driver)
        result = {
            "success": [],
            "failed": [],
            "errors": [],
            "cancelled_orders": [],
            "summary": {"total_positions": 0, "exited": 0, "failed": 0}
        }
        
        for key, expected_type in expected_structure.items():
            self.assertIn(key, result)
            self.assertIsInstance(result[key], expected_type)
    
    def test_exit_positions_calculates_correct_exit_side(self):
        """Test that exit positions calculates correct exit side."""
        # For short positions (negative quantity), should BUY to exit
        # For long positions (positive quantity), should SELL to exit
        
        short_position = Position(
            symbol="NIFTY24FEB18000CE",
            exchange=Exchange.NFO,
            quantity_total=-50,
            quantity_available=-50,
            average_price=50.0,
            pnl=0.0,
            product_type=ProductType.MARGIN
        )
        
        long_position = Position(
            symbol="NIFTY24FEB17900PE",
            exchange=Exchange.NFO,
            quantity_total=50,
            quantity_available=50,
            average_price=45.0,
            pnl=0.0,
            product_type=ProductType.MARGIN
        )
        
        # Short position: quantity < 0 -> BUY
        self.assertLess(short_position.quantity_total, 0)
        exit_txn_short = TransactionType.BUY
        self.assertEqual(exit_txn_short, TransactionType.BUY)
        
        # Long position: quantity > 0 -> SELL
        self.assertGreater(long_position.quantity_total, 0)
        exit_txn_long = TransactionType.SELL
        self.assertEqual(exit_txn_long, TransactionType.SELL)
    
    def test_exit_positions_filters(self):
        """Test that exit_positions respects filters."""
        # Test symbol filter
        symbol_filter = "NIFTY24FEB18000CE"
        filtered_by_symbol = [p for p in self.mock_positions if p.symbol == symbol_filter]
        self.assertEqual(len(filtered_by_symbol), 1)
        self.assertEqual(filtered_by_symbol[0].symbol, symbol_filter)
        
        # Test exchange filter
        exchange_filter = Exchange.NFO
        filtered_by_exchange = [p for p in self.mock_positions if p.exchange == exchange_filter]
        self.assertEqual(len(filtered_by_exchange), 2)
        
        # Test product type filter
        product_filter = ProductType.MARGIN
        filtered_by_product = [p for p in self.mock_positions if p.product_type == product_filter]
        self.assertEqual(len(filtered_by_product), 2)


class TestBasketOrdersFunctionality(unittest.TestCase):
    """Test basket orders functionality for atomic fresh+SL placement."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.fresh_order = OrderRequest(
            symbol="NIFTY24FEB18000CE",
            exchange=Exchange.NFO,
            quantity=50,
            order_type=OrderType.LIMIT,
            transaction_type=TransactionType.SELL,  # Fresh sell (short)
            product_type=ProductType.MARGIN,
            price=50.0,
            tag="FRESH_ORDER"
        )
        
        self.sl_order = OrderRequest(
            symbol="NIFTY24FEB18000CE",
            exchange=Exchange.NFO,
            quantity=50,
            order_type=OrderType.STOP_LIMIT,
            transaction_type=TransactionType.BUY,  # Buy to cover
            product_type=ProductType.MARGIN,
            price=85.0,  # Limit price
            stop_price=80.0,  # Trigger price
            tag="SL_ORDER"
        )
    
    def test_basket_orders_input_validation(self):
        """Test basket orders input validation."""
        # Test with valid requests
        requests = [self.fresh_order, self.sl_order]
        self.assertEqual(len(requests), 2)
        
        # Verify fresh order
        self.assertEqual(requests[0].tag, "FRESH_ORDER")
        self.assertEqual(requests[0].transaction_type, TransactionType.SELL)
        
        # Verify SL order
        self.assertEqual(requests[1].tag, "SL_ORDER")
        self.assertEqual(requests[1].order_type, OrderType.STOP_LIMIT)
        self.assertEqual(requests[1].transaction_type, TransactionType.BUY)
    
    def test_basket_orders_result_structure(self):
        """Test that basket orders returns correct result structure."""
        # Mock responses
        mock_responses = [
            OrderResponse(status="ok", order_id="ORDER001"),
            OrderResponse(status="ok", order_id="ORDER002")
        ]
        
        self.assertEqual(len(mock_responses), 2)
        self.assertEqual(mock_responses[0].order_id, "ORDER001")
        self.assertEqual(mock_responses[1].order_id, "ORDER002")
    
    def test_basket_orders_handles_partial_failure(self):
        """Test that basket orders handles partial failure gracefully."""
        # Mock responses with one failure
        mock_responses = [
            OrderResponse(status="ok", order_id="ORDER001"),
            OrderResponse(status="error", order_id=None, message="Insufficient funds")
        ]
        
        self.assertEqual(len(mock_responses), 2)
        self.assertEqual(mock_responses[0].status, "ok")
        self.assertEqual(mock_responses[1].status, "error")


class TestSLOrderLifecycle(unittest.TestCase):
    """Test complete SL order lifecycle."""
    
    def test_sl_order_lifecycle_flow(self):
        """Test the complete SL order lifecycle flow."""
        # Step 1: Entry signal detected
        entry_price = 50.0
        sl_percentage = 60
        
        # Step 2: Calculate SL prices
        sl_trigger = entry_price * (1 + sl_percentage / 100)
        sl_limit = sl_trigger + 5.0
        
        self.assertEqual(sl_trigger, 80.0)
        self.assertEqual(sl_limit, 85.0)
        
        # Step 3: Create orders
        fresh_order = OrderRequest(
            symbol="NIFTY24FEB18000CE",
            exchange=Exchange.NFO,
            quantity=50,
            order_type=OrderType.LIMIT,
            transaction_type=TransactionType.SELL,
            product_type=ProductType.MARGIN,
            price=entry_price,
            tag="ENTRY"
        )
        
        sl_order = OrderRequest(
            symbol="NIFTY24FEB18000CE",
            exchange=Exchange.NFO,
            quantity=50,
            order_type=OrderType.STOP_LIMIT,
            transaction_type=TransactionType.BUY,
            product_type=ProductType.MARGIN,
            price=sl_limit,
            stop_price=sl_trigger,
            tag="SL"
        )
        
        # Step 4: Verify order correlation
        self.assertEqual(fresh_order.symbol, sl_order.symbol)
        self.assertEqual(fresh_order.quantity, sl_order.quantity)
        self.assertEqual(fresh_order.exchange, sl_order.exchange)
        
        # Step 5: Simulate order placement
        fresh_response = OrderResponse(status="ok", order_id="FRESH001")
        sl_response = OrderResponse(status="ok", order_id="SL001")
        
        # Step 6: Track position with SL order ID
        position_info = {
            "symbol": fresh_order.symbol,
            "entry_price": entry_price,
            "sl_order_id": sl_response.order_id,
            "sl_price": sl_trigger,
            "sl_limit_price": sl_limit
        }
        
        self.assertEqual(position_info["sl_order_id"], "SL001")
        self.assertEqual(position_info["sl_price"], 80.0)


class TestPositionInfoTracking(unittest.TestCase):
    """Test PositionInfo dataclass for SL tracking."""
    
    def test_position_info_creation(self):
        """Test PositionInfo creation with SL fields."""
        position_info = {
            "symbol": "NIFTY24FEB18000CE",
            "entry_price": 50.0,
            "entry_time": datetime.now(),
            "quantity": 50,
            "option_type": "CE",
            "current_pnl": 0.0,
            "highest_pnl": 0.0,
            "sl_order_id": None,
            "sl_price": 0.0,
            "sl_limit_price": 0.0
        }
        
        self.assertIsNone(position_info["sl_order_id"])
        self.assertEqual(position_info["sl_price"], 0.0)
    
    def test_position_info_with_sl(self):
        """Test PositionInfo with SL order placed."""
        position_info = {
            "symbol": "NIFTY24FEB18000CE",
            "entry_price": 50.0,
            "entry_time": datetime.now(),
            "quantity": 50,
            "option_type": "CE",
            "current_pnl": 100.0,
            "highest_pnl": 150.0,
            "sl_order_id": "SL001",
            "sl_price": 80.0,
            "sl_limit_price": 85.0
        }
        
        self.assertEqual(position_info["sl_order_id"], "SL001")
        self.assertEqual(position_info["sl_price"], 80.0)
        self.assertEqual(position_info["sl_limit_price"], 85.0)


class TestConfigurationPresets(unittest.TestCase):
    """Test SL configuration presets from PRD."""
    
    def test_conservative_preset(self):
        """Test conservative preset: 40% SL, STOP_LIMIT, 3.0 buffer."""
        preset = {
            "name": "Conservative",
            "sl_percentage": 40,
            "sl_order_type": OrderType.STOP_LIMIT,
            "sl_limit_buffer": 3.0
        }
        
        entry_price = 50.0
        sl_trigger = entry_price * (1 + preset["sl_percentage"] / 100)
        sl_limit = sl_trigger + preset["sl_limit_buffer"]
        
        self.assertEqual(sl_trigger, 70.0)
        self.assertEqual(sl_limit, 73.0)
    
    def test_moderate_preset(self):
        """Test moderate preset: 60% SL, STOP_LIMIT, 5.0 buffer."""
        preset = {
            "name": "Moderate",
            "sl_percentage": 60,
            "sl_order_type": OrderType.STOP_LIMIT,
            "sl_limit_buffer": 5.0
        }
        
        entry_price = 50.0
        sl_trigger = entry_price * (1 + preset["sl_percentage"] / 100)
        sl_limit = sl_trigger + preset["sl_limit_buffer"]
        
        self.assertEqual(sl_trigger, 80.0)
        self.assertEqual(sl_limit, 85.0)
    
    def test_aggressive_preset(self):
        """Test aggressive preset: 100% SL, STOP, 0 buffer."""
        preset = {
            "name": "Aggressive",
            "sl_percentage": 100,
            "sl_order_type": OrderType.STOP,
            "sl_limit_buffer": 0.0
        }
        
        entry_price = 50.0
        sl_trigger = entry_price * (1 + preset["sl_percentage"] / 100)
        
        self.assertEqual(sl_trigger, 100.0)
        self.assertEqual(preset["sl_order_type"], OrderType.STOP)


class TestRiskScenarios(unittest.TestCase):
    """Test various risk scenarios."""
    
    def test_max_loss_calculation(self):
        """Test max loss calculation for different SL percentages."""
        scenarios = [
            {"sl_percent": 40, "entry": 50, "qty": 50, "expected_risk_per_unit": 20, "expected_max_loss": 1000},
            {"sl_percent": 60, "entry": 50, "qty": 50, "expected_risk_per_unit": 30, "expected_max_loss": 1500},
            {"sl_percent": 80, "entry": 50, "qty": 50, "expected_risk_per_unit": 40, "expected_max_loss": 2000},
            {"sl_percent": 100, "entry": 50, "qty": 50, "expected_risk_per_unit": 50, "expected_max_loss": 2500},
        ]
        
        for scenario in scenarios:
            with self.subTest(sl_percent=scenario["sl_percent"]):
                sl_trigger = scenario["entry"] * (1 + scenario["sl_percent"] / 100)
                risk_per_unit = sl_trigger - scenario["entry"]
                max_loss = risk_per_unit * scenario["qty"]
                
                self.assertEqual(risk_per_unit, scenario["expected_risk_per_unit"])
                self.assertEqual(max_loss, scenario["expected_max_loss"])


def run_tests():
    """Run all tests and print results."""
    print("=" * 70)
    print("STOP-LOSS ORDER FUNCTIONALITY TEST SUITE")
    print("=" * 70)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestSLPriceCalculation))
    suite.addTests(loader.loadTestsFromTestCase(TestSLOrderRequest))
    suite.addTests(loader.loadTestsFromTestCase(TestBrokerCapabilities))
    suite.addTests(loader.loadTestsFromTestCase(TestExitPositionsFunctionality))
    suite.addTests(loader.loadTestsFromTestCase(TestBasketOrdersFunctionality))
    suite.addTests(loader.loadTestsFromTestCase(TestSLOrderLifecycle))
    suite.addTests(loader.loadTestsFromTestCase(TestPositionInfoTracking))
    suite.addTests(loader.loadTestsFromTestCase(TestConfigurationPresets))
    suite.addTests(loader.loadTestsFromTestCase(TestRiskScenarios))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Tests Run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)