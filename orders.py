from datetime import datetime
from typing import Dict, List, Optional, Any

# Use unified logger (with backward compatibility)
try:
    from utils.logging import get_strategy_logger
    logger = get_strategy_logger("orders")
except ImportError:
    # Fallback to legacy logger for backward compatibility
    from logger import strategy_logger as logger


class OrderTracker:
    """
    Manages placing and tracking orders in-memory only.
    Stores all orders in a dictionary with order_id as key.
    Keeps track of the current active order.

    IMPORTANT: This class does NOT persist data to disk.
    All order data is stored in-memory only and will be lost when the process ends.
    For live data, use the broker API directly.
    """

    def __init__(self):
        self._all_orders: Dict[str, Dict[str, Any]] = {}
        self._current_order: Optional[Dict[str, Any]] = None
        self._order_ids_completed: List[str] = []
        self._order_types_summary: Dict[str, int] = {}

    def add_order(self, order_details: dict):
        """
        Adds a new order.
        Args:
            order_details (dict): A dictionary containing the details of the order.
                                  It MUST include a unique 'order_id'.
        """
        order_id = order_details.get(
            "order_id", order_details.get("orders", {}).get("id", None)
        )
        if not order_id:
            logger.error(
                "Cannot place order: 'order_id' is missing from order_details."
            )
            return

        # Ensure 'timestamp' is present and formatted consistently
        if "timestamp" not in order_details:
            order_details["timestamp"] = datetime.now().isoformat()

        # Update the current order
        self._current_order = order_details
        logger.debug(f"Order being placed: {self._current_order}")

        # Add or update in the dictionary
        if order_id in self._all_orders:
            logger.warning(
                f"Order with ID '{order_id}' already exists. Updating existing order."
            )
        self._all_orders[order_id] = self._current_order
        logger.debug(f"Order '{order_id}' added/updated in in-memory dictionary.")

    @property
    def current_order(self):
        """
        Property to get the most recently placed order.
        """
        return self._current_order

    @property
    def all_orders(self):
        """
        Property to get a copy of all orders placed so far (as a dictionary).
        Returns a copy to prevent external modification of the internal dictionary.
        """
        return self._all_orders.copy()  # Return a copy of the dictionary

    @property
    def completed_order_ids(self):
        """
        Returns a list of completed order IDs.
        """
        return list(self._order_ids_completed)

    @property
    def completed_orders(self):
        """
        Returns a list of completed order details (dicts).
        """
        return [
            self._all_orders[oid]
            for oid in self._order_ids_completed
            if oid in self._all_orders
        ]

    @property
    def non_completed_order_ids(self):
        """
        Returns a list of non-completed order IDs.
        """
        return [oid for oid in self._all_orders if oid not in self._order_ids_completed]

    @property
    def non_completed_orders(self):
        """
        Returns a list of non-completed order details (dicts).
        """
        return [
            self._all_orders[oid]
            for oid in self._all_orders
            if oid not in self._order_ids_completed
        ]

    def get_order_by_id(self, order_id: str):
        """
        Retrieves an order by its order_id (efficient dictionary lookup).
        """
        return self._all_orders.get(order_id)  # Use .get() for safe access

    def get_total_orders_count(self):
        """
        Returns the total number of orders managed.
        """
        return len(self._all_orders)

    def get_all_orders_as_list(self):
        """
        Returns all orders as a list of dictionaries (useful for iteration if needed).
        """
        return list(self._all_orders.values())

    def complete_order(self, order_id: str):
        """
        Marks an order as completed.
        """
        if order_id in self._all_orders:
            if order_id not in self._order_ids_completed:
                self._order_ids_completed.append(order_id)
                if (
                    self._all_orders[order_id]["transaction_type"]
                    not in self._order_types_summary
                ):
                    self._order_types_summary[
                        self._all_orders[order_id]["transaction_type"]
                    ] = 1
                else:
                    self._order_types_summary[
                        self._all_orders[order_id]["transaction_type"]
                    ] += 1
                logger.info(f"Order '{order_id}' marked as completed.")
            else:
                logger.info(f"Order '{order_id}' already marked as completed.")
            return True
        else:
            logger.error(f"Order '{order_id}' not found in the order tracker.")
            return False

    def remove_order(self, order_id: str):
        """
        Removes an order from the order tracker.
        """
        if order_id in self._all_orders:
            del self._all_orders[order_id]
            logger.info(f"Order '{order_id}' removed from the order tracker.")
            return True
        else:
            logger.error(f"Order '{order_id}' not found in the order tracker.")
            return False

    def _record_order_complete(self, order_id: str, transaction_type: str):
        """
        Records order completion statistics.
        """
        if order_id not in self._order_ids_completed:
            if transaction_type not in self._order_types_summary:
                self._order_types_summary[transaction_type] = 1
            else:
                self._order_types_summary[transaction_type] += 1

    def get_order_summary(self):
        """
        Returns a summary of order statistics.
        """
        return {
            "total_orders": len(self._all_orders),
            "completed_orders": len(self._order_ids_completed),
            "active_orders": len(self._all_orders) - len(self._order_ids_completed),
            "order_types_summary": self._order_types_summary.copy(),
            "current_order": self._current_order,
        }

    def print_status(self, additional_info=None):
        """
        Prints current order status and statistics.

        Args:
            additional_info (dict): Additional information to display
        """
        summary = self.get_order_summary()

        logger.info("=" * 50)
        logger.info(f"ORDER STATUS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 50)
        logger.info(f"Total Orders: {summary['total_orders']}")
        logger.info(f"Completed Orders: {summary['completed_orders']}")
        logger.info(f"Active Orders: {summary['active_orders']}")
        logger.info(f"Order Types Summary: {summary['order_types_summary']}")

        if self._all_orders:
            logger.info(f"Current Orders: {self._all_orders}")

        if additional_info:
            logger.info("Additional Info:")
            for key, value in additional_info.items():
                logger.info(f"  {key}: {value}")

        logger.info("=" * 50)
