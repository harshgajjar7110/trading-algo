from typing import Dict


class FyrodhaCostCalculator:
    """Calculate Zerodha transaction costs for options trading."""

    # Zerodha charges for options
    BROKERAGE_PER_ORDER = 20.0  # ₹20 per executed order (max)
    STT_SELL_PERCENT = 0.05  # 0.05% on sell side
    EXCHANGE_TURNOVER_PERCENT = 0.053  # 0.053% on premium
    GST_PERCENT = 18.0  # 18% on (brokerage + exchange)
    SEBI_PER_CRORE = 10.0  # ₹10 per crore
    STAMP_DUTY_PERCENT = 0.003  # 0.003% on buy side (varies by state)

    @classmethod
    def calculate_costs(cls, turnover: float, is_sell: bool = True) -> Dict[str, float]:
        """
        Calculate all transaction costs for an order.

        Args:
            turnover: Premium turnover (price * quantity)
            is_sell: True for sell orders (STT applies)

        Returns:
            Dict with breakdown of all costs
        """
        costs = {
            "brokerage": min(cls.BROKERAGE_PER_ORDER, turnover * 0.01),  # Max 20 or 1%
            "exchange": turnover * cls.EXCHANGE_TURNOVER_PERCENT / 100,
            "sebi": turnover * cls.SEBI_PER_CRORE / 10000000,
        }

        # STT only on sell side
        if is_sell:
            costs["stt"] = turnover * cls.STT_SELL_PERCENT / 100
        else:
            costs["stt"] = 0.0

        # GST on brokerage + exchange
        costs["gst"] = (costs["brokerage"] + costs["exchange"]) * cls.GST_PERCENT / 100

        costs["total"] = sum(costs.values())
        return costs
