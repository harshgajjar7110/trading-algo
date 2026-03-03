"""
Telegram Notification Service

Sends notifications via Telegram Bot API for trading events.
Supports order updates, position changes, errors, and daily summaries.
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Optional, Dict, Any

import aiohttp


class TelegramNotifier:
    """
    Telegram notification service for trading alerts.

    Features:
    - Order notifications (placed, filled, rejected)
    - Position updates (opened, closed, PnL)
    - Stop-loss alerts
    - Error notifications
    - Daily summary reports

    Configuration via environment variables:
    - TELEGRAM_BOT_TOKEN: Bot token from @BotFather
    - TELEGRAM_CHAT_ID: Chat ID to send messages to
    """

    _instance: Optional["TelegramNotifier"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self._chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self._enabled = bool(self._bot_token and self._chat_id)
        self._base_url = (
            f"https://api.telegram.org/bot{self._bot_token}"
            if self._bot_token
            else None
        )
        self._session: Optional[aiohttp.ClientSession] = None

        if self._enabled:
            print(f"[TelegramNotifier] Initialized with chat ID: {self._chat_id}")
        else:
            print(
                "[TelegramNotifier] Disabled - set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to enable"
            )

    @property
    def is_enabled(self) -> bool:
        """Check if notifications are enabled."""
        return self._enabled

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """
        Send a message to Telegram.

        Args:
            text: Message text (HTML formatted)
            parse_mode: Parse mode (HTML, Markdown, etc.)

        Returns:
            bool: True if sent successfully
        """
        if not self._enabled:
            return False

        try:
            session = await self._get_session()
            url = f"{self._base_url}/sendMessage"

            payload = {
                "chat_id": self._chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            }

            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    return True
                else:
                    error_text = await response.text()
                    print(f"[TelegramNotifier] Failed to send message: {error_text}")
                    return False

        except Exception as e:
            print(f"[TelegramNotifier] Error sending message: {e}")
            return False

    async def send_order_placed(self, order: Dict[str, Any]) -> bool:
        """
        Notify when an order is placed.

        Args:
            order: Order details dict
        """
        symbol = order.get("symbol", "Unknown")
        order_type = order.get("transaction_type", "BUY")
        quantity = order.get("quantity", 0)
        price = order.get("price", 0)
        order_id = order.get("order_id", "N/A")

        emoji = "🔴" if order_type == "SELL" else "🟢"

        message = f"""
<b>{emoji} ORDER PLACED</b>

<b>Symbol:</b> {symbol}
<b>Type:</b> {order_type}
<b>Quantity:</b> {quantity}
<b>Price:</b> ₹{price:.2f}
<b>Order ID:</b> <code>{order_id}</code>
<b>Time:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""
        return await self._send_message(message.strip())

    async def send_order_filled(
        self, order: Dict[str, Any], average_price: Optional[float] = None
    ) -> bool:
        """
        Notify when an order is filled.

        Args:
            order: Order details dict
            average_price: Actual fill price (if different from order price)
        """
        symbol = order.get("symbol", "Unknown")
        order_type = order.get("transaction_type", "BUY")
        quantity = order.get("quantity", 0)
        price = average_price or order.get("price", 0)
        order_id = order.get("order_id", "N/A")

        emoji = "✅"

        message = f"""
<b>{emoji} ORDER FILLED</b>

<b>Symbol:</b> {symbol}
<b>Type:</b> {order_type}
<b>Quantity:</b> {quantity}
<b>Fill Price:</b> ₹{price:.2f}
<b>Order ID:</b> <code>{order_id}</code>
<b>Time:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""
        return await self._send_message(message.strip())

    async def send_order_rejected(self, order: Dict[str, Any], reason: str) -> bool:
        """
        Notify when an order is rejected.

        Args:
            order: Order details dict
            reason: Rejection reason
        """
        symbol = order.get("symbol", "Unknown")
        order_id = order.get("order_id", "N/A")

        message = f"""
<b>❌ ORDER REJECTED</b>

<b>Symbol:</b> {symbol}
<b>Order ID:</b> <code>{order_id}</code>
<b>Reason:</b> {reason}
<b>Time:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Please check the strategy and broker configuration.
"""
        return await self._send_message(message.strip())

    async def send_position_opened(self, position: Dict[str, Any]) -> bool:
        """
        Notify when a position is opened.

        Args:
            position: Position details dict
        """
        symbol = position.get("symbol", "Unknown")
        quantity = position.get("quantity", 0)
        avg_price = position.get("average_price", 0)
        side = "SHORT" if quantity < 0 else "LONG"

        emoji = "📉" if side == "SHORT" else "📈"

        message = f"""
<b>{emoji} POSITION OPENED</b>

<b>Symbol:</b> {symbol}
<b>Side:</b> {side}
<b>Quantity:</b> {abs(quantity)}
<b>Average Price:</b> ₹{avg_price:.2f}
<b>Time:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""
        return await self._send_message(message.strip())

    async def send_position_closed(self, position: Dict[str, Any], pnl: float) -> bool:
        """
        Notify when a position is closed with PnL.

        Args:
            position: Position details dict
            pnl: Profit/Loss amount
        """
        symbol = position.get("symbol", "Unknown")
        quantity = position.get("quantity", 0)

        pnl_emoji = "🟢" if pnl > 0 else "🔴"
        pnl_text = f"+₹{pnl:.2f}" if pnl > 0 else f"-₹{abs(pnl):.2f}"

        message = f"""
<b>{pnl_emoji} POSITION CLOSED</b>

<b>Symbol:</b> {symbol}
<b>Quantity:</b> {abs(quantity)}
<b>P&L:</b> {pnl_text}
<b>Time:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""
        return await self._send_message(message.strip())

    async def send_stop_loss_triggered(
        self, position: Dict[str, Any], sl_price: float
    ) -> bool:
        """
        Notify when stop-loss is triggered.

        Args:
            position: Position details dict
            sl_price: Stop-loss price
        """
        symbol = position.get("symbol", "Unknown")

        message = f"""
<b>🛑 STOP-LOSS TRIGGERED</b>

<b>Symbol:</b> {symbol}
<b>Stop-Loss Price:</b> ₹{sl_price:.2f}
<b>Time:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Position will be exited automatically.
"""
        return await self._send_message(message.strip())

    async def send_error(
        self, error_message: str, context: Optional[str] = None
    ) -> bool:
        """
        Notify on critical errors.

        Args:
            error_message: Error description
            context: Additional context (optional)
        """
        context_text = f"\n<b>Context:</b> {context}" if context else ""

        # Truncate long error messages
        if len(error_message) > 500:
            error_message = error_message[:497] + "..."

        message = f"""
<b>⚠️ STRATEGY ERROR</b>

<b>Error:</b> <code>{error_message}</code>{context_text}
<b>Time:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Please check the logs for details.
"""
        return await self._send_message(message.strip())

    async def send_daily_summary(
        self,
        total_pnl: float,
        positions_count: int,
        orders_count: int,
        win_count: int = 0,
        loss_count: int = 0,
    ) -> bool:
        """
        Send daily trading summary.

        Args:
            total_pnl: Total profit/loss for the day
            positions_count: Number of positions
            orders_count: Number of orders
            win_count: Number of winning trades
            loss_count: Number of losing trades
        """
        pnl_emoji = "🟢" if total_pnl > 0 else "🔴" if total_pnl < 0 else "⚪"
        pnl_text = (
            f"+₹{total_pnl:.2f}"
            if total_pnl > 0
            else f"-₹{abs(total_pnl):.2f}"
            if total_pnl < 0
            else "₹0.00"
        )

        win_rate = (
            (win_count / (win_count + loss_count) * 100)
            if (win_count + loss_count) > 0
            else 0
        )

        message = f"""
<b>📊 DAILY TRADING SUMMARY</b>
<b>{datetime.now().strftime("%Y-%m-%d")}</b>

<b>Total P&L:</b> {pnl_emoji} {pnl_text}
<b>Positions:</b> {positions_count}
<b>Orders:</b> {orders_count}
<b>Wins:</b> {win_count}
<b>Losses:</b> {loss_count}
<b>Win Rate:</b> {win_rate:.1f}%

Keep trading! 📈
"""
        return await self._send_message(message.strip())

    async def send_strategy_started(
        self, strategy_name: str, config: Optional[Dict] = None
    ) -> bool:
        """
        Notify when strategy starts.

        Args:
            strategy_name: Name of the strategy
            config: Strategy configuration (optional)
        """
        config_text = ""
        if config:
            # Show key config parameters
            key_params = ["index_symbol", "gap", "sl_percentage", "target_profit"]
            for param in key_params:
                if param in config:
                    config_text += f"\n<b>{param}:</b> {config[param]}"

        message = f"""
<b>🚀 STRATEGY STARTED</b>

<b>Strategy:</b> {strategy_name}
<b>Time:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}{config_text}

Trading session started. Good luck! 🎯
"""
        return await self._send_message(message.strip())

    async def send_strategy_stopped(
        self, strategy_name: str, reason: Optional[str] = None
    ) -> bool:
        """
        Notify when strategy stops.

        Args:
            strategy_name: Name of the strategy
            reason: Stop reason (optional)
        """
        reason_text = f"\n<b>Reason:</b> {reason}" if reason else ""

        message = f"""
<b>🛑 STRATEGY STOPPED</b>

<b>Strategy:</b> {strategy_name}
<b>Time:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}{reason_text}

Trading session ended.
"""
        return await self._send_message(message.strip())

    async def close(self):
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None


# Global notifier instance
telegram_notifier = TelegramNotifier()
