"""
Local Monitoring Service

Tracks strategy performance metrics and system health.
Provides console dashboard and logging for local monitoring.
"""

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

# Add project root to path
_project_root = Path(__file__).resolve().parents[4]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


@dataclass
class TradeMetrics:
    """Metrics for a single trade."""

    symbol: str
    entry_price: float
    exit_price: float
    quantity: int
    pnl: float = 0.0
    entry_time: datetime = field(default_factory=datetime.now)
    exit_time: Optional[datetime] = None

    @property
    def pnl_percent(self) -> float:
        """Calculate PnL percentage."""
        if self.entry_price == 0:
            return 0.0
        return ((self.exit_price - self.entry_price) / self.entry_price) * 100


@dataclass
class SessionMetrics:
    """Metrics for a trading session."""

    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl: float = 0.0
    max_profit: float = 0.0
    max_loss: float = 0.0
    trades: List[TradeMetrics] = field(default_factory=list)

    @property
    def win_rate(self) -> float:
        """Calculate win rate percentage."""
        if self.total_trades == 0:
            return 0.0
        return (self.winning_trades / self.total_trades) * 100

    @property
    def avg_profit(self) -> float:
        """Calculate average profit per winning trade."""
        if self.winning_trades == 0:
            return 0.0
        winning_pnl = sum(t.pnl for t in self.trades if t.pnl > 0)
        return winning_pnl / self.winning_trades

    @property
    def avg_loss(self) -> float:
        """Calculate average loss per losing trade."""
        if self.losing_trades == 0:
            return 0.0
        losing_pnl = sum(t.pnl for t in self.trades if t.pnl < 0)
        return losing_pnl / self.losing_trades

    @property
    def profit_factor(self) -> float:
        """Calculate profit factor (gross profit / gross loss)."""
        gross_profit = sum(t.pnl for t in self.trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in self.trades if t.pnl < 0))
        if gross_loss == 0:
            return gross_profit if gross_profit > 0 else 0.0
        return gross_profit / gross_loss


class LocalMonitor:
    """
    Local monitoring service for trading strategy.

    Features:
    - Track trade metrics
    - Calculate performance statistics
    - Console dashboard display
    - Session summary reports
    """

    _instance: Optional["LocalMonitor"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._session: Optional[SessionMetrics] = None
        self._active_trades: Dict[str, TradeMetrics] = {}
        self._start_time: Optional[datetime] = None

    def start_session(self) -> None:
        """Start a new trading session."""
        self._session = SessionMetrics()
        self._active_trades.clear()
        self._start_time = datetime.now()
        print(f"[LocalMonitor] Trading session started at {self._start_time}")

    def end_session(self) -> SessionMetrics:
        """End the current trading session and return metrics."""
        if self._session:
            self._session.end_time = datetime.now()
            duration = self._session.end_time - self._session.start_time
            print(f"[LocalMonitor] Trading session ended. Duration: {duration}")
            return self._session
        return SessionMetrics()

    def record_trade_entry(
        self, symbol: str, entry_price: float, quantity: int
    ) -> None:
        """Record a trade entry."""
        trade = TradeMetrics(
            symbol=symbol,
            entry_price=entry_price,
            exit_price=0.0,
            quantity=quantity,
            entry_time=datetime.now(),
        )
        self._active_trades[symbol] = trade
        print(f"[LocalMonitor] Trade entry: {symbol} @ ₹{entry_price:.2f} x {quantity}")

    def record_trade_exit(self, symbol: str, exit_price: float, pnl: float) -> None:
        """Record a trade exit."""
        if symbol in self._active_trades:
            trade = self._active_trades[symbol]
            trade.exit_price = exit_price
            trade.pnl = pnl
            trade.exit_time = datetime.now()

            # Update session metrics
            if self._session:
                self._session.total_trades += 1
                self._session.total_pnl += pnl
                self._session.trades.append(trade)

                if pnl > 0:
                    self._session.winning_trades += 1
                    if pnl > self._session.max_profit:
                        self._session.max_profit = pnl
                else:
                    self._session.losing_trades += 1
                    if pnl < self._session.max_loss:
                        self._session.max_loss = pnl

            del self._active_trades[symbol]

            pnl_emoji = "🟢" if pnl > 0 else "🔴"
            print(
                f"[LocalMonitor] Trade exit: {symbol} @ ₹{exit_price:.2f} | PnL: {pnl_emoji} ₹{pnl:.2f}"
            )

    def get_session_summary(self) -> Dict[str, Any]:
        """Get current session summary."""
        if not self._session:
            return {}

        return {
            "start_time": self._session.start_time.isoformat(),
            "duration": str(datetime.now() - self._session.start_time),
            "total_trades": self._session.total_trades,
            "winning_trades": self._session.winning_trades,
            "losing_trades": self._session.losing_trades,
            "win_rate": f"{self._session.win_rate:.1f}%",
            "total_pnl": f"₹{self._session.total_pnl:.2f}",
            "max_profit": f"₹{self._session.max_profit:.2f}",
            "max_loss": f"₹{self._session.max_loss:.2f}",
            "avg_profit": f"₹{self._session.avg_profit:.2f}",
            "avg_loss": f"₹{self._session.avg_loss:.2f}",
            "profit_factor": f"{self._session.profit_factor:.2f}",
            "active_trades": len(self._active_trades),
        }

    def print_dashboard(self) -> None:
        """Print console dashboard."""
        if not self._session:
            print("[LocalMonitor] No active session")
            return

        summary = self.get_session_summary()

        print("\n" + "=" * 70)
        print("TRADING DASHBOARD".center(70))
        print("=" * 70)
        print(f"Session Start: {summary.get('start_time', 'N/A')}")
        print(f"Duration: {summary.get('duration', 'N/A')}")
        print("-" * 70)
        print(f"Total Trades: {summary.get('total_trades', 0)}")
        print(
            f"Winning: {summary.get('winning_trades', 0)} | Losing: {summary.get('losing_trades', 0)}"
        )
        print(f"Win Rate: {summary.get('win_rate', '0%')}")
        print("-" * 70)
        print(f"Total P&L: {summary.get('total_pnl', '₹0.00')}")
        print(f"Max Profit: {summary.get('max_profit', '₹0.00')}")
        print(f"Max Loss: {summary.get('max_loss', '₹0.00')}")
        print("-" * 70)
        print(f"Avg Profit: {summary.get('avg_profit', '₹0.00')}")
        print(f"Avg Loss: {summary.get('avg_loss', '₹0.00')}")
        print(f"Profit Factor: {summary.get('profit_factor', '0.00')}")
        print("-" * 70)
        print(f"Active Trades: {summary.get('active_trades', 0)}")
        print("=" * 70 + "\n")

    def get_active_trades(self) -> List[Dict[str, Any]]:
        """Get list of active trades."""
        return [
            {
                "symbol": trade.symbol,
                "entry_price": trade.entry_price,
                "quantity": trade.quantity,
                "entry_time": trade.entry_time.isoformat(),
                "duration": str(datetime.now() - trade.entry_time),
            }
            for trade in self._active_trades.values()
        ]

    def reset(self) -> None:
        """Reset all metrics."""
        self._session = None
        self._active_trades.clear()
        self._start_time = None
        print("[LocalMonitor] Metrics reset")


# Global monitor instance
local_monitor = LocalMonitor()
