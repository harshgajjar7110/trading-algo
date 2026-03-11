import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path


class PaperTradingStateManager:
    """Manages persistent state for paper trading using JSON files."""

    def __init__(self, data_dir: str = "data/paper_trading"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.data_dir / "state.json"

    def load_state(self) -> Dict[str, Any]:
        """Load state from JSON file or return default state."""
        if self.state_file.exists():
            with open(self.state_file, "r") as f:
                return json.load(f)
        return self._default_state()

    def save_state(self, state: Dict[str, Any]):
        """Save state to JSON file atomically."""
        temp_file = self.state_file.with_suffix(".tmp")
        with open(temp_file, "w") as f:
            json.dump(state, f, indent=2, default=str)
        temp_file.replace(self.state_file)

    def _default_state(self) -> Dict[str, Any]:
        """Return default initial state."""
        starting_capital = float(os.getenv("PAPER_TRADING_CAPITAL", "1000000"))
        return {
            "starting_capital": starting_capital,
            "current_cash": starting_capital,
            "positions": {},
            "orders": {},
            "daily_pnl": {},
            "cumulative_pnl": 0.0,
            "trades": [],
            "last_updated": datetime.now().isoformat(),
        }
