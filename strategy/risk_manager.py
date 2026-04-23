import time
from datetime import datetime
from logger import logger

class RiskManager:
    def __init__(self, config: dict, broker, position_manager):
        """
        RiskManager handles trade validation, MTM loss tracking, and dynamic gap calculation.
        """
        self.config = config.get('risk', {})
        self.broker = broker
        self.position_manager = position_manager
        
        # Daily counters
        self.daily_trade_count = 0
        self.last_reset_date = datetime.now().date()
        
        # VIX Cache
        self.vix_cache = None
        self.vix_cache_ts = 0
        self.vix_cache_ttl = 60  # seconds

    def evaluate(self, signal_type: str, current_price: float, current_mtm: float) -> tuple[bool, str]:
        """
        Returns (allowed: bool, reason: str).
        Checks in order:
          1. daily_loss_limit
          2. max_trades_per_day
        """
        self._maybe_reset_counters()

        # 1. Daily Loss Limit Check
        limit = self.config.get('daily_loss_limit', 8000)
        if current_mtm >= limit:
            return False, f"daily_loss_limit_breached: {current_mtm:.2f} >= {limit}"

        # 2. Max Trades Per Day Check
        max_trades = self.config.get('max_trades_per_day', 6)
        if self.daily_trade_count >= max_trades:
            return False, f"max_trades_per_day_reached: {self.daily_trade_count} >= {max_trades}"

        return True, "allowed"

    def get_effective_gap(self, base_gap: float) -> float:
        """
        Fetches India VIX quote. Scales base_gap by VIX ratio.
        vix_ratio = current_vix / vix_baseline
        effective_gap = base_gap * vix_ratio
        Clamps to [base_gap, base_gap * vix_max_multiplier]
        """
        if not self.config.get('vix_enabled', True):
            return base_gap

        current_vix = self._get_vix_quote()
        if current_vix is None:
            logger.warning("Could not fetch VIX, using base_gap")
            return base_gap

        vix_baseline = self.config.get('vix_baseline', 15.0)
        vix_max_multiplier = self.config.get('vix_max_multiplier', 3.0)
        
        vix_ratio = max(current_vix / vix_baseline, 1.0)
        multiplier = min(vix_ratio, vix_max_multiplier)
        
        effective_gap = base_gap * multiplier
        logger.debug(f"VIX: {current_vix}, Ratio: {vix_ratio:.2f}, Multiplier: {multiplier:.2f}, Effective Gap: {effective_gap:.2f}")
        return effective_gap

    def _get_vix_quote(self) -> float | None:
        """Fetches VIX quote with caching."""
        now = time.time()
        if now - self.vix_cache_ts < self.vix_cache_ttl and self.vix_cache is not None:
            return self.vix_cache

        try:
            vix_symbol = self.config.get('vix_symbol', "NSE:INDIA VIX")
            quote = self.broker.get_quote(vix_symbol)
            if quote and hasattr(quote, 'last_price'):
                self.vix_cache = quote.last_price
                self.vix_cache_ts = now
                return self.vix_cache
        except Exception as e:
            logger.error(f"Error fetching VIX quote: {e}")
        
        return None

    def increment_trade_count(self):
        """Called by Strategy when a trade is successfully opened."""
        self.daily_trade_count += 1

    def _maybe_reset_counters(self):
        """Resets counters if it's a new day."""
        today = datetime.now().date()
        if today > self.last_reset_date:
            self.reset_daily_counters()
            self.last_reset_date = today

    def reset_daily_counters(self):
        """Resets all daily risk counters."""
        logger.info("Resetting daily risk counters")
        self.daily_trade_count = 0
