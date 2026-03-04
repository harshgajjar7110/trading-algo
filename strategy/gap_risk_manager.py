"""
Gap Risk Manager - Protects against overnight gap losses

Implements gap risk management rules from Gap Handling Strategy:
- Day-based position sizing multipliers (Monday: 0.5x, Friday: 0.4x)
- Gap detection and trading suspension (>1.5% gap = skip trading)
- Monday 10:00 AM entry delay rule
- Pre-market GIFT Nifty analysis

This module is integrated into the strategy execution flow to automatically
adjust position sizing and trading decisions based on gap risk.
"""

from dataclasses import dataclass
from datetime import datetime, time
from enum import Enum
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class DayRiskProfile(Enum):
    """Risk profile for each day of the week"""
    HIGH = "high"      # Monday - Weekend gap risk
    MEDIUM = "medium"  # Thursday - Expiry day
    NORMAL = "normal"  # Tuesday, Wednesday
    WEEKEND = "weekend" # Friday - Pre-weekend risk


@dataclass
class GapRiskAssessment:
    """Result of gap risk assessment"""
    can_trade: bool
    position_multiplier: float
    message: str
    gap_percent: float = 0.0
    gift_nifty_gap: Optional[float] = None
    recommendation: str = ""  # 'TRADE', 'REDUCE', 'SKIP'


class GapRiskManager:
    """
    Manages gap risk for option selling strategies.
    
    Key Features:
    - Day-based position sizing (Monday=50%, Friday=40%)
    - Gap threshold-based trading suspension (>1.5% = no trade)
    - Monday entry delay (wait until 10:00 AM)
    - Pre-market context assessment
    """
    
    # Day-of-week multipliers based on gap risk
    DAY_MULTIPLIERS = {
        "Monday": 0.5,
        "Tuesday": 1.0,
        "Wednesday": 1.0,
        "Thursday": 0.75,
        "Friday": 0.4,
        "Saturday": 0.0,
        "Sunday": 0.0,
    }
    
    # Risk profiles by day
    DAY_RISK_PROFILES = {
        "Monday": DayRiskProfile.HIGH,
        "Tuesday": DayRiskProfile.NORMAL,
        "Wednesday": DayRiskProfile.NORMAL,
        "Thursday": DayRiskProfile.MEDIUM,
        "Friday": DayRiskProfile.WEEKEND,
        "Saturday": DayRiskProfile.HIGH,
        "Sunday": DayRiskProfile.HIGH,
    }
    
    # Gap thresholds (in percent)
    GAP_SKIP_THRESHOLD = 1.5    # Skip trading if gap > 1.5%
    GAP_REDUCE_THRESHOLD = 1.0  # Reduce size if gap > 1.0%
    GAP_WARN_THRESHOLD = 0.5    # Warn if gap > 0.5%
    
    # Monday entry delay
    MONDAY_ENTRY_HOUR = 10
    MONDAY_ENTRY_MINUTE = 0
    
    def __init__(self):
        self._gift_nifty_cache: Optional[Dict[str, Any]] = None
        self._cache_timestamp: Optional[datetime] = None
        self._cache_validity_minutes = 30
    
    def get_position_multiplier(self, dt: Optional[datetime] = None) -> float:
        """
        Get position size multiplier based on day of week.
        
        Args:
            dt: Datetime to check (defaults to now)
            
        Returns:
            float: Multiplier for position sizing (0.0 to 1.0)
        """
        if dt is None:
            dt = datetime.now()
        
        day_name = dt.strftime("%A")
        multiplier = self.DAY_MULTIPLIERS.get(day_name, 1.0)
        
        logger.debug(f"Day {day_name}: Position multiplier = {multiplier}")
        return multiplier
    
    def can_trade_now(self, dt: Optional[datetime] = None) -> tuple[bool, str]:
        """
        Check if trading is allowed at the current time.
        
        Implements Monday 10:00 AM entry delay rule.
        
        Args:
            dt: Datetime to check (defaults to now)
            
        Returns:
            Tuple of (can_trade: bool, reason: str)
        """
        if dt is None:
            dt = datetime.now()
        
        day_name = dt.strftime("%A")
        current_time = dt.time()
        
        # Check if it's Monday before 10:00 AM
        if day_name == "Monday":
            entry_time = time(self.MONDAY_ENTRY_HOUR, self.MONDAY_ENTRY_MINUTE)
            if current_time < entry_time:
                wait_minutes = (
                    (entry_time.hour - current_time.hour) * 60 + 
                    (entry_time.minute - current_time.minute)
                )
                msg = f"Monday gap settlement period: Wait {wait_minutes} min until 10:00 AM"
                logger.info(msg)
                return False, msg
        
        return True, "Trading allowed"
    
    def assess_gap_risk(
        self,
        overnight_gap_percent: float = 0.0,
        gift_nifty_gap: Optional[float] = None,
        dt: Optional[datetime] = None
    ) -> GapRiskAssessment:
        """
        Comprehensive gap risk assessment for trading decision.
        
        Args:
            overnight_gap_percent: Gap from previous close to current open
            gift_nifty_gap: Gap indicated by GIFT Nifty (if available)
            dt: Current datetime (defaults to now)
            
        Returns:
            GapRiskAssessment with trading recommendation
        """
        if dt is None:
            dt = datetime.now()
        
        day_name = dt.strftime("%A")
        base_multiplier = self.DAY_MULTIPLIERS.get(day_name, 1.0)
        
        # Use the larger gap value
        effective_gap = max(abs(overnight_gap_percent), abs(gift_nifty_gap or 0))
        
        # Determine trading action based on gap size
        if effective_gap > self.GAP_SKIP_THRESHOLD:
            return GapRiskAssessment(
                can_trade=False,
                position_multiplier=0.0,
                message=f"Gap {effective_gap:.2f}% exceeds {self.GAP_SKIP_THRESHOLD}% threshold. SKIP TRADING.",
                gap_percent=overnight_gap_percent,
                gift_nifty_gap=gift_nifty_gap,
                recommendation="SKIP"
            )
        
        elif effective_gap > self.GAP_REDUCE_THRESHOLD:
            # Reduce position size further
            adjusted_multiplier = base_multiplier * 0.5
            return GapRiskAssessment(
                can_trade=True,
                position_multiplier=adjusted_multiplier,
                message=f"Gap {effective_gap:.2f}% > {self.GAP_REDUCE_THRESHOLD}%. Reduced size: {adjusted_multiplier:.0%}",
                gap_percent=overnight_gap_percent,
                gift_nifty_gap=gift_nifty_gap,
                recommendation="REDUCE"
            )
        
        elif effective_gap > self.GAP_WARN_THRESHOLD:
            return GapRiskAssessment(
                can_trade=True,
                position_multiplier=base_multiplier,
                message=f"Gap {effective_gap:.2f}% detected. Normal caution advised.",
                gap_percent=overnight_gap_percent,
                gift_nifty_gap=gift_nifty_gap,
                recommendation="TRADE"
            )
        
        else:
            return GapRiskAssessment(
                can_trade=True,
                position_multiplier=base_multiplier,
                message=f"Gap {effective_gap:.2f}% within normal range.",
                gap_percent=overnight_gap_percent,
                gift_nifty_gap=gift_nifty_gap,
                recommendation="TRADE"
            )
    
    def calculate_position_size(
        self,
        base_quantity: int,
        overnight_gap_percent: float = 0.0,
        dt: Optional[datetime] = None
    ) -> int:
        """
        Calculate final position size with gap risk adjustments.
        
        Args:
            base_quantity: Original calculated quantity
            overnight_gap_percent: Overnight gap percentage
            dt: Current datetime (defaults to now)
            
        Returns:
            int: Adjusted position quantity
        """
        assessment = self.assess_gap_risk(overnight_gap_percent, None, dt)
        
        if not assessment.can_trade:
            logger.warning(f"Trading blocked: {assessment.message}")
            return 0
        
        adjusted_quantity = int(base_quantity * assessment.position_multiplier)
        
        if adjusted_quantity != base_quantity:
            logger.info(
                f"Position size adjusted: {base_quantity} → {adjusted_quantity} "
                f"({assessment.position_multiplier:.0%} multiplier). {assessment.message}"
            )
        
        return adjusted_quantity
    
    def get_trading_context(self, dt: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get complete trading context for the current session.
        
        Args:
            dt: Current datetime (defaults to now)
            
        Returns:
            Dict with day, multiplier, risk profile, and recommendations
        """
        if dt is None:
            dt = datetime.now()
        
        day_name = dt.strftime("%A")
        can_trade, reason = self.can_trade_now(dt)
        multiplier = self.get_position_multiplier(dt)
        risk_profile = self.DAY_RISK_PROFILES.get(day_name, DayRiskProfile.NORMAL)
        
        return {
            "day": day_name,
            "date": dt.strftime("%Y-%m-%d"),
            "time": dt.strftime("%H:%M"),
            "can_trade": can_trade,
            "reason": reason,
            "position_multiplier": multiplier,
            "risk_profile": risk_profile.value,
            "monday_delay_applies": day_name == "Monday" and dt.hour < 10,
        }


# Global instance for easy access
gap_risk_manager = GapRiskManager()


def get_gap_risk_manager() -> GapRiskManager:
    """Get the global gap risk manager instance"""
    return gap_risk_manager
