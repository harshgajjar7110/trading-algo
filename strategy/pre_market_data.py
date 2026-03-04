"""
Pre-Market Data Service - Fetches overnight market data for gap analysis

Provides:
- GIFT Nifty data (Singapore exchange Nifty futures)
- Overnight US market changes (S&P 500, Nasdaq)
- Asian market indicators
- Nifty 50 pre-open gap calculation

Note: Optional dependency 'yfinance' required for fetching market data.
      Install with: pip install yfinance
"""

import os
import json
import logging
from dataclasses import dataclass
from datetime import datetime, time
from typing import Optional, Dict, Any, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class PreMarketSnapshot:
    """Pre-market data snapshot for gap analysis"""
    timestamp: datetime
    nifty_previous_close: float
    gift_nifty_price: Optional[float]
    gift_nifty_gap_percent: float
    us_markets_change: Dict[str, float]
    asian_markets: Dict[str, float]
    vix_level: Optional[float]
    overall_sentiment: str  # 'BULLISH', 'BEARISH', 'NEUTRAL'
    trading_recommendation: str  # 'TRADE', 'REDUCE', 'SKIP'


class PreMarketDataService:
    """
    Fetches pre-market data for gap risk assessment.
    
    Data sources:
    - GIFT Nifty: Singapore Exchange (SGX)
    - US Markets: Yahoo Finance API
    - VIX: Yahoo Finance or NSE
    """
    
    # API endpoints
    GIFT_NIFTY_SYMBOL = "^SGXNIFTY"  # GIFT Nifty index
    NIFTY_SYMBOL = "^NSEI"  # Nifty 50 index
    
    def __init__(self, cache_dir: Optional[str] = None):
        self.cache_dir = cache_dir or os.path.join(
            Path(__file__).parent.parent, "cache", "premarket"
        )
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Cache settings
        self._cache: Dict[str, Any] = {}
        self._cache_timestamp: Optional[datetime] = None
        self._cache_validity_minutes = 15
    
    def _is_cache_valid(self) -> bool:
        """Check if cached data is still valid"""
        if self._cache_timestamp is None:
            return False
        
        elapsed = (datetime.now() - self._cache_timestamp).total_seconds() / 60
        return elapsed < self._cache_validity_minutes
    
    def _save_to_cache(self, data: Dict[str, Any]):
        """Save data to cache"""
        self._cache = data
        self._cache_timestamp = datetime.now()
        
        # Also save to file for persistence
        cache_file = os.path.join(self.cache_dir, f"premarket_{datetime.now().strftime('%Y%m%d')}.json")
        try:
            with open(cache_file, 'w') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'data': data
                }, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save pre-market cache: {e}")
    
    def _load_from_file_cache(self) -> Optional[Dict[str, Any]]:
        """Try to load from file cache"""
        cache_file = os.path.join(self.cache_dir, f"premarket_{datetime.now().strftime('%Y%m%d')}.json")
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    cached = json.load(f)
                    cache_time = datetime.fromisoformat(cached['timestamp'])
                    elapsed = (datetime.now() - cache_time).total_seconds() / 60
                    if elapsed < self._cache_validity_minutes:
                        return cached['data']
        except Exception as e:
            logger.warning(f"Failed to load pre-market cache: {e}")
        return None
    
    def fetch_gift_nifty(self) -> Tuple[Optional[float], Optional[float]]:
        """
        Fetch GIFT Nifty price and calculate gap.
        
        Returns:
            Tuple of (gift_nifty_price, gap_percent)
        """
        try:
            # Note: In production, use actual SGX or Yahoo Finance API
            # This is a placeholder implementation
            
            # Example using Yahoo Finance API (requires yfinance package)
            try:
                import yfinance as yf
                gift = yf.Ticker(self.GIFT_NIFTY_SYMBOL)
                hist = gift.history(period="2d")
                
                if len(hist) >= 2:
                    previous_close = hist['Close'].iloc[-2]
                    current_price = hist['Close'].iloc[-1]
                    gap_percent = ((current_price - previous_close) / previous_close) * 100
                    
                    logger.info(f"GIFT Nifty: {current_price}, Gap: {gap_percent:.2f}%")
                    return current_price, gap_percent
                    
            except ImportError:
                logger.debug("yfinance not installed, using fallback")
            
            # Fallback: Try to fetch from alternative source or use mock
            return self._fetch_gift_nifty_fallback()
            
        except Exception as e:
            logger.error(f"Failed to fetch GIFT Nifty: {e}")
            return None, None
    
    def _fetch_gift_nifty_fallback(self) -> Tuple[Optional[float], Optional[float]]:
        """
        Fallback method for GIFT Nifty data.
        In production, implement actual API calls to SGX or data provider.
        """
        # Placeholder - implement actual API integration
        # Options:
        # 1. SGX API (requires subscription)
        # 2. Yahoo Finance (free but delayed)
        # 3. Broker API (if available)
        # 4. Third-party data provider
        
        logger.warning("Using fallback GIFT Nifty - implement actual data source")
        return None, None
    
    def fetch_nifty_previous_close(self) -> Optional[float]:
        """Fetch Nifty 50 previous close price"""
        try:
            # Try to get from broker or cache
            cache_file = os.path.join(self.cache_dir, "nifty_previous_close.json")
            
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                    # Check if it's from today (written after market close)
                    cache_date = datetime.fromisoformat(data['timestamp']).date()
                    if cache_date == datetime.now().date():
                        return data['close']
            
            # Try yfinance
            try:
                import yfinance as yf
                nifty = yf.Ticker(self.NIFTY_SYMBOL)
                hist = nifty.history(period="5d")
                if len(hist) > 0:
                    previous_close = hist['Close'].iloc[-1]
                    return previous_close
            except ImportError:
                pass
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to fetch Nifty previous close: {e}")
            return None
    
    def fetch_us_markets(self) -> Dict[str, float]:
        """Fetch overnight US market changes"""
        try:
            # Try yfinance
            try:
                import yfinance as yf
                
                symbols = {
                    'S&P 500': '^GSPC',
                    'Nasdaq': '^IXIC',
                    'Dow Jones': '^DJI'
                }
                
                changes = {}
                for name, symbol in symbols.items():
                    try:
                        ticker = yf.Ticker(symbol)
                        hist = ticker.history(period="2d")
                        if len(hist) >= 2:
                            prev_close = hist['Close'].iloc[-2]
                            curr_close = hist['Close'].iloc[-1]
                            change_pct = ((curr_close - prev_close) / prev_close) * 100
                            changes[name] = round(change_pct, 2)
                    except Exception as e:
                        logger.debug(f"Failed to fetch {name}: {e}")
                
                return changes
                
            except ImportError:
                pass
            
            return {}
            
        except Exception as e:
            logger.error(f"Failed to fetch US markets: {e}")
            return {}
    
    def calculate_overall_sentiment(
        self,
        gift_nifty_gap: Optional[float],
        us_changes: Dict[str, float]
    ) -> str:
        """
        Calculate overall market sentiment based on pre-market data.
        
        Returns:
            'BULLISH', 'BEARISH', or 'NEUTRAL'
        """
        signals = []
        
        # GIFT Nifty signal
        if gift_nifty_gap is not None:
            if gift_nifty_gap > 0.5:
                signals.append(1)  # Bullish
            elif gift_nifty_gap < -0.5:
                signals.append(-1)  # Bearish
            else:
                signals.append(0)  # Neutral
        
        # US markets signal (average)
        if us_changes:
            avg_change = sum(us_changes.values()) / len(us_changes)
            if avg_change > 0.5:
                signals.append(1)
            elif avg_change < -0.5:
                signals.append(-1)
            else:
                signals.append(0)
        
        if not signals:
            return "NEUTRAL"
        
        avg_signal = sum(signals) / len(signals)
        
        if avg_signal > 0.3:
            return "BULLISH"
        elif avg_signal < -0.3:
            return "BEARISH"
        else:
            return "NEUTRAL"
    
    def get_trading_recommendation(
        self,
        gift_nifty_gap: Optional[float],
        overnight_gap: float = 0.0
    ) -> str:
        """
        Get trading recommendation based on gap analysis.
        
        Returns:
            'TRADE', 'REDUCE', or 'SKIP'
        """
        effective_gap = max(
            abs(gift_nifty_gap or 0),
            abs(overnight_gap)
        )
        
        if effective_gap > 1.5:
            return "SKIP"
        elif effective_gap > 1.0:
            return "REDUCE"
        else:
            return "TRADE"
    
    def fetch_all_data(self) -> PreMarketSnapshot:
        """
        Fetch all pre-market data in one call.
        
        Returns:
            PreMarketSnapshot with all available indicators
        """
        # Check cache first
        if self._is_cache_valid():
            logger.debug("Using cached pre-market data")
            return PreMarketSnapshot(**self._cache)
        
        # Try file cache
        file_cache = self._load_from_file_cache()
        if file_cache:
            return PreMarketSnapshot(**file_cache)
        
        # Fetch fresh data
        logger.info("Fetching fresh pre-market data...")
        
        nifty_prev = self.fetch_nifty_previous_close()
        gift_price, gift_gap = self.fetch_gift_nifty()
        us_changes = self.fetch_us_markets()
        
        sentiment = self.calculate_overall_sentiment(gift_gap, us_changes)
        recommendation = self.get_trading_recommendation(gift_gap)
        
        data = PreMarketSnapshot(
            timestamp=datetime.now(),
            nifty_previous_close=nifty_prev or 0.0,
            gift_nifty_price=gift_price,
            gift_nifty_gap_percent=gift_gap or 0.0,
            us_markets_change=us_changes,
            asian_markets={},  # TODO: Add Asian markets
            vix_level=None,  # TODO: Add VIX
            overall_sentiment=sentiment,
            trading_recommendation=recommendation
        )
        
        # Save to cache
        self._save_to_cache(data.__dict__)
        
        return data
    
    def get_overnight_gap_percent(self, current_nifty_price: float) -> float:
        """
        Calculate overnight gap percentage.
        
        Args:
            current_nifty_price: Current Nifty 50 price
            
        Returns:
            Gap percentage from previous close
        """
        previous_close = self.fetch_nifty_previous_close()
        
        if previous_close and previous_close > 0:
            gap = ((current_nifty_price - previous_close) / previous_close) * 100
            return gap
        
        return 0.0


# Global instance
_pre_market_service: Optional[PreMarketDataService] = None


def get_pre_market_service() -> PreMarketDataService:
    """Get the global pre-market data service"""
    global _pre_market_service
    if _pre_market_service is None:
        _pre_market_service = PreMarketDataService()
    return _pre_market_service


def fetch_premarket_summary() -> Dict[str, Any]:
    """
    Convenience function to get pre-market summary.
    
    Returns:
        Dict with key pre-market indicators
    """
    service = get_pre_market_service()
    data = service.fetch_all_data()
    
    return {
        "timestamp": data.timestamp.isoformat(),
        "gift_nifty_gap": data.gift_nifty_gap_percent,
        "us_markets": data.us_markets_change,
        "sentiment": data.overall_sentiment,
        "recommendation": data.trading_recommendation,
    }


# Backward compatibility alias
PreMarketData = PreMarketSnapshot
