"""
Black-Scholes Greeks Calculator for Options

This module calculates option Greeks (Delta, Gamma, Theta, Vega, Rho) using
the Black-Scholes model. It supports both individual option calculations and
portfolio-level Greeks aggregation.
"""

import re
import math
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from enum import Enum
from scipy.stats import norm


class OptionType(Enum):
    CALL = "CE"
    PUT = "PE"


@dataclass
class Greeks:
    """Option Greeks container"""
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    rho: float = 0.0
    
    def multiply(self, multiplier: float) -> "Greeks":
        """Scale Greeks by a multiplier (for position sizing)"""
        return Greeks(
            delta=self.delta * multiplier,
            gamma=self.gamma * multiplier,
            theta=self.theta * multiplier,
            vega=self.vega * multiplier,
            rho=self.rho * multiplier
        )
    
    def add(self, other: "Greeks") -> "Greeks":
        """Add another Greeks object"""
        return Greeks(
            delta=self.delta + other.delta,
            gamma=self.gamma + other.gamma,
            theta=self.theta + other.theta,
            vega=self.vega + other.vega,
            rho=self.rho + other.rho
        )


@dataclass
class OptionPosition:
    """Option position details for Greeks calculation"""
    symbol: str
    option_type: OptionType
    strike: float
    underlying_price: float
    entry_price: float
    quantity: int  # Positive for long, negative for short
    expiry_date: date
    evaluation_date: Optional[date] = None
    
    # Optional parameters (will use defaults if not provided)
    risk_free_rate: float = 0.10  # 10% annual (typical for India)
    dividend_yield: float = 0.0   # NIFTY has minimal dividends
    volatility: Optional[float] = None  # Will be calculated from price if not provided


class BlackScholesGreeks:
    """
    Black-Scholes Option Greeks Calculator
    
    Calculates Greeks using closed-form Black-Scholes formulas.
    
    Reference formulas:
    - d1 = (ln(S/K) + (r - q + σ²/2) * T) / (σ * √T)
    - d2 = d1 - σ * √T
    
    Greeks:
    - Delta: ∂V/∂S (price sensitivity to underlying)
    - Gamma: ∂²V/∂S² (rate of change of delta)
    - Theta: -∂V/∂T (time decay, per day)
    - Vega: ∂V/∂σ (volatility sensitivity, per 1% change)
    - Rho: ∂V/∂r (interest rate sensitivity, per 1% change)
    """
    
    # Default implied volatility for NIFTY options (annualized)
    DEFAULT_VOLATILITY = 0.15  # 15%
    
    # Month name mapping
    MONTH_MAP = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
        'JA': 1, 'FE': 2, 'MR': 3, 'AP': 4, 'MY': 5, 'JU': 6,  # 2-letter variants
        'JL': 7, 'AU': 8, 'SE': 9, 'OC': 10, 'NO': 11, 'DE': 12,
    }
    
    @staticmethod
    def parse_option_symbol(symbol: str) -> Optional[Dict]:
        """
        Parse option symbol to extract strike, type, and expiry.
        
        Supports multiple formats:
        - NIFTY Monthly (text month): NIFTY24FEB24000CE
        - NIFTY Weekly (text month): NIFTY24FEB1324000CE (Feb 13 weekly)
        - NIFTY Weekly (numeric date): NIFTY2630224750PE (March 22, 2026)
        - BANKNIFTY: BANKNIFTY24FEB45000CE or BANKNIFTY26302245000PE
        - FINNIFTY: FINNIFTY24FEB23000CE or FINNIFTY26302223000PE
        - MIDCPNIFTY: MIDCPNIFTY24FEB12000CE
        
        Numeric date format (YYMMDDstrikeCE/PE):
        - NIFTY2630224750PE = NIFTY + 26 (year) + 3022 (Mar 22) + 24750 + PE
        - NIFTY2630624000CE = NIFTY + 26 (year) + 3062 (Mar 06) + 24000 + CE
        
        Also handles 2-letter month variants:
        - NIFTY24FE24000CE
        
        Returns:
            Dict with strike, option_type, expiry_date, index_name or None if parsing fails
        """
        if not symbol:
            return None
            
        symbol = symbol.upper().strip()
        
        # Try to extract CE/PE first
        if symbol.endswith('CE'):
            option_type = OptionType.CALL
            base = symbol[:-2]
        elif symbol.endswith('PE'):
            option_type = OptionType.PUT
            base = symbol[:-2]
        else:
            return None
        
        # Extract strike (last 4-5 digits before CE/PE)
        strike_match = re.search(r'(\d{4,5})$', base)
        if not strike_match:
            return None
        
        strike = int(strike_match.group(1))
        prefix = base[:strike_match.start()]  # Everything before strike
        
        # Try weekly format with TEXT month: INDEX + YY + MMM + DD
        # Examples: NIFTY24FEB13, BANKNIFTY24FEB06
        weekly_text_pattern = r'^([A-Z]+)(\d{2})([A-Z]{2,3})(\d{2})$'
        weekly_text_match = re.match(weekly_text_pattern, prefix)
        
        if weekly_text_match:
            index_name = weekly_text_match.group(1)
            year_str = weekly_text_match.group(2)
            month_str = weekly_text_match.group(3)
            day_str = weekly_text_match.group(4)
            
            expiry_date = BlackScholesGreeks._parse_expiry_date(
                year_str, month_str, day_str
            )
            
            if expiry_date:
                return {
                    'strike': strike,
                    'option_type': option_type,
                    'expiry_date': expiry_date,
                    'index_name': index_name,
                    'expiry_type': 'WEEKLY'
                }
        
        # Try monthly format with TEXT month: INDEX + YY + MMM
        # Examples: NIFTY24FEB, BANKNIFTY24FEB
        monthly_text_pattern = r'^([A-Z]+)(\d{2})([A-Z]{2,3})$'
        monthly_text_match = re.match(monthly_text_pattern, prefix)
        
        if monthly_text_match:
            index_name = monthly_text_match.group(1)
            year_str = monthly_text_match.group(2)
            month_str = monthly_text_match.group(3)
            
            expiry_date = BlackScholesGreeks._parse_monthly_expiry(
                year_str, month_str
            )
            
            if expiry_date:
                return {
                    'strike': strike,
                    'option_type': option_type,
                    'expiry_date': expiry_date,
                    'index_name': index_name,
                    'expiry_type': 'MONTHLY'
                }
        
        # Try NUMERIC date format: INDEX + YY + MMDD or INDEX + YY + MDD
        # Examples: 
        #   NIFTY26302224... (March 22, 2026 - 4 digit MMDD)
        #   NIFTY2630224750PE (March 02?, 2026 - 3 digit MDD where 302 = March 02)
        # This handles the format where month/day are numeric
        
        # Try 4-digit MMDD first
        numeric_4digit_pattern = r'^([A-Z]+)(\d{2})(\d{4})$'
        numeric_4digit_match = re.match(numeric_4digit_pattern, prefix)
        
        if numeric_4digit_match:
            index_name = numeric_4digit_match.group(1)
            year_str = numeric_4digit_match.group(2)
            date_str = numeric_4digit_match.group(3)  # MMDD format
            
            expiry_date = BlackScholesGreeks._parse_numeric_date(
                year_str, date_str, is_mdd=False
            )
            
            if expiry_date:
                return {
                    'strike': strike,
                    'option_type': option_type,
                    'expiry_date': expiry_date,
                    'index_name': index_name,
                    'expiry_type': 'WEEKLY'
                }
        
        # Try 3-digit MDD (Month 1-9 + Day 01-31)
        # Example: 302 = March 02, 311 = March 11
        numeric_3digit_pattern = r'^([A-Z]+)(\d{2})(\d{3})$'
        numeric_3digit_match = re.match(numeric_3digit_pattern, prefix)
        
        if numeric_3digit_match:
            index_name = numeric_3digit_match.group(1)
            year_str = numeric_3digit_match.group(2)
            date_str = numeric_3digit_match.group(3)  # MDD format (3 digits)
            
            expiry_date = BlackScholesGreeks._parse_numeric_date(
                year_str, date_str, is_mdd=True
            )
            
            if expiry_date:
                return {
                    'strike': strike,
                    'option_type': option_type,
                    'expiry_date': expiry_date,
                    'index_name': index_name,
                    'expiry_type': 'WEEKLY'
                }
        
        # Fallback: try to infer index from prefix
        if 'BANKNIFTY' in prefix:
            index_name = 'BANKNIFTY'
        elif 'FINNIFTY' in prefix:
            index_name = 'FINNIFTY'
        elif 'MIDCPNIFTY' in prefix:
            index_name = 'MIDCPNIFTY'
        elif 'SENSEX' in prefix:
            index_name = 'SENSEX'
        elif 'NIFTY' in prefix:
            index_name = 'NIFTY'
        else:
            index_name = 'UNKNOWN'
        
        # Use a default expiry (next Thursday)
        expiry_date = BlackScholesGreeks._get_next_thursday(date.today())
        
        return {
            'strike': strike,
            'option_type': option_type,
            'expiry_date': expiry_date,
            'index_name': index_name,
            'expiry_type': 'UNKNOWN'
        }
    
    @staticmethod
    def _parse_expiry_date(year_str: str, month_str: str, day_str: str) -> Optional[date]:
        """Parse expiry date from year, month name, and day"""
        try:
            year = 2000 + int(year_str)
            month = BlackScholesGreeks.MONTH_MAP.get(month_str.upper())
            day = int(day_str)
            
            if not month:
                return None
            
            # The day in weekly symbol is the Thursday expiry date
            expiry = date(year, month, day)
            
            # Verify it's a Thursday (NIFTY weekly options expire on Thursdays)
            # If not, adjust to the nearest Thursday
            if expiry.weekday() != 3:  # 3 = Thursday
                # Adjust to next Thursday
                days_until_thursday = (3 - expiry.weekday()) % 7
                if days_until_thursday == 0:
                    days_until_thursday = 7
                expiry = expiry + timedelta(days=days_until_thursday)
            
            return expiry
            
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _parse_monthly_expiry(year_str: str, month_str: str) -> Optional[date]:
        """Parse monthly expiry date (last Thursday of month)"""
        try:
            year = 2000 + int(year_str)
            month = BlackScholesGreeks.MONTH_MAP.get(month_str.upper())
            
            if not month:
                return None
            
            return BlackScholesGreeks._last_thursday(year, month)
            
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _parse_numeric_date(year_str: str, date_str: str, is_mdd: bool = False) -> Optional[date]:
        """
        Parse numeric date format MMDD or MDD
        
        Args:
            year_str: 2-digit year (e.g., '26')
            date_str: Date portion - either MMDD (4 digits) or MDD (3 digits)
            is_mdd: True if date_str is in MDD format (3 digits), False for MMDD (4 digits)
        
        Examples:
        - MMDD: 3022 = March 22
        - MMDD: 0306 = March 06
        - MDD: 302 = March 02 (3-digit format where month is single digit 1-9)
        - MDD: 319 = March 19
        """
        try:
            year = 2000 + int(year_str)
            
            if is_mdd:
                # 3-digit MDD format: First digit is month (1-9), last 2 are day
                if len(date_str) != 3:
                    return None
                
                month = int(date_str[0])  # Single digit month (1-9)
                day = int(date_str[1:])   # Last 2 digits are day
            else:
                # 4-digit MMDD format
                if len(date_str) != 4:
                    return None
                
                month = int(date_str[:2])
                day = int(date_str[2:])
            
            # Validate month and day
            if month < 1 or month > 12:
                return None
            if day < 1 or day > 31:
                return None
            
            expiry = date(year, month, day)
            
            # Ensure it's a Thursday (NIFTY weekly options expire on Thursdays)
            if expiry.weekday() != 3:  # 3 = Thursday
                days_until_thursday = (3 - expiry.weekday()) % 7
                if days_until_thursday == 0:
                    days_until_thursday = 7
                expiry = expiry + timedelta(days=days_until_thursday)
            
            return expiry
            
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _last_thursday(year: int, month: int) -> date:
        """Get the last Thursday of a given month"""
        import calendar
        
        # Get the last day of the month
        last_day = calendar.monthrange(year, month)[1]
        
        # Work backwards to find the last Thursday
        for day in range(last_day, 0, -1):
            d = date(year, month, day)
            if d.weekday() == 3:  # Thursday (Monday=0, Thursday=3)
                return d
        
        return date(year, month, last_day)
    
    @staticmethod
    def _get_next_thursday(from_date: date) -> date:
        """Get the next Thursday from a given date"""
        days_until_thursday = (3 - from_date.weekday()) % 7
        if days_until_thursday == 0:
            days_until_thursday = 7  # If today is Thursday, get next Thursday
        return from_date + timedelta(days=days_until_thursday)
    
    @staticmethod
    def calculate_greeks(position: OptionPosition) -> Greeks:
        """
        Calculate all Greeks for an option position
        
        Args:
            position: OptionPosition with all required details
            
        Returns:
            Greeks object with delta, gamma, theta, vega, rho
        """
        S = position.underlying_price
        K = position.strike
        r = position.risk_free_rate
        q = position.dividend_yield
        
        # Time to expiry in years
        eval_date = position.evaluation_date or date.today()
        T = max((position.expiry_date - eval_date).days / 365.0, 0.0001)  # Min to avoid division by zero
        
        # Use provided volatility or calculate implied volatility
        sigma = position.volatility or BlackScholesGreeks.DEFAULT_VOLATILITY
        
        # Calculate d1 and d2
        sqrt_T = math.sqrt(T)
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T
        
        # Standard normal PDF and CDF
        N_d1 = norm.cdf(d1)
        N_d2 = norm.cdf(d2)
        N_minus_d1 = norm.cdf(-d1)
        N_minus_d2 = norm.cdf(-d2)
        pdf_d1 = norm.pdf(d1)
        
        # Calculate Greeks based on option type
        if position.option_type == OptionType.CALL:
            delta = math.exp(-q * T) * N_d1
            theta = (-(S * sigma * math.exp(-q * T) * pdf_d1) / (2 * sqrt_T)
                     - r * K * math.exp(-r * T) * N_d2
                     + q * S * math.exp(-q * T) * N_d1)
            rho = K * T * math.exp(-r * T) * N_d2 / 100  # Per 1% rate change
        else:  # PUT
            delta = -math.exp(-q * T) * N_minus_d1
            theta = (-(S * sigma * math.exp(-q * T) * pdf_d1) / (2 * sqrt_T)
                     + r * K * math.exp(-r * T) * N_minus_d2
                     - q * S * math.exp(-q * T) * N_minus_d1)
            rho = -K * T * math.exp(-r * T) * N_minus_d2 / 100  # Per 1% rate change
        
        # Gamma is the same for calls and puts
        gamma = (math.exp(-q * T) * pdf_d1) / (S * sigma * sqrt_T)
        
        # Vega is the same for calls and puts (per 1% change in volatility)
        vega = S * math.exp(-q * T) * pdf_d1 * sqrt_T / 100
        
        # Adjust for position direction (long/short)
        multiplier = 1.0 if position.quantity > 0 else -1.0
        
        # Scale by lot size/quantity
        lot_multiplier = abs(position.quantity)
        
        return Greeks(
            delta=delta * multiplier * lot_multiplier,
            gamma=gamma * multiplier * lot_multiplier,
            theta=theta * multiplier * lot_multiplier / 365,  # Convert to daily theta
            vega=vega * multiplier * lot_multiplier,
            rho=rho * multiplier * lot_multiplier
        )
    
    @staticmethod
    def calculate_implied_volatility(
        option_price: float,
        underlying_price: float,
        strike: float,
        time_to_expiry: float,
        option_type: OptionType,
        risk_free_rate: float = 0.10,
        dividend_yield: float = 0.0,
        max_iterations: int = 100,
        precision: float = 0.0001
    ) -> Optional[float]:
        """
        Calculate implied volatility using Newton-Raphson method
        
        Args:
            option_price: Market price of the option
            underlying_price: Current underlying price
            strike: Option strike price
            time_to_expiry: Time to expiry in years
            option_type: CALL or PUT
            risk_free_rate: Risk-free interest rate
            dividend_yield: Dividend yield
            max_iterations: Maximum iterations for convergence
            precision: Convergence precision
            
        Returns:
            Implied volatility or None if calculation fails
        """
        # Initial guess
        sigma = 0.2
        
        for _ in range(max_iterations):
            # Calculate option price with current sigma
            price = BlackScholesGreeks._bs_price(
                underlying_price, strike, time_to_expiry,
                risk_free_rate, dividend_yield, sigma, option_type
            )
            
            # Calculate vega (derivative of price with respect to volatility)
            vega = BlackScholesGreeks._bs_vega(
                underlying_price, strike, time_to_expiry,
                risk_free_rate, dividend_yield, sigma
            )
            
            # Check for convergence
            price_diff = price - option_price
            if abs(price_diff) < precision:
                return sigma
            
            # Update sigma using Newton-Raphson
            if vega != 0:
                sigma = sigma - price_diff / vega
            
            # Ensure sigma stays positive and reasonable
            sigma = max(0.001, min(sigma, 5.0))
        
        return None  # Failed to converge
    
    @staticmethod
    def _bs_price(S, K, T, r, q, sigma, option_type):
        """Calculate Black-Scholes option price"""
        if T <= 0:
            if option_type == OptionType.CALL:
                return max(S - K, 0)
            else:
                return max(K - S, 0)
        
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        
        if option_type == OptionType.CALL:
            price = S * math.exp(-q * T) * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
        else:
            price = K * math.exp(-r * T) * norm.cdf(-d2) - S * math.exp(-q * T) * norm.cdf(-d1)
        
        return price
    
    @staticmethod
    def _bs_vega(S, K, T, r, q, sigma):
        """Calculate Black-Scholes vega (derivative w.r.t. volatility)"""
        if T <= 0:
            return 0
        
        d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        return S * math.exp(-q * T) * norm.pdf(d1) * math.sqrt(T)


class PortfolioGreeksCalculator:
    """
    Portfolio-level Greeks calculator
    
    Aggregates Greeks across all positions to show total portfolio risk exposure.
    """
    
    def __init__(self, risk_free_rate: float = 0.10):
        self.risk_free_rate = risk_free_rate
        self.bs = BlackScholesGreeks()
    
    def calculate_portfolio_greeks(
        self,
        positions: List[Dict],
        underlying_price: float,
        evaluation_date: Optional[date] = None
    ) -> Dict:
        """
        Calculate aggregate Greeks for a portfolio of positions
        
        Args:
            positions: List of position dictionaries with symbol, quantity, average_price
            underlying_price: Current price of underlying (NIFTY)
            evaluation_date: Date for calculation (default: today)
            
        Returns:
            Dictionary with individual position Greeks and portfolio totals
        """
        if evaluation_date is None:
            evaluation_date = date.today()
        
        position_greeks = []
        portfolio_greeks = Greeks()
        skipped_positions = []
        
        for pos in positions:
            symbol = pos.get('symbol', '')
            quantity = pos.get('quantity', 0)
            entry_price = pos.get('average_price', 0)
            
            # Parse option details from symbol
            option_info = self.bs.parse_option_symbol(symbol)
            
            if not option_info:
                skipped_positions.append({
                    'symbol': symbol,
                    'reason': 'Could not parse symbol'
                })
                continue  # Skip non-option positions
            
            # Log for debugging
            print(f"[Greeks] Parsed {symbol}: {option_info['index_name']} "
                  f"{option_info['expiry_type']} expiry "
                  f"{option_info['expiry_date']}")
            
            # Create option position
            option_pos = OptionPosition(
                symbol=symbol,
                option_type=option_info['option_type'],
                strike=option_info['strike'],
                underlying_price=underlying_price,
                entry_price=entry_price,
                quantity=quantity,  # Negative for short positions
                expiry_date=option_info['expiry_date'],
                evaluation_date=evaluation_date,
                risk_free_rate=self.risk_free_rate
            )
            
            # Calculate Greeks for this position
            greeks = self.bs.calculate_greeks(option_pos)
            
            # Store individual position Greeks
            position_greeks.append({
                'symbol': symbol,
                'option_type': option_info['option_type'].value,
                'strike': option_info['strike'],
                'quantity': quantity,
                'entry_price': entry_price,
                'index_name': option_info.get('index_name', 'UNKNOWN'),
                'expiry_type': option_info.get('expiry_type', 'UNKNOWN'),
                'expiry_date': option_info['expiry_date'].isoformat(),
                'greeks': {
                    'delta': round(greeks.delta, 4),
                    'gamma': round(greeks.gamma, 6),
                    'theta': round(greeks.theta, 2),
                    'vega': round(greeks.vega, 2),
                    'rho': round(greeks.rho, 2)
                }
            })
            
            # Add to portfolio totals
            portfolio_greeks = portfolio_greeks.add(greeks)
        
        return {
            'positions': position_greeks,
            'portfolio_totals': {
                'delta': round(portfolio_greeks.delta, 4),
                'gamma': round(portfolio_greeks.gamma, 6),
                'theta': round(portfolio_greeks.theta, 2),
                'vega': round(portfolio_greeks.vega, 2),
                'rho': round(portfolio_greeks.rho, 2)
            },
            'interpretation': self._interpret_greeks(portfolio_greeks),
            'calculation_date': evaluation_date.isoformat(),
            'underlying_price': underlying_price,
            'skipped_positions': skipped_positions,
            'total_positions': len(positions),
            'calculated_positions': len(position_greeks)
        }
    
    def _interpret_greeks(self, greeks: Greeks) -> Dict[str, str]:
        """
        Provide human-readable interpretation of portfolio Greeks
        
        Returns explanations of what the Greek values mean in practical terms.
        """
        interpretations = {}
        
        # Delta interpretation
        if abs(greeks.delta) < 0.1:
            delta_desc = "Delta neutral"
        elif greeks.delta > 0:
            delta_desc = f"Bullish bias: +₹{greeks.delta:.0f} for every 1 point NIFTY rise"
        else:
            delta_desc = f"Bearish bias: ₹{greeks.delta:.0f} for every 1 point NIFTY rise"
        interpretations['delta'] = delta_desc
        
        # Gamma interpretation
        if abs(greeks.gamma) < 0.001:
            gamma_desc = "Low gamma risk"
        elif greeks.gamma > 0:
            gamma_desc = f"Long gamma: Delta increases by +{greeks.gamma:.4f} per point move"
        else:
            gamma_desc = f"Short gamma: Delta decreases by {greeks.gamma:.4f} per point move"
        interpretations['gamma'] = gamma_desc
        
        # Theta interpretation
        if abs(greeks.theta) < 10:
            theta_desc = "Low time decay"
        elif greeks.theta > 0:
            theta_desc = f"Positive theta: Gaining ₹{greeks.theta:.0f}/day from time decay"
        else:
            theta_desc = f"Negative theta: Losing ₹{abs(greeks.theta):.0f}/day to time decay"
        interpretations['theta'] = theta_desc
        
        # Vega interpretation
        if abs(greeks.vega) < 100:
            vega_desc = "Low volatility exposure"
        elif greeks.vega > 0:
            vega_desc = f"Long volatility: +₹{greeks.vega:.0f} per 1% vol increase"
        else:
            vega_desc = f"Short volatility: ₹{greeks.vega:.0f} per 1% vol increase"
        interpretations['vega'] = vega_desc
        
        # Rho interpretation
        if abs(greeks.rho) < 10:
            rho_desc = "Low interest rate sensitivity"
        elif greeks.rho > 0:
            rho_desc = f"Benefits from rate hikes: +₹{greeks.rho:.0f} per 1% rate increase"
        else:
            rho_desc = f"Hurt by rate hikes: ₹{greeks.rho:.0f} per 1% rate increase"
        interpretations['rho'] = rho_desc
        
        return interpretations
    
    def get_scenario_analysis(
        self,
        positions: List[Dict],
        underlying_price: float,
        price_changes: List[float] = None,
        vol_changes: List[float] = None,
        days_forward: int = 1
    ) -> Dict:
        """
        Perform scenario analysis on portfolio Greeks
        
        Shows how P&L would change under different market scenarios.
        
        Args:
            positions: List of positions
            underlying_price: Current underlying price
            price_changes: List of price change percentages (e.g., [-5, -2, 0, 2, 5])
            vol_changes: List of volatility changes (e.g., [-2, -1, 0, 1, 2])
            days_forward: Days to project forward
            
        Returns:
            Scenario analysis results
        """
        if price_changes is None:
            price_changes = [-5, -3, -1, 0, 1, 3, 5]
        if vol_changes is None:
            vol_changes = [-2, -1, 0, 1, 2]
        
        # Get current Greeks
        current = self.calculate_portfolio_greeks(positions, underlying_price)
        totals = current['portfolio_totals']
        
        scenarios = []
        
        for price_pct in price_changes:
            for vol_chg in vol_changes:
                # Calculate estimated P&L change
                price_change = underlying_price * (price_pct / 100)
                
                # Delta contribution (linear)
                delta_pnl = totals['delta'] * price_change
                
                # Gamma contribution (convexity)
                gamma_pnl = 0.5 * totals['gamma'] * (price_change ** 2)
                
                # Theta contribution (time decay)
                theta_pnl = totals['theta'] * days_forward
                
                # Vega contribution (volatility)
                vega_pnl = totals['vega'] * vol_chg
                
                total_pnl = delta_pnl + gamma_pnl + theta_pnl + vega_pnl
                
                scenarios.append({
                    'price_change_pct': price_pct,
                    'price_change_points': round(price_change, 2),
                    'volatility_change': vol_chg,
                    'days_forward': days_forward,
                    'estimated_pnl': round(total_pnl, 2),
                    'breakdown': {
                        'delta_pnl': round(delta_pnl, 2),
                        'gamma_pnl': round(gamma_pnl, 2),
                        'theta_pnl': round(theta_pnl, 2),
                        'vega_pnl': round(vega_pnl, 2)
                    }
                })
        
        return {
            'current_greeks': totals,
            'scenarios': scenarios,
            'underlying_price': underlying_price
        }
