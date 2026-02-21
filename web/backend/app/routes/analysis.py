"""
Payoff analysis routes.
"""
import re
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends

from ..models.schemas import (
    Position,
    PositionPayoff,
    PortfolioPayoff,
    PayoffPoint,
    OptionType,
)
from ..services.broker_service import BrokerService

router = APIRouter(prefix="/api/analysis", tags=["analysis"])


def parse_option_symbol(symbol: str) -> Optional[dict]:
    """
    Parse an option symbol to extract strike and type.
    
    Supports multiple formats:
    - Monthly: NIFTY24FEB24000CE, BANKNIFTY24FEB45000PE
    - Weekly: NIFTY24FEB1324000CE (Feb 13 weekly expiry)
    - Different indices: BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX
    
    Examples:
    - NIFTY24FEB24000PE -> {strike: 24000, type: PE, expiry: 24FEB}
    - NIFTY24FEB24000CE -> {strike: 24000, type: CE, expiry: 24FEB}
    - NIFTY24FEB1324000CE -> {strike: 24000, type: CE, weekly expiry Feb 13}
    """
    if not symbol:
        return None
        
    symbol = symbol.upper().strip()
    
    # Extract CE/PE first
    if symbol.endswith('CE'):
        option_type = OptionType.CALL
        base = symbol[:-2]
    elif symbol.endswith('PE'):
        option_type = OptionType.PUT
        base = symbol[:-2]
    else:
        return None
    
    # Extract strike (last 4-5 digits)
    strike_match = re.search(r'(\d{4,5})$', base)
    if not strike_match:
        return None
    
    strike = int(strike_match.group(1))
    prefix = base[:strike_match.start()]
    
    # Try weekly format: INDEX + YY + MMM + DD
    weekly_pattern = r'^([A-Z]+)(\d{2})([A-Z]{2,3})(\d{2})$'
    weekly_match = re.match(weekly_pattern, prefix)
    
    if weekly_match:
        return {
            'strike': strike,
            'option_type': option_type,
            'expiry': f"{weekly_match.group(2)}{weekly_match.group(3)}-{weekly_match.group(4)}",
            'index_name': weekly_match.group(1),
            'expiry_type': 'WEEKLY'
        }
    
    # Try monthly format: INDEX + YY + MMM
    monthly_pattern = r'^([A-Z]+)(\d{2})([A-Z]{2,3})$'
    monthly_match = re.match(monthly_pattern, prefix)
    
    if monthly_match:
        return {
            'strike': strike,
            'option_type': option_type,
            'expiry': f"{monthly_match.group(2)}{monthly_match.group(3)}",
            'index_name': monthly_match.group(1),
            'expiry_type': 'MONTHLY'
        }
    
    # Fallback: try to infer index name
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
    
    return {
        'strike': strike,
        'option_type': option_type,
        'expiry': None,
        'index_name': index_name,
        'expiry_type': 'UNKNOWN'
    }


def calculate_option_payoff(
    option_type: OptionType,
    side: str,  # LONG or SHORT
    strike: float,
    premium: float,
    quantity: int,
    underlying_prices: List[float],
) -> List[PayoffPoint]:
    """
    Calculate P&L at various underlying prices for an option position.
    
    For LONG CALL: P&L = (max(underlying - strike, 0) - premium) * |qty|
    For LONG PUT: P&L = (max(strike - underlying, 0) - premium) * |qty|
    For SHORT CALL: P&L = (premium - max(underlying - strike, 0)) * |qty|
    For SHORT PUT: P&L = (premium - max(strike - underlying, 0)) * |qty|
    
    Note: quantity can be negative for SHORT positions, we use abs() for P&L calculation.
    """
    payoff_points = []
    abs_quantity = abs(quantity)
    
    for price in underlying_prices:
        if option_type == OptionType.CALL:
            intrinsic_value = max(price - strike, 0)
        else:  # PUT
            intrinsic_value = max(strike - price, 0)
        
        if side.upper() == 'LONG':
            pnl = (intrinsic_value - premium) * abs_quantity
        else:  # SHORT
            pnl = (premium - intrinsic_value) * abs_quantity
        
        pnl_percent = (pnl / (premium * abs_quantity)) * 100 if premium * abs_quantity != 0 else 0
        
        payoff_points.append(PayoffPoint(
            underlying_price=price,
            pnl=round(pnl, 2),
            pnl_percent=round(pnl_percent, 2)
        ))
    
    return payoff_points


def calculate_breakeven(
    option_type: OptionType,
    side: str,
    strike: float,
    premium: float,
) -> float:
    """
    Calculate breakeven price for an option position.
    
    For LONG CALL: breakeven = strike + premium
    For LONG PUT: breakeven = strike - premium
    For SHORT CALL: breakeven = strike + premium
    For SHORT PUT: breakeven = strike - premium
    """
    if option_type == OptionType.CALL:
        return strike + premium
    else:  # PUT
        return strike - premium


def calculate_max_profit_loss(
    option_type: OptionType,
    side: str,
    strike: float,
    premium: float,
    quantity: int,
) -> tuple:
    """
    Calculate max profit and max loss for an option position.
    
    Returns (max_profit, max_loss) where None means unlimited.
    
    For LONG CALL: max_loss = premium * |qty|, max_profit = unlimited
    For LONG PUT: max_loss = premium * |qty|, max_profit = (strike - premium) * |qty|
    For SHORT CALL: max_loss = unlimited, max_profit = premium * |qty|
    For SHORT PUT: max_loss = (strike - premium) * |qty|, max_profit = premium * |qty|
    
    Note: quantity can be negative for SHORT positions, we use abs() for calculations.
    """
    abs_quantity = abs(quantity)
    total_premium = premium * abs_quantity
    
    if side.upper() == 'LONG':
        max_loss = total_premium  # Can only lose what you paid
        if option_type == OptionType.CALL:
            max_profit = None  # Unlimited upside
        else:  # PUT
            max_profit = (strike - premium) * abs_quantity  # Max if underlying goes to 0
    else:  # SHORT
        max_profit = total_premium  # Keep the premium received
        if option_type == OptionType.CALL:
            max_loss = None  # Unlimited risk (market can go to infinity)
        else:  # PUT
            max_loss = (strike - premium) * abs_quantity  # Max if underlying goes to 0
    
    return max_profit, max_loss


@router.get("/payoff", response_model=PortfolioPayoff)
async def get_payoff_analysis():
    """
    Get payoff analysis for all open positions.
    
    Calculates:
    - Individual position payoffs
    - Combined portfolio payoff
    - Breakeven points
    - Max profit/loss scenarios
    """
    broker_service = BrokerService()
    
    # Get current positions
    positions_response = broker_service.get_positions()
    positions = positions_response.positions
    
    if not positions:
        raise HTTPException(status_code=404, detail="No open positions found")
    
    # Get current NIFTY price
    try:
        nifty_quote = broker_service.get_quote("NSE:NIFTY 50")
        current_nifty = nifty_quote.last_price
    except Exception as e:
        # Fallback: estimate from positions
        print(f"[PayoffAnalysis] Error getting NIFTY quote: {e}")
        current_nifty = 24000  # Default fallback
    
    # Define price range for analysis (±10% of current NIFTY)
    price_range_pct = 0.10
    price_range_start = current_nifty * (1 - price_range_pct)
    price_range_end = current_nifty * (1 + price_range_pct)
    
    # Generate price points for the curve (every 50 points)
    step = 50
    underlying_prices = list(range(
        int(price_range_start // step * step),
        int(price_range_end // step * step) + step,
        step
    ))
    
    position_payoffs: List[PositionPayoff] = []
    combined_pnl = {price: 0.0 for price in underlying_prices}
    total_current_pnl = 0.0
    total_max_profit = 0.0
    total_max_loss = 0.0
    has_unlimited_profit = False
    has_unlimited_loss = False
    
    for pos in positions:
        # Parse option details from symbol
        option_info = parse_option_symbol(pos.symbol)
        
        if not option_info:
            # Skip non-option positions or unparseable symbols
            continue
        
        strike = option_info['strike']
        option_type = option_info['option_type']
        
        # Calculate payoff curve
        payoff_curve = calculate_option_payoff(
            option_type=option_type,
            side=pos.side,
            strike=strike,
            premium=pos.average_price,
            quantity=pos.quantity,
            underlying_prices=underlying_prices,
        )
        
        # Add to combined P&L
        for point in payoff_curve:
            combined_pnl[point.underlying_price] += point.pnl
        
        # Calculate breakeven
        breakeven = calculate_breakeven(
            option_type=option_type,
            side=pos.side,
            strike=strike,
            premium=pos.average_price,
        )
        
        # Calculate max profit/loss
        max_profit, max_loss = calculate_max_profit_loss(
            option_type=option_type,
            side=pos.side,
            strike=strike,
            premium=pos.average_price,
            quantity=pos.quantity,
        )
        
        # Track totals
        if max_profit is None:
            has_unlimited_profit = True
        else:
            total_max_profit += max_profit
        
        if max_loss is None:
            has_unlimited_loss = True
        else:
            total_max_loss += max_loss
        
        current_pnl = pos.pnl if pos.pnl else 0.0
        total_current_pnl += current_pnl
        
        # Calculate breakeven percentage (distance from current price to breakeven)
        breakeven_percent = ((breakeven - current_nifty) / current_nifty) * 100
        
        position_payoffs.append(PositionPayoff(
            symbol=pos.symbol,
            option_type=option_type,
            strike_price=strike,
            side=pos.side,
            quantity=pos.quantity,
            premium=pos.average_price,
            breakeven=round(breakeven, 2),
            breakeven_percent=round(breakeven_percent, 2),
            max_profit=round(max_profit, 2) if max_profit is not None else None,
            max_loss=round(max_loss, 2) if max_loss is not None else None,
            current_underlying_price=current_nifty,
            current_pnl=round(current_pnl, 2),
            payoff_curve=payoff_curve,
        ))
    
    if not position_payoffs:
        raise HTTPException(status_code=404, detail="No option positions found for analysis")
    
    # Build combined payoff curve
    combined_curve = [
        PayoffPoint(
            underlying_price=price,
            pnl=round(pnl, 2),
            pnl_percent=round((pnl / total_max_loss * 100) if total_max_loss != 0 else 0, 2)
        )
        for price, pnl in combined_pnl.items()
    ]
    
    # Find combined breakeven points (where combined P&L crosses zero)
    combined_breakeven_points = []
    prev_pnl = None
    for point in combined_curve:
        if prev_pnl is not None:
            # Check if P&L crosses zero between previous and current price
            if (prev_pnl < 0 and point.pnl >= 0) or (prev_pnl >= 0 and point.pnl < 0):
                # Linear interpolation to find exact breakeven
                prev_price = combined_curve[combined_curve.index(point) - 1].underlying_price
                if point.pnl != prev_pnl:
                    breakeven_price = prev_price + (0 - prev_pnl) * (point.underlying_price - prev_price) / (point.pnl - prev_pnl)
                    combined_breakeven_points.append(round(breakeven_price, 2))
        prev_pnl = point.pnl
    
    return PortfolioPayoff(
        positions=position_payoffs,
        combined_payoff_curve=combined_curve,
        combined_breakeven_points=combined_breakeven_points,
        total_max_profit=round(total_max_profit, 2) if not has_unlimited_profit else None,
        total_max_loss=round(total_max_loss, 2) if not has_unlimited_loss else None,
        current_nifty_price=current_nifty,
        total_current_pnl=round(total_current_pnl, 2),
        price_range_start=price_range_start,
        price_range_end=price_range_end,
    )
