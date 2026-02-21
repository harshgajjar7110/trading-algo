"""
Greeks API Routes

Endpoints for calculating and retrieving option Greeks (Delta, Gamma, Theta, Vega, Rho)
for the portfolio using Black-Scholes model.
"""
from typing import List, Optional
from datetime import date
from fastapi import APIRouter, HTTPException, Query

from ..models.schemas import (
    GreeksResponse,
    PositionGreeks,
    PortfolioGreeks,
    GreeksInterpretation,
    ScenarioAnalysis,
)
from ..services.broker_service import BrokerService
from ..services.greeks_calculator import PortfolioGreeksCalculator

router = APIRouter(prefix="/api/greeks", tags=["greeks"])


@router.get("/portfolio", response_model=GreeksResponse)
async def get_portfolio_greeks(
    underlying_price: Optional[float] = Query(None, description="Override underlying price (default: current NIFTY)"),
    risk_free_rate: float = Query(0.067, description="Risk-free interest rate (default: 6.7%)"),
):
    """
    Calculate Greeks for all open option positions in the portfolio.
    
    Returns:
    - Individual position Greeks (Delta, Gamma, Theta, Vega, Rho)
    - Portfolio aggregate Greeks
    - Human-readable interpretations
    
    The Greeks are calculated using the Black-Scholes model:
    - **Delta**: Sensitivity to underlying price changes (₹ per point)
    - **Gamma**: Rate of change of Delta (per point squared)
    - **Theta**: Time decay (₹ per day)
    - **Vega**: Volatility sensitivity (₹ per 1% vol change)
    - **Rho**: Interest rate sensitivity (₹ per 1% rate change)
    """
    broker_service = BrokerService()
    
    # Get current positions
    positions_response = broker_service.get_positions()
    positions = positions_response.positions
    
    if not positions:
        raise HTTPException(status_code=404, detail="No open positions found")
    
    # Get current NIFTY price
    try:
        if underlying_price is None:
            nifty_quote = broker_service.get_quote("NSE:NIFTY 50")
            underlying_price = nifty_quote.last_price
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get underlying price: {str(e)}")
    
    # Calculate Greeks
    calculator = PortfolioGreeksCalculator(risk_free_rate=risk_free_rate)
    
    try:
        result = calculator.calculate_portfolio_greeks(
            positions=[p.model_dump() for p in positions],
            underlying_price=underlying_price
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Greeks calculation failed: {str(e)}")
    
    # Log summary for debugging
    print(f"[Greeks API] Calculated {result['calculated_positions']}/{result['total_positions']} positions")
    if result['skipped_positions']:
        print(f"[Greeks API] Skipped: {[p['symbol'] for p in result['skipped_positions']]}")
    
    # Group positions by expiry for debugging
    expiry_groups = {}
    for p in result['positions']:
        exp = p.get('expiry_date', 'unknown')
        if exp not in expiry_groups:
            expiry_groups[exp] = []
        expiry_groups[exp].append(p['symbol'])
    
    print(f"[Greeks API] Expiry breakdown: { {k: len(v) for k, v in expiry_groups.items()} }")
    
    # Convert to response model
    position_greeks = [
        PositionGreeks(
            symbol=p['symbol'],
            option_type=p['option_type'],
            strike=p['strike'],
            quantity=p['quantity'],
            entry_price=p['entry_price'],
            delta=p['greeks']['delta'],
            gamma=p['greeks']['gamma'],
            theta=p['greeks']['theta'],
            vega=p['greeks']['vega'],
            rho=p['greeks']['rho'],
            expiry_date=p.get('expiry_date'),
            index_name=p.get('index_name'),
        )
        for p in result['positions']
    ]
    
    totals = result['portfolio_totals']
    interpretations = result['interpretation']
    
    return GreeksResponse(
        positions=position_greeks,
        portfolio=PortfolioGreeks(
            delta=totals['delta'],
            gamma=totals['gamma'],
            theta=totals['theta'],
            vega=totals['vega'],
            rho=totals['rho'],
        ),
        interpretation=GreeksInterpretation(
            delta=interpretations['delta'],
            gamma=interpretations['gamma'],
            theta=interpretations['theta'],
            vega=interpretations['vega'],
            rho=interpretations['rho'],
        ),
        underlying_price=underlying_price,
        calculation_date=result['calculation_date'],
    )


@router.get("/scenario", response_model=ScenarioAnalysis)
async def get_scenario_analysis(
    underlying_price: Optional[float] = Query(None, description="Override underlying price"),
    days_forward: int = Query(1, description="Days to project forward", ge=1, le=30),
    risk_free_rate: float = Query(0.10, description="Risk-free interest rate"),
):
    """
    Perform scenario analysis on portfolio Greeks.
    
    Shows estimated P&L under various market scenarios:
    - Price movements: -5%, -3%, -1%, 0%, +1%, +3%, +5%
    - Volatility changes: -2%, -1%, 0%, +1%, +2%
    
    Useful for stress testing and understanding portfolio risk exposure.
    """
    broker_service = BrokerService()
    
    # Get current positions
    positions_response = broker_service.get_positions()
    positions = positions_response.positions
    
    if not positions:
        raise HTTPException(status_code=404, detail="No open positions found")
    
    # Get current NIFTY price
    try:
        if underlying_price is None:
            nifty_quote = broker_service.get_quote("NSE:NIFTY 50")
            underlying_price = nifty_quote.last_price
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get underlying price: {str(e)}")
    
    # Calculate scenario analysis
    calculator = PortfolioGreeksCalculator(risk_free_rate=risk_free_rate)
    
    try:
        result = calculator.get_scenario_analysis(
            positions=[p.model_dump() for p in positions],
            underlying_price=underlying_price,
            days_forward=days_forward
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scenario analysis failed: {str(e)}")
    
    return ScenarioAnalysis(
        current_greeks=PortfolioGreeks(**result['current_greeks']),
        scenarios=result['scenarios'],
        underlying_price=underlying_price,
        days_forward=days_forward,
    )


@router.get("/position/{symbol}", response_model=PositionGreeks)
async def get_position_greeks(
    symbol: str,
    underlying_price: Optional[float] = Query(None, description="Override underlying price"),
    risk_free_rate: float = Query(0.10, description="Risk-free interest rate"),
):
    """
    Calculate Greeks for a specific option position by symbol.
    
    Example symbols:
    - NIFTY24FEB24000CE
    - NIFTY24FEB24000PE
    """
    broker_service = BrokerService()
    
    # Get position details
    positions_response = broker_service.get_positions()
    position = None
    
    for p in positions_response.positions:
        if p.symbol.upper() == symbol.upper():
            position = p
            break
    
    if not position:
        raise HTTPException(status_code=404, detail=f"Position not found: {symbol}")
    
    # Get current NIFTY price
    try:
        if underlying_price is None:
            nifty_quote = broker_service.get_quote("NSE:NIFTY 50")
            underlying_price = nifty_quote.last_price
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get underlying price: {str(e)}")
    
    # Calculate Greeks
    calculator = PortfolioGreeksCalculator(risk_free_rate=risk_free_rate)
    
    try:
        result = calculator.calculate_portfolio_greeks(
            positions=[position.model_dump()],
            underlying_price=underlying_price
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Greeks calculation failed: {str(e)}")
    
    if not result['positions']:
        raise HTTPException(status_code=400, detail=f"Could not parse option symbol: {symbol}")
    
    p = result['positions'][0]
    
    return PositionGreeks(
        symbol=p['symbol'],
        option_type=p['option_type'],
        strike=p['strike'],
        quantity=p['quantity'],
        entry_price=p['entry_price'],
        delta=p['greeks']['delta'],
        gamma=p['greeks']['gamma'],
        theta=p['greeks']['theta'],
        vega=p['greeks']['vega'],
        rho=p['greeks']['rho'],
    )
